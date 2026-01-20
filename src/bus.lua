-- bus.lua
--
-- In-process pub/sub bus built on fibers + trie.
--  - wildcard subscriptions (pubsub trie: wildcards allowed in stored keys; literal queries)
--  - retained messages (retained trie: literal stored keys; wildcards allowed in queries)
--  - request/response helpers (reply_to topic)
--
-- Optional extension (lane B):
--  - bind/publish_one/call_op for admission-signalled point-to-point delivery
---@module 'bus'

local mailbox   = require 'fibers.mailbox'
local op        = require 'fibers.op'
local performer = require 'fibers.performer'
local scope_mod = require 'fibers.scope'
local runtime   = require 'fibers.runtime'
local sleep     = require 'fibers.sleep'

local trie = require 'trie'
local uuid = require 'uuid'

local perform = performer.perform

---@alias Topic any[]
---@alias FullPolicy '"drop_oldest"'|'"reject_newest"'

local DEFAULT_Q_LEN  = 10
local DEFAULT_POLICY = 'drop_oldest'

--------------------------------------------------------------------------------
-- trie literal helper (escape hatch)
--------------------------------------------------------------------------------

-- Latest trie supports trie.literal(v) to force literal matching even when v equals
-- the wildcard symbols (e.g. "+" or "#"). To interoperate cleanly, the bus must:
--   * allow literal tokens in topics, and
--   * treat literal tokens as concrete (non-wild) for "concrete topic" checks.
local LIT_MT = getmetatable(trie.literal('x'))

---@param tok any
---@return boolean
local function is_lit(tok)
	return type(tok) == 'table' and getmetatable(tok) == LIT_MT
end

---@param tok any
---@return any v
---@return boolean was_lit
local function unwrap_token(tok)
	if is_lit(tok) then
		return tok.v, true
	end
	return tok, false
end

--------------------------------------------------------------------------------
-- Validation / helpers
--------------------------------------------------------------------------------

---@param p any
---@param level? integer
---@return FullPolicy|nil
local function assert_full_policy(p, level)
	level = (level or 1) + 1
	if p == nil then return nil end

	if p == 'drop_newest' then
		error('mailbox full policy "drop_newest" is deprecated; use "reject_newest"', level)
	end

	if p == 'drop_oldest' or p == 'reject_newest' then
		return p
	end

	if p == 'block' then
		error('bus delivery must be bounded; mailbox full policy "block" is not supported', level)
	end

	error('invalid mailbox full policy: ' .. tostring(p), level)
end

--- Dense array validation (mirrors trie.lua behaviour).
---@param t any
---@param errlvl integer
---@return integer n
local function array_len(t, errlvl)
	if type(t) ~= 'table' then
		error('tokens must be a table (dense array)', errlvl)
	end

	local n = 0
	for _ in ipairs(t) do n = n + 1 end

	for k in pairs(t) do
		if type(k) ~= 'number' or k < 1 or k % 1 ~= 0 or k > n then
			error('token arrays must use 1..n integer keys only', errlvl)
		end
	end

	return n
end

--- Topic validation: array elements must be strings/numbers or trie.literal(v).
---@param topic any
---@param what string
---@param level? integer
---@return Topic
local function assert_topic(topic, what, level)
	level = (level or 1) + 1
	local n = array_len(topic, level)

	for i = 1, n do
		local v = topic[i]
		local uv = unwrap_token(v)
		local tv = type(uv)
		if tv ~= 'string' and tv ~= 'number' then
			error(('%s[%d] must be a string or number (got %s)'):format(what, i, tv), level)
		end
	end

	return topic
end

---@param s_wild string|number
---@param m_wild string|number
---@param topic Topic
---@param what string
---@param level? integer
local function assert_concrete_topic(s_wild, m_wild, topic, what, level)
	level = (level or 1) + 1
	local n = array_len(topic, level)
	for i = 1, n do
		local raw, was_lit = unwrap_token(topic[i])
		if not was_lit and (raw == s_wild or raw == m_wild) then
			error(('%s must be a concrete topic (no wildcards)'):format(what), level)
		end
	end
end

--- Stable key for a concrete topic (within a single process).
---@param topic Topic
---@return string
local function topic_key(topic)
	local n = array_len(topic, 3)
	local parts = {}
	for i = 1, n do
		local raw = unwrap_token(topic[i])
		if type(raw) == 'string' then
			parts[#parts + 1] = 's' .. #raw .. ':' .. raw
		else
			local s = tostring(raw)
			parts[#parts + 1] = 'n' .. #s .. ':' .. s
		end
	end
	return table.concat(parts, '|')
end

--------------------------------------------------------------------------------
-- Message
--------------------------------------------------------------------------------

---@class Message
---@field topic Topic
---@field payload any
---@field reply_to Topic|nil
---@field id any|nil
local Message = {}
Message.__index = Message

---@param topic Topic
---@param payload any
---@param reply_to? Topic
---@param id? any
---@return Message
local function new_msg(topic, payload, reply_to, id)
	return setmetatable({
		topic    = topic,
		payload  = payload,
		reply_to = reply_to,
		id       = id,
	}, Message)
end

--------------------------------------------------------------------------------
-- Subscription (lane A)
--------------------------------------------------------------------------------

---@class Subscription
---@field _conn Connection|nil
---@field _topic Topic
---@field _tx MailboxTx
---@field _rx MailboxRx
---@field _detach_finaliser (fun())|nil
local Subscription = {}
Subscription.__index = Subscription

local function new_subscription(conn, topic, tx, rx)
	return setmetatable({
		_conn             = conn,
		_topic            = topic,
		_tx               = tx,
		_rx               = rx,
		_detach_finaliser = nil,
	}, Subscription)
end

function Subscription:topic() return self._topic end

function Subscription:why() return self._rx:why() end

function Subscription:dropped()
	local tx = self._tx
	return (tx and tx.dropped and tx:dropped()) or 0
end

---@param reason any
function Subscription:_close(reason)
	if self._tx then self._tx:close(reason) end
end

function Subscription:unsubscribe()
	local conn = self._conn
	if not conn then
		self:_close('unsubscribed')
		return true
	end
	return conn:unsubscribe(self)
end

--- recv_op: when performed -> Message|nil, err|string|nil
---@return Op
function Subscription:recv_op()
	return self._rx:recv_op():wrap(function (msg)
		if msg == nil then
			return nil, tostring(self._rx:why() or 'closed')
		end
		return msg, nil
	end)
end

function Subscription:recv()
	return perform(self:recv_op())
end

function Subscription:iter()
	-- Iterator yields Message values until nil.
	return self._rx:iter()
end

function Subscription:payloads()
	local it = self._rx:iter()
	return function ()
		local msg = it()
		return msg and msg.payload or nil
	end
end

function Subscription:stats()
	return { dropped = self:dropped(), topic = self._topic }
end

--------------------------------------------------------------------------------
-- Endpoint (lane B)
--------------------------------------------------------------------------------

---@class Endpoint
---@field _conn Connection|nil
---@field _topic Topic
---@field _key string
---@field _tx MailboxTx
---@field _rx MailboxRx
---@field _detach_finaliser (fun())|nil
local Endpoint = {}
Endpoint.__index = Endpoint

local function new_endpoint(conn, topic, key, tx, rx)
	return setmetatable({
		_conn             = conn,
		_topic            = topic,
		_key              = key,
		_tx               = tx,
		_rx               = rx,
		_detach_finaliser = nil,
	}, Endpoint)
end

function Endpoint:topic() return self._topic end

function Endpoint:why() return self._rx:why() end

function Endpoint:dropped()
	local tx = self._tx
	return (tx and tx.dropped and tx:dropped()) or 0
end

function Endpoint:unbind()
	local conn = self._conn
	if not conn then
		if self._tx then self._tx:close('unbound') end
		return true
	end
	return conn:unbind(self)
end

---@return Op
function Endpoint:recv_op()
	return self._rx:recv_op():wrap(function (msg)
		if msg == nil then
			return nil, tostring(self._rx:why() or 'closed')
		end
		return msg, nil
	end)
end

function Endpoint:recv()
	return perform(self:recv_op())
end

function Endpoint:iter()
	return self._rx:iter()
end

function Endpoint:payloads()
	local it = self._rx:iter()
	return function ()
		local msg = it()
		return msg and msg.payload or nil
	end
end

--------------------------------------------------------------------------------
-- Bus
--------------------------------------------------------------------------------

---@class Bus
---@field _q_length integer
---@field _full FullPolicy
---@field _topics any
---@field _retained any
---@field _conns table<Connection, boolean>
---@field _s_wild string|number
---@field _m_wild string|number
---@field _endpoints table<string, Endpoint>
local Bus = {}
Bus.__index = Bus

--- Best-effort delivery:
--- - never raises for congestion/closure
--- - avoids raising scope cancellation/failure by using scope:try when running
---@param tx MailboxTx
---@param msg Message
local function deliver_best_effort(tx, msg)
	local s = scope_mod.current()
	if s and s.try and s.status then
		local st = select(1, s:status())
		if st == 'running' then
			s:try(tx:send_op(msg))
			return
		end
	end
	-- Fallback: raw perform. With non-blocking policies, this remains bounded.
	op.perform_raw(tx:send_op(msg))
end

---@param conn Connection
---@param topic Topic
---@param qlen integer
---@param full FullPolicy
---@return Subscription
function Bus:_subscribe(conn, topic, qlen, full)
	local subs = self._topics:retrieve(topic)
	if not subs then
		subs = {}
		self._topics:insert(topic, subs)
	end

	local tx, rx = mailbox.new(qlen, { full = full })
	local sub    = new_subscription(conn, topic, tx, rx)
	subs[sub]    = true

	-- Retained replay is best-effort and bounded.
	self._retained:each(topic, function (_k, retained_msg)
		deliver_best_effort(tx, retained_msg)
	end)

	return sub
end

function Bus:_unsubscribe(sub)
	local subs = self._topics:retrieve(sub._topic)
	if not subs then return end
	subs[sub] = nil
	if next(subs) == nil then
		self._topics:delete(sub._topic)
	end
end

function Bus:_publish(msg)
	self._topics:each(msg.topic, function (_k, subs)
		for sub in pairs(subs) do
			if sub and sub._tx then
				deliver_best_effort(sub._tx, msg)
			end
		end
	end)
end

function Bus:_retain(msg)
	self:_publish(msg)
	self._retained:insert(msg.topic, msg)
end

function Bus:_unretain(topic)
	self._retained:delete(topic)
end

--------------------------------------------------------------------------------
-- Connection
--------------------------------------------------------------------------------

---@class Connection
---@field _bus Bus|nil
---@field _q_length integer
---@field _full FullPolicy
---@field _subs table<Subscription, boolean>
---@field _eps table<Endpoint, boolean>
---@field _disconnected boolean
local Connection = {}
Connection.__index = Connection

local function assert_connected(self, level)
	if self._disconnected or not self._bus then
		error('connection is disconnected', (level or 1) + 1)
	end
end

local function new_connection(bus, q_length, full)
	return setmetatable({
		_bus          = bus,
		_q_length     = q_length,
		_full         = full,
		_subs         = {},
		_eps          = {},
		_disconnected = false,
	}, Connection)
end

function Connection:is_disconnected()
	return self._disconnected
end

function Connection:dropped()
	local n = 0
	for sub in pairs(self._subs) do
		n = n + (sub:dropped() or 0)
	end
	for ep in pairs(self._eps) do
		n = n + (ep:dropped() or 0)
	end
	return n
end

--- publish(topic, payload[, opts])
--- opts: { reply_to?: Topic, id?: any }
function Connection:publish(topic, payload, opts)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)

	opts = opts or {}
	local reply_to = opts.reply_to
	if reply_to ~= nil then
		assert_topic(reply_to, 'reply_to', 2)
	end

	self._bus:_publish(new_msg(topic, payload, reply_to, opts.id))
	return true
end

function Connection:retain(topic, payload, opts)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)

	opts = opts or {}
	local reply_to = opts.reply_to
	if reply_to ~= nil then
		assert_topic(reply_to, 'reply_to', 2)
	end

	self._bus:_retain(new_msg(topic, payload, reply_to, opts.id))
	return true
end

function Connection:unretain(topic)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)
	self._bus:_unretain(topic)
	return true
end

--- subscribe(topic[, opts]) -> Subscription
--- opts: { queue_len?: integer, full?: FullPolicy }
function Connection:subscribe(topic, opts)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)

	opts = opts or {}
	if type(opts) ~= 'table' then
		error('subscribe: opts must be a table (or nil)', 2)
	end

	local qlen = opts.queue_len
	if qlen == nil then qlen = self._q_length end
	if type(qlen) ~= 'number' or qlen < 0 then
		error('subscribe: queue_len must be >= 0', 2)
	end

	local fullp = assert_full_policy(opts.full or self._full, 2) or DEFAULT_POLICY

	local sub = self._bus:_subscribe(self, topic, qlen, fullp)
	self._subs[sub] = true

	local s = scope_mod.current()
	sub._detach_finaliser = s:finally(function () sub:unsubscribe() end)

	return sub
end

function Connection:unsubscribe(sub)
	if not sub or getmetatable(sub) ~= Subscription then
		error('unsubscribe expects a Subscription', 2)
	end

	local owned = not not self._subs[sub]

	sub:_close('unsubscribed')

	if sub._detach_finaliser then
		sub._detach_finaliser()
		sub._detach_finaliser = nil
	end

	if owned then
		self._subs[sub] = nil
		self._bus:_unsubscribe(sub)
	end
	if sub._conn == self then sub._conn = nil end
	return true
end

function Connection:disconnect()
	if self._disconnected then return true end
	self._disconnected = true

	local bus = self._bus
	self._bus = nil

	-- Snapshot to avoid mutation during pairs().
	local subs = {}
	for sub in pairs(self._subs) do subs[#subs + 1] = sub end
	for i = 1, #subs do
		local sub = subs[i]
		if sub then
			sub:_close('disconnected')
			if sub._detach_finaliser then
				sub._detach_finaliser()
				sub._detach_finaliser = nil
			end
			if bus then bus:_unsubscribe(sub) end
			self._subs[sub] = nil
			if sub._conn == self then sub._conn = nil end
		end
	end

	local eps = {}
	for ep in pairs(self._eps) do eps[#eps + 1] = ep end
	for i = 1, #eps do
		local ep = eps[i]
		if ep then
			if ep._tx then ep._tx:close('disconnected') end
			if bus and bus._endpoints and bus._endpoints[ep._key] == ep then
				bus._endpoints[ep._key] = nil
			end
			if ep._detach_finaliser then
				ep._detach_finaliser()
				ep._detach_finaliser = nil
			end
			self._eps[ep] = nil
			if ep._conn == self then ep._conn = nil end
		end
	end

	if bus and bus._conns then bus._conns[self] = nil end
	return true
end

function Connection:stats()
	local nsubs, neps = 0, 0
	for _ in pairs(self._subs) do nsubs = nsubs + 1 end
	for _ in pairs(self._eps) do neps = neps + 1 end
	return { dropped = self:dropped(), subscriptions = nsubs, endpoints = neps }
end

--------------------------------------------------------------------------------
-- Request helpers (lane A)
--------------------------------------------------------------------------------

--- request_sub(topic, payload[, opts]) -> Subscription
--- opts: { queue_len?: integer, full?: FullPolicy }
function Connection:request_sub(topic, payload, opts)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)

	opts = opts or {}
	if type(opts) ~= 'table' then
		error('request_sub: opts must be a table (or nil)', 2)
	end

	local reply_to = { uuid.new() }
	local msg      = new_msg(topic, payload, reply_to)

	-- Subscribe first to avoid racing a fast responder.
	local sub = self:subscribe(reply_to, opts)
	self._bus:_publish(msg)
	return sub
end

--- request_once_op(topic, payload[, opts]) -> Op
--- opts: { timeout?: number } is intentionally not built-in; caller composes.
function Connection:request_once_op(topic, payload, opts)
	opts = opts or {}
	if type(opts) ~= 'table' then
		error('request_once_op: opts must be a table (or nil)', 2)
	end

	return op.guard(function ()
		assert_connected(self, 1)
		assert_topic(topic, 'topic', 1)

		local reply_to = { uuid.new() }
		local msg      = new_msg(topic, payload, reply_to)

		return op.bracket(
			function ()
				-- Keep the first reply; reject duplicates.
				return self:subscribe(reply_to, { queue_len = 1, full = 'reject_newest' })
			end,
			function (sub) sub:unsubscribe() end,
			function (sub)
				self._bus:_publish(msg)
				return sub:recv_op()
			end
		)
	end)
end

--------------------------------------------------------------------------------
-- Lane B: endpoints + publish_one + call_op (opt-in)
--------------------------------------------------------------------------------

--- bind(topic[, opts]) -> Endpoint
--- opts: { queue_len?: integer }  (full policy is fixed to reject_newest)
function Connection:bind(topic, opts)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)

	opts = opts or {}
	if type(opts) ~= 'table' then
		error('bind: opts must be a table (or nil)', 2)
	end

	local bus = assert(self._bus)
	assert_concrete_topic(bus._s_wild, bus._m_wild, topic, 'bind topic', 2)

	local qlen = opts.queue_len
	if qlen == nil then qlen = 1 end
	if type(qlen) ~= 'number' or qlen < 0 then
		error('bind: queue_len must be >= 0', 2)
	end

	local key = topic_key(topic)
	if bus._endpoints[key] ~= nil then
		error('bind: topic is already bound', 2)
	end

	-- Endpoint mailboxes use reject_newest for explicit admission signalling.
	local tx, rx = mailbox.new(qlen, { full = 'reject_newest' })
	local ep     = new_endpoint(self, topic, key, tx, rx)

	bus._endpoints[key] = ep
	self._eps[ep] = true

	local s = scope_mod.current()
	ep._detach_finaliser = s:finally(function () ep:unbind() end)

	return ep
end

function Connection:unbind(ep)
	if not ep or getmetatable(ep) ~= Endpoint then
		error('unbind expects an Endpoint', 2)
	end
	local bus = self._bus

	if ep._tx then ep._tx:close('unbound') end

	if ep._detach_finaliser then
		ep._detach_finaliser()
		ep._detach_finaliser = nil
	end

	if self._eps[ep] then
		self._eps[ep] = nil
		if bus and bus._endpoints and bus._endpoints[ep._key] == ep then
			bus._endpoints[ep._key] = nil
		end
	end

	if ep._conn == self then ep._conn = nil end
	return true
end

--- publish_one_op(topic, payload[, opts]) -> Op
--- opts: { reply_to?: Topic, id?: any }
--- when performed -> ok:boolean, reason:any|nil
function Connection:publish_one_op(topic, payload, opts)
	opts = opts or {}
	if type(opts) ~= 'table' then
		error('publish_one_op: opts must be a table (or nil)', 2)
	end

	return op.guard(function ()
		assert_connected(self, 1)
		assert_topic(topic, 'topic', 1)

		local bus = assert(self._bus)
		assert_concrete_topic(bus._s_wild, bus._m_wild, topic, 'publish_one topic', 2)

		local reply_to = opts.reply_to
		if reply_to ~= nil then
			assert_topic(reply_to, 'reply_to', 2)
			assert_concrete_topic(bus._s_wild, bus._m_wild, reply_to, 'reply_to', 2)
		end

		local key = topic_key(topic)
		local ep  = bus._endpoints[key]
		if not ep or not ep._tx then
			return op.always(false, 'no_route')
		end

		local msg = new_msg(topic, payload, reply_to, opts.id)

		-- Leaf op: do not perform inside guard.
		return ep._tx:send_op(msg):wrap(function (ok, reason)
			-- mailbox send semantics:
			--   true            accepted
			--   false, "full"   rejected
			--   nil             closed
			if ok == true then return true, nil end
			if ok == nil then return false, 'closed' end
			return false, reason or 'full'
		end)
	end)
end

function Connection:publish_one(topic, payload, opts)
	return perform(self:publish_one_op(topic, payload, opts))
end

--- call_op(topic, payload[, opts]) -> Op
--- opts: { timeout?: number, deadline?: number, backoff?: number, backoff_max?: number, request_id?: any }
--- when performed -> reply_payload|nil, err|string|nil
function Connection:call_op(topic, payload, opts)
	opts = opts or {}
	if type(opts) ~= 'table' then
		error('call_op: opts must be a table (or nil)', 2)
	end

	return op.guard(function ()
		assert_connected(self, 1)
		assert_topic(topic, 'topic', 1)

		local bus = assert(self._bus)
		assert_concrete_topic(bus._s_wild, bus._m_wild, topic, 'call topic', 2)

		local timeout     = (type(opts.timeout) == 'number') and opts.timeout or 1.0
		local deadline    = (type(opts.deadline) == 'number') and opts.deadline or (runtime.now() + timeout)
		local backoff     = (type(opts.backoff) == 'number') and opts.backoff or 0.01
		local backoff_max = (type(opts.backoff_max) == 'number') and opts.backoff_max or 0.2

		local request_id  = (opts.request_id ~= nil) and opts.request_id or uuid.new()
		local reply_topic = { request_id }

		-- One-shot worker control.
		local cancelled = false
		local rep_ep    = nil

		local function try_fn()
			return false
		end

		local function block_fn(suspension, wrap_fn)
			-- Ensure we unbind reply endpoint (if created) on abort/cleanup.
			suspension:add_cleanup(function ()
				cancelled = true
				if rep_ep then
					-- idempotent; also detaches its finaliser
					pcall(function () rep_ep:unbind() end)
					rep_ep = nil
				end
			end)

			-- Worker fibre does the retry+wait and completes the suspension.
			runtime.spawn_raw(function ()
				-- Bind reply endpoint first to avoid racing a fast responder.
				local ok_bind, bind_err = pcall(function ()
					rep_ep = self:bind(reply_topic, { queue_len = 1 })
				end)
				if not ok_bind then
					if suspension:waiting() then suspension:complete(wrap_fn, nil, tostring(bind_err)) end
					return
				end

				local function finish(v, err)
					if rep_ep then
						pcall(function () rep_ep:unbind() end)
						rep_ep = nil
					end
					if suspension:waiting() then
						suspension:complete(wrap_fn, v, err)
					end
				end

				-- Retry publish_one until accepted or deadline/cancel.
				local b = backoff
				while true do
					if cancelled then return finish(nil, 'cancelled') end

					local now = runtime.now()
					if now >= deadline then
						return finish(nil, 'timeout')
					end

					local ok_pub, reason = perform(self:publish_one_op(topic, payload, {
						reply_to = reply_topic,
						id       = request_id,
					}))

					if ok_pub then break end

					-- Decide retry policy.
					if reason ~= 'full' and reason ~= 'no_route' and reason ~= 'closed' then
						return finish(nil, tostring(reason))
					end

					local remaining = deadline - runtime.now()
					local dt = math.min(b, backoff_max, remaining)
					if dt <= 0 then
						return finish(nil, 'timeout')
					end
					perform(sleep.sleep_op(dt))
					b = math.min(b * 2, backoff_max)
				end

				-- Wait for reply or deadline.
				local is_reply, msg, err = perform(op.boolean_choice(
					rep_ep:recv_op(), -- returns msg, err
					sleep.sleep_until_op(deadline):wrap(function ()
						return nil, 'timeout'
					end)
				))

				if cancelled then return finish(nil, 'cancelled') end
				if not is_reply then return finish(nil, 'timeout') end
				if err then return finish(nil, err) end
				if not msg then return finish(nil, 'closed') end

				return finish(msg.payload, nil)
			end)
		end

		-- Primitive op: completion values are already (reply, err).
		return op.new_primitive(nil, try_fn, block_fn)
	end)
end

function Connection:call(topic, payload, opts)
	return perform(self:call_op(topic, payload, opts))
end

--------------------------------------------------------------------------------
-- Bus public API
--------------------------------------------------------------------------------

function Bus:connect()
	local s = scope_mod.current()
	local conn = new_connection(self, self._q_length, self._full)
	self._conns[conn] = true
	s:finally(function () conn:disconnect() end)
	return conn
end

function Bus:stats()
	local connections, dropped = 0, 0
	for conn in pairs(self._conns) do
		connections = connections + 1
		dropped = dropped + conn:dropped()
	end
	return {
		connections = connections,
		dropped     = dropped,
		queue_len   = self._q_length,
		full_policy = self._full,
		s_wild      = self._s_wild,
		m_wild      = self._m_wild,
	}
end

--------------------------------------------------------------------------------
-- Constructor
--------------------------------------------------------------------------------

---@param params? { q_length?: integer, full?: FullPolicy, s_wild?: string|number, m_wild?: string|number }
---@return Bus
local function new(params)
	params = params or {}

	local q_length = params.q_length
	if q_length == nil then q_length = DEFAULT_Q_LEN end
	if type(q_length) ~= 'number' or q_length < 0 then
		error('bus.new: q_length must be >= 0', 2)
	end

	local full_default = assert_full_policy(params.full, 2) or DEFAULT_POLICY
	local s_wild = params.s_wild or '+'
	local m_wild = params.m_wild or '#'

	return setmetatable({
		_q_length  = q_length,
		_full      = full_default,
		_s_wild    = s_wild,
		_m_wild    = m_wild,
		_topics    = trie.new_pubsub(s_wild, m_wild),
		_retained  = trie.new_retained(s_wild, m_wild),
		_conns     = setmetatable({}, { __mode = 'k' }),
		_endpoints = {}, -- lane B store
	}, Bus)
end

return {
	new = new,

	Bus          = Bus,
	Connection   = Connection,
	Subscription = Subscription,
	Endpoint     = Endpoint,
	Message      = Message,

	-- Re-export trie literal helper for convenience.
	literal = trie.literal,
}
