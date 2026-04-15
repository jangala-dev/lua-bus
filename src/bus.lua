-- bus.lua
--
-- In-process bus built on fibers + trie.
--
-- Public planes:
--   * state/event plane : publish, retain, unretain, subscribe, watch_retained
--   * command plane     : bind, call
--
-- Key properties:
--   * wildcard subscriptions (pubsub trie: wildcards allowed in stored keys; literal queries)
--   * retained messages (retained trie: literal stored keys; wildcards allowed in queries)
--   * retained watch feeds (wildcard watch patterns over retain/unretain lifecycle)
--   * bounded endpoint calls with native request/reply (no public reply topics)
--   * immutable origin metadata attached to delivered bus objects
--   * principal-aware authorisation hooks on connection actions
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

---@param topic Topic
---@return string
local function topic_debug(topic)
	local n = array_len(topic, 3)
	local parts = {}
	for i = 1, n do
		local raw, was_lit = unwrap_token(topic[i])
		parts[i] = was_lit and ('=' .. tostring(raw)) or tostring(raw)
	end
	return table.concat(parts, '/')
end

local function copy_table(t)
	local out = {}
	if not t then return out end
	for k, v in pairs(t) do out[k] = v end
	return out
end

local function freeze_origin(origin)
	local data = origin
	return setmetatable({}, {
		__index = data,
		__newindex = function () error('origin is immutable', 2) end,
		__pairs = function () return next, data, nil end,
		__metatable = false,
	})
end

---@param conn Connection
---@param extra? table
---@return Origin
local function build_origin(conn, extra)
	local base = conn._origin_base or {}
	local out  = {}

	for k, v in pairs(base) do out[k] = v end
	if extra then
		for k, v in pairs(extra) do out[k] = v end
	end

	if out.kind == nil then out.kind = 'local' end
	out.conn_id   = conn._conn_id
	out.principal = conn._principal
	return freeze_origin(out)
end

--------------------------------------------------------------------------------
-- Authorisation
--------------------------------------------------------------------------------

---@param auth any
---@return function|nil
local function authoriser_callable(auth)
	if auth == nil then return nil end
	if type(auth) == 'function' then
		return function(ctx)
			return auth(ctx)
		end
	end
	if type(auth) == 'table' then
		if type(auth.allow) == 'function' then
			return function(ctx)
				return auth:allow(ctx)
			end
		end
		if type(auth.authorize) == 'function' then
			return function(ctx)
				return auth:authorize(ctx)
			end
		end
	end
	return nil
end

---@param bus Bus
---@param principal any
---@param action string
---@param topic Topic
---@param extra? table
---@param level? integer
---@return boolean ok
---@return any reason
local function authorize_action(bus, principal, action, topic, extra, level)
	level = (level or 1) + 1

	local auth = bus and bus._authoriser or nil
	if auth == nil then
		return true, nil
	end

	local fn = authoriser_callable(auth)
	if not fn then
		error('bus authoriser must be a function or table with :allow(ctx) / :authorize(ctx)', level)
	end

	local ok, reason = fn({
		bus       = bus,
		principal = principal,
		action    = action,
		topic     = topic,
		extra     = extra,
	})

	if ok == false or ok == nil then
		return false, reason or 'forbidden'
	end
	return true, nil
end

---@param self Connection
---@param action string
---@param topic Topic
---@param extra? table
---@param level? integer
local function assert_authorized(self, action, topic, extra, level)
	level = (level or 1) + 1
	local ok, reason = authorize_action(self._bus, self._principal, action, topic, extra, level)
	if not ok then
		error(
			('permission denied for %s on %s: %s'):format(
				tostring(action),
				topic_debug(topic),
				tostring(reason or 'forbidden')
			),
			level
		)
	end
end

--------------------------------------------------------------------------------
-- Delivered objects
--------------------------------------------------------------------------------

---@class Origin
---@field kind string
---@field conn_id string|nil
---@field principal any|nil
---@field link_id string|nil
---@field peer_node string|nil
---@field peer_sid string|nil
---@field generation integer|nil
---@field extra table|nil

---@class Message
---@field topic Topic
---@field payload any
---@field origin Origin
local Message = {}
Message.__index = Message

---@param topic Topic
---@param payload any
---@param origin Origin
---@return Message
local function new_msg(topic, payload, origin)
	return setmetatable({
		topic   = topic,
		payload = payload,
		origin  = origin,
	}, Message)
end

---@class RetainedEvent
---@field op '"retain"'|'"unretain"'
---@field topic Topic
---@field payload any|nil
---@field origin Origin|nil
local RetainedEvent = {}
RetainedEvent.__index = RetainedEvent

---@param op_name '"retain"'|'"unretain"'
---@param topic Topic
---@param payload? any
---@param origin? Origin
---@return RetainedEvent
local function new_retained_event(op_name, topic, payload, origin)
	return setmetatable({
		op      = op_name,
		topic   = topic,
		payload = payload,
		origin  = origin,
	}, RetainedEvent)
end

---@class Request
---@field topic Topic
---@field payload any
---@field origin Origin
---@field _reply_tx MailboxTx
---@field _done boolean
local Request = {}
Request.__index = Request

local function request_deliver(self, record)
	if self._done then return false end
	self._done = true

	local send_op = self._reply_tx:send_op(record)
	local ready, ok, reason = send_op.try_fn()
	assert(ready, 'request reply mailbox unexpectedly blocked')
	self._reply_tx:close('done')
	return ok == true and reason == nil
end

---@param topic Topic
---@param payload any
---@param origin Origin
---@param reply_tx MailboxTx
---@return Request
local function new_request(topic, payload, origin, reply_tx)
	return setmetatable({
		topic     = topic,
		payload   = payload,
		origin    = origin,
		_reply_tx = reply_tx,
		_done     = false,
	}, Request)
end

---@param value any
---@return boolean ok
function Request:reply(value)
	return request_deliver(self, { ok = true, value = value })
end

---@param err any
---@return boolean ok
function Request:fail(err)
	return request_deliver(self, { ok = false, err = err })
end

---@return boolean
function Request:done()
	return self._done
end

--------------------------------------------------------------------------------
-- Subscription (state/event plane)
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
-- Retained watch feed
--------------------------------------------------------------------------------

---@class RetainedWatch
---@field _conn Connection|nil
---@field _topic Topic
---@field _tx MailboxTx
---@field _rx MailboxRx
---@field _detach_finaliser (fun())|nil
local RetainedWatch = {}
RetainedWatch.__index = RetainedWatch

local function new_retained_watch(conn, topic, tx, rx)
	return setmetatable({
		_conn             = conn,
		_topic            = topic,
		_tx               = tx,
		_rx               = rx,
		_detach_finaliser = nil,
	}, RetainedWatch)
end

function RetainedWatch:topic() return self._topic end
function RetainedWatch:why() return self._rx:why() end

function RetainedWatch:dropped()
	local tx = self._tx
	return (tx and tx.dropped and tx:dropped()) or 0
end

---@param reason any
function RetainedWatch:_close(reason)
	if self._tx then self._tx:close(reason) end
end

function RetainedWatch:unwatch()
	local conn = self._conn
	if not conn then
		self:_close('unwatched')
		return true
	end
	return conn:unwatch_retained(self)
end

---@return Op
function RetainedWatch:recv_op()
	return self._rx:recv_op():wrap(function (ev)
		if ev == nil then
			return nil, tostring(self._rx:why() or 'closed')
		end
		return ev, nil
	end)
end

function RetainedWatch:recv()
	return perform(self:recv_op())
end

function RetainedWatch:iter()
	return self._rx:iter()
end

function RetainedWatch:stats()
	return { dropped = self:dropped(), topic = self._topic }
end

--------------------------------------------------------------------------------
-- Endpoint (command plane)
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
	return self._rx:recv_op():wrap(function (req)
		if req == nil then
			return nil, tostring(self._rx:why() or 'closed')
		end
		return req, nil
	end)
end

function Endpoint:recv()
	return perform(self:recv_op())
end

function Endpoint:iter()
	return self._rx:iter()
end

--------------------------------------------------------------------------------
-- Bus
--------------------------------------------------------------------------------

---@class Bus
---@field _q_length integer
---@field _full FullPolicy
---@field _topics any
---@field _retained any
---@field _retained_watchers any
---@field _conns table<Connection, boolean>
---@field _s_wild string|number
---@field _m_wild string|number
---@field _endpoints table<string, Endpoint>
---@field _authoriser any|nil
local Bus = {}
Bus.__index = Bus

---@param tx MailboxTx
---@param value any
local function deliver_best_effort(tx, value)
	local s = scope_mod.current()
	if s and s.try and s.status then
		local st = select(1, s:status())
		if st == 'running' then
			s:try(tx:send_op(value))
			return
		end
	end
	op.perform_raw(tx:send_op(value))
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

function Bus:_notify_retained(ev)
	self._retained_watchers:each(ev.topic, function (_k, watchers)
		for rw in pairs(watchers) do
			if rw and rw._tx then
				deliver_best_effort(rw._tx, ev)
			end
		end
	end)
end

function Bus:_retain(msg)
	self:_publish(msg)
	self._retained:insert(msg.topic, msg)
	self:_notify_retained(new_retained_event('retain', msg.topic, msg.payload, msg.origin))
end

function Bus:_unretain(topic, origin)
	self._retained:delete(topic)
	self:_notify_retained(new_retained_event('unretain', topic, nil, origin))
end

---@param conn Connection
---@param topic Topic
---@param qlen integer
---@param full FullPolicy
---@param replay boolean
---@return RetainedWatch
function Bus:_watch_retained(conn, topic, qlen, full, replay)
	local watchers = self._retained_watchers:retrieve(topic)
	if not watchers then
		watchers = {}
		self._retained_watchers:insert(topic, watchers)
	end

	local tx, rx = mailbox.new(qlen, { full = full })
	local rw     = new_retained_watch(conn, topic, tx, rx)
	watchers[rw] = true

	if replay then
		self._retained:each(topic, function (_k, retained_msg)
			deliver_best_effort(tx,
				new_retained_event('retain', retained_msg.topic, retained_msg.payload, retained_msg.origin))
		end)
	end

	return rw
end

function Bus:_unwatch_retained(rw)
	local watchers = self._retained_watchers:retrieve(rw._topic)
	if not watchers then return end
	watchers[rw] = nil
	if next(watchers) == nil then
		self._retained_watchers:delete(rw._topic)
	end
end

--------------------------------------------------------------------------------
-- Connection
--------------------------------------------------------------------------------

---@class Connection
---@field _bus Bus|nil
---@field _principal any|nil
---@field _q_length integer
---@field _full FullPolicy
---@field _subs table<Subscription, boolean>
---@field _eps table<Endpoint, boolean>
---@field _rws table<RetainedWatch, boolean>
---@field _disconnected boolean
---@field _conn_id string
---@field _origin_base table
local Connection = {}
Connection.__index = Connection

local function assert_connected(self, level)
	if self._disconnected or not self._bus then
		error('connection is disconnected', (level or 1) + 1)
	end
end

local function new_connection(bus, principal, q_length, full, origin_base)
	return setmetatable({
		_bus          = bus,
		_principal    = principal,
		_q_length     = q_length,
		_full         = full,
		_subs         = {},
		_eps          = {},
		_rws          = {},
		_disconnected = false,
		_conn_id      = tostring(uuid.new()),
		_origin_base  = copy_table(origin_base),
	}, Connection)
end

function Connection:is_disconnected()
	return self._disconnected
end

function Connection:principal()
	return self._principal
end

function Connection:dropped()
	local n = 0
	for sub in pairs(self._subs) do
		n = n + (sub:dropped() or 0)
	end
	for ep in pairs(self._eps) do
		n = n + (ep:dropped() or 0)
	end
	for rw in pairs(self._rws) do
		n = n + (rw:dropped() or 0)
	end
	return n
end

function Connection:publish(topic, payload, opts)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)

	opts = opts or {}
	assert_authorized(self, 'publish', topic, {
		payload = payload,
		opts    = opts,
	}, 1)

	self._bus:_publish(new_msg(topic, payload, build_origin(self, opts.origin)))
	return true
end

function Connection:retain(topic, payload, opts)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)

	opts = opts or {}
	assert_authorized(self, 'retain', topic, {
		payload = payload,
		opts    = opts,
	}, 1)

	self._bus:_retain(new_msg(topic, payload, build_origin(self, opts.origin)))
	return true
end

function Connection:unretain(topic, opts)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)

	opts = opts or {}
	assert_authorized(self, 'unretain', topic, {
		opts = opts,
	}, 1)

	self._bus:_unretain(topic, build_origin(self, opts.origin))
	return true
end

---@param topic Topic
---@param opts? table
---@return Subscription
function Connection:_subscribe_internal(topic, opts)
	assert_connected(self, 1)

	opts = opts or {}
	if type(opts) ~= 'table' then
		error('_subscribe_internal: opts must be a table (or nil)', 2)
	end

	local qlen = opts.queue_len
	if qlen == nil then qlen = self._q_length end
	if type(qlen) ~= 'number' or qlen < 0 then
		error('_subscribe_internal: queue_len must be >= 0', 2)
	end

	local fullp = assert_full_policy(opts.full or self._full, 2) or DEFAULT_POLICY

	local sub = self._bus:_subscribe(self, topic, qlen, fullp)
	self._subs[sub] = true

	local s = scope_mod.current()
	sub._detach_finaliser = s:finally(function () sub:unsubscribe() end)

	return sub
end

function Connection:subscribe(topic, opts)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)

	assert_authorized(self, 'subscribe', topic, {
		opts = opts,
	}, 1)

	return self:_subscribe_internal(topic, opts)
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

---@param topic Topic
---@param opts? table
---@return RetainedWatch
function Connection:_watch_retained_internal(topic, opts)
	assert_connected(self, 1)

	opts = opts or {}
	if type(opts) ~= 'table' then
		error('_watch_retained_internal: opts must be a table (or nil)', 2)
	end

	local qlen = opts.queue_len
	if qlen == nil then qlen = self._q_length end
	if type(qlen) ~= 'number' or qlen < 0 then
		error('_watch_retained_internal: queue_len must be >= 0', 2)
	end

	local fullp = assert_full_policy(opts.full or self._full, 2) or DEFAULT_POLICY
	local replay = not not opts.replay

	local rw = self._bus:_watch_retained(self, topic, qlen, fullp, replay)
	self._rws[rw] = true

	local s = scope_mod.current()
	rw._detach_finaliser = s:finally(function () rw:unwatch() end)

	return rw
end

function Connection:watch_retained(topic, opts)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)

	assert_authorized(self, 'watch_retained', topic, {
		opts = opts,
	}, 1)

	return self:_watch_retained_internal(topic, opts)
end

function Connection:unwatch_retained(rw)
	if not rw or getmetatable(rw) ~= RetainedWatch then
		error('unwatch_retained expects a RetainedWatch', 2)
	end

	local owned = not not self._rws[rw]

	rw:_close('unwatched')

	if rw._detach_finaliser then
		rw._detach_finaliser()
		rw._detach_finaliser = nil
	end

	if owned then
		self._rws[rw] = nil
		self._bus:_unwatch_retained(rw)
	end
	if rw._conn == self then rw._conn = nil end
	return true
end

---@param topic Topic
---@param opts? table
---@return Endpoint
function Connection:_bind_internal(topic, opts)
	assert_connected(self, 1)

	opts = opts or {}
	if type(opts) ~= 'table' then
		error('_bind_internal: opts must be a table (or nil)', 2)
	end

	local bus = assert(self._bus)
	assert_concrete_topic(bus._s_wild, bus._m_wild, topic, 'bind topic', 2)

	local qlen = opts.queue_len
	if qlen == nil then qlen = 1 end
	if type(qlen) ~= 'number' or qlen < 0 then
		error('_bind_internal: queue_len must be >= 0', 2)
	end

	local key = topic_key(topic)
	if bus._endpoints[key] ~= nil then
		error('bind: topic is already bound', 2)
	end

	local tx, rx = mailbox.new(qlen, { full = 'reject_newest' })
	local ep     = new_endpoint(self, topic, key, tx, rx)

	bus._endpoints[key] = ep
	self._eps[ep] = true

	local s = scope_mod.current()
	ep._detach_finaliser = s:finally(function () ep:unbind() end)

	return ep
end

function Connection:bind(topic, opts)
	assert_connected(self, 1)
	assert_topic(topic, 'topic', 1)

	assert_authorized(self, 'bind', topic, {
		opts = opts,
	}, 1)

	return self:_bind_internal(topic, opts)
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

local function call_result_op(reply_rx, reply_tx, deadline)
	local now = runtime.now()
	if deadline <= now then
		reply_tx:close('timeout')
		return op.always(nil, 'timeout')
	end

	local timeout_ev = sleep.sleep_op(deadline - now):wrap(function ()
		reply_tx:close('timeout')
		return nil, 'timeout'
	end)

	local reply_ev = reply_rx:recv_op():wrap(function (reply)
		if reply == nil then
			return nil, tostring(reply_rx:why() or 'closed')
		end
		if reply.ok then
			return reply.value, nil
		end
		return nil, reply.err
	end)

	return op.choice(reply_ev, timeout_ev)
end

function Connection:call_op(topic, payload, opts)
	opts = opts or {}
	if type(opts) ~= 'table' then
		error('call_op: opts must be a table (or nil)', 2)
	end

	return op.guard(function ()
		assert_connected(self, 1)
		assert_topic(topic, 'topic', 1)

		assert_authorized(self, 'call', topic, {
			payload = payload,
			opts    = opts,
		}, 1)

		local bus = assert(self._bus)
		assert_concrete_topic(bus._s_wild, bus._m_wild, topic, 'call topic', 2)

		local key = topic_key(topic)
		local ep  = bus._endpoints[key]
		if not ep or not ep._tx then
			return op.always(nil, 'no_route')
		end

		local timeout  = (type(opts.timeout) == 'number') and opts.timeout or 1.0
		local deadline = (type(opts.deadline) == 'number') and opts.deadline or (runtime.now() + timeout)
		local origin   = build_origin(self, opts.origin)

		return op.bracket(
			function ()
				local reply_tx, reply_rx = mailbox.new(1, { full = 'reject_newest' })
				local req = new_request(topic, payload, origin, reply_tx)

				local send_op = ep._tx:send_op(req)
				local ready, ok, reason = send_op.try_fn()
				assert(ready, 'endpoint admission unexpectedly blocked')

				if ok ~= true then
					reply_tx:close(reason or 'closed')
					return {
						accepted = false,
						err      = (ok == nil) and 'closed' or (reason or 'full'),
						reply_tx = reply_tx,
						reply_rx = reply_rx,
					}
				end

				return {
					accepted = true,
					reply_tx = reply_tx,
					reply_rx = reply_rx,
				}
			end,
			function (res)
				if res and res.reply_tx then
					res.reply_tx:close('done')
				end
			end,
			function (res)
				if not res.accepted then
					return op.always(nil, res.err)
				end
				return call_result_op(res.reply_rx, res.reply_tx, deadline)
			end
		)
	end)
end

function Connection:call(topic, payload, opts)
	return perform(self:call_op(topic, payload, opts))
end

function Connection:disconnect()
	if self._disconnected then return true end
	self._disconnected = true

	local bus = self._bus
	self._bus = nil

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

	local rws = {}
	for rw in pairs(self._rws) do rws[#rws + 1] = rw end
	for i = 1, #rws do
		local rw = rws[i]
		if rw then
			rw:_close('disconnected')
			if rw._detach_finaliser then
				rw._detach_finaliser()
				rw._detach_finaliser = nil
			end
			if bus then bus:_unwatch_retained(rw) end
			self._rws[rw] = nil
			if rw._conn == self then rw._conn = nil end
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
	local nsubs, neps, nrws = 0, 0, 0
	for _ in pairs(self._subs) do nsubs = nsubs + 1 end
	for _ in pairs(self._eps) do neps = neps + 1 end
	for _ in pairs(self._rws) do nrws = nrws + 1 end
	return {
		dropped          = self:dropped(),
		subscriptions    = nsubs,
		endpoints        = neps,
		retained_watches = nrws,
	}
end

--------------------------------------------------------------------------------
-- Bus public API
--------------------------------------------------------------------------------

function Bus:connect(opts)
	opts = opts or {}
	if type(opts) ~= 'table' then
		error('connect: opts must be a table (or nil)', 2)
	end

	local s = scope_mod.current()
	local conn = new_connection(self, opts.principal, self._q_length, self._full, opts.origin_base)
	self._conns[conn] = true
	s:finally(function () conn:disconnect() end)
	return conn
end

function Bus:stats()
	local connections, dropped, endpoints = 0, 0, 0
	local retained_watches = 0
	for conn in pairs(self._conns) do
		connections = connections + 1
		dropped = dropped + conn:dropped()
		for _ in pairs(conn._rws or {}) do retained_watches = retained_watches + 1 end
		for _ in pairs(conn._eps or {}) do endpoints = endpoints + 1 end
	end
	return {
		connections      = connections,
		dropped          = dropped,
		queue_len        = self._q_length,
		full_policy      = self._full,
		s_wild           = self._s_wild,
		m_wild           = self._m_wild,
		retained_watches = retained_watches,
		endpoints        = endpoints,
	}
end

--------------------------------------------------------------------------------
-- Constructor
--------------------------------------------------------------------------------

---@param params? { q_length?: integer, full?: FullPolicy, s_wild?: string|number, m_wild?: string|number, authoriser?: any }
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
		_q_length          = q_length,
		_full              = full_default,
		_s_wild            = s_wild,
		_m_wild            = m_wild,
		_topics            = trie.new_pubsub(s_wild, m_wild),
		_retained          = trie.new_retained(s_wild, m_wild),
		_retained_watchers = trie.new_pubsub(s_wild, m_wild),
		_conns             = setmetatable({}, { __mode = 'k' }),
		_endpoints         = {},
		_authoriser        = params.authoriser,
	}, Bus)
end

return {
	new = new,

	Bus           = Bus,
	Connection    = Connection,
	Subscription  = Subscription,
	RetainedWatch = RetainedWatch,
	RetainedEvent = RetainedEvent,
	Endpoint      = Endpoint,
	Message       = Message,
	Request       = Request,

	-- Re-export trie literal helper for convenience.
	literal = trie.literal,
}
