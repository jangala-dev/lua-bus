--- In-process pub/sub bus built on fibers + trie.
---  - wildcard subscriptions (pubsub trie: wildcards allowed in stored keys)
---  - retained messages (retained trie: literal stored keys; wildcards in queries)
---  - request/response helper (reply_to topic)
---
--- Notes:
---  - Intended to be used from inside fibres (inside fibers.run).
---  - Delivery is bounded and non-blocking: publish never blocks.
---  - Timeouts are composed externally using choice + sleep.
---@module 'bus'

local mailbox   = require 'fibers.mailbox'
local op        = require 'fibers.op'
local performer = require 'fibers.performer'
local scope_mod = require 'fibers.scope'

local trie = require 'trie'
local uuid = require 'uuid'

local perform = performer.perform

---@alias Topic any[]
---@alias FullPolicy 'drop_oldest'|'drop_newest'

local DEFAULT_Q_LEN  = 10
local DEFAULT_POLICY = 'drop_oldest'

---@param p any
---@param level? integer
---@return FullPolicy|nil
local function assert_full_policy(p, level)
	level = (level or 1) + 1
	if p == nil then return nil end
	if p == 'drop_oldest' or p == 'drop_newest' then return p end
	if p == 'block' then
		error('bus delivery must be bounded; mailbox full policy "block" is not supported', level)
	end
	error('invalid mailbox full policy: ' .. tostring(p), level)
end

--- Topic validation: array elements must be strings or numbers.
--- Non-array keys are ignored.
---@param topic any
---@param what string
---@param level? integer
---@return Topic
local function assert_topic(topic, what, level)
	level = (level or 1) + 1
	if type(topic) ~= "table" then
		error(("%s must be a topic token array (table)"):format(what), level)
	end

	for i = 1, #topic do
		local v = topic[i]
		local tv = type(v)
		if tv ~= "string" and tv ~= "number" then
			error(("%s[%d] must be a string or number (got %s)"):format(what, i, tv), level)
		end
	end

	return topic
end

--------------------------------------------------------------------------------
-- Message (data shape; exposed via Subscription)
--------------------------------------------------------------------------------

---@class Message
---@field topic Topic        # message topic token array
---@field payload any        # message payload (user-defined)
---@field reply_to Topic|nil # reply topic (request/response helper)
---@field id any|nil         # optional message id (unused by default)
local Message = {}
Message.__index = Message

---@param topic Topic
---@param payload any
---@param reply_to? Topic
---@return Message
local function new_msg(topic, payload, reply_to)
	return setmetatable({
		topic    = topic,
		payload  = payload,
		reply_to = reply_to,
		id       = nil,
	}, Message)
end

--------------------------------------------------------------------------------
-- Subscription
--------------------------------------------------------------------------------

---@class Subscription
---@field _conn Connection|nil
---@field _topic Topic
---@field _tx MailboxTx
---@field _rx MailboxRx
---@field _detach_finaliser (fun())|nil
local Subscription = {}
Subscription.__index = Subscription

---@param conn Connection
---@param topic Topic
---@param tx MailboxTx
---@param rx MailboxRx
---@return Subscription
local function new_subscription(conn, topic, tx, rx)
	return setmetatable({
		_conn             = conn,
		_topic            = topic,
		_tx               = tx,
		_rx               = rx,
		_detach_finaliser = nil,
	}, Subscription)
end

---@return number
function Subscription:dropped()
	local tx = self._tx
	return (tx and tx.dropped and tx:dropped()) or 0
end

---@return Topic
function Subscription:topic()
	return self._topic
end

---@param reason any
function Subscription:_close(reason)
	if self._tx then self._tx:close(reason) end
end

---@return boolean ok
function Subscription:unsubscribe()
	local conn = self._conn
	if not conn then
		self:_close('unsubscribed')
		return true
	end
	return conn:unsubscribe(self)
end

---@return Op  -- when performed: Message|nil, string|nil
function Subscription:next_msg_op()
	return self._rx:recv_op():wrap(function (msg)
		if msg == nil then
			return nil, tostring(self._rx:why() or 'closed')
		end
		return msg, nil
	end)
end

---@return Message|nil msg
---@return string|nil err
function Subscription:next_msg()
	return perform(self:next_msg_op())
end

---@return fun(): Message|nil
function Subscription:messages()
	return self._rx:iter()
end

---@return fun(): any|nil
function Subscription:payloads()
	local it = self._rx:iter()
	return function ()
		local msg = it()
		return msg and msg.payload or nil
	end
end

---@return any|nil
function Subscription:why()
	return self._rx:why()
end

---@return table
function Subscription:stats()
	return { dropped = self:dropped(), topic = self._topic }
end

--------------------------------------------------------------------------------
-- Bus
--------------------------------------------------------------------------------

---@class Bus
---@field _q_length integer
---@field _full FullPolicy
---@field _topics any     -- pubsub trie: value is table<Subscription, boolean>
---@field _retained any   -- retained trie: value is Message
---@field _conns table<Connection, boolean>
local Bus = {}
Bus.__index = Bus

--- Deliver a message to a subscription without blocking; drops if it would block
--- or if the current scope is cancelled/failed.
---@param sub Subscription
---@param msg Message
function Bus:_deliver(sub, msg)
	-- Avoid raising cancellation/failure from inside publish fanout.
	local s = scope_mod.current()
	if s and s.try then
		local st, ok = s:try(sub._tx:send_op(msg))
		if st == 'ok' then return ok end
		return nil
	end
	return perform(sub._tx:send_op(msg))
end

---@param conn Connection
---@param topic Topic
---@param qlen integer
---@param full FullPolicy
---@return Subscription
function Bus:_subscribe(conn, topic, qlen, full)
	---@type table<Subscription, boolean>|nil
	local subs = self._topics:retrieve(topic)
	if not subs then
		subs = {}
		self._topics:insert(topic, subs)
	end

	local tx, rx = mailbox.new(qlen, { full = full })
	local sub    = new_subscription(conn, topic, tx, rx)
	subs[sub] = true

	-- Best-effort retained replay (bounded + non-blocking).
	self._retained:each(topic, function (retained_msg)
		self:_deliver(sub, retained_msg)
	end)

	return sub
end

---@param sub Subscription
function Bus:_unsubscribe(sub)
	local subs = self._topics:retrieve(sub._topic)
	if not subs then return end
	subs[sub] = nil
	if next(subs) == nil then
		self._topics:delete(sub._topic)
	end
end

---@param msg Message
function Bus:_publish(msg)
	self._topics:each(msg.topic, function (subs)
		for sub in pairs(subs) do
			self:_deliver(sub, msg)
		end
	end)
end

---@param msg Message
function Bus:_retain(msg)
	self:_publish(msg)
	self._retained:insert(msg.topic, msg)
end

---@param topic Topic
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
---@field _disconnected boolean
local Connection = {}
Connection.__index = Connection

---@param self Connection
---@param level? integer
local function assert_connected(self, level)
	if self._disconnected then
		error('connection is disconnected', (level or 1) + 1)
	end
end

---@param topic any
---@param level? integer
---@return Topic
local function check_topic(topic, level)
	return assert_topic(topic, 'topic', (level or 1) + 1)
end

---@param bus Bus
---@param q_length integer
---@param full FullPolicy
---@return Connection
local function new_connection(bus, q_length, full)
	return setmetatable({
		_bus          = bus,
		_q_length     = q_length,
		_full         = full,
		_subs         = {},
		_disconnected = false,
	}, Connection)
end

---@return boolean
function Connection:is_disconnected()
	return self._disconnected
end

---@return number
function Connection:dropped()
	local n = 0
	for sub in pairs(self._subs) do
		n = n + sub:dropped()
	end
	return n
end

---@param topic Topic
---@param payload any
---@return boolean ok
function Connection:publish(topic, payload)
	assert_connected(self, 1)
	check_topic(topic, 1)
	self._bus:_publish(new_msg(topic, payload))
	return true
end

---@param topic Topic
---@param payload any
---@return boolean ok
function Connection:retain(topic, payload)
	assert_connected(self, 1)
	check_topic(topic, 1)
	self._bus:_retain(new_msg(topic, payload))
	return true
end

---@param topic Topic
---@return boolean ok
function Connection:unretain(topic)
	assert_connected(self, 1)
	check_topic(topic, 1)
	self._bus:_unretain(topic)
	return true
end

---@param topic Topic
---@param queue_len? integer
---@param full_policy? FullPolicy
---@return Subscription
function Connection:subscribe(topic, queue_len, full_policy)
	assert_connected(self, 1)
	check_topic(topic, 1)

	local qlen = queue_len
	if qlen == nil then qlen = self._q_length end
	if type(qlen) ~= 'number' or qlen < 0 then error('subscribe: queue_len must be >= 0', 2) end

	local fullp = assert_full_policy(full_policy or self._full, 2) or DEFAULT_POLICY

	local sub = self._bus:_subscribe(self, topic, qlen, fullp)
	self._subs[sub] = true

	local s = scope_mod.current()
	sub._detach_finaliser = s:finally(function () sub:unsubscribe() end)

	return sub
end

---@param sub Subscription
---@param reason any
---@param remove_from_bus boolean
function Connection:_drop_sub(sub, reason, remove_from_bus)
	sub:_close(reason)

	if sub._detach_finaliser then
		sub._detach_finaliser()
		sub._detach_finaliser = nil
	end

	if remove_from_bus then self._bus:_unsubscribe(sub) end
	if sub._conn == self then sub._conn = nil end
end

---@param sub Subscription
---@return boolean ok
function Connection:unsubscribe(sub)
	if not sub or getmetatable(sub) ~= Subscription then error('unsubscribe expects a Subscription', 2) end

	local owned = not not self._subs[sub]
	self:_drop_sub(sub, 'unsubscribed', owned)
	if owned then self._subs[sub] = nil end
	return true
end

---@return boolean ok
function Connection:disconnect()
	if self._disconnected then return true end
	self._disconnected = true

	for sub in pairs(self._subs) do
		self:_drop_sub(sub, 'disconnected', true)
		self._subs[sub] = nil
	end

	local bus = self._bus
	if bus and bus._conns then bus._conns[self] = nil end
	self._bus = nil

	return true
end

---@return table
function Connection:stats()
	local nsubs = 0
	for _ in pairs(self._subs) do nsubs = nsubs + 1 end
	return { dropped = self:dropped(), subscriptions = nsubs }
end

--------------------------------------------------------------------------------
-- Request helpers
--------------------------------------------------------------------------------

---@param topic Topic
---@param payload any
---@param queue_len? integer
---@param full_policy? FullPolicy
---@return Subscription
function Connection:request_sub(topic, payload, queue_len, full_policy)
	assert_connected(self, 1)
	check_topic(topic, 1)

	local reply_to = { uuid.new() }
	local msg      = new_msg(topic, payload, reply_to)

	-- Subscribe first to avoid racing a fast responder.
	local sub = self:subscribe(reply_to, queue_len, full_policy)
	self._bus:_publish(msg)
	return sub
end

---@param topic Topic
---@param payload any
---@return Op  -- when performed: Message|nil, string|nil
function Connection:request_once_op(topic, payload)
	return op.guard(function ()
		assert_connected(self, 1)
		check_topic(topic, 1)

		local reply_to = { uuid.new() }
		local msg      = new_msg(topic, payload, reply_to)

		return op.bracket(
			function ()
				return self:subscribe(reply_to, 1, 'drop_newest')
			end,
			function (sub) sub:unsubscribe() end,
			function (sub)
				self._bus:_publish(msg)
				return sub:next_msg_op()
			end
		)
	end)
end

--------------------------------------------------------------------------------
-- Bus public API
--------------------------------------------------------------------------------

---@return Connection conn
function Bus:connect()
	-- Intended for use inside fibres; errors propagate normally.
	local s = scope_mod.current()

	local conn = new_connection(self, self._q_length, self._full)

	self._conns[conn] = true
	s:finally(function () conn:disconnect() end)

	return conn
end

---@return table
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
	}
end

--------------------------------------------------------------------------------
-- Constructor
--------------------------------------------------------------------------------

---@param params? { q_length?: integer, full?: FullPolicy, s_wild?: string, m_wild?: string }
---@return Bus bus
local function new(params)
	params = params or {}

	local q_length = params.q_length
	if q_length == nil then q_length = DEFAULT_Q_LEN end
	if type(q_length) ~= 'number' or q_length < 0 then error('bus.new: q_length must be >= 0', 2) end

	local full_default = assert_full_policy(params.full, 2) or DEFAULT_POLICY
	local s_wild = params.s_wild or '+'
	local m_wild = params.m_wild or '#'

	return setmetatable({
		_q_length = q_length,
		_full     = full_default,
		_topics   = trie.new_pubsub(s_wild, m_wild),
		_retained = trie.new_retained(s_wild, m_wild),
		_conns    = setmetatable({}, { __mode = 'k' }),
	}, Bus)
end

return { new = new }
