--- In-process pub/sub bus built on fibers + trie.
---  - wildcard subscriptions (pubsub trie: wildcards allowed in stored keys)
---  - retained messages (retained trie: literal stored keys; wildcards in queries)
---  - request/response helper (reply_to topic)
---
--- Intended use:
---   local bus  = require('bus').new(...)
---   local conn = bus:connect()
---   local sub  = conn:subscribe({ "a", "b" })
---   conn:publish({ "a", "b" }, "payload")
---   local msg, err = sub:next_msg()
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
---@alias FullPolicy '"drop_oldest"'|'"drop_newest"'

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

---@param tx any
---@return number
local function mailbox_dropped(tx)
	return (tx and tx.dropped and tx:dropped()) or 0
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

--- Return the number of messages dropped for this subscription (best-effort).
---@return number
function Subscription:dropped()
	return mailbox_dropped(self._tx)
end

--- Return the topic pattern this subscription was created with.
---@return Topic
function Subscription:topic()
	return self._topic
end

---@param reason any
function Subscription:_close(reason)
	if self._tx then self._tx:close(reason) end
end

--- Unsubscribe this subscription (idempotent).
---@return boolean ok
function Subscription:unsubscribe()
	local conn = self._conn
	if not conn then
		self:_close('unsubscribed')
		return true
	end
	return conn:unsubscribe(self)
end

--- Op yielding the next message, or (nil, err) once closed and drained.
---@return Op  -- when performed: Message|nil, string|nil
function Subscription:next_msg_op()
	return self._rx:recv_op():wrap(function (msg)
		if msg == nil then
			return nil, tostring(self._rx:why() or 'closed')
		end
		return msg, nil
	end)
end

--- Receive the next message, or (nil, err) once closed and drained.
---@return Message|nil msg
---@return string|nil err
function Subscription:next_msg()
	return perform(self:next_msg_op())
end

--- Iterator over messages until the subscription closes.
---@return fun(): Message|nil
function Subscription:messages()
	return self._rx:iter()
end

--- Iterator over payloads until the subscription closes.
---@return fun(): any|nil
function Subscription:payloads()
	local it = self._rx:iter()
	return function ()
		local msg = it()
		return msg and msg.payload or nil
	end
end

--- Return the close reason, once the subscription has closed.
---@return any|nil
function Subscription:why()
	return self._rx:why()
end

--- Return a small stats snapshot for this subscription.
---@return table
function Subscription:stats()
	return { dropped = self:dropped(), topic = self._topic }
end

--------------------------------------------------------------------------------
-- Bus
--------------------------------------------------------------------------------

---@class BusBucket
---@field subs table<Subscription, boolean>

---@class Bus
---@field _q_length integer
---@field _full FullPolicy
---@field _topics any
---@field _retained any
---@field _conns table<Connection, boolean>
local Bus = {}
Bus.__index = Bus

---@param sub Subscription
local function detach_finaliser(sub)
	if sub._detach_finaliser then
		sub._detach_finaliser()
		sub._detach_finaliser = nil
	end
end

--- Deliver a message to a subscription without blocking; drops if it would block.
---@param sub Subscription
---@param msg Message
function Bus:_deliver(sub, msg)
	perform(sub._tx:send_op(msg):or_else(function () return nil end))
end

---@param conn Connection
---@param topic Topic
---@param qlen integer
---@param full FullPolicy
---@return Subscription
function Bus:_subscribe(conn, topic, qlen, full)
	---@type BusBucket|nil
	local bucket = self._topics:retrieve(topic)
	if not bucket then
		bucket = { subs = {} }
		self._topics:insert(topic, bucket)
	end

	local tx, rx = mailbox.new(qlen, { full = full })
	local sub    = new_subscription(conn, topic, tx, rx)
	bucket.subs[sub] = true

	-- Best-effort retained replay (bounded + non-blocking).
	self._retained:each(topic, function (retained_msg)
		self:_deliver(sub, retained_msg)
	end)

	return sub
end

---@param sub Subscription
function Bus:_unsubscribe(sub)
	local bucket = self._topics:retrieve(sub._topic)
	if not bucket then return end
	bucket.subs[sub] = nil
	if next(bucket.subs) == nil then
		self._topics:delete(sub._topic)
	end
end

---@param msg Message
function Bus:_publish(msg)
	self._topics:each(msg.topic, function (bucket)
		for sub in pairs(bucket.subs) do
			if sub and sub._tx then
				self:_deliver(sub, msg)
			end
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
---@field _bus Bus
---@field _q_length integer
---@field _full FullPolicy
---@field _subs table<Subscription, boolean>
---@field _disconnected boolean
local Connection = {}
Connection.__index = Connection

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

--- Return whether this connection has been disconnected.
---@return boolean
function Connection:is_disconnected()
	return self._disconnected
end

--- Return total drops across all subscriptions owned by this connection.
---@return number
function Connection:dropped()
	local n = 0
	for sub in pairs(self._subs) do
		n = n + sub:dropped()
	end
	return n
end

--- Publish a message to the bus (best-effort fanout; never blocks).
---@param topic Topic
---@param payload any
---@return boolean ok
function Connection:publish(topic, payload)
	assert(not self._disconnected, 'connection is disconnected')
	self._bus:_publish(new_msg(topic, payload))
	return true
end

--- Publish and retain a message under its exact topic.
---@param topic Topic
---@param payload any
---@return boolean ok
function Connection:retain(topic, payload)
	assert(not self._disconnected, 'connection is disconnected')
	self._bus:_retain(new_msg(topic, payload))
	return true
end

--- Remove any retained message stored under the exact topic.
---@param topic Topic
---@return boolean ok
function Connection:unretain(topic)
	assert(type(topic) == 'table', 'unretain expects a topic token array (table)')
	assert(not self._disconnected, 'connection is disconnected')
	self._bus:_unretain(topic)
	return true
end

--- Subscribe to a topic pattern.
---
--- queue_len:
---   * 0 creates a rendezvous-style subscription (no buffering).
---   * >0 buffers up to queue_len messages.
---
--- full_policy:
---   * 'drop_newest' or 'drop_oldest' (bounded, non-blocking)
---   * 'block' is rejected by this bus.
---@param topic Topic
---@param queue_len? integer
---@param full_policy? FullPolicy
---@return Subscription
function Connection:subscribe(topic, queue_len, full_policy)
	if self._disconnected then error('connection is disconnected') end

	local qlen = queue_len
	if qlen == nil then qlen = self._q_length end
	assert(type(qlen) == 'number' and qlen >= 0, 'subscribe: queue_len must be >= 0')

	local full = assert_full_policy(full_policy or self._full, 2) or DEFAULT_POLICY

	local sub = self._bus:_subscribe(self, topic, qlen, full)
	self._subs[sub] = true

	-- Scope-bound cleanup using the current scope.
	local s = scope_mod.current()
	sub._detach_finaliser = s:finally(function () sub:unsubscribe() end)

	return sub
end

---@param sub Subscription
---@param reason any
---@param remove_from_bus boolean
function Connection:_drop_sub(sub, reason, remove_from_bus)
	sub:_close(reason)
	detach_finaliser(sub)
	if remove_from_bus then self._bus:_unsubscribe(sub) end
	if sub._conn == self then sub._conn = nil end
end

--- Unsubscribe a subscription owned by this connection (idempotent).
---@param sub Subscription
---@return boolean ok
function Connection:unsubscribe(sub)
	if not sub or getmetatable(sub) ~= Subscription then
		error('unsubscribe expects a Subscription')
	end

	local owned = not not self._subs[sub]
	self:_drop_sub(sub, 'unsubscribed', owned)
	if owned then self._subs[sub] = nil end
	return true
end

--- Disconnect this connection and close all owned subscriptions (idempotent).
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
	return true
end

--- Return a small stats snapshot for this connection.
---@return table
function Connection:stats()
	local nsubs = 0
	for _ in pairs(self._subs) do nsubs = nsubs + 1 end
	return { dropped = self:dropped(), subscriptions = nsubs }
end

--------------------------------------------------------------------------------
-- Request helpers
--------------------------------------------------------------------------------

--- Publish a request with a reply_to topic; returns a subscription for replies.
---@param topic Topic
---@param payload any
---@param queue_len? integer
---@param full_policy? FullPolicy
---@return Subscription
function Connection:request_sub(topic, payload, queue_len, full_policy)
	local reply_to = { uuid.new() }
	local msg      = new_msg(topic, payload, reply_to)

	-- Subscribe first to avoid racing a fast responder.
	local sub = self:subscribe(reply_to, queue_len, full_policy)
	self._bus:_publish(msg)
	return sub
end

--- Request/response helper returning exactly one reply (first reply wins).
--- Compose timeouts externally using choice + sleep.
---@param topic Topic
---@param payload any
---@return Op  -- when performed: Message|nil, string|nil
function Connection:request_once_op(topic, payload)
	return op.guard(function ()
		local reply_to = { uuid.new() }
		local msg      = new_msg(topic, payload, reply_to)

		return op.bracket(
			function ()
				-- Keep the first reply; drop later ones.
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

--- Create a connection bound to the current scope.
---@return Connection conn
function Bus:connect()
	local s = scope_mod.current()
	local conn = new_connection(self, self._q_length, self._full)

	self._conns[conn] = true
	s:finally(function () conn:disconnect() end)

	return conn
end

--- Return a small stats snapshot for this bus instance.
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

--- Create a new in-process bus.
---@param params? { q_length?: integer, full?: FullPolicy, s_wild?: string, m_wild?: string }
---@return Bus bus
local function new(params)
	params = params or {}

	local q_length = params.q_length
	if q_length == nil then q_length = DEFAULT_Q_LEN end
	assert(type(q_length) == 'number' and q_length >= 0, 'bus.new: q_length must be >= 0')

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
