--- In-process pub/sub bus built on fibers + trie.
---  - wildcard subscriptions (pubsub trie: wildcards allowed in stored keys)
---  - retained messages (retained trie: literal stored keys; wildcards in queries)
---  - request/response helper (reply_to topic)
---
--- Assumptions:
---  - This module is used only from within fibers (inside fibers.run).
---  - Cancellation is terminal; fibers.perform may raise on scope cancellation.
---@module 'bus'

local channel    = require 'fibers.channel'
local op         = require 'fibers.op'
local sleep      = require 'fibers.sleep'
local performer  = require 'fibers.performer'
local scope_mod  = require 'fibers.scope'
local cond       = require 'fibers.cond'

local trie       = require 'trie'
local uuid       = require 'uuid'

local perform = performer.perform

--------------------------------------------------------------------------------
-- Message
--------------------------------------------------------------------------------

---@class Message
---@field topic table
---@field payload any
---@field retained boolean|nil
---@field reply_to table|nil
---@field headers table
---@field id any
local Message = {}
Message.__index = Message

---@param topic table
---@param payload any
---@param opts? { retained?:boolean, reply_to?:table|string|number, headers?:table, id?:any }
---@return Message
local function new_msg(topic, payload, opts)
	opts = opts or {}

	local reply_to = opts.reply_to
	if reply_to ~= nil then
		local tr = type(reply_to)
		if tr == 'table' then
			-- ok
		elseif tr == 'string' or tr == 'number' then
			reply_to = { reply_to }
		else
			error('reply_to must be a token array, string, or number', 2)
		end
	end

	return setmetatable({
		topic     = topic,
		payload   = payload,
		retained  = opts.retained,
		reply_to  = reply_to,
		headers   = opts.headers or {},
		id        = (opts.id ~= nil) and opts.id or uuid.new(),
	}, Message)
end

--------------------------------------------------------------------------------
-- Subscription
--------------------------------------------------------------------------------

---@class Subscription
---@field _conn Connection|nil
---@field _topic table
---@field _ch Channel
---@field _closed boolean
---@field _close_reason string|nil
---@field _close_cond Cond
---@field _detach_finaliser fun()|nil
local Subscription = {}
Subscription.__index = Subscription

local function new_subscription(conn, topic, ch)
	return setmetatable({
		_conn             = conn,
		_topic            = topic,
		_ch               = ch,
		_closed           = false,
		_close_reason     = nil,
		_close_cond       = cond.new(),
		_detach_finaliser = nil,
	}, Subscription)
end

---@return table
function Subscription:topic()
	return self._topic
end

---@return boolean, string|nil
function Subscription:is_closed()
	return self._closed, self._close_reason
end

function Subscription:_close(reason)
	if self._closed then return end
	self._closed = true
	self._close_reason = reason or self._close_reason or 'closed'
	self._close_cond:signal()
end

--- Unsubscribe this subscription (idempotent).
function Subscription:unsubscribe()
	local conn = self._conn
	if not conn then
		-- Ensure receivers wake even if already detached.
		self:_close('unsubscribed')
		return true
	end
	return conn:unsubscribe(self)
end

--- Op yielding the next message, or (nil, err) on timeout/closed.
---@param timeout? number
---@return Op
function Subscription:next_msg_op(timeout)
  return op.guard(function ()
    -- Fast path: already closed dominates everything.
    if self._closed then return op.always(nil, self._close_reason or 'closed') end

    local closed_ev = self._close_cond:wait_op():wrap(function ()
      return nil, self._close_reason or 'closed'
    end)

    local msg_ev = self._ch:get_op():wrap(function (msg)
      -- Close dominates: if we were closed at any point before commit, drop.
      if self._closed then return nil, self._close_reason or 'closed' end
      return msg, nil
    end)

    if timeout ~= nil then
      local to_ev = sleep.sleep_op(timeout):wrap(function ()
        -- Also let close dominate over a concurrent timeout.
        if self._closed then return nil, self._close_reason or 'closed' end
        return nil, 'timeout'
      end)

      return op.choice(msg_ev, closed_ev, to_ev)
    end

    return op.choice(msg_ev, closed_ev)
  end)
end

---@param timeout? number
---@return Message|nil msg, string|nil err
function Subscription:next_msg(timeout)
	return perform(self:next_msg_op(timeout))
end

--------------------------------------------------------------------------------
-- Connection
--------------------------------------------------------------------------------

---@class Connection
---@field _bus Bus|nil
---@field _subs table<Subscription, boolean>
---@field _disconnected boolean
local Connection = {}
Connection.__index = Connection

local function new_connection(bus)
	return setmetatable({
		_bus          = bus,
		_subs         = {},
		_disconnected = false,
	}, Connection)
end

---@return boolean
function Connection:is_disconnected()
	return self._disconnected
end

---@param msg Message
function Connection:publish(msg)
	assert(getmetatable(msg) == Message, 'publish expects a Message')
	assert(self._bus, 'connection is closed')
	return self._bus:publish(msg)
end

---@param topic table
---@param payload any
---@param opts? table
function Connection:publish_topic(topic, payload, opts)
	return self:publish(new_msg(topic, payload, opts))
end

--- Subscribe to a topic pattern.
--- opts.scope_bound (default true):
---   - true  : unsubscribe automatically when the *current scope* exits
---   - false : caller is responsible for unsubscribing (bracket is typical)
---@param topic table
---@param opts? { replay_retained?: boolean, queue_len?: integer, scope_bound?: boolean }
---@return Subscription
function Connection:subscribe(topic, opts)
	if self._disconnected then
		error('connection is disconnected', 2)
	end
	local bus = assert(self._bus, 'connection is closed')

	opts = opts or {}
	local qlen = opts.queue_len or bus._q_length

	local sub = bus:_subscribe(self, topic, qlen, opts.replay_retained ~= false)
	self._subs[sub] = true

	-- Optional scope-bound cleanup using the *current* scope.
	if opts.scope_bound ~= false then
		local s = scope_mod.current()
		sub._detach_finaliser = s:finally(function ()
			sub:unsubscribe()
		end)
	end

	return sub
end

--- Unsubscribe a subscription owned by this connection (idempotent).
---@param sub Subscription
function Connection:unsubscribe(sub)
	if not sub or getmetatable(sub) ~= Subscription then
		error('unsubscribe expects a Subscription', 2)
	end

	-- Always mark closed (wakes any receivers), even for foreign/already-removed subs.
	sub:_close('unsubscribed')

	-- Idempotent: ignore unknown subs (already removed / foreign).
	if not self._subs[sub] then
		-- Break any remaining ownership link if it still points at us.
		if sub._conn == self then sub._conn = nil end
		return true
	end

	self._subs[sub] = nil

	-- Detach any scope finaliser now that the subscription is explicitly ended.
	if sub._detach_finaliser then
		sub._detach_finaliser()
		sub._detach_finaliser = nil
	end

	local bus = self._bus
	if bus then
		bus:_unsubscribe(sub) -- pure removal; does not close
	end

	-- Break reference cycles / ownership.
	sub._conn = nil

	return true
end

--- Disconnect the connection (idempotent).
function Connection:disconnect()
	if self._disconnected then return true end
	self._disconnected = true

	local bus = self._bus
	self._bus = nil

	-- Snapshot subscriptions to avoid mutation hazards.
	local snap = {}
	for sub in pairs(self._subs) do
		snap[#snap + 1] = sub
	end
	self._subs = {}

	for i = 1, #snap do
		local sub = snap[i]

		if sub._detach_finaliser then
			sub._detach_finaliser()
			sub._detach_finaliser = nil
		end

		sub:_close('disconnected')

		if bus then
			bus:_unsubscribe(sub)
		end

		sub._conn = nil
		snap[i] = nil
	end

	return true
end

--------------------------------------------------------------------------------
-- Request helpers
--------------------------------------------------------------------------------

local function ensure_reply_to(msg, opts)
	if msg.reply_to then
		return msg.reply_to
	end

	local id = uuid.new()
	local prefix = opts and opts.reply_topic_prefix or nil

	if prefix then
		local t = {}
		for i = 1, #prefix do t[i] = prefix[i] end
		t[#t + 1] = id
		msg.reply_to = t
	else
		msg.reply_to = { id }
	end

	return msg.reply_to
end

--- Request/response: subscribe to replies and publish request.
--- Returns a Subscription on the reply topic (multi-reply).
---@param msg Message
---@param opts? { reply_topic_prefix?: table, queue_len?: integer, scope_bound?: boolean }
---@return Subscription
function Connection:request_sub(msg, opts)
	assert(getmetatable(msg) == Message, 'request_sub expects a Message')
	opts = opts or {}

	local reply_to = ensure_reply_to(msg, opts)

	-- Subscribe first to avoid racing a fast responder.
	local sub = self:subscribe(reply_to, {
		replay_retained = false,
		queue_len       = opts.queue_len,
		scope_bound     = (opts.scope_bound ~= false),
	})

	self:publish(msg)
	return sub
end

--- Request/response: return exactly one reply (first reply wins).
---@param msg Message
---@param opts? { timeout?: number, reply_topic_prefix?: table, queue_len?: integer }
---@return Op  -- when performed: Message|nil reply, string|nil err
function Connection:request_once_op(msg, opts)
	assert(getmetatable(msg) == Message, 'request_once_op expects a Message')
	opts = opts or {}

	local timeout = opts.timeout

	return op.guard(function ()
		local reply_to = ensure_reply_to(msg, opts)

		-- Temporary subscription: not scope-bound; bracket guarantees cleanup.
		return op.bracket(
			function ()
				return self:subscribe(reply_to, {
					replay_retained = false,
					queue_len       = opts.queue_len,
					scope_bound     = false,
				})
			end,
			function (sub, _aborted)
				sub:unsubscribe()
			end,
			function (sub)
				self:publish(msg)
				return sub:next_msg_op(timeout)
			end
		)
	end)
end

---@param msg Message
---@param opts? table
---@return Message|nil reply, string|nil err
function Connection:request_once(msg, opts)
	return perform(self:request_once_op(msg, opts))
end

-- Backwards-compatibility / requested naming:
Connection.request = Connection.request_sub

--------------------------------------------------------------------------------
-- Bus
--------------------------------------------------------------------------------

---@class Bus
---@field _q_length integer
---@field _topics any
---@field _retained any
local Bus = {}
Bus.__index = Bus

local DEFAULT_Q_LEN = 10

---@param params? { q_length?:integer, s_wild?:any, m_wild?:any }
---@return Bus
local function new(params)
	params = params or {}

	local s_wild = params.s_wild or '+'
	local m_wild = params.m_wild or '#'

	return setmetatable({
		_q_length = params.q_length or DEFAULT_Q_LEN,
		_topics   = trie.new_pubsub(s_wild, m_wild),
		_retained = trie.new_retained(s_wild, m_wild),
	}, Bus)
end

--- Create a connection bound to the current scope.
---@return Connection
function Bus:connect()
	local s = scope_mod.current()
	local conn = new_connection(self)

	s:finally(function ()
		conn:disconnect()
	end)

	return conn
end

--- Internal: best-effort enqueue (never blocks).
local function best_effort_put(ch, msg)
	-- Drop if not immediately possible.
	perform(ch:put_op(msg):or_else(function () end))
end

--- Internal: subscribe implementation.
function Bus:_subscribe(conn, topic, qlen, replay_retained)
	local bucket = self._topics:retrieve(topic)
	if not bucket then
		bucket = { subs = {} }
		self._topics:insert(topic, bucket)
	end

	local ch  = channel.new(qlen)
	local sub = new_subscription(conn, topic, ch)
	bucket.subs[#bucket.subs + 1] = sub

	-- Replay retained messages that match the subscription pattern.
	if replay_retained then
		self._retained:each(topic, function (retained_msg)
			best_effort_put(ch, retained_msg)
		end)
	end

	return sub
end

--- Internal: unsubscribe implementation (idempotent).
--- Pure removal from the pubsub trie; does not touch subscription closure.
function Bus:_unsubscribe(sub)
	local topic  = sub._topic
	local bucket = self._topics:retrieve(topic)
	if not bucket then
		return true
	end

	local subs = bucket.subs
	for i = #subs, 1, -1 do
		if subs[i] == sub then
			table.remove(subs, i)
			break
		end
	end

	if #subs == 0 then
		self._topics:delete(topic)
	end

	return true
end

--- Publish a message (best-effort fanout).
---@param msg Message
function Bus:publish(msg)
	assert(getmetatable(msg) == Message, 'publish expects a Message')

	-- Fanout to matching subscriptions.
	self._topics:each(msg.topic, function (bucket)
		local subs = bucket.subs
		for i = 1, #subs do
			local sub = subs[i]
			if sub and not sub._closed then
				best_effort_put(sub._ch, msg)
			end
		end
	end)

	-- Retained message policy.
	if msg.retained then
		if msg.payload == nil then
			self._retained:delete(msg.topic)
		else
			self._retained:insert(msg.topic, msg)
		end
	end

	return true
end

--------------------------------------------------------------------------------
-- Public API
--------------------------------------------------------------------------------

return {
	new     = new,
	Bus     = Bus,

	Message = Message,
	new_msg = new_msg,
}
