--- A pubsub message bus built with fibers and a Trie for topic matching.
-- This module provides a concurrent pubsub system that supports wildcard subscriptions,
-- retained messages, and a request/response model.
-- @module bus

local queue = require 'fibers.queue'
local op = require 'fibers.op'
local sleep = require 'fibers.sleep'
local trie = require 'trie'
local uuid = require 'uuid'

--------------------------------------------------------------------------------
-- Message
--------------------------------------------------------------------------------

--- Message class.
-- Represents a message that can be published on the bus.
local Message = {}
Message.__index = Message

--- Creates a new Message.
-- @tparam any topic The topic of the message.
-- @tparam any payload The payload of the message.
-- @tparam[opt] table opts Options table.
-- @tparam[opt] boolean opts.retained Whether the message should be retained.
-- @tparam[opt] any opts.reply_to The reply-to topic.
-- @tparam[opt] table opts.headers Headers to attach to the message.
-- @tparam[opt] any opts.id The message ID; if not provided, a new UUID is generated.
-- @treturn Message The new message.
local function new_msg(topic, payload, opts)
    opts = opts or {}
    return setmetatable({
        topic    = topic,
        payload  = payload,
        retained  = opts.retained,
        reply_to  = opts.reply_to,
        headers   = opts.headers or {},
        id        = opts.id or uuid.new(),
    }, Message)
end

--------------------------------------------------------------------------------
-- Subscription
--------------------------------------------------------------------------------

--- Subscription class.
-- Represents a subscription to a topic on the bus.
local Subscription = {}
Subscription.__index = Subscription

--- Creates a new subscription.
-- @tparam Connection conn The connection that owns the subscription.
-- @tparam any topic The topic subscribed to.
-- @tparam queue q The queue for incoming messages.
-- @treturn Subscription The new subscription.
local function new_subscription(conn, topic, q)
    return setmetatable({
        connection = conn,
        topic = topic,
        q = q
    }, Subscription)
end

--- Waits for the next message or a timeout.
-- Uses op.choice to combine a get operation on the queue and a sleep op.
-- @tparam[opt] number timeout The maximum time to wait, in seconds.
-- @treturn op The operation representing the next message retrieval.
function Subscription:next_msg_op(timeout)
    local msg_op = op.choice(
        self.q:get_op(),
        timeout and sleep.sleep_op(timeout):wrap(function()
            return nil, "Timeout"
        end)
    )
    return msg_op
end

--- Retrieves the next message, blocking until available or timeout.
-- @tparam[opt] number timeout The maximum time to wait, in seconds.
-- @treturn any The next message.
-- @treturn nil|string nil, or an error message on timeout.
function Subscription:next_msg(timeout)
    return self:next_msg_op(timeout):perform()
end

--- Waits for the next message or context cancellation.
-- Combines the queue get op with a context's done op.
-- @tparam any ctx The context object.
-- @treturn op The operation representing the next message retrieval.
function Subscription:next_msg_with_context_op(ctx)
    local msg_op = op.choice(
        self.q:get_op(),
        ctx:done_op():wrap(function()
            return nil, ctx:err()
        end)
    )
    return msg_op
end

--- Retrieves the next message with context support, blocking until available or cancelled.
-- @tparam any context The context object.
-- @treturn any The next message.
-- @treturn nil|string nil, or an error message if cancelled.
function Subscription:next_msg_with_context(context)
    return self:next_msg_with_context_op(context):perform()
end

--- Unsubscribes from the topic.
-- Calls the unsubscribe method on the connection.
function Subscription:unsubscribe()
    self.connection:unsubscribe(self.topic, self)
end

--------------------------------------------------------------------------------
-- Connection
--------------------------------------------------------------------------------

--- Connection class.
-- Represents a connection to the bus, encapsulating subscriptions and publish/subscribe methods.
local Connection = {}
Connection.__index = Connection

--- Creates a new connection for the bus.
-- @tparam Bus bus The bus to connect to.
-- @treturn Connection The new connection.
local function new_connection(bus)
    return setmetatable({
        bus = bus,
        subscriptions = {}
    }, Connection)
end

--- Publishes a message on the bus.
-- @tparam Message msg The message to publish.
-- @treturn boolean true on success.
function Connection:publish(msg)
    assert(getmetatable(msg)==Message, "Only message types can be published")
    self.bus:publish(msg)
    return true
end

--- Publishes multiple messages derived from a nested payload.
-- Iterates over the nested_payload table to publish messages for each key/value pair.
-- @tparam any root_topic The base topic as an array of tokens.
-- @tparam table nested_payload A nested table of payloads.
-- @tparam[opt] table opts Options to pass to new_msg.
-- @treturn boolean true on success.
function Connection:publish_multiple(root_topic, nested_payload, opts)
    opts = opts or {}
    local stack = {{prefix = root_topic, payload = nested_payload}}

    while #stack > 0 do
        local current = table.remove(stack)
        local prefix = current.prefix
        local payload = current.payload

        for key, value in pairs(payload) do
            local new_topic = {}
            for i, token in ipairs(prefix) do
                new_topic[i] = token
            end
            table.insert(new_topic, key)

            if type(value) == "table" then
                table.insert(stack, {prefix = new_topic, payload = value})
            else
                local msg = new_msg(new_topic, value, opts)
                self:publish(msg)
            end
        end
    end

    return true
end

--- Subscribes to a topic.
-- @tparam any topic The topic to subscribe to.
-- @treturn Subscription The subscription object.
-- @treturn nil|string nil, or an error message on failure.
function Connection:subscribe(topic)
    local subscription, err = self.bus:subscribe(self, topic)
    if err then
        return nil, err
    end
    table.insert(self.subscriptions, subscription)
    return subscription, nil
end

--- Unsubscribes from a topic.
-- Removes the subscription from the connection and notifies the bus.
-- @tparam any topic The topic to unsubscribe from.
-- @tparam Subscription subscription The subscription to remove.
function Connection:unsubscribe(topic, subscription)
    self.bus:unsubscribe(topic, subscription)
    for i, sub in ipairs(self.subscriptions) do
        if sub == subscription then
            table.remove(self.subscriptions, i)
            return
        end
    end
end

--- Disconnects the connection.
-- Unsubscribes from all topics and clears subscriptions.
function Connection:disconnect()
    for _, subscription in ipairs(self.subscriptions) do
        self:unsubscribe(subscription.topic, subscription)
    end
    self.subscriptions = {}
end

--- Sends a request message and subscribes to the reply.
-- @tparam Message msg The request message.
-- @treturn Subscription The subscription for the reply.
function Connection:request(msg)
    msg.reply_to = msg.reply_to or uuid.new()
    self:publish(msg)
    return self:subscribe({ msg.reply_to })
end

--------------------------------------------------------------------------------
-- Bus
--------------------------------------------------------------------------------

--- Bus class.
-- Manages topics, subscriptions, and message publishing.
local Bus = {}
Bus.__index = Bus

local DEFAULT_Q_LEN = 10

--- Creates a new connection to the bus.
-- @treturn Connection A new connection instance.
function Bus:connect()
    return new_connection(self)
end

--- Subscribes a connection to a topic.
-- If the topic doesn't exist in the Trie, it is created.
-- Retained messages matching the topic are immediately delivered.
-- @tparam Connection connection The connection subscribing.
-- @tparam any topic The topic to subscribe to.
-- @treturn Subscription The subscription object.
function Bus:subscribe(connection, topic)
    local topic_entry = self.topics:retrieve(topic)
    if not topic_entry then
        topic_entry = { subs = {} }
        self.topics:insert(topic, topic_entry)
    end

    local q = queue.new(self.q_length)
    local subscription = new_subscription(connection, topic, q)
    table.insert(topic_entry.subs, subscription)

    -- Deliver retained messages.
    for retained_msg in self.retained_messages:match_values_iter(topic) do
        q:put_op(retained_msg):perform_alt(function() end)
    end

    return subscription
end

--- Publishes a message to all subscribers matching the topic.
-- Also handles retained messages if the message is marked as retained.
-- @tparam Message msg The message to publish.
function Bus:publish(msg)
    for topic_entry in self.topics:match_values_iter(msg.topic) do
        for _, sub in ipairs(topic_entry.subs) do
            sub.q:put_op(msg):perform_alt(function() end)
        end
    end

    if msg.retained then
        if not msg.payload then
            -- Clearing retained message.
            self.retained_messages:delete(msg.topic)
        else
            self.retained_messages:insert(msg.topic, msg)
        end
    end
end

--- Unsubscribes a subscription from a topic.
-- If a topic has no more subscribers, it is removed from the Trie.
-- @tparam any topic The topic to unsubscribe from.
-- @tparam Subscription subscription The subscription to remove.
function Bus:unsubscribe(topic, subscription)
    local topic_entry = self.topics:retrieve(topic)
    if not topic_entry then
        error("Unsubscribe from non-existent topic")
    end

    for i, sub in ipairs(topic_entry.subs) do
        if sub == subscription then
            table.remove(topic_entry.subs, i)
            break
        end
    end

    if #topic_entry.subs == 0 then
        self.topics:delete(topic)
    end
end

--- Creates a new Bus instance.
-- @tparam[opt] table params A table of parameters.
-- @tparam[opt] number params.q_length The maximum length of subscription queues.
-- @tparam[opt] any params.s_wild The token for a single-level wildcard (default: "+").
-- @tparam[opt] any params.m_wild The token for a multi-level wildcard (default: "#").
-- @treturn Bus The new Bus instance.
local function new(params)
    params = params or {}
    local self = {
        q_length = params.q_length or DEFAULT_Q_LEN,
        topics = trie.new(params.s_wild or "+", params.m_wild or "#"),
        retained_messages = trie.new(params.s_wild or "+", params.m_wild or "#"),
    }
    return setmetatable(self, Bus)
end

return {
    new = new,
    new_msg = new_msg
}
