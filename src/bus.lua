-- bus.lua

local queue = require 'fibers.queue'
local op = require 'fibers.op'
local sleep = require 'fibers.sleep'
local trie = require 'trie'
local uuid = require 'uuid'

--------------------------------------------------------------------------------
-- Message 
--------------------------------------------------------------------------------
local Message = {}
Message.__index = Message

local function new_msg(topic, payload, opts)
    opts = opts or {}
    return setmetatable({
        topic     = topic, 
        payload   = payload, 
        retained  = opts.retained,
        reply_to  = opts.reply_to,
        headers   = opts.headers or {},
        id        = opts.id or uuid.new(),
    }, Message)
end

--------------------------------------------------------------------------------
-- Subscription
--------------------------------------------------------------------------------
local Subscription = {}
Subscription.__index = Subscription

local function new_subscription(conn, topic, q)
    return setmetatable({
        connection = conn,
        topic = topic, 
        q = q
    }, Subscription)
end

function Subscription:next_msg_op(timeout)
    local msg_op = op.choice(
        self.q:get_op(),
        timeout and sleep.sleep_op(timeout):wrap(function()
            return nil, "Timeout"
        end)
    )
    return msg_op
end

function Subscription:next_msg(timeout)
    return self:next_msg_op(timeout):perform()
end

function Subscription:next_msg_with_context_op(ctx)
    local msg_op = op.choice(
        self.q:get_op(),
        ctx:done_op():wrap(function()
            return nil, ctx:err()
        end)
    )
    return msg_op
end

function Subscription:next_msg_with_context(context)
    return self:next_msg_with_context_op(context):perform()
end


function Subscription:unsubscribe()
    self.connection:unsubscribe(self.topic, self)
end

--------------------------------------------------------------------------------
-- Connection
--------------------------------------------------------------------------------
local Connection = {}
Connection.__index = Connection

local function new_connection(bus)
    return setmetatable({
        bus = bus,
        subscriptions = {}
    }, Connection)
end

function Connection:publish(msg)
    assert(getmetatable(msg)==Message, "Only message types can be published")
    self.bus:publish(msg)
    return true
end

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
                local msg = self:new_msg(new_topic, value, opts)
                self:publish(msg)
            end
        end
    end

    return true
end

function Connection:subscribe(topic)
    local subscription, err = self.bus:subscribe(self, topic)
    if err then
        return nil, err
    end
    table.insert(self.subscriptions, subscription)
    return subscription, nil
end

function Connection:unsubscribe(topic, subscription)
    self.bus:unsubscribe(topic, subscription)
    for i, sub in ipairs(self.subscriptions) do
        if sub == subscription then
            table.remove(self.subscriptions, i)
            return
        end
    end
end

function Connection:disconnect()
    for _, subscription in ipairs(self.subscriptions) do
        self:unsubscribe(subscription.topic, subscription)
    end
    self.subscriptions = {}
end

-- Since we prefer explicit blocking methods, we simply return a subscription
-- Caller can then use any of sub methods to handle conc/cancellation. 
-- Responsibility of the caller to :unsub from the returned sub.
function Connection:request(msg)
    msg.reply_to = msg.reply_to or uuid.new()
    self:publish(msg)
    return self:subscribe({msg.reply_to}) 
end


--------------------------------------------------------------------------------
-- Bus
--------------------------------------------------------------------------------
local Bus = {}
Bus.__index = Bus

local DEFAULT_Q_LEN = 10

function Bus:connect()
    return new_connection(self)
end

function Bus:subscribe(connection, topic)
    local topic_entry = self.topics:retrieve(topic)
    if not topic_entry then
        topic_entry = { subs = {} }
        self.topics:insert(topic, topic_entry)
    end

    local q = queue.new(self.q_length)
    local subscription = new_subscription(connection, topic, q)
    table.insert(topic_entry.subs, subscription)

    -- Deliver retained messages
    for retained_msg in self.retained_messages:match_values_iter(topic) do
        q:put_op(retained_msg):perform_alt(function() end)
    end

    return subscription
end

function Bus:publish(msg)
    for topic_entry in self.topics:match_values_iter(msg.topic) do
        for _, sub in ipairs(topic_entry.subs) do
            sub.q:put_op(msg):perform_alt(function() end)
        end
    end

    if msg.retained then
        if not msg.payload then
            -- Clearing retained message
            self.retained_messages:delete(msg.topic)
        else
            self.retained_messages:insert(msg.topic, msg)
        end
    end
end

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