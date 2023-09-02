local queue = require 'fibers.queue'
local op = require 'fibers.op'
local sleep = require 'fibers.sleep'
local trie = require 'trie'

local DEFAULT_Q_LEN = 10

local CREDS = {
    ['user'] = 'pass',
    ['user1'] = 'pass1',
    ['user2'] = 'pass2',
}

local Bus = {}
Bus.__index = Bus

local function new(params)
    params = params or {}
    return setmetatable({
        q_length = params.q_length or DEFAULT_Q_LEN,
        topics = trie.new(params.s_wild, params.m_wild, params.sep), --sets single_wild, multi_wild, separator
        retained_messages = trie.new(params.s_wild, params.m_wild, params.sep)
    }, Bus)
end

local Subscription = {}
Subscription.__index = Subscription

function Subscription.new(conn, topic, q)
    return setmetatable({
        connection = conn,
        topic = topic,
        q = q
    }, Subscription)
end

function Subscription:next_msg(timeout)
    local msg
    if timeout then
        msg = op.choice(
            self.q:get_op(),
            sleep.sleep_op(timeout)
        ):perform()
    else
        msg = self.q:get()
    end
    if msg then return msg else return nil, "Timeout" end
end

function Subscription:unsubscribe()
    self.connection:unsubscribe(self.topic, self)
end

local Connection = {}
Connection.__index = Connection

function Connection.new(bus)
    return setmetatable({bus = bus, subscriptions = {}}, Connection)
end

function Connection:publish(message)
    self.bus:publish(message)
    return true
end

function Connection:subscribe(topic)
    local subscription = self.bus:subscribe(self, topic)
    table.insert(self.subscriptions, subscription)
    return subscription
end

function Connection:unsubscribe(topic, subscription)
    self.bus:unsubscribe(topic, subscription)

    for i, sub in ipairs(self.subscriptions) do -- slow O(n)
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

function Bus:connect(username, password)
    if CREDS[username] == password then -- production ready!
        return Connection.new(self)
    else
        return nil, 'Authentication failed'
    end
end

-- Bus:subscribe function
function Bus:subscribe(connection, topic)
    -- get topic from the trie, or make and add to the trie
    local topic_entry = self.topics:retrieve(topic)
    if not topic_entry then
        topic_entry = {subs = {}}
        self.topics:insert(topic, topic_entry)
    end
    
    -- create the subscription - we have no identity yet, UUID?
    local q = queue.new(self.q_length)
    local subscription = Subscription.new(connection, topic, q)
    table.insert(topic_entry.subs, subscription)

    -- send any relevant retained messages
    for _, v in ipairs(self.retained_messages:match(topic)) do  -- wildcard search in trie
        local put_operation = subscription.q:put_op(v.value)
        put_operation:perform_alt(function ()
            -- print 'QUEUE FULL, not sent' --need to log blocked queue properly
        end)
    end

    return subscription
end

-- Bus:publish function
function Bus:publish(message)
    local matches = self.topics:match(message.topic)
    for _, topic_entry in ipairs(matches) do
        for _, sub in ipairs(topic_entry.value.subs) do
            local put_operation = sub.q:put_op(message)
            put_operation:perform_alt(function ()
                -- TODO: log this properly
            end)
        end
        -- add logic here for nats style q_subs if we go this route
    end

    if message.retained then
        if not message.payload then  -- send msg with empty payload + ret flag to clear ret message
            self.retained_messages:delete(message.topic)
        else
            self.retained_messages:insert(message.topic, message)
        end
    end
end

-- Bus:unsubscribe function
function Bus:unsubscribe(topic, subscription)
    local topic_entry = self.topics:retrieve(topic)
    assert(topic_entry, "error: unsubscribing from a non-existent topic")

    for i, sub in ipairs(topic_entry.subs) do  -- slow O(n)
        if sub == subscription then
            table.remove(topic_entry.subs, i)
        end
    end

    if #topic_entry.subs == 0 then
        self.topics:delete(topic)
    end
end

return {
    new = new
}
