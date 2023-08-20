local queue = require 'fibers.queue'
local op = require 'fibers.op'
local sleep = require 'fibers.sleep'

local Bus = {}
Bus.__index = Bus

function Bus.new(q_length)
    return setmetatable({q_length = q_length, topics = {}}, Bus)
end

local Subscription = {}
Subscription.__index = Subscription

function Subscription.new(conn, topic, q)
    return setmetatable({connection = conn, topic = topic, q = q}, Subscription)
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
    return msg or nil, "Timeout"
end

function Subscription:unsubscribe()
    self.connection:unsubscribe(self.topic, self)
end

local Connection = {}
Connection.__index = Connection

function Connection.new(bus, creds)
    return setmetatable({bus = bus, creds = creds, subscriptions = {}}, Connection)
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

function Bus:connect(creds)
    if creds.username == 'user' and creds.password == 'pass' then
        return Connection.new(self)
    else
        return nil, 'Authentication failed'
    end
end

function Bus:subscribe(connection, topic)
    local q = queue.new(self.q_length)

    if not self.topics[topic] then
        self.topics[topic] = {subscribers = {}, retained = nil}
    end

    local subscription = Subscription.new(connection, topic, q)
    table.insert(self.topics[topic].subscribers, subscription)

    if self.topics[topic].retained then
        local put_operation = subscription.q:put_op(self.topics[topic].retained)
        put_operation:perform_alt(function ()
            print 'QUEUE FULL, not sent'
        end)
    end
    
    return subscription
end

function Bus:publish(message)
    local topic_data = self.topics[message.topic] or {subscribers = {}, retained = nil}
    self.topics[message.topic] = topic_data

    if message.retained then
        topic_data.retained = message
    end
    
    for _, subscription in ipairs(topic_data.subscribers) do
        local put_operation = subscription.q:put_op(message)
        put_operation:perform_alt(function ()
            -- TODO: log this properly
            print 'QUEUE FULL, not sent'
        end)
    end
end

function Bus:unsubscribe(topic, subscription)
    local topic_data = self.topics[topic]
    if not topic_data then
        return
    end
    
    for i, sub in ipairs(topic_data.subscribers) do
        if sub == subscription then
            table.remove(topic_data.subscribers, i)
            return
        end
    end

    if #topic_data.subscribers == 0 and not topic_data.retained then
        self.topics[topic] = nil
    end
end

return Bus