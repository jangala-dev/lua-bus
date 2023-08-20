package.path = "../src/?.lua;" .. package.path

local Fiber = require 'fibers.fiber'
local Bus = require 'bus'

local bus = Bus.new(10)

Fiber.spawn(function ()
    -- Connect to the bus with credentials
    local credentials = {username = 'user', password = 'pass'}
    local connection, err = bus:connect(credentials)
    if not connection then
        print(err)
        return
    end

    -- Publish retained message
    connection:publish({topic="some/topic", payload=0, retained=true})

    -- Subscribe to a topic
    local subscription = assert(connection:subscribe("some/topic"))

    -- Test retained message
    assert(subscription:next_msg().payload == 0)

    -- Publish messages to that topic
    for i=1, 10 do
        connection:publish({topic="some/topic", payload=i})
    end

    -- 'Publish failing message to that topic'
    connection:publish({topic="some/topic", payload=11})

    -- 'next_msg for messages'
    for i=1, 10 do
        local message = subscription:next_msg()
        assert(message.payload == i)
    end

    subscription:unsubscribe()

    Fiber.stop()
end)

Fiber.main()