package.path = "../src/?.lua;" .. package.path

local Bus = require 'bus'
local Fiber = require 'fibers.fiber'

-- Create an instance of Bus
local bus = Bus.new({s_wild='+', m_wild='#', sep = '/'})

-- Fiber 1: Publisher
Fiber.spawn(function()
    -- Create a connection and subscribe to a topic
    local conn = assert(bus:connect({username='user', password ='pass'}))
    conn:publish({topic="topic/path", payload="Hello World!", retained=true})
end)

-- Fiber 2: Subscriber
Fiber.spawn(function()
    -- Create a connection and subscribe to a topic
    local conn = assert(bus:connect({username='user', password='pass'}))
    local sub = conn:subscribe("topic/path")

    -- Listen for a message
    local msg, _ = sub:next_msg() -- blocks fiber if no timeout specified
    print("Fiber 2:", msg.payload)

    Fiber.stop()
end)

Fiber.main()