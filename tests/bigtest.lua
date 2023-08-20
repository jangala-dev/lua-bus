package.path = "../src/?.lua;" .. package.path

local Fiber = require 'fibers.fiber'
local Bus = require 'bus'

local bus = Bus.new(10)

-- Test Unauthorized Access
Fiber.spawn(function ()
    local wrongCredentials = {username = 'wrong', password = 'wrong'}
    local _, err = bus:connect(wrongCredentials)
    assert(err)

    print("Unauthorized access test passed!")
end)

-- Test Multiple Subscribers
Fiber.spawn(function ()
    local credentials = {username = 'user', password = 'pass'}
    local connection1 = assert(bus:connect(credentials))
    local connection2 = assert(bus:connect(credentials))

    local subscription1 = assert(connection1:subscribe("multi/topic"))
    local subscription2 = assert(connection2:subscribe("multi/topic"))

    connection1:publish({topic="multi/topic", payload="Hello"})

    assert(subscription1:next_msg().payload == "Hello")
    assert(subscription2:next_msg().payload == "Hello")

    print("Multiple subscribers test passed!")
end)

-- Test Multiple Topics
Fiber.spawn(function ()
    local credentials = {username = 'user', password = 'pass'}
    local connection = assert(bus:connect(credentials))

    local subscriptionA = assert(connection:subscribe("topic/A"))
    local subscriptionB = assert(connection:subscribe("topic/B"))

    connection:publish({topic="topic/A", payload="MessageA"})

    assert(subscriptionA:next_msg().payload == "MessageA")
    assert(subscriptionB:next_msg(0) == nil) -- There shouldn't be any message for topic/B

    print("Multiple topics test passed!")
end)

-- Test Clean Subscription
Fiber.spawn(function ()
    local credentials = {username = 'user', password = 'pass'}
    local connection = assert(bus:connect(credentials))

    connection:publish({topic="clean/topic", payload="OldMessage"})
    local subscription = assert(connection:subscribe("clean/topic"))

    -- Since the old message was not retained, the new subscriber shouldn't receive it.
    assert(subscription:next_msg(0) == nil) 

    print("Clean subscription test passed!")
end)

-- Test Connection Cleanup
Fiber.spawn(function ()
    local credentials = {username = 'user', password = 'pass'}
    local connection = assert(bus:connect(credentials))

    local subscription = assert(connection:subscribe("cleanup/topic"))

    connection:publish({topic="cleanup/topic", payload="CleanupTest"})
    assert(subscription:next_msg().payload == "CleanupTest")

    -- Disconnect the connection and clean up subscriptions
    connection:disconnect()

    -- Verify the subscription is cleaned up
    local topicData = bus.topics["cleanup/topic"]
    assert(not topicData or #topicData.subscribers == 0, "Subscription was not cleaned up")

    print("Connection cleanup test passed!")
end)


Fiber.main()
