package.path = "../src/?.lua;" .. package.path

local Fiber = require 'fibers.fiber'
local Bus = require 'bus'

-- Test Simple PubSub
local function test_simple()
    local bus = Bus.new({sep = "/"})

    local credentials = {username = 'user', password = 'pass'}
    local connection = assert(bus:connect(credentials))

    local subscription = assert(connection:subscribe("simple/topic"))

    connection:publish({topic="simple/topic", payload="Hello"})

    local msg, err = subscription:next_msg()
    assert(msg.payload == "Hello" and not err)

    print("Simple test passed!")
end

-- Test Unauthorised Access
local function test_unauth_access()
    local bus = Bus.new()
    local wrong_credentials = {username = 'wrong', password = 'wrong'}
    local _, err = bus:connect(wrong_credentials)
    assert(err)

    print("Unauthorized access test passed!")
end

-- Test Multiple Subscribers
local function test_multi_sub()
    local bus = Bus.new({sep = "/"})
    local credentials = {username = 'user', password = 'pass'}
    local connection1 = assert(bus:connect(credentials))
    local connection2 = assert(bus:connect(credentials))

    local subscription1 = assert(connection1:subscribe("multi/topic"))
    local subscription2 = assert(connection2:subscribe("multi/topic"))

    connection1:publish({topic="multi/topic", payload="Hello"})

    assert(subscription1:next_msg().payload == "Hello")
    assert(subscription2:next_msg().payload == "Hello")

    print("Multiple subscribers test passed!")
end

-- Test Multiple Topics
local function test_multi_topics()
    local bus = Bus.new({sep = "/"})

    local credentials = {username = 'user', password = 'pass'}
    local connection = assert(bus:connect(credentials))

    local subscriptionA = assert(connection:subscribe("topic/A"))
    local subscriptionB = assert(connection:subscribe("topic/B"))

    connection:publish({topic="topic/A", payload="MessageA"})

    assert(subscriptionA:next_msg().payload == "MessageA")
    assert(subscriptionB:next_msg(1e-3) == nil) -- There shouldn't be any message for topic/B

    print("Multiple topics test passed!")
end

-- Test Clean Subscription
local function test_clean_sub()
    local bus = Bus.new({sep = "/"})

    local credentials = {username = 'user', password = 'pass'}
    local connection = assert(bus:connect(credentials))

    connection:publish({topic="clean/topic", payload="OldMessage"})
    local subscription = assert(connection:subscribe("clean/topic"))

    -- Since the old message was not retained, the new subscriber shouldn't receive it.
    assert(subscription:next_msg(1e-3) == nil) 

    print("Clean subscription test passed!")
end


-- Test Connection Cleanup
local function test_conn_clean()
    local bus = Bus.new({sep = "/"})

    local credentials = {username = 'user', password = 'pass'}
    local connection = assert(bus:connect(credentials))

    local subscription = assert(connection:subscribe("cleanup/topic"))

    connection:publish({topic="cleanup/topic", payload="CleanupTest"})
    assert(subscription:next_msg().payload == "CleanupTest")

    -- Disconnect the connection and clean up subscriptions
    connection:disconnect()

    -- Verify the subscription is cleaned up
    local topic_data = bus.topics["cleanup/topic"]
    assert(not topic_data or #topic_data.subscribers == 0, "Subscription was not cleaned up")

    print("Connection cleanup test passed!")
end


-- Retained Message Test
local function test_retained_msg()
    local bus = Bus.new({sep = "/"})

    local credentials = {username = 'user', password = 'pass'}
    local connection = assert(bus:connect(credentials))

    -- Publish a retained message
    connection:publish({topic="retained/topic", payload="RetainedMessage", retained=true})

    -- A new subscriber should receive the last retained message
    local subscription = assert(connection:subscribe("retained/topic"))
    assert(subscription:next_msg().payload == "RetainedMessage")

    print("Retained message test passed!")
end

-- Unsubscribe Test
local function test_unsubscribe()
    local bus = Bus.new({sep = "/"})

    local credentials = {username = 'user', password = 'pass'}
    local connection = assert(bus:connect(credentials))

    local subscription = assert(connection:subscribe("unsubscribe/topic"))
    connection:unsubscribe("unsubscribe/topic", subscription)

    connection:publish({topic="unsubscribe/topic", payload="NoReceive"})
    assert(subscription:next_msg(1e-3) == nil) -- The subscriber should not receive the message

    print("Unsubscribe test passed!")
end

-- Queue Overflow Test
local function test_q_overflow()
    local bus = Bus.new({sep = "/"})

    local credentials = {username = 'user', password = 'pass'}
    local connection = assert(bus:connect(credentials))

    local subscription = assert(connection:subscribe("overflow/topic"))

    for i = 1, 10 do
        connection:publish({topic="overflow/topic", payload="Message" .. i})
    end

    -- The 11th message should not be queued, so the last message should be "Message10"
    for i = 1, 10 do
        assert(subscription:next_msg().payload == "Message" .. i)
    end
    assert(subscription:next_msg(1e-3) == nil)

    print("Queue overflow test passed!")
end

-- Multiple Connections with Different Credentials
local function test_multi_creds()
    local bus = Bus.new({sep = "/"})

    local creds1 = {username = 'user1', password = 'pass1'}
    local connection1 = assert(bus:connect(creds1))
    local subscription1 = assert(connection1:subscribe("multi/creds/2"))
    
    local creds2 = {username = 'user2', password = 'pass2'}
    local connection2 = assert(bus:connect(creds2))
    local subscription2 = assert(connection2:subscribe("multi/creds/1"))
    
    connection1:publish({topic="multi/creds/1", payload="FromUser1"})
    connection2:publish({topic="multi/creds/2", payload="FromUser2"})

    assert(subscription1:next_msg().payload == "FromUser2")
    assert(subscription2:next_msg().payload == "FromUser1")

    print("Multiple credentials test passed!")
end

-- Multiple Connections with Different Credentials
local function test_wildcard()
    local bus = Bus.new({sep = "/", m_wild = "#", s_wild = "+"})

    local creds = {username = 'user', password = 'pass'}
    local connection = assert(bus:connect(creds))

    local working_sub_strings = {
        "wild/cards/are/fun",
        "wild/cards/are/+",
        "wild/+/are/fun",
        "wild/+/are/#",
        "wild/+/#",
        "#"
    }
    local working_subs = {}
    for _, v in ipairs(working_sub_strings) do 
        table.insert(working_subs, assert(connection:subscribe(v)))
    end

    local not_working_sub_strings = {
        "wild/cards/are/funny",
        "wild/cards/are/+/fun",
        "wild/+/+",
        "tame/#",
    }
    local not_working_subs = {}
    for _, v in ipairs(not_working_sub_strings) do 
        table.insert(not_working_subs, assert(connection:subscribe(v)))
    end
    
    connection:publish({topic="wild/cards/are/fun", payload="payload"})

    for i, v in ipairs(working_subs) do 
        assert(v:next_msg().payload=="payload")
    end

    for i, v in ipairs(not_working_subs) do 
        assert(not v:next_msg(1e-3))
    end

    print("Wildcard test passed!")
end


Fiber.spawn(function ()
    test_simple()
    test_unauth_access()
    test_multi_sub()
    test_multi_topics()
    test_clean_sub()
    test_conn_clean()
    test_unsubscribe()
    test_retained_msg()
    test_q_overflow()
    test_multi_creds()
    test_wildcard()
    print("ALL TESTS PASSED!")
    Fiber.stop()
end)

Fiber.main()
