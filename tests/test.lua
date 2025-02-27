package.path = "../src/?.lua;" .. package.path

local Fiber = require 'fibers.fiber'
local Sleep = require 'fibers.sleep'
local Bus = require 'bus'

local new_msg = Bus.new_msg

-- Test Simple PubSub
local function test_simple()
    local bus = Bus.new({m_wild = "#", s_wild = "+"})

    local conn = assert(bus:connect())

    local sub = assert(conn:subscribe({"simple", "topic"}))

    conn:publish(new_msg({"simple", "topic"}, "Hello"))

    local msg, err = sub:next_msg()
    assert(msg.payload == "Hello" and not err)

    print("Simple test passed!")
end

-- Test Multiple Subscribers
local function test_multi_sub()
    local bus = Bus.new({m_wild = "#", s_wild = "+"})
    local conn_1 = assert(bus:connect())
    local conn_2 = assert(bus:connect())

    local sub_1 = assert(conn_1:subscribe({"multi", "topic"}))
    local sub_2 = assert(conn_2:subscribe({"multi", "topic"}))

    conn_1:publish(new_msg({"multi", "topic"}, "Hello"))

    assert(sub_1:next_msg().payload == "Hello")
    assert(sub_2:next_msg().payload == "Hello")

    print("Multiple subscribers test passed!")
end

-- Test Multiple Topics
local function test_multi_topics()
    local bus = Bus.new({m_wild = "#", s_wild = "+"})

    local conn = assert(bus:connect())

    local sub_A = assert(conn:subscribe({"topic", "A"}))
    local sub_B = assert(conn:subscribe({"topic", "B"}))

    conn:publish(new_msg({"topic", "A"}, "MessageA"))

    assert(sub_A:next_msg().payload == "MessageA")
    assert(sub_B:next_msg(1e-3) == nil) -- There shouldn't be any message for topic/B

    print("Multiple topics test passed!")
end

-- Test Clean Subscription
local function test_clean_sub()
    local bus = Bus.new({m_wild = "#", s_wild = "+"})

    local conn = assert(bus:connect())

    conn:publish(new_msg({"clean", "topic"}, "OldMessage"))
    local sub = assert(conn:subscribe({"clean", "topic"}))

    -- Since the old message was not retained, the new subscriber shouldn't receive it.
    assert(sub:next_msg(1e-3) == nil) 

    print("Clean subscription test passed!")
end


-- Test conn Cleanup
local function test_conn_clean()
    local bus = Bus.new({m_wild = "#", s_wild = "+"})

    local conn = assert(bus:connect())

    local sub = assert(conn:subscribe({"cleanup", "topic"}))

    conn:publish(new_msg({"cleanup", "topic"}, "CleanupTest"))
    assert(sub:next_msg().payload == "CleanupTest")

    -- Disconnect the conn and clean up subscriptions
    conn:disconnect()

    -- Verify the subscription is cleaned up using the Trie API
    local topic_data = bus.topics:retrieve({"cleanup", "topic"})
    assert(not topic_data or #topic_data.subs == 0, "Subscription was not cleaned up")

    print("conn cleanup test passed!")
end


-- Retained Message Test
local function test_retained_msg()
    local bus = Bus.new({m_wild = "#", s_wild = "+"})

    local conn = assert(bus:connect())

    -- Publish a retained message
    conn:publish(new_msg({"retained", "topic"}, "RetainedMessage", {retained=true}))

    -- A new subscriber should receive the last retained message
    local sub = assert(conn:subscribe({"retained", "topic"}))
    assert(sub:next_msg().payload == "RetainedMessage")

    print("Retained message test passed!")
end

-- Unsubscribe Test
local function test_unsubscribe()
    local bus = Bus.new({m_wild = "#", s_wild = "+"})
    local conn = assert(bus:connect())

    local subscription = assert(conn:subscribe({"unsubscribe", "topic"}))
    subscription:unsubscribe()

    conn:publish(new_msg({"unsubscribe", "topic"}, "NoReceive"))
    assert(subscription:next_msg(1e-3) == nil) -- The subscriber should not receive the message

    print("Unsubscribe test passed!")
end

-- Queue Overflow Test
local function test_q_overflow()
    local bus = Bus.new({m_wild = "#", s_wild = "+"})
    local conn = assert(bus:connect())

    local sub = assert(conn:subscribe({"overflow", "topic"}))

    for i = 1, 11 do
        conn:publish(new_msg({"overflow", "topic"}, "Message"..i))
    end

    -- The 11th message should not be queued, so the last message should be "Message10"
    for i = 1, 10 do
        assert(sub:next_msg().payload == "Message"..i)
    end
    assert(sub:next_msg(1e-3) == nil)

    print("Queue overflow test passed!")
end

-- Wildcard Test
local function test_wildcard()
    local bus = Bus.new({m_wild = "#", s_wild = "+"})

    local conn = assert(bus:connect())

    local working_sub_strings = {
        {"wild", "cards", "are", "fun"},
        {"wild", "cards", "are", "+"},
        {"wild", "+", "are", "fun"},
        {"wild", "+", "are", "#"},
        {"wild", "+", "#"},
        {"#"}
    }
    local working_subs = {}
    for _, v in ipairs(working_sub_strings) do
        local sub, _ = assert(conn:subscribe(v))
        table.insert(working_subs, sub)
    end

    local not_working_sub_strings = {
       { "wild", "cards", "are", "funny"},
       { "wild", "cards", "are", "+", "fun"},
       { "wild", "+", "+"},
       { "tame", "#"},
    }
    local not_working_subs = {}
    for _, v in ipairs(not_working_sub_strings) do
        local sub, _ = assert(conn:subscribe(v))
        table.insert(not_working_subs, sub)
    end
    
    conn:publish(new_msg({"wild", "cards", "are", "fun"}, "payload"))

    for i, v in ipairs(working_subs) do 
        assert(v:next_msg().payload=="payload")
    end

    for i, v in ipairs(not_working_subs) do 
        assert(not v:next_msg(1e-3))
    end

    print("Wildcard test passed!")
end

-- Test Request Reply
local function test_request()
    local bus = Bus.new({sep = "/"})

    Fiber.spawn(function()
        local conn = assert(bus:connect())
        local helper = assert(conn:subscribe({"helpme"}))
        local rec = helper:next_msg()
        conn:publish(new_msg({rec.reply_to}, "Sure "..rec.payload))
    end)

    Fiber.spawn(function()
        local conn = assert(bus:connect())
        local helper = assert(conn:subscribe({"helpme"}))
        local rec = helper:next_msg()
        Sleep.sleep(0.1)
        conn:publish(new_msg({rec.reply_to}, "No problem "..rec.payload))
    end)

    Sleep.sleep(0.1)

    local conn3 = assert(bus:connect())
    local request = assert(conn3:request(new_msg({"helpme"}, "John")))

    local msg, err = request:next_msg()
    assert(msg.payload == "Sure John")
    local msg, err = request:next_msg()
    assert(msg.payload == "No problem John")

    print("Request reply test passed!")
end


Fiber.spawn(function ()
    test_simple()
    test_multi_sub()
    test_multi_topics()
    test_clean_sub()
    test_conn_clean()
    test_unsubscribe()
    test_retained_msg()
    test_q_overflow()
    test_wildcard()
    test_request()
    print("ALL TESTS PASSED!")
    Fiber.stop()
end)

Fiber.main()
