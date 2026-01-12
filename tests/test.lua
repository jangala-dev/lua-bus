package.path = "../src/?.lua;" .. package.path

local fibers = require 'fibers'
local Sleep  = require 'fibers.sleep'
local Bus    = require 'bus'

local new_msg = Bus.new_msg

-- Use a slightly non-trivial timeout to avoid flakiness on slower systems.
local TMO = 0.05

local function assert_timeout(msg, err)
	assert(msg == nil, 'expected nil message on timeout')
	assert(err == 'timeout', ('expected timeout, got %s'):format(tostring(err)))
end

-- Test Simple PubSub
local function test_simple()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()
	local sub  = conn:subscribe({ "simple", "topic" })

	conn:publish(new_msg({ "simple", "topic" }, "Hello"))

	local msg, err = sub:next_msg()
	assert(msg and msg.payload == "Hello" and err == nil)

	print("Simple test passed!")
end

-- Test Multiple Subscribers
local function test_multi_sub()
	local bus    = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn_1 = bus:connect()
	local conn_2 = bus:connect()

	local sub_1 = conn_1:subscribe({ "multi", "topic" })
	local sub_2 = conn_2:subscribe({ "multi", "topic" })

	conn_1:publish(new_msg({ "multi", "topic" }, "Hello"))

	local m1 = assert(sub_1:next_msg())
	local m2 = assert(sub_2:next_msg())

	assert(m1.payload == "Hello")
	assert(m2.payload == "Hello")

	print("Multiple subscribers test passed!")
end

-- Test Multiple Topics
local function test_multi_topics()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	local sub_A = conn:subscribe({ "topic", "A" })
	local sub_B = conn:subscribe({ "topic", "B" })

	conn:publish(new_msg({ "topic", "A" }, "MessageA"))

	local mA, eA = sub_A:next_msg()
	assert(mA and mA.payload == "MessageA" and eA == nil)

	local mB, eB = sub_B:next_msg(TMO)
	assert_timeout(mB, eB)

	print("Multiple topics test passed!")
end

-- Test Clean Subscription (no replay for non-retained)
local function test_clean_sub()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	conn:publish(new_msg({ "clean", "topic" }, "OldMessage"))
	local sub = conn:subscribe({ "clean", "topic" })

	local m, e = sub:next_msg(TMO)
	assert_timeout(m, e)

	print("Clean subscription test passed!")
end

-- Test conn Cleanup (disconnect removes subs from pubsub trie)
local function test_conn_clean()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	local sub = conn:subscribe({ "cleanup", "topic" })

	conn:publish(new_msg({ "cleanup", "topic" }, "CleanupTest"))
	local m = assert(sub:next_msg())
	assert(m.payload == "CleanupTest")

	conn:disconnect()

	-- Internal structure is now _topics (pubsub trie).
	local bucket = bus._topics:retrieve({ "cleanup", "topic" })
	assert(bucket == nil or bucket.subs == nil or #bucket.subs == 0, "Subscription was not cleaned up")

	print("conn cleanup test passed!")
end

-- Retained Message Test
local function test_retained_msg()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	conn:publish(new_msg({ "retained", "topic" }, "RetainedMessage", { retained = true }))

	local sub = conn:subscribe({ "retained", "topic" })
	local msg, err = sub:next_msg()
	assert(msg and msg.payload == "RetainedMessage" and err == nil)

	print("Retained message test passed!")
end

-- Unsubscribe Test
local function test_unsubscribe()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	local subscription = conn:subscribe({ "unsubscribe", "topic" })
	subscription:unsubscribe()

	conn:publish(new_msg({ "unsubscribe", "topic" }, "NoReceive"))

	-- Unsubscribe closes the subscription; next_msg should complete promptly with a non-nil err.
	local m, e = subscription:next_msg(TMO)
	assert(m == nil, 'expected no message after unsubscribe')
	assert(e ~= nil and e ~= 'timeout', 'expected closed/unsubscribed error rather than timeout')

	print("Unsubscribe test passed!")
end

-- Queue Overflow Test (best-effort drop when buffer full)
local function test_q_overflow()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+", q_length = 10 })
	local conn = bus:connect()

	local sub = conn:subscribe({ "overflow", "topic" })

	for i = 1, 11 do
		conn:publish(new_msg({ "overflow", "topic" }, "Message" .. i))
	end

	for i = 1, 10 do
		local msg = assert(sub:next_msg())
		assert(msg.payload == "Message" .. i)
	end

	local m, e = sub:next_msg(TMO)
	assert_timeout(m, e)

	print("Queue overflow test passed!")
end

-- Wildcard Test
local function test_wildcard()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	local match_subs = {
		{ "wild", "cards", "are", "fun" },
		{ "wild", "cards", "are", "+" },
		{ "wild", "+",     "are", "fun" },
		{ "wild", "+",     "are", "#" },
		{ "wild", "+",     "#" },
		{ "#" },
	}

	local nonmatch_subs = {
		{ "wild", "cards", "are", "funny" },     -- literal mismatch
		{ "wild", "cards", "are", "+", "fun" },  -- length mismatch (no MW)
		{ "wild", "+", "+" },                    -- length mismatch
		{ "tame", "#" },                         -- topic root mismatch
	}

	local match = {}
	for _, pat in ipairs(match_subs) do
		match[#match + 1] = conn:subscribe(pat)
	end

	local nonmatch = {}
	for _, pat in ipairs(nonmatch_subs) do
		nonmatch[#nonmatch + 1] = conn:subscribe(pat)
	end

	conn:publish(new_msg({ "wild", "cards", "are", "fun" }, "payload"))

	for _, sub in ipairs(match) do
		local m, e = sub:next_msg()
		assert(m and m.payload == "payload" and e == nil)
	end

	for _, sub in ipairs(nonmatch) do
		local m, e = sub:next_msg(TMO)
		assert_timeout(m, e)
	end

	print("Wildcard test passed!")
end

-- Test Request Reply (multi-reply via request_sub / request alias)
local function test_request_sub()
	local bus = Bus.new({ m_wild = "#", s_wild = "+" })

	fibers.spawn(function ()
		local conn   = bus:connect()
		local helper = conn:subscribe({ "helpme" })
		local rec    = assert(helper:next_msg())
		conn:publish(new_msg(rec.reply_to, "Sure " .. tostring(rec.payload)))
	end)

	fibers.spawn(function ()
		local conn   = bus:connect()
		local helper = conn:subscribe({ "helpme" })
		local rec    = assert(helper:next_msg())
		Sleep.sleep(0.1)
		conn:publish(new_msg(rec.reply_to, "No problem " .. tostring(rec.payload)))
	end)

	Sleep.sleep(0.05)

	local conn3 = bus:connect()

	-- request is now an alias of request_sub and returns a Subscription.
	local sub = conn3:request(new_msg({ "helpme" }, "John"))

	local msg1, err1 = sub:next_msg(0.5)
	assert(err1 == nil and msg1 and msg1.payload == "Sure John")

	local msg2, err2 = sub:next_msg(0.5)
	assert(err2 == nil and msg2 and msg2.payload == "No problem John")

	sub:unsubscribe()

	print("Request_sub (multi-reply) test passed!")
end

-- Test Request Once (single reply, first reply wins, auto-cleanup)
local function test_request_once()
	local bus = Bus.new({ m_wild = "#", s_wild = "+" })

	-- Fast responder
	fibers.spawn(function ()
		local conn   = bus:connect()
		local helper = conn:subscribe({ "helpme" })
		local rec    = assert(helper:next_msg())
		conn:publish(new_msg(rec.reply_to, "Sure " .. tostring(rec.payload)))
	end)

	-- Slow responder (should lose)
	fibers.spawn(function ()
		local conn   = bus:connect()
		local helper = conn:subscribe({ "helpme" })
		local rec    = assert(helper:next_msg())
		Sleep.sleep(0.1)
		conn:publish(new_msg(rec.reply_to, "No problem " .. tostring(rec.payload)))
	end)

	Sleep.sleep(0.05)

	local conn3 = bus:connect()

	local reply, err = conn3:request_once(new_msg({ "helpme" }, "John"), { timeout = 0.5 })
	assert(err == nil, tostring(err))
	assert(reply and reply.payload == "Sure John")

	print("Request_once (single reply) test passed!")
end

-- Test Request Once Timeout (no responders)
local function test_request_once_timeout()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	local reply, err = conn:request_once(new_msg({ "noonehome" }, "John"), { timeout = 0.05 })
	assert(reply == nil)
	assert(err == "timeout", ('expected timeout, got %s'):format(tostring(err)))

	print("Request_once timeout test passed!")
end

fibers.run(function ()
	test_simple()
	test_multi_sub()
	test_multi_topics()
	test_clean_sub()
	test_conn_clean()
	test_unsubscribe()
	test_retained_msg()
	test_q_overflow()
	test_wildcard()
	test_request_sub()
	test_request_once()
	test_request_once_timeout()
	print("ALL TESTS PASSED!")
end)
