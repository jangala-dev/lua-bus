package.path = "../src/?.lua;" .. package.path

local fibers  = require 'fibers'
local Sleep   = require 'fibers.sleep'
local Bus     = require 'bus'
local Cond    = require 'fibers.cond'
local Channel = require 'fibers.channel'

require('fibers.scope').set_debug(true)

-- Use a slightly non-trivial timeout to avoid flakiness on slower systems.
local TMO      = 0.05
local LONG_TMO = 0.5

local function assert_timeout(v, err)
	assert(v == nil, 'expected nil value on timeout')
	assert(err == 'timeout', ('expected timeout, got %s'):format(tostring(err)))
end

-- A standard “deadline arm”: returns (nil, 'timeout').
local function timeout_op(dt)
	return Sleep.sleep_op(dt):wrap(function ()
		return nil, 'timeout'
	end)
end

-- Named-choice convenience: returns (winner_name, ...arm_results...)
local function select_named(arms)
	return fibers.perform(fibers.named_choice(arms))
end

--------------------------------------------------------------------------------
-- Test Simple PubSub (direct op performance)
--------------------------------------------------------------------------------

local function test_simple()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()
	local sub  = conn:subscribe({ "simple", "topic" })

	conn:publish({ "simple", "topic" }, "Hello")

	local msg, err = fibers.perform(sub:next_msg_op())
	assert(msg and msg.payload == "Hello" and err == nil)

	print("Simple test passed!")
end

--------------------------------------------------------------------------------
-- Test Selecting Across Subscriptions (fan-in / select)
--------------------------------------------------------------------------------

local function test_select_across_subs()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	local sub_A = conn:subscribe({ "select", "A" })
	local sub_B = conn:subscribe({ "select", "B" })

	conn:publish({ "select", "B" }, "MessageB")

	local which, msg, err = select_named({
		A        = sub_A:next_msg_op(),
		B        = sub_B:next_msg_op(),
		deadline = timeout_op(LONG_TMO),
	})

	assert(which == 'B', ('expected B to win, got %s'):format(tostring(which)))
	assert(msg and msg.payload == "MessageB" and err == nil)

	print("Select across subscriptions test passed!")
end

--------------------------------------------------------------------------------
-- Test Absence as a Race (no baked-in timeouts)
--------------------------------------------------------------------------------

local function test_absence_via_deadline()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	local sub = conn:subscribe({ "absent", "topic" })

	local which, msg, err = select_named({
		msg      = sub:next_msg_op(),
		deadline = timeout_op(TMO),
	})

	assert(which == 'deadline', ('expected deadline to win, got %s'):format(tostring(which)))
	assert_timeout(msg, err)

	print("Absence via deadline race test passed!")
end

--------------------------------------------------------------------------------
-- Test Graceful Consumer Stop (external control op + acknowledgements)
--------------------------------------------------------------------------------

local function test_graceful_stop_signal()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()
	local sub  = conn:subscribe({ "loop", "topic" })

	local stop = Cond.new()

	-- Consumer acks each processed message so the test can be deterministic.
	local ack  = Channel.new(10)
	local done = Channel.new(1)

	fibers.spawn(function ()
		local out = {}

		while true do
			local which, msg, err = select_named({
				msg  = sub:next_msg_op(),
				stop = stop:wait_op():wrap(function () return nil, 'stopped' end),
			})

			if which == 'stop' then
				break
			end

			assert(err == nil, tostring(err))
			out[#out + 1] = msg.payload

			-- Ack should not block in the test.
			ack:put(msg.payload)
		end

		done:put(out)
	end)

	-- Publish three messages.
	conn:publish({ "loop", "topic" }, "one")
	conn:publish({ "loop", "topic" }, "two")
	conn:publish({ "loop", "topic" }, "three")

	-- Wait for three acks (each is itself a race).
	local expect = { "one", "two", "three" }
	for i = 1, #expect do
		local which, v, err = select_named({
			ack      = ack:get_op():wrap(function (x) return x, nil end),
			deadline = timeout_op(LONG_TMO),
		})
		assert(which == 'ack', ('expected ack to win, got %s'):format(tostring(which)))
		assert(err == nil)
		assert(v == expect[i], ('expected ack %q, got %q'):format(tostring(expect[i]), tostring(v)))
	end

	-- Now request stop, and ensure the consumer terminates promptly.
	stop:signal()

	local which, out, err = select_named({
		done     = done:get_op():wrap(function (v) return v, nil end),
		deadline = timeout_op(LONG_TMO),
	})

	assert(which == 'done', ('expected done to win, got %s'):format(tostring(which)))
	assert(err == nil)
	assert(type(out) == 'table')
	assert(#out == 3 and out[1] == 'one' and out[2] == 'two' and out[3] == 'three')

	print("Graceful stop signal test passed!")
end

--------------------------------------------------------------------------------
-- Test Connection Cleanup
--  - disconnect closes subscriptions
--  - publish on a disconnected connection errors
--------------------------------------------------------------------------------

local function test_conn_clean()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	local sub = conn:subscribe({ "cleanup", "topic" })

	conn:publish({ "cleanup", "topic" }, "CleanupTest")
	local msg, err = fibers.perform(sub:next_msg_op())
	assert(err == nil and msg and msg.payload == "CleanupTest")

	conn:disconnect()

	-- Subscription should close promptly with reason "disconnected".
	local which, m2, e2 = select_named({
		msg      = sub:next_msg_op(),
		deadline = timeout_op(LONG_TMO),
	})
	assert(which == 'msg', ('expected closed msg arm to win, got %s'):format(tostring(which)))
	assert(m2 == nil)
	assert(e2 == 'disconnected', ('expected disconnected, got %s'):format(tostring(e2)))

	-- Disconnected connections must not be usable.
	local ok = pcall(function ()
		conn:publish({ "cleanup", "topic" }, "should error")
	end)
	assert(not ok, 'expected publish to error after disconnect')

	print("Connection cleanup test passed!")
end

--------------------------------------------------------------------------------
-- Retained Message Test
--------------------------------------------------------------------------------

local function test_retained_msg()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	conn:retain({ "retained", "topic" }, "RetainedMessage")

	local sub = conn:subscribe({ "retained", "topic" })
	local msg, err = fibers.perform(sub:next_msg_op())
	assert(msg and msg.payload == "RetainedMessage" and err == nil)

	print("Retained message test passed!")
end

--------------------------------------------------------------------------------
-- Unsubscribe Test (close wins immediately; no deadline required)
--------------------------------------------------------------------------------

local function test_unsubscribe()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	local subscription = conn:subscribe({ "unsubscribe", "topic" })
	subscription:unsubscribe()

	conn:publish({ "unsubscribe", "topic" }, "NoReceive")

	local m, e = subscription:next_msg()
	assert(m == nil, 'expected no message after unsubscribe')
	assert(e == 'unsubscribed', ('expected "unsubscribed", got %s'):format(tostring(e)))

	print("Unsubscribe test passed!")
end

--------------------------------------------------------------------------------
-- Mailbox policy surfaces through the bus:
--  - default policy is drop_oldest
--  - per-subscription override to drop_newest
--------------------------------------------------------------------------------

local function test_q_overflow_drop_oldest_default()
	-- Default bus/sub policy: drop_oldest.
	local bus  = Bus.new({ m_wild = "#", s_wild = "+", q_length = 10 })
	local conn = bus:connect()
	local sub  = conn:subscribe({ "overflow", "oldest" })

	for i = 1, 11 do
		conn:publish({ "overflow", "oldest" }, "Message" .. i)
	end

	-- With drop_oldest, the oldest message ("Message1") is dropped; we should see 2..11.
	for i = 2, 11 do
		local msg, err = fibers.perform(sub:next_msg_op())
		assert(err == nil and msg)
		assert(msg.payload == ("Message" .. i), ('expected %q, got %q'):format("Message" .. i, tostring(msg.payload)))
	end

	-- Demonstrate “absence is a race”.
	local which, msg, err = select_named({
		msg      = sub:next_msg_op(),
		deadline = timeout_op(TMO),
	})
	assert(which == 'deadline', ('expected deadline to win, got %s'):format(tostring(which)))
	assert_timeout(msg, err)

	-- Drop counters should reflect one drop.
	assert(sub:dropped() == 1, ('expected 1 drop, got %d'):format(sub:dropped()))
	assert(conn:dropped() == 1, ('expected conn 1 drop, got %d'):format(conn:dropped()))
	assert(bus:stats().dropped == 1, ('expected bus 1 drop, got %d'):format(bus:stats().dropped))

	print("Queue overflow (drop_oldest default) test passed!")
end

local function test_q_overflow_drop_newest_override()
	-- Per-subscription override: drop_newest.
	local bus  = Bus.new({ m_wild = "#", s_wild = "+", q_length = 10 })
	local conn = bus:connect()
	local sub  = conn:subscribe({ "overflow", "newest" }, nil, 'drop_newest')

	for i = 1, 11 do
		conn:publish({ "overflow", "newest" }, "Message" .. i)
	end

	-- With drop_newest, the newest message ("Message11") is dropped; we should see 1..10.
	for i = 1, 10 do
		local msg, err = fibers.perform(sub:next_msg_op())
		assert(err == nil and msg)
		assert(msg.payload == ("Message" .. i), ('expected %q, got %q'):format("Message" .. i, tostring(msg.payload)))
	end

	local which, msg, err = select_named({
		msg      = sub:next_msg_op(),
		deadline = timeout_op(TMO),
	})
	assert(which == 'deadline', ('expected deadline to win, got %s'):format(tostring(which)))
	assert_timeout(msg, err)

	assert(sub:dropped() == 1, ('expected 1 drop, got %d'):format(sub:dropped()))
	assert(conn:dropped() == 1, ('expected conn 1 drop, got %d'):format(conn:dropped()))
	assert(bus:stats().dropped == 1, ('expected bus 1 drop, got %d'):format(bus:stats().dropped))

	print("Queue overflow (drop_newest override) test passed!")
end

--------------------------------------------------------------------------------
-- Zero-length subscription queues are still bounded (non-blocking delivery):
-- publishing when no receiver is waiting should drop under a drop policy.
--------------------------------------------------------------------------------

local function test_zero_len_subscription_is_bounded()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	-- Rendezvous mailbox, but explicitly non-blocking policy.
	local sub = conn:subscribe({ "zero", "len" }, 0, 'drop_newest')

	conn:publish({ "zero", "len" }, "Hello")

	-- No receiver was waiting at publish time; message should be dropped.
	local which, msg, err = select_named({
		msg      = sub:next_msg_op(),
		deadline = timeout_op(TMO),
	})
	assert(which == 'deadline', ('expected deadline to win, got %s'):format(tostring(which)))
	assert_timeout(msg, err)

	assert(sub:dropped() == 1, ('expected 1 drop, got %d'):format(sub:dropped()))

	print("Zero-length subscription is bounded test passed!")
end

--------------------------------------------------------------------------------
-- Bus must reject blocking delivery policy explicitly.
--------------------------------------------------------------------------------

local function test_reject_block_policy()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	local ok = pcall(function ()
		conn:subscribe({ "no", "blocking" }, 1, 'block')
	end)

	assert(not ok, 'expected subscribe(..., "block") to error')
	print("Reject block policy test passed!")
end

--------------------------------------------------------------------------------
-- Wildcard Test (positive matches receive; non-matches demonstrate racing)
--------------------------------------------------------------------------------

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

	conn:publish({ "wild", "cards", "are", "fun" }, "payload")

	for _, sub in ipairs(match) do
		local m, e = fibers.perform(sub:next_msg_op())
		assert(m and m.payload == "payload" and e == nil)
	end

	for _, sub in ipairs(nonmatch) do
		local which, m, e = select_named({
			msg      = sub:next_msg_op(),
			deadline = timeout_op(TMO),
		})
		assert(which == 'deadline', ('expected deadline to win, got %s'):format(tostring(which)))
		assert_timeout(m, e)
	end

	print("Wildcard test passed!")
end

--------------------------------------------------------------------------------
-- Request_sub (multi-reply) with “read until deadline” policy
--------------------------------------------------------------------------------

local function test_request_sub()
	local bus = Bus.new({ m_wild = "#", s_wild = "+" })

	fibers.spawn(function ()
		local conn   = bus:connect()
		local helper = conn:subscribe({ "helpme" })
		local rec    = assert(helper:next_msg())
		conn:publish(rec.reply_to, "Sure " .. tostring(rec.payload))
	end)

	fibers.spawn(function ()
		local conn   = bus:connect()
		local helper = conn:subscribe({ "helpme" })
		local rec    = assert(helper:next_msg())
		Sleep.sleep(0.1)
		conn:publish(rec.reply_to, "No problem " .. tostring(rec.payload))
	end)

	Sleep.sleep(0.05)

	local conn3 = bus:connect()
	local sub   = conn3:request_sub({ "helpme" }, "John")

	do
		local which, msg, err = select_named({
			reply    = sub:next_msg_op(),
			deadline = timeout_op(LONG_TMO),
		})
		assert(which == 'reply', ('expected reply to win, got %s'):format(tostring(which)))
		assert(err == nil and msg and msg.payload == "Sure John")
	end

	do
		local which, msg, err = select_named({
			reply    = sub:next_msg_op(),
			deadline = timeout_op(LONG_TMO),
		})
		assert(which == 'reply', ('expected reply to win, got %s'):format(tostring(which)))
		assert(err == nil and msg and msg.payload == "No problem John")
	end

	-- Stop reading after a deadline.
	do
		local which, msg, err = select_named({
			reply    = sub:next_msg_op(),
			deadline = timeout_op(TMO),
		})
		assert(which == 'deadline', ('expected deadline to win, got %s'):format(tostring(which)))
		assert_timeout(msg, err)
	end

	sub:unsubscribe()

	print("Request_sub (multi-reply) test passed!")
end

--------------------------------------------------------------------------------
-- Request_once as a plain Op (caller composes policy)
--------------------------------------------------------------------------------

local function test_request_once_composed()
	local bus = Bus.new({ m_wild = "#", s_wild = "+" })

	-- Fast responder.
	fibers.spawn(function ()
		local conn   = bus:connect()
		local helper = conn:subscribe({ "helpme" })
		local rec    = assert(helper:next_msg())
		conn:publish(rec.reply_to, "Sure " .. tostring(rec.payload))
	end)

	-- Slow responder (should lose).
	fibers.spawn(function ()
		local conn   = bus:connect()
		local helper = conn:subscribe({ "helpme" })
		local rec    = assert(helper:next_msg())
		Sleep.sleep(0.1)
		conn:publish(rec.reply_to, "No problem " .. tostring(rec.payload))
	end)

	Sleep.sleep(0.05)

	local conn3 = bus:connect()

	local which, reply, err = select_named({
		reply    = conn3:request_once_op({ "helpme" }, "John"),
		deadline = timeout_op(LONG_TMO),
	})

	assert(which == 'reply', ('expected reply to win, got %s'):format(tostring(which)))
	assert(err == nil, tostring(err))
	assert(reply and reply.payload == "Sure John")

	print("Request_once (reply wins) test passed!")
end

local function test_request_once_deadline_no_responder()
	local bus  = Bus.new({ m_wild = "#", s_wild = "+" })
	local conn = bus:connect()

	local which, reply, err = select_named({
		reply    = conn:request_once_op({ "noonehome" }, "John"),
		deadline = timeout_op(TMO),
	})

	assert(which == 'deadline', ('expected deadline to win, got %s'):format(tostring(which)))
	assert_timeout(reply, err)

	print("Request_once (deadline wins) test passed!")
end

--------------------------------------------------------------------------------
-- Scope cancellation as control flow (no local exception handling)
--------------------------------------------------------------------------------

local function test_scope_cancellation_terminates_waits()
	local bus = Bus.new({ m_wild = "#", s_wild = "+" })

	local st, _, why = fibers.run_scope(function (s)
		local conn = bus:connect()

		s:spawn(function ()
			local sub = conn:subscribe({ "cancel", "topic" })
			-- This should be interrupted by scope cancellation.
			fibers.perform(sub:next_msg_op())
		end)

		Sleep.sleep(TMO)
		s:cancel('test cancel')
	end)

	assert(st == 'cancelled', ('expected cancelled, got %s'):format(tostring(st)))
	assert(tostring(why) == 'test cancel')

	print("Scope cancellation terminates waits test passed!")
end

--------------------------------------------------------------------------------
-- Run
--------------------------------------------------------------------------------

fibers.run(function ()
	test_simple()
	test_select_across_subs()
	test_absence_via_deadline()
	test_graceful_stop_signal()
	test_conn_clean()
	test_unsubscribe()
	test_retained_msg()
	test_q_overflow_drop_oldest_default()
	test_q_overflow_drop_newest_override()
	test_zero_len_subscription_is_bounded()
	test_reject_block_policy()
	test_wildcard()
	test_request_sub()
	test_request_once_composed()
	test_request_once_deadline_no_responder()
	test_scope_cancellation_terminates_waits()
	print("ALL TESTS PASSED!")
end)
