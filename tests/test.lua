package.path = '../src/?.lua;' .. package.path

local fibers  = require 'fibers'
local Sleep   = require 'fibers.sleep'
local Bus     = require 'bus'
local Cond    = require 'fibers.cond'
local Channel = require 'fibers.channel'
local Scope   = require 'fibers.scope'

Scope.set_debug(true)

-- Use a slightly non-trivial timeout to avoid flakiness on slower systems.
local TMO      = 0.05
local LONG_TMO = 0.5

local function assert_timeout(v, err)
	assert(v == nil, 'expected nil value on timeout')
	assert(err == 'timeout', ('expected timeout, got %s'):format(tostring(err)))
end

local function assert_eq(a, b, msg)
	if a ~= b then
		error((msg or 'assert_eq failed') .. (': expected ' .. tostring(b) .. ', got ' .. tostring(a)), 2)
	end
end

local function assert_in_set(v, set, msg)
	if not set[v] then
		error((msg or 'value not in set') .. (': got ' .. tostring(v)), 2)
	end
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

-- Reply helper for mixed modalities:
-- - If reply_to is a Lane B endpoint, publish_one will deliver.
-- - If reply_to is a Lane A subscription topic, publish_one returns no_route; fall back to publish.
local function reply_best_effort(conn, reply_to, payload, opts)
	if not reply_to then return true end
	opts = opts or {}

	local ok, reason = conn:publish_one(reply_to, payload, opts)
	if ok then return true end

	if reason == 'no_route' then
		conn:publish(reply_to, payload, opts)
		return true
	end

	-- For tests: surface other failures explicitly.
	return false, reason
end

--------------------------------------------------------------------------------
-- Test Simple PubSub (direct op performance)
--------------------------------------------------------------------------------

local function test_simple()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()
	local sub  = conn:subscribe({ 'simple', 'topic' })

	conn:publish({ 'simple', 'topic' }, 'Hello')

	local msg, err = fibers.perform(sub:recv_op())
	assert(msg and msg.payload == 'Hello' and err == nil)

	print('Simple test passed!')
end

--------------------------------------------------------------------------------
-- Test Selecting Across Subscriptions (fan-in / select)
--------------------------------------------------------------------------------

local function test_select_across_subs()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local sub_A = conn:subscribe({ 'select', 'A' })
	local sub_B = conn:subscribe({ 'select', 'B' })

	conn:publish({ 'select', 'B' }, 'MessageB')

	local which, msg, err = select_named({
		A        = sub_A:recv_op(),
		B        = sub_B:recv_op(),
		deadline = timeout_op(LONG_TMO),
	})

	assert(which == 'B', ('expected B to win, got %s'):format(tostring(which)))
	assert(msg and msg.payload == 'MessageB' and err == nil)

	print('Select across subscriptions test passed!')
end

--------------------------------------------------------------------------------
-- Test Absence as a Race (no baked-in timeouts)
--------------------------------------------------------------------------------

local function test_absence_via_deadline()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local sub = conn:subscribe({ 'absent', 'topic' })

	local which, msg, err = select_named({
		msg      = sub:recv_op(),
		deadline = timeout_op(TMO),
	})

	assert(which == 'deadline', ('expected deadline to win, got %s'):format(tostring(which)))
	assert_timeout(msg, err)

	print('Absence via deadline race test passed!')
end

--------------------------------------------------------------------------------
-- Test Graceful Consumer Stop (external control op + acknowledgements)
--------------------------------------------------------------------------------

local function test_graceful_stop_signal()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()
	local sub  = conn:subscribe({ 'loop', 'topic' })

	local stop = Cond.new()

	-- Consumer acks each processed message so the test can be deterministic.
	local ack  = Channel.new(10)
	local done = Channel.new(1)

	fibers.spawn(function ()
		local out = {}

		while true do
			local which, msg, err = select_named({
				msg  = sub:recv_op(),
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
	conn:publish({ 'loop', 'topic' }, 'one')
	conn:publish({ 'loop', 'topic' }, 'two')
	conn:publish({ 'loop', 'topic' }, 'three')

	-- Wait for three acks (each is itself a race).
	local expect = { 'one', 'two', 'three' }
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

	print('Graceful stop signal test passed!')
end

--------------------------------------------------------------------------------
-- Test Connection Cleanup
--------------------------------------------------------------------------------

local function test_conn_clean()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local sub = conn:subscribe({ 'cleanup', 'topic' })

	conn:publish({ 'cleanup', 'topic' }, 'CleanupTest')
	local msg, err = fibers.perform(sub:recv_op())
	assert(err == nil and msg and msg.payload == 'CleanupTest')

	conn:disconnect()

	-- Subscription should close promptly with reason "disconnected".
	local which, m2, e2 = select_named({
		msg      = sub:recv_op(),
		deadline = timeout_op(LONG_TMO),
	})
	assert(which == 'msg', ('expected closed msg arm to win, got %s'):format(tostring(which)))
	assert(m2 == nil)
	assert(e2 == 'disconnected', ('expected disconnected, got %s'):format(tostring(e2)))

	-- Disconnected connections must not be usable.
	local ok = pcall(function ()
		conn:publish({ 'cleanup', 'topic' }, 'should error')
	end)
	assert(not ok, 'expected publish to error after disconnect')

	print('Connection cleanup test passed!')
end

--------------------------------------------------------------------------------
-- Retained Message Tests
--------------------------------------------------------------------------------

local function test_retained_msg_basic()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	conn:retain({ 'retained', 'topic' }, 'RetainedMessage')

	local sub = conn:subscribe({ 'retained', 'topic' })
	local msg, err = fibers.perform(sub:recv_op())
	assert(msg and msg.payload == 'RetainedMessage' and err == nil)

	print('Retained message (basic) test passed!')
end

local function test_retained_query_wildcards_and_unretain()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	conn:retain({ 'ret', 'a' }, 'A')
	conn:retain({ 'ret', 'b' }, 'B')
	conn:retain({ 'ret', 'c', 'd' }, 'CD')

	local sub = conn:subscribe({ 'ret', '#' }, { queue_len = 10, full = 'drop_oldest' })

	local got = {}
	for _ = 1, 3 do
		local which, msg, err = select_named({
			msg      = sub:recv_op(),
			deadline = timeout_op(LONG_TMO),
		})
		assert_eq(which, 'msg', 'expected retained replay')
		assert(err == nil and msg, tostring(err))
		got[msg.payload] = true
	end

	assert_in_set('A', got, 'missing retained A')
	assert_in_set('B', got, 'missing retained B')
	assert_in_set('CD', got, 'missing retained CD')

	conn:unretain({ 'ret', 'b' })

	local sub2 = conn:subscribe({ 'ret', '#' }, { queue_len = 10, full = 'drop_oldest' })

	local got2 = {}
	for _ = 1, 2 do
		local which, msg, err = select_named({
			msg      = sub2:recv_op(),
			deadline = timeout_op(LONG_TMO),
		})
		assert_eq(which, 'msg', 'expected retained replay after unretain')
		assert(err == nil and msg, tostring(err))
		got2[msg.payload] = true
	end

	assert(got2.A and got2.CD, 'expected A and CD retained after unretain(b)')

	local which, msg, err = select_named({
		msg      = sub2:recv_op(),
		deadline = timeout_op(TMO),
	})
	assert_eq(which, 'deadline', 'expected no third retained message after unretain')
	assert_timeout(msg, err)

	print('Retained message (wildcards + unretain) test passed!')
end

--------------------------------------------------------------------------------
-- Unsubscribe Test
--------------------------------------------------------------------------------

local function test_unsubscribe()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local subscription = conn:subscribe({ 'unsubscribe', 'topic' })
	subscription:unsubscribe()

	conn:publish({ 'unsubscribe', 'topic' }, 'NoReceive')

	local m, e = subscription:recv()
	assert(m == nil, 'expected no message after unsubscribe')
	assert(e == 'unsubscribed', ('expected "unsubscribed", got %s'):format(tostring(e)))

	print('Unsubscribe test passed!')
end

--------------------------------------------------------------------------------
-- Queue policies
--------------------------------------------------------------------------------

local function test_q_overflow_drop_oldest_default()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+', q_length = 10 })
	local conn = bus:connect()
	local sub  = conn:subscribe({ 'overflow', 'oldest' })

	for i = 1, 11 do
		conn:publish({ 'overflow', 'oldest' }, 'Message' .. i)
	end

	for i = 2, 11 do
		local msg, err = fibers.perform(sub:recv_op())
		assert(err == nil and msg)
		assert(msg.payload == ('Message' .. i), ('expected %q, got %q'):format('Message' .. i, tostring(msg.payload)))
	end

	local which, msg, err = select_named({
		msg      = sub:recv_op(),
		deadline = timeout_op(TMO),
	})
	assert_eq(which, 'deadline')
	assert_timeout(msg, err)

	assert_eq(sub:dropped(), 1)
	assert_eq(conn:dropped(), 1)
	assert_eq(bus:stats().dropped, 1)

	print('Queue overflow (drop_oldest default) test passed!')
end

local function test_q_overflow_reject_newest_override()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+', q_length = 10 })
	local conn = bus:connect()
	local sub  = conn:subscribe({ 'overflow', 'newest' }, { full = 'reject_newest' })

	for i = 1, 11 do
		conn:publish({ 'overflow', 'newest' }, 'Message' .. i)
	end

	for i = 1, 10 do
		local msg, err = fibers.perform(sub:recv_op())
		assert(err == nil and msg)
		assert(msg.payload == ('Message' .. i), ('expected %q, got %q'):format('Message' .. i, tostring(msg.payload)))
	end

	local which, msg, err = select_named({
		msg      = sub:recv_op(),
		deadline = timeout_op(TMO),
	})
	assert_eq(which, 'deadline')
	assert_timeout(msg, err)

	assert_eq(sub:dropped(), 1)
	assert_eq(conn:dropped(), 1)
	assert_eq(bus:stats().dropped, 1)

	print('Queue overflow (reject_newest override) test passed!')
end

local function test_zero_len_subscription_is_bounded()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local sub = conn:subscribe({ 'zero', 'len' }, { queue_len = 0, full = 'reject_newest' })

	conn:publish({ 'zero', 'len' }, 'Hello')

	local which, msg, err = select_named({
		msg      = sub:recv_op(),
		deadline = timeout_op(TMO),
	})
	assert_eq(which, 'deadline')
	assert_timeout(msg, err)

	assert_eq(sub:dropped(), 1)

	print('Zero-length subscription is bounded test passed!')
end

local function test_reject_block_policy()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local ok = pcall(function ()
		conn:subscribe({ 'no', 'blocking' }, { queue_len = 1, full = 'block' })
	end)

	assert(not ok, 'expected subscribe(..., { full = "block" }) to error')
	print('Reject block policy test passed!')
end

--------------------------------------------------------------------------------
-- Wildcards and literal wrapper modalities
--------------------------------------------------------------------------------

local function test_wildcard()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local match_subs = {
		{ 'wild', 'cards', 'are', 'fun' },
		{ 'wild', 'cards', 'are', '+' },
		{ 'wild', '+',     'are', 'fun' },
		{ 'wild', '+',     'are', '#' },
		{ 'wild', '+',     '#' },
		{ '#' },
	}

	local nonmatch_subs = {
		{ 'wild', 'cards', 'are', 'funny' },
		{ 'wild', 'cards', 'are', '+',    'fun' },
		{ 'wild', '+',     '+' },
		{ 'tame', '#' },
	}

	local match = {}
	for _, pat in ipairs(match_subs) do
		match[#match + 1] = conn:subscribe(pat)
	end

	local nonmatch = {}
	for _, pat in ipairs(nonmatch_subs) do
		nonmatch[#nonmatch + 1] = conn:subscribe(pat)
	end

	conn:publish({ 'wild', 'cards', 'are', 'fun' }, 'payload')

	for _, sub in ipairs(match) do
		local m, e = fibers.perform(sub:recv_op())
		assert(m and m.payload == 'payload' and e == nil)
	end

	for _, sub in ipairs(nonmatch) do
		local which, m, e = select_named({
			msg      = sub:recv_op(),
			deadline = timeout_op(TMO),
		})
		assert_eq(which, 'deadline')
		assert_timeout(m, e)
	end

	print('Wildcard test passed!')
end

local function test_literal_tokens_in_pubsub()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local sub_wild = conn:subscribe({ 'metrics', '+' }, { queue_len = 10 })
	local sub_lit_plus = conn:subscribe({ 'metrics', Bus.literal('+') }, { queue_len = 10 })
	local sub_lit_hash_mid = conn:subscribe({ 'lit', Bus.literal('#'), 'x' }, { queue_len = 10 })

	conn:publish({ 'metrics', '+' }, 'PLUS')
	conn:publish({ 'metrics', 'abc' }, 'ABC')
	conn:publish({ 'lit', '#', 'x' }, 'HASHMID')

	local got_wild = {}
	for _ = 1, 2 do
		local which, m, e = select_named({ msg = sub_wild:recv_op(), deadline = timeout_op(LONG_TMO) })
		assert_eq(which, 'msg')
		assert(e == nil and m)
		got_wild[m.payload] = true
	end
	assert(got_wild.PLUS and got_wild.ABC, 'wild sub should see both PLUS and ABC')

	local m1, e1 = fibers.perform(sub_lit_plus:recv_op())
	assert(e1 == nil and m1 and m1.payload == 'PLUS')

	local which, m2, e2 = select_named({ msg = sub_lit_plus:recv_op(), deadline = timeout_op(TMO) })
	assert_eq(which, 'deadline')
	assert_timeout(m2, e2)

	local m3, e3 = fibers.perform(sub_lit_hash_mid:recv_op())
	assert(e3 == nil and m3 and m3.payload == 'HASHMID')

	print('Literal token modalities (pubsub) test passed!')
end

local function test_multi_wildcard_must_be_last()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local ok = pcall(function ()
		conn:subscribe({ 'bad', '#', 'mid' })
	end)

	assert(not ok, 'expected multi wildcard not-last to error')
	print('Multi wildcard must be last test passed!')
end

--------------------------------------------------------------------------------
-- Lane A fan-out and independent drops across subscriptions
--------------------------------------------------------------------------------

local function test_laneA_fanout_independent_backpressure()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local sub1 = conn:subscribe({ 'fanout', 'topic' }, { queue_len = 1, full = 'reject_newest' })
	local sub2 = conn:subscribe({ 'fanout', 'topic' }, { queue_len = 1, full = 'reject_newest' })

	conn:publish({ 'fanout', 'topic' }, 'one')
	conn:publish({ 'fanout', 'topic' }, 'two')

	local m1, e1 = fibers.perform(sub1:recv_op())
	local m2, e2 = fibers.perform(sub2:recv_op())
	assert(e1 == nil and m1 and m1.payload == 'one')
	assert(e2 == nil and m2 and m2.payload == 'one')

	assert_eq(sub1:dropped(), 1)
	assert_eq(sub2:dropped(), 1)
	assert_eq(conn:dropped(), 2)
	assert_eq(bus:stats().dropped, 2)

	print('Lane A fan-out + independent backpressure test passed!')
end

--------------------------------------------------------------------------------
-- Lane B: endpoints, publish_one admission, and call
--------------------------------------------------------------------------------

local function test_laneB_publish_one_modalities()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	do
		local ok, reason = conn:publish_one({ 'svc', 'missing' }, 'x')
		assert_eq(ok, false)
		assert_eq(reason, 'no_route')
	end

	local ep = conn:bind({ 'svc', 'rzv' }, { queue_len = 0 })

	do
		local ok, reason = conn:publish_one({ 'svc', 'rzv' }, 'x')
		assert_eq(ok, false)
		assert_eq(reason, 'full')
		assert_eq(ep:dropped(), 1)
	end

	local got = Channel.new(1)
	fibers.spawn(function ()
		local msg, err = ep:recv()
		assert(msg, tostring(err))
		got:put(msg.payload)
	end)

	Sleep.sleep(TMO)

	do
		local ok, reason = conn:publish_one({ 'svc', 'rzv' }, 'hello')
		assert_eq(ok, true)
		assert(reason == nil)
	end

	do
		local which, v, err = select_named({
			got      = got:get_op():wrap(function (x) return x, nil end),
			deadline = timeout_op(LONG_TMO),
		})
		assert_eq(which, 'got')
		assert(err == nil)
		assert_eq(v, 'hello')
	end

	ep._tx:close('manual close')

	do
		local ok, reason = conn:publish_one({ 'svc', 'rzv' }, 'x')
		assert_eq(ok, false)
		assert_eq(reason, 'closed')
	end

	print('Lane B publish_one modalities test passed!')
end

local function test_laneB_concrete_topic_enforcement_and_literal_ok()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local ok1 = pcall(function ()
		conn:bind({ 'svc', '+' }, { queue_len = 1 })
	end)
	assert(not ok1, 'expected bind with wildcard to error')

	local ok2 = pcall(function ()
		conn:publish_one({ 'svc', '+' }, 'x')
	end)
	assert(not ok2, 'expected publish_one with wildcard topic to error')

	local ep = conn:bind({ 'svc', Bus.literal('+') }, { queue_len = 1 })
	assert(ep, 'expected bind with literal "+" to succeed')

	local ok3, reason3 = conn:publish_one({ 'svc', Bus.literal('+') }, 'ok')
	assert(ok3 == true and reason3 == nil)

	local msg, err = ep:recv()
	assert(err == nil and msg and msg.payload == 'ok')

	print('Lane B concrete-topic enforcement + literal wrapper test passed!')
end

-- Explicit modality check: publish() does not reach endpoints; publish_one() does.
local function test_laneB_endpoint_not_reached_by_publish()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local ep = conn:bind({ 'endpoint', 'only' }, { queue_len = 1 })

	conn:publish({ 'endpoint', 'only' }, 'x')

	local which, msg, err = select_named({
		msg      = ep:recv_op(),
		deadline = timeout_op(TMO),
	})
	assert_eq(which, 'deadline')
	assert_timeout(msg, err)

	local ok, reason = conn:publish_one({ 'endpoint', 'only' }, 'y')
	assert_eq(ok, true)
	assert(reason == nil)

	local m2, e2 = fibers.perform(ep:recv_op())
	assert(e2 == nil and m2 and m2.payload == 'y')

	print('Lane B endpoint not reached by publish test passed!')
end

local function test_laneB_call_success()
	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local server = bus:connect()
	local client = bus:connect()

	local ep = server:bind({ 'rpc', 'echo' }, { queue_len = 1 })

	fibers.spawn(function ()
		while true do
			local msg, err = ep:recv()
			if not msg then return end
			if msg.reply_to then
				local ok, why = reply_best_effort(server, msg.reply_to, 'reply:' .. tostring(msg.payload),
					{ id = msg.id })
				assert(ok, tostring(why))
			end
		end
	end)

	local reply, err = client:call({ 'rpc', 'echo' }, 'hi', { timeout = LONG_TMO })
	assert(err == nil, tostring(err))
	assert_eq(reply, 'reply:hi')
	ep:unbind()     -- closes the endpoint mailbox; recv() returns nil; fibre exits

	print('Lane B call (success) test passed!')
end

local function test_laneB_call_timeout_no_route()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local reply, err = conn:call({ 'rpc', 'nobody' }, 'hi', { timeout = TMO })
	assert(reply == nil)
	assert_eq(err, 'timeout')

	print('Lane B call (timeout no_route) test passed!')
end

--------------------------------------------------------------------------------
-- Request_sub (multi-reply) with “read until deadline” policy
--------------------------------------------------------------------------------

local function test_request_sub()
	local bus = Bus.new({ m_wild = '#', s_wild = '+' })

	fibers.spawn(function ()
		local conn     = bus:connect()
		local helper   = conn:subscribe({ 'helpme' })
		local rec, err = helper:recv()
		assert(rec, tostring(err))
		local ok, why = reply_best_effort(conn, rec.reply_to, 'Sure ' .. tostring(rec.payload), { id = rec.id })
		assert(ok, tostring(why))
	end)

	fibers.spawn(function ()
		local conn     = bus:connect()
		local helper   = conn:subscribe({ 'helpme' })
		local rec, err = helper:recv()
		assert(rec, tostring(err))
		Sleep.sleep(0.1)
		local ok, why = reply_best_effort(conn, rec.reply_to, 'No problem ' .. tostring(rec.payload), { id = rec.id })
		assert(ok, tostring(why))
	end)

	Sleep.sleep(0.05)

	local conn3 = bus:connect()
	local sub   = conn3:request_sub({ 'helpme' }, 'John')

	do
		local which, msg, err = select_named({
			reply    = sub:recv_op(),
			deadline = timeout_op(LONG_TMO),
		})
		assert_eq(which, 'reply')
		assert(err == nil and msg and msg.payload == 'Sure John')
	end

	do
		local which, msg, err = select_named({
			reply    = sub:recv_op(),
			deadline = timeout_op(LONG_TMO),
		})
		assert_eq(which, 'reply')
		assert(err == nil and msg and msg.payload == 'No problem John')
	end

	do
		local which, msg, err = select_named({
			reply    = sub:recv_op(),
			deadline = timeout_op(TMO),
		})
		assert_eq(which, 'deadline')
		assert_timeout(msg, err)
	end

	sub:unsubscribe()

	print('Request_sub (multi-reply) test passed!')
end

--------------------------------------------------------------------------------
-- Request_once as a plain Op (caller composes policy)
--------------------------------------------------------------------------------

local function test_request_once_composed()
	local bus = Bus.new({ m_wild = '#', s_wild = '+' })

	fibers.spawn(function ()
		local conn     = bus:connect()
		local helper   = conn:subscribe({ 'helpme' })
		local rec, err = helper:recv()
		assert(rec, tostring(err))
		local ok, why = reply_best_effort(conn, rec.reply_to, 'Sure ' .. tostring(rec.payload), { id = rec.id })
		assert(ok, tostring(why))
	end)

	fibers.spawn(function ()
		local conn     = bus:connect()
		local helper   = conn:subscribe({ 'helpme' })
		local rec, err = helper:recv()
		assert(rec, tostring(err))
		Sleep.sleep(0.1)
		local ok, why = reply_best_effort(conn, rec.reply_to, 'No problem ' .. tostring(rec.payload), { id = rec.id })
		assert(ok, tostring(why))
	end)

	Sleep.sleep(0.05)

	local conn3 = bus:connect()

	local which, reply, err = select_named({
		reply    = conn3:request_once_op({ 'helpme' }, 'John'),
		deadline = timeout_op(LONG_TMO),
	})

	assert_eq(which, 'reply')
	assert(err == nil, tostring(err))
	assert(reply and reply.payload == 'Sure John')

	print('Request_once (reply wins) test passed!')
end

local function test_request_once_deadline_no_responder()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local which, reply, err = select_named({
		reply    = conn:request_once_op({ 'noonehome' }, 'John'),
		deadline = timeout_op(TMO),
	})

	assert_eq(which, 'deadline')
	assert_timeout(reply, err)

	print('Request_once (deadline wins) test passed!')
end

--------------------------------------------------------------------------------
-- Scope cancellation as control flow (no local exception handling)
--------------------------------------------------------------------------------

local function test_scope_cancellation_terminates_waits()
	local bus = Bus.new({ m_wild = '#', s_wild = '+' })

	local st, _, why = fibers.run_scope(function (s)
		local conn = bus:connect()

		s:spawn(function ()
			local sub = conn:subscribe({ 'cancel', 'topic' })
			fibers.perform(sub:recv_op())
		end)

		Sleep.sleep(TMO)
		s:cancel('test cancel')
	end)

	assert(st == 'cancelled', ('expected cancelled, got %s'):format(tostring(st)))
	assert(tostring(why) == 'test cancel')

	print('Scope cancellation terminates waits test passed!')
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

	test_retained_msg_basic()
	test_retained_query_wildcards_and_unretain()

	test_q_overflow_drop_oldest_default()
	test_q_overflow_reject_newest_override()
	test_zero_len_subscription_is_bounded()
	test_reject_block_policy()

	test_wildcard()
	test_literal_tokens_in_pubsub()
	test_multi_wildcard_must_be_last()

	test_laneA_fanout_independent_backpressure()

	test_laneB_publish_one_modalities()
	test_laneB_concrete_topic_enforcement_and_literal_ok()
	test_laneB_endpoint_not_reached_by_publish()
	test_laneB_call_success()
	test_laneB_call_timeout_no_route()

	test_request_sub()
	test_request_once_composed()
	test_request_once_deadline_no_responder()

	test_scope_cancellation_terminates_waits()

	print('ALL TESTS PASSED!')
end)
