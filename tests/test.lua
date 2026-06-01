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

local function topic_str(topic)
	local parts = {}
	for i = 1, #(topic or {}) do
		parts[i] = tostring(topic[i])
	end
	return table.concat(parts, '/')
end

local function assert_local_origin(origin, principal)
	assert(type(origin) == 'table', 'expected origin table')
	assert_eq(origin.kind, 'local', 'expected local origin kind')
	assert(origin.conn_id ~= nil, 'expected conn_id in origin')
	if principal ~= nil then
		assert(origin.principal == principal, 'expected origin principal to match')
	end
end

local function assert_origin_immutable(origin)
	local ok = pcall(function () origin.kind = 'mutated' end)
	assert(not ok, 'expected origin to be immutable')
end

local function assert_bus_replay_done(ev)
	assert(ev, 'expected retained event')
	assert_eq(ev.op, 'replay_done')
	assert(ev.topic == nil, 'expected nil topic for replay_done')
	assert(ev.payload == nil, 'expected nil payload for replay_done')
	assert(type(ev.origin) == 'table', 'expected origin table on replay_done')
	assert_eq(ev.origin.kind, 'bus', 'expected bus origin kind')
	assert_origin_immutable(ev.origin)
end

local function assert_origin_fields(origin, expected)
	assert(type(origin) == 'table', 'expected origin table')
	for k, v in pairs(expected) do
		assert_eq(origin[k], v, 'unexpected origin field ' .. tostring(k))
	end
end

local function assert_origin_extra_immutable(origin)
	assert(type(origin.extra) == 'table', 'expected origin.extra table')
	local ok = pcall(function () origin.extra.changed = true end)
	assert(not ok, 'expected origin.extra to be immutable')
end

-- A standard deadline arm: returns (nil, 'timeout').
local function timeout_op(dt)
	return Sleep.sleep_op(dt):wrap(function ()
		return nil, 'timeout'
	end)
end

-- Named-choice convenience: returns (winner_name, ...arm_results...)
local function select_named(arms)
	return fibers.perform(fibers.named_choice(arms))
end

local function wait_view_changed(view, last_seen, label)
	local which, version, err = select_named({
		changed  = view:changed_op(last_seen),
		deadline = timeout_op(LONG_TMO),
	})

	assert_eq(which, 'changed', label or 'expected retained view to change')
	assert(err == nil, tostring(err))
	assert(type(version) == 'number', 'expected numeric retained view version')
	return version
end

local function wait_view_closed(view, last_seen, expected_reason, label)
	local which, version, err = select_named({
		changed  = view:changed_op(last_seen),
		deadline = timeout_op(LONG_TMO),
	})

	assert_eq(which, 'changed', label or 'expected retained view close to wake changed_op')
	assert(version == nil, 'expected nil version when retained view is closed')
	assert_eq(err, expected_reason or 'closed')
end

--------------------------------------------------------------------------------
-- Authz test helpers
--------------------------------------------------------------------------------

local function principal_with_roles(kind, id, roles)
	return {
		kind  = kind,
		id    = id,
		roles = roles or {},
	}
end

local function admin_principal(id)
	return principal_with_roles('user', id or 'admin', { 'admin' })
end

local function viewer_principal(id)
	return principal_with_roles('user', id or 'viewer', { 'viewer' })
end

local function has_role(principal, wanted)
	local roles = principal and principal.roles
	if type(roles) ~= 'table' then return false end
	for i = 1, #roles do
		if roles[i] == wanted then return true end
	end
	return false
end

local function new_admin_only_authoriser(log)
	return {
		allow = function(_, ctx)
			if log then
				log[#log + 1] = ctx
			end

			if has_role(ctx.principal, 'admin') then
				return true, nil
			end

			return false, 'forbidden'
		end,
	}
end

--------------------------------------------------------------------------------
-- Test Simple PubSub
--------------------------------------------------------------------------------

local function test_simple()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()
	local sub  = conn:subscribe({ 'simple', 'topic' })

	conn:publish({ 'simple', 'topic' }, 'Hello')

	local msg, err = fibers.perform(sub:recv_op())
	assert(msg and msg.payload == 'Hello' and err == nil)
	assert_local_origin(msg.origin)
	assert_origin_immutable(msg.origin)

	print('Simple test passed!')
end

--------------------------------------------------------------------------------
-- Test Selecting Across Subscriptions
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
-- Test Absence as a Race
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
-- Test Graceful Consumer Stop
--------------------------------------------------------------------------------

local function test_graceful_stop_signal()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()
	local sub  = conn:subscribe({ 'loop', 'topic' })

	local stop = Cond.new()

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

			ack:put(msg.payload)
		end

		done:put(out)
	end)

	conn:publish({ 'loop', 'topic' }, 'one')
	conn:publish({ 'loop', 'topic' }, 'two')
	conn:publish({ 'loop', 'topic' }, 'three')

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

	local which, m2, e2 = select_named({
		msg      = sub:recv_op(),
		deadline = timeout_op(LONG_TMO),
	})
	assert(which == 'msg', ('expected closed msg arm to win, got %s'):format(tostring(which)))
	assert(m2 == nil)
	assert(e2 == 'disconnected', ('expected disconnected, got %s'):format(tostring(e2)))

	local ok = pcall(function ()
		conn:publish({ 'cleanup', 'topic' }, 'should error')
	end)
	assert(not ok, 'expected publish to error after disconnect')

	print('Connection cleanup test passed!')
end


--------------------------------------------------------------------------------
-- Test Connection Finaliser Detachment on Explicit Disconnect
--------------------------------------------------------------------------------

-- Regression test for explicit Connection:disconnect(). Bus:connect()
-- installs a scope finaliser so that connections are disconnected on scope
-- exit. Explicit disconnect must detach that finaliser immediately; otherwise
-- long-lived scopes retain one finaliser closure, and therefore one connection
-- object, for every connection ever opened and disconnected.
local function test_disconnect_detaches_connection_finaliser()
	local bus = Bus.new({ m_wild = '#', s_wild = '+' })
	local scope = Scope.current()
	local base = scope._finalisers:length()

	for _ = 1, 1000 do
		local conn = bus:connect()
		assert_eq(scope._finalisers:length(), base + 1, 'connect should install one finaliser')
		conn:disconnect()
		assert_eq(scope._finalisers:length(), base, 'disconnect should detach its finaliser')
	end

	collectgarbage('collect')
	assert_eq(scope._finalisers:length(), base, 'finaliser list should remain at baseline after GC')
	assert_eq(bus:stats().connections, 0, 'all connections should be removed from bus stats')

	print('Connection finaliser detachment test passed!')
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
	assert_local_origin(msg.origin)
	assert_origin_immutable(msg.origin)

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
-- Retained watch feed tests
--------------------------------------------------------------------------------

local function test_retained_watch_replay_and_live_changes()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	conn:retain({ 'watch', 'a' }, 'A1')

	local rw = conn:watch_retained({ 'watch', '#' }, {
		queue_len = 10,
		full      = 'drop_oldest',
		replay    = true,
	})

	assert_eq(conn:stats().retained_watches, 1)
	assert_eq(bus:stats().retained_watches, 1)

	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_eq(ev.op, 'retain')
		assert_eq(topic_str(ev.topic), 'watch/a')
		assert_eq(ev.payload, 'A1')
		assert_local_origin(ev.origin)
		assert_origin_immutable(ev.origin)
	end

	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_bus_replay_done(ev)
	end

	conn:retain({ 'watch', 'b' }, 'B1')

	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_eq(ev.op, 'retain')
		assert_eq(topic_str(ev.topic), 'watch/b')
		assert_eq(ev.payload, 'B1')
		assert_local_origin(ev.origin)
	end

	conn:unretain({ 'watch', 'a' })

	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_eq(ev.op, 'unretain')
		assert_eq(topic_str(ev.topic), 'watch/a')
		assert(ev.payload == nil, 'expected nil payload for unretain')
		assert_local_origin(ev.origin)
	end

	conn:publish({ 'watch', 'c' }, 'PUBONLY')

	do
		local which, ev, err = select_named({
			ev       = rw:recv_op(),
			deadline = timeout_op(TMO),
		})
		assert_eq(which, 'deadline')
		assert_timeout(ev, err)
	end

	rw:unwatch()

	assert_eq(conn:stats().retained_watches, 0)
	assert_eq(bus:stats().retained_watches, 0)

	print('Retained watch (replay + live changes) test passed!')
end

local function test_retained_watch_no_replay()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	conn:retain({ 'watch', 'nr' }, 'OLD')

	local rw = conn:watch_retained({ 'watch', 'nr' }, {
		queue_len = 4,
		full      = 'drop_oldest',
		replay    = false,
	})

	do
		local which, ev, err = select_named({
			ev       = rw:recv_op(),
			deadline = timeout_op(TMO),
		})
		assert_eq(which, 'deadline')
		assert_timeout(ev, err)
	end

	conn:retain({ 'watch', 'nr' }, 'NEW')

	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_eq(ev.op, 'retain')
		assert_eq(topic_str(ev.topic), 'watch/nr')
		assert_eq(ev.payload, 'NEW')
	end

	do
		local which, ev, err = select_named({
			ev       = rw:recv_op(),
			deadline = timeout_op(TMO),
		})
		assert_eq(which, 'deadline')
		assert_timeout(ev, err)
	end

	rw:unwatch()

	print('Retained watch (no replay) test passed!')
end

local function test_retained_watch_wildcards_and_literals()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	conn:retain({ 'metrics', '+' }, 'PLUS')
	conn:retain({ 'metrics', 'abc' }, 'ABC')
	conn:retain({ 'lit', '#', 'x' }, 'HASHMID')

	local rw_wild = conn:watch_retained({ 'metrics', '+' }, {
		queue_len = 10,
		full      = 'drop_oldest',
		replay    = true,
	})
	local rw_lit_plus = conn:watch_retained({ 'metrics', Bus.literal('+') }, {
		queue_len = 10,
		full      = 'drop_oldest',
		replay    = true,
	})
	local rw_lit_hash_mid = conn:watch_retained({ 'lit', Bus.literal('#'), 'x' }, {
		queue_len = 10,
		full      = 'drop_oldest',
		replay    = true,
	})

	do
		local got = {}
		for _ = 1, 2 do
			local ev, err = fibers.perform(rw_wild:recv_op())
			assert(err == nil and ev, tostring(err))
			assert_eq(ev.op, 'retain')
			got[ev.payload] = true
		end
		assert(got.PLUS and got.ABC, 'wild retained watch should replay PLUS and ABC')
		local ev, err = fibers.perform(rw_wild:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_bus_replay_done(ev)
	end

	do
		local ev, err = fibers.perform(rw_lit_plus:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_eq(ev.op, 'retain')
		assert_eq(ev.payload, 'PLUS')
		assert_eq(topic_str(ev.topic), 'metrics/+')
		local ev2, err2 = fibers.perform(rw_lit_plus:recv_op())
		assert(err2 == nil and ev2, tostring(err2))
		assert_bus_replay_done(ev2)
	end

	do
		local ev, err = fibers.perform(rw_lit_hash_mid:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_eq(ev.op, 'retain')
		assert_eq(ev.payload, 'HASHMID')
		assert_eq(topic_str(ev.topic), 'lit/#/x')
		local ev2, err2 = fibers.perform(rw_lit_hash_mid:recv_op())
		assert(err2 == nil and ev2, tostring(err2))
		assert_bus_replay_done(ev2)
	end

	rw_wild:unwatch()
	rw_lit_plus:unwatch()
	rw_lit_hash_mid:unwatch()

	print('Retained watch (wildcards + literal wrapper) test passed!')
end

local function test_retained_watch_unwatch_and_disconnect()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local rw = conn:watch_retained({ 'watch', 'close' }, {
		queue_len = 4,
		full      = 'drop_oldest',
	})

	rw:unwatch()

	do
		local which, ev, err = select_named({
			ev       = rw:recv_op(),
			deadline = timeout_op(LONG_TMO),
		})
		assert_eq(which, 'ev')
		assert(ev == nil)
		assert_eq(err, 'unwatched')
	end

	local rw2 = conn:watch_retained({ 'watch', 'disconnect' }, {
		queue_len = 4,
		full      = 'drop_oldest',
	})

	conn:disconnect()

	do
		local which, ev, err = select_named({
			ev       = rw2:recv_op(),
			deadline = timeout_op(LONG_TMO),
		})
		assert_eq(which, 'ev')
		assert(ev == nil)
		assert_eq(err, 'disconnected')
	end

	print('Retained watch (unwatch + disconnect) test passed!')
end

local function test_retained_watch_bounded_queue()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+', q_length = 10 })
	local conn = bus:connect()

	local rw = conn:watch_retained({ 'watch', 'bounded' }, {
		queue_len = 1,
		full      = 'reject_newest',
	})

	conn:retain({ 'watch', 'bounded' }, 'one')
	conn:retain({ 'watch', 'bounded' }, 'two')

	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_eq(ev.op, 'retain')
		assert_eq(ev.payload, 'one')
	end

	do
		local which, ev, err = select_named({
			ev       = rw:recv_op(),
			deadline = timeout_op(TMO),
		})
		assert_eq(which, 'deadline')
		assert_timeout(ev, err)
	end

	assert_eq(rw:dropped(), 1)
	assert_eq(conn:dropped(), 1)
	assert_eq(bus:stats().dropped, 1)

	rw:unwatch()

	print('Retained watch (bounded queue) test passed!')
end

local function test_retained_watch_replay_done_empty()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local rw = conn:watch_retained({ 'empty', '#' }, {
		queue_len = 1,
		full      = 'reject_newest',
		replay    = true,
	})

	local ev, err = fibers.perform(rw:recv_op())
	assert(err == nil and ev, tostring(err))
	assert_bus_replay_done(ev)

	do
		local which, ev2, err2 = select_named({
			ev       = rw:recv_op(),
			deadline = timeout_op(TMO),
		})
		assert_eq(which, 'deadline')
		assert_timeout(ev2, err2)
	end

	rw:unwatch()
	print('Retained watch replay_done (empty replay) test passed!')
end

local function test_retained_watch_replay_overflow_closes_watch()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	conn:retain({ 'overflow', 'a' }, 'A')
	conn:retain({ 'overflow', 'b' }, 'B')

	local rw = conn:watch_retained({ 'overflow', '#' }, {
		queue_len = 1,
		full      = 'reject_newest',
		replay    = true,
	})

	local ev, err = fibers.perform(rw:recv_op())
	assert(err == nil and ev and ev.op == 'retain', tostring(err))

	local ev2, err2 = fibers.perform(rw:recv_op())
	assert(ev2 == nil, 'expected watch to close after replay overflow')
	assert_eq(err2, 'replay_overflow')

	print('Retained watch replay overflow test passed!')
end

--------------------------------------------------------------------------------
-- Feed handle shape tests
--------------------------------------------------------------------------------

local function test_feed_distinct_metatables_and_method_guards()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local sub = conn:subscribe({ 'feed', 'sub' })
	local rw  = conn:watch_retained({ 'feed', 'rw' })
	local ep  = conn:bind({ 'feed', 'ep' }, { queue_len = 1 })

	assert_eq(getmetatable(sub), Bus.Subscription, 'subscription metatable')
	assert_eq(getmetatable(rw),  Bus.RetainedWatch, 'retained watch metatable')
	assert_eq(getmetatable(ep),  Bus.Endpoint, 'endpoint metatable')

	assert(Bus.Subscription ~= Bus.RetainedWatch, 'Subscription and RetainedWatch should be distinct')
	assert(Bus.Subscription ~= Bus.Endpoint, 'Subscription and Endpoint should be distinct')
	assert(Bus.RetainedWatch ~= Bus.Endpoint, 'RetainedWatch and Endpoint should be distinct')

	assert_eq(sub:kind(), 'subscription')
	assert_eq(rw:kind(), 'retained_watch')
	assert_eq(ep:kind(), 'endpoint')

	assert_eq(topic_str(sub:topic()), 'feed/sub')
	assert_eq(topic_str(rw:topic()), 'feed/rw')
	assert_eq(topic_str(ep:topic()), 'feed/ep')

	local ok_payloads = pcall(function () rw:payloads() end)
	assert(not ok_payloads, 'expected retained watch payloads() to error')

	local ok_unwatch_sub = pcall(function () sub:unwatch() end)
	assert(not ok_unwatch_sub, 'expected subscription:unwatch() to error')

	local ok_unsub_rw = pcall(function () rw:unsubscribe() end)
	assert(not ok_unsub_rw, 'expected retained_watch:unsubscribe() to error')

	local ok_unsub_ep = pcall(function () ep:unsubscribe() end)
	assert(not ok_unsub_ep, 'expected endpoint:unsubscribe() to error')

	local ok_unbind_sub = pcall(function () sub:unbind() end)
	assert(not ok_unbind_sub, 'expected subscription:unbind() to error')

	sub:unsubscribe()
	rw:unwatch()
	ep:unbind()

	print('Feed distinct metatables + method guards test passed!')
end

local function test_feed_closed_op_reason()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local sub = conn:subscribe({ 'feed', 'closed', 'sub' })
	local rw  = conn:watch_retained({ 'feed', 'closed', 'rw' })
	local ep  = conn:bind({ 'feed', 'closed', 'ep' }, { queue_len = 1 })

	sub:unsubscribe()
	rw:unwatch()
	ep:unbind()

	do
		local reason = fibers.perform(sub:closed_op())
		assert_eq(reason, 'unsubscribed')
	end

	do
		local reason = fibers.perform(rw:closed_op())
		assert_eq(reason, 'unwatched')
	end

	do
		local reason = fibers.perform(ep:closed_op())
		assert_eq(reason, 'unbound')
	end

	print('Feed closed_op reason test passed!')
end

--------------------------------------------------------------------------------
-- Retained materialised view tests
--------------------------------------------------------------------------------

local function test_retained_view_empty_replay_ready()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local view = conn:retained_view({ 'view', 'empty', '#' })

	assert_eq(getmetatable(view), Bus.RetainedView)
	assert_eq(view:version(), 0)
	assert_eq(conn:stats().retained_views, 1)
	assert_eq(bus:stats().retained_views, 1)
	assert(view:get({ 'view', 'empty', 'missing' }) == nil, 'expected empty view')

	do
		local which, version, err = select_named({
			changed  = view:changed_op(view:version()),
			deadline = timeout_op(TMO),
		})
		assert_eq(which, 'deadline')
		assert_timeout(version, err)
	end

	view:close()

	assert_eq(conn:stats().retained_views, 0)
	assert_eq(bus:stats().retained_views, 0)

	do
		local reason = fibers.perform(view:closed_op())
		assert_eq(reason, 'closed')
	end

	print('Retained view empty replay ready test passed!')
end

local function test_retained_view_replay_snapshot_and_live_changes()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	conn:retain({ 'view', 'state', 'a' }, 'A1')
	conn:retain({ 'view', 'state', 'b' }, 'B1')
	conn:retain({ 'view', 'other', 'x' }, 'X1')

	local view = conn:retained_view({ 'view', 'state', '#' })

	assert_eq(conn:stats().retained_views, 1)
	assert_eq(bus:stats().retained_views, 1)

	local ma = view:get({ 'view', 'state', 'a' })
	local mb = view:get({ 'view', 'state', 'b' })
	local mx = view:get({ 'view', 'other', 'x' })

	assert(ma and ma.payload == 'A1', 'expected replayed A1')
	assert(mb and mb.payload == 'B1', 'expected replayed B1')
	assert(mx == nil, 'expected non-matching retained topic to be absent')
	assert_eq(#view:snapshot(), 2, 'expected two retained view items')
	assert(view:version() >= 2, 'expected replay to advance version')

	local last = view:version()

	conn:retain({ 'view', 'state', 'a' }, 'A2')
	last = wait_view_changed(view, last, 'expected retain update to change view')

	do
		local msg = view:get({ 'view', 'state', 'a' })
		assert(msg and msg.payload == 'A2', 'expected updated retained payload')
		assert_local_origin(msg.origin)
	end

	conn:retain({ 'view', 'other', 'x' }, 'X2')

	do
		local which, version, err = select_named({
			changed  = view:changed_op(last),
			deadline = timeout_op(TMO),
		})
		assert_eq(which, 'deadline')
		assert_timeout(version, err)
	end

	conn:unretain({ 'view', 'state', 'b' })
	last = wait_view_changed(view, last, 'expected unretain to change view')

	assert(view:get({ 'view', 'state', 'b' }) == nil, 'expected b to be removed')
	assert_eq(#view:items(), 1, 'expected one retained view item after unretain')

	view:close()

	print('Retained view replay snapshot + live changes test passed!')
end

local function test_retained_view_wildcards_and_literal_tokens()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	conn:retain({ 'viewlit', 'metrics', '+' }, 'PLUS')
	conn:retain({ 'viewlit', 'metrics', 'abc' }, 'ABC')
	conn:retain({ 'viewlit', 'lit', '#', 'x' }, 'HASHMID')

	local view_wild = conn:retained_view({ 'viewlit', 'metrics', '+' })
	local view_lit_plus = conn:retained_view({ 'viewlit', 'metrics', Bus.literal('+') })
	local view_lit_hash_mid = conn:retained_view({ 'viewlit', 'lit', Bus.literal('#'), 'x' })

	assert_eq(#view_wild:snapshot(), 2, 'wild view should include PLUS and ABC')
	assert(view_wild:get({ 'viewlit', 'metrics', Bus.literal('+') }).payload == 'PLUS')
	assert(view_wild:get({ 'viewlit', 'metrics', 'abc' }).payload == 'ABC')

	assert_eq(#view_lit_plus:snapshot(), 1, 'literal plus view should include only literal plus')
	assert(view_lit_plus:get({ 'viewlit', 'metrics', Bus.literal('+') }).payload == 'PLUS')
	assert(view_lit_plus:get({ 'viewlit', 'metrics', 'abc' }) == nil)

	assert_eq(#view_lit_hash_mid:snapshot(), 1, 'literal hash view should include only literal hash mid-token')
	assert(view_lit_hash_mid:get({ 'viewlit', 'lit', Bus.literal('#'), 'x' }).payload == 'HASHMID')

	local ok, err = pcall(function()
		view_wild:get({ 'viewlit', 'metrics', '+' })
	end)
	assert(not ok, 'retained_view:get should reject wildcard lookup topics')
	assert(tostring(err):match('concrete topic'), 'error should explain concrete-topic requirement')

	ok, err = pcall(function()
		view_lit_hash_mid:get({ 'viewlit', 'lit', '#', 'x' })
	end)
	assert(not ok, 'retained_view:get should reject wildcard lookup topics')
	assert(tostring(err):match('concrete topic'), 'error should explain concrete-topic requirement')

	view_wild:close()
	view_lit_plus:close()
	view_lit_hash_mid:close()

	print('Retained view wildcards + literal tokens test passed!')
end

local function test_retained_view_changed_op_integer_validation()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()
	local view = conn:retained_view({ 'view', 'validation' })

	local bad_values = {
		nil,
		1.5,
		'0',
		{},
	}

	for _, v in ipairs(bad_values) do
		local ok = pcall(function ()
			view:changed_op(v)
		end)
		assert(not ok, 'expected changed_op to reject non-integer last_seen')
	end

	local ok = pcall(function ()
		view:changed_op(0)
	end)
	assert(ok, 'expected changed_op to accept integer last_seen')

	view:close()

	print('Retained view changed_op integer validation test passed!')
end

local function test_retained_view_close_and_disconnect()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local view = conn:retained_view({ 'view', 'close' })
	assert_eq(bus:stats().retained_views, 1)

	local last = view:version()
	view:close()

	wait_view_closed(view, last, 'closed')

	do
		local reason = fibers.perform(view:closed_op())
		assert_eq(reason, 'closed')
	end

	assert_eq(conn:stats().retained_views, 0)
	assert_eq(bus:stats().retained_views, 0)

	local conn2 = bus:connect()
	local view2 = conn2:retained_view({ 'view', 'disconnect' })
	assert_eq(bus:stats().retained_views, 1)

	local last2 = view2:version()
	conn2:disconnect()

	wait_view_closed(view2, last2, 'disconnected')

	do
		local reason = fibers.perform(view2:closed_op())
		assert_eq(reason, 'disconnected')
	end

	assert_eq(bus:stats().retained_views, 0)

	print('Retained view close + disconnect test passed!')
end

local function test_retained_view_scope_cleanup()
	local bus = Bus.new({ m_wild = '#', s_wild = '+' })

	local st, rep = fibers.run_scope(function ()
		local conn = bus:connect()
		local view = conn:retained_view({ 'view', 'scope' })
		assert_eq(conn:stats().retained_views, 1)
		assert_eq(bus:stats().retained_views, 1)
		assert(view ~= nil)
	end)

	assert_eq(st, 'ok', 'expected scope to finish cleanly')
	assert(rep ~= nil, 'expected scope report')
	assert_eq(bus:stats().retained_views, 0, 'expected scoped retained view to be removed')

	print('Retained view scope cleanup test passed!')
end

local function test_retained_view_no_queue_overflow_or_background_fibre()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	for i = 1, 20 do
		conn:retain({ 'view', 'many', i }, 'V' .. i)
	end

	local view = conn:retained_view({ 'view', 'many', '#' }, {
		queue_len = 0,
		full      = 'reject_newest',
	})

	assert_eq(#view:snapshot(), 20, 'retained view should materialise all matching retained state')
	assert_eq(conn:dropped(), 0, 'retained view should not use a lossy mailbox')
	assert_eq(bus:stats().dropped, 0, 'retained view should not affect bus dropped count')
	assert_eq(conn:stats().retained_watches, 0, 'retained view should not create retained watch feed')
	assert_eq(conn:stats().retained_views, 1)

	view:close()

	print('Retained view no queue overflow/background fibre test passed!')
end

--------------------------------------------------------------------------------
-- Additional origin / provenance tests
--------------------------------------------------------------------------------

local function test_origin_factory_function_and_extra_on_publish()
	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local pub_pr = admin_principal('origin-pub')
	local seq    = 0

	local conn = bus:connect({
		principal = pub_pr,
		origin_factory = function ()
			seq = seq + 1
			return {
				link_id    = 'link-pub',
				peer_node  = 'peer-pub',
				peer_sid   = 'sid-pub',
				generation = seq,
			}
		end,
	})

	local sub = conn:subscribe({ 'origin', 'publish' }, { queue_len = 10 })

	conn:publish({ 'origin', 'publish' }, 'one', {
		extra = {
			note    = 'n1',
			kind    = 'spoof-attempt',
			link_id = 'fake-link',
		},
	})

	conn:publish({ 'origin', 'publish' }, 'two')

	do
		local msg, err = fibers.perform(sub:recv_op())
		assert(err == nil and msg, tostring(err))
		assert_eq(msg.payload, 'one')

		assert_local_origin(msg.origin, pub_pr)
		assert_origin_fields(msg.origin, {
			link_id    = 'link-pub',
			peer_node  = 'peer-pub',
			peer_sid   = 'sid-pub',
			generation = 1,
		})

		assert(type(msg.origin.extra) == 'table', 'expected origin.extra')
		assert_eq(msg.origin.extra.note, 'n1')
		assert_eq(msg.origin.extra.kind, 'spoof-attempt')
		assert_eq(msg.origin.extra.link_id, 'fake-link')

		assert_origin_immutable(msg.origin)
		assert_origin_extra_immutable(msg.origin)
	end

	do
		local msg, err = fibers.perform(sub:recv_op())
		assert(err == nil and msg, tostring(err))
		assert_eq(msg.payload, 'two')

		assert_local_origin(msg.origin, pub_pr)
		assert_origin_fields(msg.origin, {
			link_id    = 'link-pub',
			peer_node  = 'peer-pub',
			peer_sid   = 'sid-pub',
			generation = 2,
		})

		assert(msg.origin.extra == nil, 'expected no origin.extra on second publish')
	end

	print('Origin factory function + extra on publish test passed!')
end

local function test_origin_conn_ids_distinct_between_connections()
	local bus   = Bus.new({ m_wild = '#', s_wild = '+' })
	local sink  = bus:connect()
	local sub   = sink:subscribe({ 'origin', 'connid' }, { queue_len = 10 })

	local p1 = admin_principal('c1')
	local p2 = admin_principal('c2')

	local c1 = bus:connect({ principal = p1 })
	local c2 = bus:connect({ principal = p2 })

	c1:publish({ 'origin', 'connid' }, 'm1')
	c2:publish({ 'origin', 'connid' }, 'm2')

	local m1, e1 = fibers.perform(sub:recv_op())
	local m2, e2 = fibers.perform(sub:recv_op())

	assert(e1 == nil and m1, tostring(e1))
	assert(e2 == nil and m2, tostring(e2))

	assert_local_origin(m1.origin, p1)
	assert_local_origin(m2.origin, p2)

	assert(m1.origin.conn_id ~= nil, 'expected conn_id on first origin')
	assert(m2.origin.conn_id ~= nil, 'expected conn_id on second origin')
	assert(m1.origin.conn_id ~= m2.origin.conn_id, 'expected distinct conn_id values')

	print('Origin conn_id distinctness test passed!')
end

local function test_retained_replay_preserves_original_origin()
	local bus = Bus.new({ m_wild = '#', s_wild = '+' })

	local source = bus:connect({
		principal = admin_principal('ret-source'),
		origin_factory = {
			kind       = 'fabric_import',
			link_id    = 'link-ret',
			peer_node  = 'peer-ret',
			peer_sid   = 'sid-ret',
			generation = 17,
		},
	})

	source:retain({ 'origin', 'retained' }, 'R1', {
		extra = { trace = 'retain-trace' },
	})

	local sub = bus:connect():subscribe({ 'origin', 'retained' })
	do
		local msg, err = fibers.perform(sub:recv_op())
		assert(err == nil and msg, tostring(err))
		assert_eq(msg.payload, 'R1')

		assert_origin_fields(msg.origin, {
			kind       = 'fabric_import',
			link_id    = 'link-ret',
			peer_node  = 'peer-ret',
			peer_sid   = 'sid-ret',
			generation = 17,
		})
		assert_eq(msg.origin.extra.trace, 'retain-trace')
		assert_origin_immutable(msg.origin)
		assert_origin_extra_immutable(msg.origin)
	end

	local rw = bus:connect():watch_retained({ 'origin', 'retained' }, {
		queue_len = 4,
		full      = 'reject_newest',
		replay    = true,
	})

	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_eq(ev.op, 'retain')
		assert_eq(ev.payload, 'R1')

		assert_origin_fields(ev.origin, {
			kind       = 'fabric_import',
			link_id    = 'link-ret',
			peer_node  = 'peer-ret',
			peer_sid   = 'sid-ret',
			generation = 17,
		})
		assert_eq(ev.origin.extra.trace, 'retain-trace')
	end

	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_bus_replay_done(ev)
	end

	rw:unwatch()

	print('Retained replay preserves original origin test passed!')
end

local function test_retained_view_preserves_original_origin()
	local bus = Bus.new({ m_wild = '#', s_wild = '+' })

	local source = bus:connect({
		principal = admin_principal('view-ret-source'),
		origin_factory = {
			kind       = 'fabric_import',
			link_id    = 'link-view-ret',
			peer_node  = 'peer-view-ret',
			peer_sid   = 'sid-view-ret',
			generation = 18,
		},
	})

	source:retain({ 'origin', 'viewret' }, 'VR1', {
		extra = { trace = 'view-retain-trace' },
	})

	local view = bus:connect():retained_view({ 'origin', 'viewret' })
	local msg = view:get({ 'origin', 'viewret' })

	assert(msg and msg.payload == 'VR1', 'expected retained view to contain VR1')
	assert_origin_fields(msg.origin, {
		kind       = 'fabric_import',
		link_id    = 'link-view-ret',
		peer_node  = 'peer-view-ret',
		peer_sid   = 'sid-view-ret',
		generation = 18,
	})
	assert_eq(msg.origin.extra.trace, 'view-retain-trace')
	assert_origin_immutable(msg.origin)
	assert_origin_extra_immutable(msg.origin)

	view:close()

	print('Retained view preserves original origin test passed!')
end

local function test_unretain_event_origin_metadata()
	local bus = Bus.new({ m_wild = '#', s_wild = '+' })
	local pr  = admin_principal('origin-unretain')

	local conn = bus:connect({
		principal = pr,
		origin_factory = {
			link_id    = 'link-unretain',
			peer_node  = 'peer-unretain',
			peer_sid   = 'sid-unretain',
			generation = 33,
		},
	})

	local rw = conn:watch_retained({ 'origin', 'gone' }, {
		queue_len = 4,
		full      = 'reject_newest',
		replay    = false,
	})

	conn:retain({ 'origin', 'gone' }, 'value')
	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev and ev.op == 'retain', tostring(err))
	end

	conn:unretain({ 'origin', 'gone' }, {
		extra = {
			reason     = 'manual-clear',
			generation = 'fake-generation',
		},
	})

	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_eq(ev.op, 'unretain')
		assert_eq(topic_str(ev.topic), 'origin/gone')
		assert(ev.payload == nil, 'expected nil payload on unretain')

		assert_local_origin(ev.origin, pr)
		assert_origin_fields(ev.origin, {
			link_id    = 'link-unretain',
			peer_node  = 'peer-unretain',
			peer_sid   = 'sid-unretain',
			generation = 33,
		})
		assert_eq(ev.origin.extra.reason, 'manual-clear')
		assert_eq(ev.origin.extra.generation, 'fake-generation')

		assert_origin_immutable(ev.origin)
		assert_origin_extra_immutable(ev.origin)
	end

	rw:unwatch()

	print('Unretain event origin metadata test passed!')
end

local function test_call_request_origin_factory_and_extra()
	local bus       = Bus.new({ m_wild = '#', s_wild = '+' })
	local server    = bus:connect({ principal = admin_principal('rpc-server') })
	local caller_pr = admin_principal('rpc-caller')

	local client = bus:connect({
		principal = caller_pr,
		origin_factory = function ()
			return {
				link_id    = 'link-call',
				peer_node  = 'peer-call',
				peer_sid   = 'sid-call',
				generation = 44,
			}
		end,
	})

	local ep = server:bind({ 'origin', 'rpc' }, { queue_len = 1 })

	fibers.spawn(function ()
		local req, err = ep:recv()
		assert(req, tostring(err))

		assert_eq(topic_str(req.topic), 'origin/rpc')
		assert_eq(req.payload, 'ping')

		assert_local_origin(req.origin, caller_pr)
		assert_origin_fields(req.origin, {
			link_id    = 'link-call',
			peer_node  = 'peer-call',
			peer_sid   = 'sid-call',
			generation = 44,
		})

		assert(type(req.origin.extra) == 'table', 'expected origin.extra on request')
		assert_eq(req.origin.extra.note, 'rpc-extra')
		assert_eq(req.origin.extra.link_id, 'fake-link')

		assert_origin_immutable(req.origin)
		assert_origin_extra_immutable(req.origin)

		assert(req:reply('pong'))
	end)

	local reply, err = client:call({ 'origin', 'rpc' }, 'ping', {
		timeout = LONG_TMO,
		extra   = {
			note    = 'rpc-extra',
			link_id = 'fake-link',
		},
	})

	assert(err == nil, tostring(err))
	assert_eq(reply, 'pong')

	ep:unbind()

	print('Call request origin factory + extra test passed!')
end

--------------------------------------------------------------------------------
-- Derive tests
--------------------------------------------------------------------------------

local function test_derive_inherits_principal_but_not_origin_factory()
	local bus       = Bus.new({ m_wild = '#', s_wild = '+' })
	local principal = admin_principal('derive-base')
	local seq       = 0

	local base = bus:connect({
		principal = principal,
		origin_factory = function ()
			seq = seq + 1
			return {
				kind       = 'fabric_import',
				link_id    = 'link-base',
				peer_node  = 'peer-base',
				peer_sid   = 'sid-base',
				generation = seq,
			}
		end,
	})

	local sink = bus:connect()
	local sub  = sink:subscribe({ 'derive', 'inherit' }, { queue_len = 4 })
	local child = base:derive()

	assert(child:principal() == principal, 'expected derived principal to inherit')

	child:publish({ 'derive', 'inherit' }, 'ok')

	local msg, err = fibers.perform(sub:recv_op())
	assert(err == nil and msg, tostring(err))
	assert_eq(msg.payload, 'ok')
	assert_local_origin(msg.origin, principal)
	assert(msg.origin.link_id == nil, 'expected origin_factory not to be inherited')
	assert(msg.origin.peer_node == nil, 'expected origin_factory not to be inherited')
	assert(msg.origin.peer_sid == nil, 'expected origin_factory not to be inherited')
	assert(msg.origin.generation == nil, 'expected origin_factory not to be inherited')

	print('Derive inherits principal but not origin_factory test passed!')
end

local function test_derive_allows_override_principal_and_origin_factory()
	local bus   = Bus.new({ m_wild = '#', s_wild = '+' })
	local base  = bus:connect({ principal = admin_principal('parent') })
	local child_pr = admin_principal('child')
	local sink  = bus:connect()
	local sub   = sink:subscribe({ 'derive', 'override' }, { queue_len = 4 })

	local child = base:derive({
		principal = child_pr,
		origin_factory = {
			kind       = 'fabric_import',
			link_id    = 'link-child',
			peer_node  = 'peer-child',
			peer_sid   = 'sid-child',
			generation = 7,
		},
	})

	child:publish({ 'derive', 'override' }, 'payload', {
		extra = { marker = 'x' },
	})

	local msg, err = fibers.perform(sub:recv_op())
	assert(err == nil and msg, tostring(err))
	assert_eq(msg.payload, 'payload')
	assert_origin_fields(msg.origin, {
		kind       = 'fabric_import',
		link_id    = 'link-child',
		peer_node  = 'peer-child',
		peer_sid   = 'sid-child',
		generation = 7,
		principal  = child_pr,
	})
	assert_eq(msg.origin.extra.marker, 'x')

	print('Derive override principal and origin_factory test passed!')
end

local function test_derive_disconnected_errors()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()
	conn:disconnect()

	local ok = pcall(function ()
		conn:derive()
	end)
	assert(not ok, 'expected derive on disconnected connection to error')

	print('Derive on disconnected connection test passed!')
end

local function test_derive_connection_scope_cleanup()
	local bus = Bus.new({ m_wild = '#', s_wild = '+' })
	local outer = bus:connect()
	local sub = outer:subscribe({ 'derive', 'scope' }, { queue_len = 4 })

	local st, rep = fibers.run_scope(function ()
		local parent = bus:connect({ principal = admin_principal('scoped-parent') })
		local child  = parent:derive()
		child:publish({ 'derive', 'scope' }, 'before')
		Sleep.sleep(TMO)
	end)

	assert_eq(st, 'ok', 'expected scoped run to complete')
	assert(rep ~= nil, 'expected scope report')

	do
		local msg, err = fibers.perform(sub:recv_op())
		assert(err == nil and msg, tostring(err))
		assert_eq(msg.payload, 'before')
	end

	local stats = bus:stats()
	assert_eq(stats.connections, 1, 'expected only outer connection to remain after scoped cleanup')

	print('Derive connection scope cleanup test passed!')
end

--------------------------------------------------------------------------------
-- Request lifecycle tests
--------------------------------------------------------------------------------

local function test_request_abandon_after_timeout_rejects_late_reply()
	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local server = bus:connect()
	local client = bus:connect()

	local ep = server:bind({ 'rpc', 'abandon' }, { queue_len = 1 })
	local late = Channel.new(1)

	fibers.spawn(function ()
		local req, err = ep:recv()
		assert(req, tostring(err))
		Sleep.sleep(LONG_TMO)
		late:put(req:reply('too-late'))
	end)

	local reply, err = client:call({ 'rpc', 'abandon' }, 'x', { timeout = TMO })
	assert(reply == nil)
	assert_eq(err, 'timeout')

	local which, ok, cherr = select_named({
		late     = late:get_op():wrap(function (v) return v, nil end),
		deadline = timeout_op(LONG_TMO),
	})
	assert_eq(which, 'late')
	assert(cherr == nil)
	assert_eq(ok, false)

	print('Request abandon after timeout rejects late reply test passed!')
end

local function test_request_done_state_for_fail_and_abandon()
	local req = Bus.Request and Bus.Request or error('Bus.Request missing')
	local origin = setmetatable({}, { __index = { kind = 'local' }, __newindex = function () error('immutable') end })
	local r = req.__index and nil
	assert(origin ~= nil and r == nil)

	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local server = bus:connect()
	local client = bus:connect()

	local ep = server:bind({ 'rpc', 'done' }, { queue_len = 1 })
	fibers.spawn(function ()
		local request, err = ep:recv()
		assert(request, tostring(err))
		assert(not request:done(), 'request should not be done initially')
		assert(request:fail('failed-now'))
		assert(request:done(), 'request should be done after fail')
		assert(request:abandon('later') == false, 'abandon after fail should return false')
		assert(request:reply('later') == false, 'reply after fail should return false')
	end)

	local value, err = client:call({ 'rpc', 'done' }, 'x', { timeout = LONG_TMO })
	assert(value == nil)
	assert_eq(err, 'failed-now')

	print('Request done state for fail/abandon test passed!')
end


local function test_request_status_and_done_op_for_all_terminal_states()
	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local server = bus:connect()
	local client = bus:connect()

	local ep = server:bind({ 'rpc', 'status' }, { queue_len = 3 })

	fibers.spawn(function ()
		local req = assert(ep:recv())
		assert_eq(select(1, req:status()), 'pending')
		assert(req:reply('ok'))
	end)
	local v, err = client:call({ 'rpc', 'status' }, 'reply', { timeout = LONG_TMO })
	assert_eq(v, 'ok')
	assert(err == nil, tostring(err))

	fibers.spawn(function ()
		local req = assert(ep:recv())
		assert(req:fail('bad'))
		local status, value, ferr = fibers.perform(req:done_op())
		assert_eq(status, 'failed')
		assert(value == nil)
		assert_eq(ferr, 'bad')
	end)
	v, err = client:call({ 'rpc', 'status' }, 'fail', { timeout = LONG_TMO })
	assert(v == nil)
	assert_eq(err, 'bad')

	local abandoned = Channel.new(1)
	fibers.spawn(function ()
		local req = assert(ep:recv())
		local status, value, aerr = fibers.perform(req:done_op())
		abandoned:put({ status = status, value = value, err = aerr })
	end)
	v, err = client:call({ 'rpc', 'status' }, 'abandon', { timeout = TMO })
	assert(v == nil)
	assert_eq(err, 'timeout')
	local rec = abandoned:get()
	assert_eq(rec.status, 'abandoned')
	assert(rec.value == nil)
	assert_eq(rec.err, 'timeout')

	print('Request status and done_op terminal-state test passed!')
end

local function test_call_op_abort_abandons_request_and_wakes_done_op()
	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local server = bus:connect()
	local client = bus:connect()
	local ep     = server:bind({ 'rpc', 'abort' }, { queue_len = 1 })
	local seen   = Channel.new(1)

	fibers.spawn(function ()
		local req = assert(ep:recv())
		local status, value, err = fibers.perform(req:done_op())
		seen:put({ status = status, value = value, err = err, late_reply = req:reply('late') })
	end)

	local which, value, err = select_named({
		call = client:call_op({ 'rpc', 'abort' }, 'payload', { timeout = false }),
		stop = timeout_op(TMO),
	})
	assert_eq(which, 'stop')
	assert(value == nil)
	assert_eq(err, 'timeout')

	local rec = seen:get()
	assert_eq(rec.status, 'abandoned')
	assert(rec.value == nil)
	assert_eq(rec.err, 'aborted')
	assert_eq(rec.late_reply, false)

	print('call_op abort abandons request and wakes done_op test passed!')
end

local function test_call_op_timeout_false_has_no_hidden_deadline()
	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local server = bus:connect()
	local client = bus:connect()
	local ep     = server:bind({ 'rpc', 'no-hidden-timeout' }, { queue_len = 1 })
	local done   = Channel.new(1)

	fibers.spawn(function ()
		local req = assert(ep:recv())
		local status, _value, err = fibers.perform(req:done_op())
		done:put({ status = status, err = err })
	end)

	local which = select_named({
		call = client:call_op({ 'rpc', 'no-hidden-timeout' }, 'payload', { timeout = false }),
		stop = timeout_op(TMO),
	})
	assert_eq(which, 'stop')

	local rec = done:get()
	assert_eq(rec.status, 'abandoned')
	assert_eq(rec.err, 'aborted')

	print('call_op timeout=false no-hidden-deadline test passed!')
end

local function test_call_no_timeout_options_survive_default_deadline()
	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local server = bus:connect()
	local client = bus:connect()
	local ep     = server:bind({ 'rpc', 'no-default-deadline' }, { queue_len = 3 })
	local results = Channel.new(3)

	fibers.spawn(function ()
		for _ = 1, 3 do
			local req = assert(ep:recv())
			fibers.spawn(function ()
				-- The bus default call timeout is one second.  Sleeping slightly longer
				-- proves that timeout=false/deadline=false really disables that
				-- hidden deadline rather than merely racing a shorter outer abort.
				fibers.perform(Sleep.sleep_op(1.1))
				assert(req:reply('reply:' .. tostring(req.payload)))
			end)
		end
	end)

	fibers.spawn(function ()
		local value, err = fibers.perform(client:call_op(
			{ 'rpc', 'no-default-deadline' },
			'timeout_false_op',
			{ timeout = false }
		))
		results:put({ case = 'timeout_false_op', value = value, err = err })
	end)

	fibers.spawn(function ()
		local value, err = fibers.perform(client:call_op(
			{ 'rpc', 'no-default-deadline' },
			'deadline_false_op',
			{ deadline = false }
		))
		results:put({ case = 'deadline_false_op', value = value, err = err })
	end)

	fibers.spawn(function ()
		local value, err = client:call(
			{ 'rpc', 'no-default-deadline' },
			'timeout_false_call',
			{ timeout = false }
		)
		results:put({ case = 'timeout_false_call', value = value, err = err })
	end)

	local got = {}
	for _ = 1, 3 do
		local which, rec, err = select_named({
			result   = results:get_op():wrap(function (v) return v, nil end),
			deadline = timeout_op(1.7),
		})
		assert_eq(which, 'result', 'expected no-timeout call result before outer deadline')
		assert(err == nil, tostring(err))
		got[rec.case] = rec
	end

	for _, case in ipairs({ 'timeout_false_op', 'deadline_false_op', 'timeout_false_call' }) do
		local rec = got[case]
		assert(rec, 'missing result for ' .. case)
		assert_eq(rec.value, 'reply:' .. case, case .. ' should survive past bus default timeout')
		assert_eq(rec.err, nil, case .. ' should not time out')
	end

	print('call timeout=false/deadline=false no hidden default-deadline test passed!')
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
-- Lane B: endpoints and call
--------------------------------------------------------------------------------

local function test_laneB_call_full_and_closed_modalities()
	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local server = bus:connect()
	local client = bus:connect()

	local ep = server:bind({ 'svc', 'rzv' }, { queue_len = 0 })

	do
		local reply, err = client:call({ 'svc', 'rzv' }, 'x', { timeout = TMO })
		assert(reply == nil)
		assert_eq(err, 'full')
		assert_eq(ep:dropped(), 1)
	end

	local got = Channel.new(1)
	fibers.spawn(function ()
		local req, err = ep:recv()
		assert(req, tostring(err))
		got:put(req.payload)
		assert(req:reply('reply:' .. tostring(req.payload)))
	end)

	Sleep.sleep(TMO)

	do
		local reply, err = client:call({ 'svc', 'rzv' }, 'hello', { timeout = LONG_TMO })
		assert(err == nil, tostring(err))
		assert_eq(reply, 'reply:hello')
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
		local reply, err = client:call({ 'svc', 'rzv' }, 'x', { timeout = TMO })
		assert(reply == nil)
		assert_eq(err, 'closed')
	end

	print('Lane B call full/closed modalities test passed!')
end

local function test_laneB_concrete_topic_enforcement_and_literal_ok()
	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local server = bus:connect()
	local client = bus:connect()

	local ok1 = pcall(function ()
		server:bind({ 'svc', '+' }, { queue_len = 1 })
	end)
	assert(not ok1, 'expected bind with wildcard to error')

	local ok2 = pcall(function ()
		fibers.perform(client:call_op({ 'svc', '+' }, 'x', { timeout = TMO }))
	end)
	assert(not ok2, 'expected call with wildcard topic to error')

	local ep = server:bind({ 'svc', Bus.literal('+') }, { queue_len = 1 })
	assert(ep, 'expected bind with literal "+" to succeed')

	fibers.spawn(function ()
		local req, err = ep:recv()
		assert(req, tostring(err))
		assert(req:reply('ok'))
	end)

	local reply, err = client:call({ 'svc', Bus.literal('+') }, 'go', { timeout = LONG_TMO })
	assert(err == nil, tostring(err))
	assert_eq(reply, 'ok')

	print('Lane B concrete-topic enforcement + literal wrapper test passed!')
end

local function test_laneB_endpoint_not_reached_by_publish()
	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local server = bus:connect()
	local client = bus:connect()

	local ep = server:bind({ 'endpoint', 'only' }, { queue_len = 1 })

	client:publish({ 'endpoint', 'only' }, 'x')

	local which, msg, err = select_named({
		msg      = ep:recv_op(),
		deadline = timeout_op(TMO),
	})
	assert_eq(which, 'deadline')
	assert_timeout(msg, err)

	fibers.spawn(function ()
		local req, rerr = ep:recv()
		assert(req, tostring(rerr))
		assert(req:reply('y'))
	end)

	local reply, cerr = client:call({ 'endpoint', 'only' }, 'call', { timeout = LONG_TMO })
	assert(cerr == nil, tostring(cerr))
	assert_eq(reply, 'y')

	print('Lane B endpoint not reached by publish test passed!')
end

local function test_laneB_call_success()
	local bus       = Bus.new({ m_wild = '#', s_wild = '+' })
	local server    = bus:connect({ principal = admin_principal('server') })
	local caller_pr = admin_principal('client')
	local client    = bus:connect({ principal = caller_pr })

	local ep = server:bind({ 'rpc', 'echo' }, { queue_len = 1 })

	fibers.spawn(function ()
		local req, err = ep:recv()
		assert(req, tostring(err))
		assert_eq(req.payload, 'hi')
		assert_local_origin(req.origin, caller_pr)
		assert_origin_immutable(req.origin)
		assert(not req:done(), 'request should not be done before reply')
		assert(req:reply('reply:' .. tostring(req.payload)))
		assert(req:done(), 'request should be done after reply')
		assert(req:reply('again') == false, 'second reply should fail')
	end)

	local reply, err = client:call({ 'rpc', 'echo' }, 'hi', { timeout = LONG_TMO })
	assert(err == nil, tostring(err))
	assert_eq(reply, 'reply:hi')
	ep:unbind()

	print('Lane B call (success) test passed!')
end

local function test_laneB_call_no_route()
	local bus  = Bus.new({ m_wild = '#', s_wild = '+' })
	local conn = bus:connect()

	local reply, err = conn:call({ 'rpc', 'nobody' }, 'hi', { timeout = TMO })
	assert(reply == nil)
	assert_eq(err, 'no_route')

	print('Lane B call (no_route) test passed!')
end

local function test_laneB_call_timeout_no_reply()
	local bus       = Bus.new({ m_wild = '#', s_wild = '+' })
	local server    = bus:connect()
	local caller_pr = admin_principal('timeout-client')
	local client    = bus:connect({ principal = caller_pr })

	local ep      = server:bind({ 'rpc', 'timeout' }, { queue_len = 1 })
	local late_ok = Channel.new(1)

	fibers.spawn(function ()
		local req, err = ep:recv()
		assert(req, tostring(err))
		assert_local_origin(req.origin, caller_pr)
		Sleep.sleep(LONG_TMO)
		late_ok:put(req:reply('late'))
	end)

	local reply, err = client:call({ 'rpc', 'timeout' }, 'slow', { timeout = TMO })
	assert(reply == nil)
	assert_eq(err, 'timeout')

	local which, ok, cherr = select_named({
		late     = late_ok:get_op():wrap(function (v) return v, nil end),
		deadline = timeout_op(LONG_TMO),
	})
	assert_eq(which, 'late')
	assert(cherr == nil)
	assert_eq(ok, false)

	print('Lane B call (timeout no reply) test passed!')
end

local function test_laneB_call_fail_propagates_error()
	local bus    = Bus.new({ m_wild = '#', s_wild = '+' })
	local server = bus:connect()
	local client = bus:connect()

	local ep = server:bind({ 'rpc', 'fail' }, { queue_len = 1 })

	fibers.spawn(function ()
		local req, err = ep:recv()
		assert(req, tostring(err))
		assert(req:fail('boom'))
	end)

	local reply, err = client:call({ 'rpc', 'fail' }, 'x', { timeout = LONG_TMO })
	assert(reply == nil)
	assert_eq(err, 'boom')

	print('Lane B call (fail propagates error) test passed!')
end

--------------------------------------------------------------------------------
-- Scope cancellation as control flow
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
-- Authz / principals
--------------------------------------------------------------------------------

local function test_authz_denies_without_admin_role()
	local bus = Bus.new({
		m_wild      = '#',
		s_wild      = '+',
		authoriser  = new_admin_only_authoriser(),
	})

	local conn_missing = bus:connect()
	local ok1, err1 = pcall(function()
		conn_missing:subscribe({ 'authz', 'deny', 'sub' })
	end)
	assert(not ok1, 'expected subscribe without principal to be denied')
	assert(tostring(err1):match('permission denied'), tostring(err1))

	local conn_viewer = bus:connect({ principal = viewer_principal('alice') })
	local ok2, err2 = pcall(function()
		conn_viewer:publish({ 'authz', 'deny', 'pub' }, 'x')
	end)
	assert(not ok2, 'expected publish without admin role to be denied')
	assert(tostring(err2):match('permission denied'), tostring(err2))

	local ok3, err3 = pcall(function()
		conn_viewer:bind({ 'authz', 'deny', 'bind' })
	end)
	assert(not ok3, 'expected bind without admin role to be denied')
	assert(tostring(err3):match('permission denied'), tostring(err3))

	local ok4, err4 = pcall(function()
		fibers.perform(conn_viewer:call_op({ 'authz', 'deny', 'call' }, 'x', { timeout = TMO }))
	end)
	assert(not ok4, 'expected call_op perform without admin role to be denied')
	assert(tostring(err4):match('permission denied'), tostring(err4))

	local ok5, err5 = pcall(function()
		conn_viewer:watch_retained({ 'authz', 'deny', 'watch' })
	end)
	assert(not ok5, 'expected watch_retained without admin role to be denied')
	assert(tostring(err5):match('permission denied'), tostring(err5))

	local ok6, err6 = pcall(function()
		conn_viewer:retained_view({ 'authz', 'deny', 'view' })
	end)
	assert(not ok6, 'expected retained_view without admin role to be denied')
	assert(tostring(err6):match('permission denied'), tostring(err6))

	local ok7, err7 = pcall(function()
		conn_viewer:derive()
	end)
	assert(ok7, 'expected derive itself not to be authoriser-gated: ' .. tostring(err7))

	print('Authz deny test passed!')
end

local function test_authz_admin_allows_and_records_actions()
	local seen = {}
	local bus = Bus.new({
		m_wild      = '#',
		s_wild      = '+',
		authoriser  = new_admin_only_authoriser(seen),
	})

	local admin1 = admin_principal('root')
	local admin2 = admin_principal('helper')

	local conn1 = bus:connect({ principal = admin1 })
	local conn2 = bus:connect({ principal = admin2 })

	assert(conn1:principal() == admin1, 'expected principal() to return supplied principal')

	local sub = conn1:subscribe({ 'authz', 'pubsub' })
	conn1:publish({ 'authz', 'pubsub' }, 'hello')
	do
		local msg, err = fibers.perform(sub:recv_op())
		assert(err == nil and msg and msg.payload == 'hello')
		assert_local_origin(msg.origin, admin1)
	end

	conn1:retain({ 'authz', 'retained' }, 'R')
	local sub_ret = conn1:subscribe({ 'authz', 'retained' })
	do
		local msg, err = fibers.perform(sub_ret:recv_op())
		assert(err == nil and msg and msg.payload == 'R')
	end
	conn1:unretain({ 'authz', 'retained' })
	local sub_ret2 = conn1:subscribe({ 'authz', 'retained' })
	do
		local which, msg, err = select_named({
			msg      = sub_ret2:recv_op(),
			deadline = timeout_op(TMO),
		})
		assert_eq(which, 'deadline')
		assert_timeout(msg, err)
	end

	local rw = conn1:watch_retained({ 'authz', 'watch' }, { replay = true })

	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev, tostring(err))
		assert_bus_replay_done(ev)
	end

	conn1:retain({ 'authz', 'watch' }, 'W')

	do
		local ev, err = fibers.perform(rw:recv_op())
		assert(err == nil and ev and ev.op == 'retain')
		assert_eq(ev.payload, 'W')
		assert_local_origin(ev.origin, admin1)
	end

	rw:unwatch()

	local view = conn1:retained_view({ 'authz', 'view' })
	conn1:retain({ 'authz', 'view' }, 'VW')
	do
		local msg = view:get({ 'authz', 'view' })
		assert(msg and msg.payload == 'VW', 'expected authorised retained view to observe retained state')
	end
	view:close()

	local rpc_ep = conn2:bind({ 'authz', 'rpc' }, { queue_len = 1 })
	fibers.spawn(function()
		local req, err = rpc_ep:recv()
		assert(req, tostring(err))
		assert_local_origin(req.origin, admin1)
		assert(req:reply('rpc:' .. tostring(req.payload)))
	end)
	do
		local reply, err = conn1:call({ 'authz', 'rpc' }, 'C', { timeout = LONG_TMO })
		assert(err == nil, tostring(err))
		assert_eq(reply, 'rpc:C')
	end
	rpc_ep:unbind()

	local actions = {}
	for i = 1, #seen do
		local ctx = seen[i]
		actions[ctx.action] = true
	end

	assert(actions.subscribe,      'expected subscribe action to be authorised')
	assert(actions.publish,        'expected publish action to be authorised')
	assert(actions.retain,         'expected retain action to be authorised')
	assert(actions.unretain,       'expected unretain action to be authorised')
	assert(actions.watch_retained, 'expected watch_retained action to be authorised')
	assert(actions.bind,           'expected bind action to be authorised')
	assert(actions.call,           'expected call action to be authorised')

	print('Authz admin allow/record test passed!')
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
	test_disconnect_detaches_connection_finaliser()

	test_unsubscribe()

	test_retained_msg_basic()
	test_retained_query_wildcards_and_unretain()

	test_retained_watch_replay_and_live_changes()
	test_retained_watch_no_replay()
	test_retained_watch_wildcards_and_literals()
	test_retained_watch_unwatch_and_disconnect()
	test_retained_watch_bounded_queue()
	test_retained_watch_replay_done_empty()
	test_retained_watch_replay_overflow_closes_watch()

	test_feed_distinct_metatables_and_method_guards()
	test_feed_closed_op_reason()

	test_retained_view_empty_replay_ready()
	test_retained_view_replay_snapshot_and_live_changes()
	test_retained_view_wildcards_and_literal_tokens()
	test_retained_view_changed_op_integer_validation()
	test_retained_view_close_and_disconnect()
	test_retained_view_scope_cleanup()
	test_retained_view_no_queue_overflow_or_background_fibre()

	test_origin_factory_function_and_extra_on_publish()
	test_origin_conn_ids_distinct_between_connections()
	test_retained_replay_preserves_original_origin()
	test_retained_view_preserves_original_origin()
	test_unretain_event_origin_metadata()
	test_call_request_origin_factory_and_extra()

	test_derive_inherits_principal_but_not_origin_factory()
	test_derive_allows_override_principal_and_origin_factory()
	test_derive_disconnected_errors()
	test_derive_connection_scope_cleanup()

	test_request_abandon_after_timeout_rejects_late_reply()
	test_request_done_state_for_fail_and_abandon()
	test_request_status_and_done_op_for_all_terminal_states()
	test_call_op_abort_abandons_request_and_wakes_done_op()
	test_call_op_timeout_false_has_no_hidden_deadline()
	test_call_no_timeout_options_survive_default_deadline()

	test_q_overflow_drop_oldest_default()
	test_q_overflow_reject_newest_override()
	test_zero_len_subscription_is_bounded()
	test_reject_block_policy()

	test_wildcard()
	test_literal_tokens_in_pubsub()
	test_multi_wildcard_must_be_last()

	test_laneA_fanout_independent_backpressure()

	test_laneB_call_full_and_closed_modalities()
	test_laneB_concrete_topic_enforcement_and_literal_ok()
	test_laneB_endpoint_not_reached_by_publish()
	test_laneB_call_success()
	test_laneB_call_no_route()
	test_laneB_call_timeout_no_reply()
	test_laneB_call_fail_propagates_error()

	test_scope_cancellation_terminates_waits()
	test_authz_denies_without_admin_role()
	test_authz_admin_allows_and_records_actions()

	print('ALL TESTS PASSED!')
end)
