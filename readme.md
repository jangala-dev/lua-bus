# `lua-bus`

## Overview

The `bus` module provides a bounded, in-process messaging fabric built on `fibers` and trie-based topic matching.

It is intended for cooperative, single-threaded systems running under `fibers.run(...)`, where:

- delivery is **bounded**: every subscription, retained watch, and endpoint has a finite queue, which may be zero-length
- routing is **non-blocking**: a slow consumer does not stall publishers or callers
- retained observation can be either **feed-based** or **materialised** through a versioned retained view

The bus exposes two public planes:

- a **state/event plane** for publish/subscribe, retained state, retained lifecycle feeds, and retained materialised views
- a **command plane** for concrete point-to-point request/reply

That gives a small programming model:

- **publish** facts
- **retain** current truth
- **observe** retained truth
- **call** owned actions

The bus also attaches immutable, bus-owned provenance (`origin`) to delivered values, so higher layers such as `fabric` can reason about source and session without relying on payload conventions.

## Internal indexes

The bus uses trie-based topic indexes:

- a **pubsub trie** for ordinary subscriptions
  Wildcards are allowed in stored subscription patterns; published topics are concrete queries.

- a **retained trie** for retained state
  Retained topics are stored as concrete keys; subscriptions, retained watches, and retained views may query with wildcards.

- a **retained-watch trie** for retained lifecycle feeds
  Wildcards are allowed in stored watch patterns; retained writes and removals are concrete queries.

- a **retained-view trie** for retained materialised views
  Wildcards are allowed in stored view patterns; retained writes and removals are concrete queries.

Retained materialised views observe retained state directly and maintain a local versioned snapshot for event-driven assertions and observation.

Endpoints are stored separately in a concrete-topic registry.

## Core concepts

### Bus

The shared router, retained store, endpoint registry, retained-watch registry, and retained-view observer owner.

### Connection

The capability handed to services. A connection is scope-bound by default: when the current scope exits, the connection is disconnected automatically.

A connection may also derive a sibling connection on the same bus with `conn:derive(opts)`.

### Subscription

A bounded mailbox receiving ordinary published `Message` values.

### RetainedWatch

A bounded mailbox receiving retained-state lifecycle `RetainedEvent` values.

### RetainedView

A versioned materialised view of retained state matching a topic pattern.

Unlike `RetainedWatch`, a retained view is not a queue. It does not expose every retained lifecycle event. It maintains the latest matching retained messages and provides `changed_op(last_seen)` so callers can wait for the view version to change or for the view to close.

This is useful for tests, probes, health reporting, and components that care about current retained truth rather than every lifecycle event.

### Endpoint

A bounded mailbox receiving concrete point-to-point `Request` values.

### Message

Delivered to ordinary subscriptions and stored in retained views:

```lua
{
  topic   = <Topic>,
  payload = <any>,
  origin  = <Origin>,
}
````

### RetainedEvent

Delivered to retained watches:

```lua
{
  op      = 'retain' | 'unretain' | 'replay_done',
  topic   = <Topic|nil>,
  payload = <any|nil>,
  origin  = <Origin|nil>,
}
```

`replay_done` is a synthetic control event emitted only for retained watches created with `replay = true`.

### Request

Delivered to bound endpoints:

```lua
{
  topic   = <Topic>,
  payload = <any>,
  origin  = <Origin>,

  reply   = function(self, value) ... end,
  fail    = function(self, err) ... end,
  abandon = function(self, reason) ... end,
  done    = function(self) ... end,
}
```

A `Request` is the command-plane reply mechanism. There are no public reply topics.

### Origin

Immutable provenance attached by the bus:

```lua
{
  kind       = <string>,
  conn_id    = <string|nil>,
  principal  = <any|nil>,
  link_id    = <string|nil>,
  peer_node  = <string|nil>,
  peer_sid   = <string|nil>,
  generation = <integer|nil>,
  extra      = <table|nil>,
}
```

For ordinary local traffic, only some fields are populated. Fields such as `link_id`, `peer_node`, `peer_sid`, and `generation` are intended for provenance-aware federation layers such as `fabric`.

The bus owns trusted provenance fields. Callers may attach only `origin.extra` through publish, retain, unretain, and call options.

## Assumptions and semantics

* Use from within `fibers.run(...)`.
* Delivery is always bounded.
* The bus never uses blocking queue policy.
* Slow consumers lose data according to their mailbox policy; they do not stall the system.
* Timeouts are not built into subscriptions, retained watches, or retained views; compose them externally with `fibers.choice`, `fibers.named_choice`, and `fibers.sleep.sleep_op(...)`.
* `call(...)` supports timeout/deadline directly because bounded request/reply is part of the command plane.
* Origins are immutable once created.
* `origin.extra`, if present, is immutable too.
* Retained views are versioned snapshots, not event logs.
* Retained views do not use a mailbox and therefore do not have queue overflow semantics.
* A retained view’s `changed_op(last_seen)` is close-aware.

## Topics, wildcards, and literals

A topic is a dense token array, for example:

```lua
{ 'net', 'link' }
```

Tokens must be strings or numbers.

### Wildcards

Subscription, retained-watch, and retained-view patterns may use:

* single-level wildcard: `+`
* multi-level wildcard: `#`

These tokens are configurable with `s_wild` and `m_wild`.

### Literal wildcard tokens

If the wildcard symbols must be treated as ordinary literal topic elements, wrap them with `Bus.literal(...)`:

```lua
local Bus = require 'bus'

local topic = { 'cfg', Bus.literal('+') }
```

A literal wildcard token is treated as concrete for:

* endpoint binding
* point-to-point calls
* ordinary literal matching
* retained matching
* retained views

## Delivery, queues, and full policy

Each subscription, retained watch, and endpoint mailbox has:

* `queue_len` — integer, `>= 0`
* `full` policy:

  * `"drop_oldest"` default
  * `"reject_newest"`

`"block"` is rejected. The bus is intentionally bounded and non-blocking.

Retained views are not mailboxes and do not take queue policy for delivery. They hold the current matching retained state and expose changes through a version counter.

### Queue length `0`

A queue length of `0` creates rendezvous-style delivery:

* delivery succeeds only if a receiver is waiting at send time
* otherwise the item is rejected by mailbox policy

This can be useful for highly transient traffic where stale queueing is undesirable.

### Drop accounting

Drops are tracked per queued handle and in aggregate:

* `sub:dropped()`
* `watch:dropped()`
* `ep:dropped()`
* `conn:dropped()`
* `bus:stats().dropped`

The count includes both:

* buffered evictions under `"drop_oldest"`
* admission failures under `"reject_newest"`

Retained views do not contribute to drop counts.

## State/event plane

The state/event plane consists of:

* `publish`
* `retain`
* `unretain`
* `subscribe`
* `watch_retained`
* `retained_view`

### Ordinary publish

An ordinary publish fans out to matching subscriptions only.

Publishing is best-effort per subscriber:

* the bus attempts one immediate enqueue into each matching subscription mailbox
* if that subscriber cannot accept the item, it is dropped for that subscriber

### Retained state

A retained write:

```lua
conn:retain({ 'fw', 'version' }, '1.2.3')
```

does three things:

1. publishes the message to matching ordinary subscriptions
2. stores the retained value under that concrete topic
3. updates matching retained watches and retained views

A retained removal:

```lua
conn:unretain({ 'fw', 'version' })
```

removes the retained value and updates matching retained watches and retained views. It does not publish an ordinary message.

### Retained lifecycle feeds

Retained watches receive `RetainedEvent` values when retained state is written or removed.

On retain:

```lua
{
  op      = 'retain',
  topic   = ...,
  payload = ...,
  origin  = ...,
}
```

On unretain:

```lua
{
  op      = 'unretain',
  topic   = ...,
  payload = nil,
  origin  = ...,
}
```

On replay completion:

```lua
{
  op      = 'replay_done',
  topic   = nil,
  payload = nil,
  origin  = { kind = 'bus', ... },
}
```

Retained watches are independent of ordinary subscriptions and retained views.

### Replay on retained watch creation

`watch_retained(...)` accepts `replay = true`, which emits synthetic `retain` events for the current retained values matching the pattern, followed by exactly one synthetic `replay_done` event.

This marker means the initial replay scan has completed. It does **not** imply global quiescence. Live retained updates may occur while replay is in progress and may be observed before or after `replay_done`, depending on timing.

If the bus cannot deliver `replay_done`, the watch is closed rather than silently dropping the marker.

### Retained materialised views

A retained view gives a current snapshot of retained state matching a pattern:

```lua
local view = conn:retained_view({ 'config', '#' })
```

It is immediately initialised from the retained trie and then kept current by subsequent `retain` and `unretain` operations.

A view has a monotonically increasing integer version:

```lua
local version = view:version()
```

Wait for it to change or close:

```lua
local new_version, err = fibers.perform(view:changed_op(version))

if new_version then
  version = new_version
else
  print('view closed:', err)
end
```

The return shape is:

```text
version, nil  -- retained view changed
nil, reason   -- retained view closed
```

Read one retained message:

```lua
local msg = view:get({ 'config', 'network' })
if msg then
  print(msg.payload)
end
```

Read all retained messages currently in the view:

```lua
local snapshot = view:snapshot()
```

`snapshot()` returns an array of `Message` objects. It does not expose the bus’s internal topic-key map. The array is sorted deterministically by the internal topic key so that tests and diagnostics can compare it reliably.

`items()` is an alias for `snapshot()` and also returns an array.

A retained view is useful when the consumer wants current truth rather than a lifecycle event stream. Changes may be coalesced between observations; the version tells you that the view changed, not how many individual retained operations occurred.

## Command plane

The command plane consists of:

* `bind`
* `call`

A call targets exactly one concrete endpoint topic.

Endpoint handlers do not reply by publishing to reply topics. They receive a `Request` object and complete it directly with:

* `req:reply(value)`
* `req:fail(err)`

The bus may also abandon a request internally when the caller times out or aborts. Endpoint code should therefore treat `req:reply(...) == false` or `req:fail(...) == false` as a normal outcome.

This keeps request/reply separate from ordinary pub/sub.

### Endpoint delivery

Bound endpoints are point-to-point and admission-signalled:

* if no endpoint is bound, the call fails with `no_route`
* if the endpoint queue is full, the call fails with `full`
* if the endpoint closes before replying, the caller sees `closed`
* if the deadline expires first, the caller sees `timeout`

Exact error values depend on the close reason, but these are the intended categories.

## Derived connections

A connection can derive a sibling connection on the same bus:

```lua
local child = conn:derive()
```

This is useful when one component needs more than one connection with different roles or provenance decoration, without exposing the bus object itself.

By default:

* the derived connection uses the same bus
* the derived connection inherits `principal`
* the derived connection does **not** automatically inherit provenance decoration unless you pass it explicitly

Example:

```lua
local peer_conn = conn:derive{
  origin_factory = function ()
    return {
      kind       = 'fabric_import',
      link_id    = 'link-1',
      peer_node  = 'peer-a',
      peer_sid   = 'sid-42',
      generation = 7,
    }
  end,
}
```

## Installation

Dependencies:

* `fibers`
* `trie` with pubsub and retained support
* `trie.literal`
* `uuid`

Load with:

```lua
local Bus = require 'bus'
```

## API summary

### Bus

* `Bus.new(params?) -> bus`
* `bus:connect(opts?) -> conn`
* `bus:stats() -> table`
* `Bus.literal(v) -> literal_token`

Constructor parameters:

```lua
{
  q_length?   = integer,
  full?       = "drop_oldest" | "reject_newest",
  s_wild?     = string | number,
  m_wild?     = string | number,
  authoriser? = function | table,
}
```

`connect` options:

```lua
{
  principal?      = any,
  origin_factory? = table|function|nil,
  origin_base?    = table|function|nil,
}
```

`origin_factory` and `origin_base` are aliases for the trusted provenance source used when the bus builds `origin`.

### Connection

State/event plane:

* `conn:publish(topic, payload[, opts]) -> true`
* `conn:retain(topic, payload[, opts]) -> true`
* `conn:unretain(topic[, opts]) -> true`
* `conn:subscribe(topic[, opts]) -> sub`
* `conn:unsubscribe(sub) -> true`
* `conn:watch_retained(topic[, opts]) -> watch`
* `conn:unwatch_retained(watch) -> true`
* `conn:retained_view(topic[, opts]) -> view`

Command plane:

* `conn:bind(topic[, opts]) -> endpoint`
* `conn:unbind(endpoint) -> true`
* `conn:call_op(topic, payload[, opts]) -> Op`
* `conn:call(topic, payload[, opts]) -> value|nil, err|nil`

Lifecycle and stats:

* `conn:derive([opts]) -> conn`
* `conn:disconnect() -> true`
* `conn:is_disconnected() -> boolean`
* `conn:principal() -> any|nil`
* `conn:dropped() -> integer`
* `conn:stats() -> table`

Options for `publish`, `retain`, `unretain`, and `call`:

```lua
{
  extra? = table,
}
```

Additional options for `call`:

```lua
{
  timeout?  = number,
  deadline? = number,
}
```

`extra` becomes immutable `origin.extra` on delivered objects.

### Subscription

* `sub:recv_op() -> Op` yielding `(Message|nil, err|string|nil)`
* `sub:recv() -> Message|nil, err|string|nil`
* `sub:unsubscribe() -> true`
* `sub:close() -> true`
* `sub:closed_op() -> Op` yielding `reason`
* `sub:iter() -> iterator<Message>`
* `sub:payloads() -> iterator<any>`
* `sub:why() -> any|nil`
* `sub:dropped() -> integer`
* `sub:kind() -> "subscription"`
* `sub:topic() -> Topic`
* `sub:stats() -> table`

### RetainedWatch

* `watch:recv_op() -> Op` yielding `(RetainedEvent|nil, err|string|nil)`
* `watch:recv() -> RetainedEvent|nil, err|string|nil`
* `watch:unwatch() -> true`
* `watch:close() -> true`
* `watch:closed_op() -> Op` yielding `reason`
* `watch:iter() -> iterator<RetainedEvent>`
* `watch:why() -> any|nil`
* `watch:dropped() -> integer`
* `watch:kind() -> "retained_watch"`
* `watch:topic() -> Topic`
* `watch:stats() -> table`

### RetainedView

* `view:version() -> integer`
* `view:changed_op(last_seen) -> Op` yielding `(new_version|nil, err|string|nil)`
* `view:get(topic) -> Message|nil`
* `view:snapshot() -> Message[]`
* `view:items() -> Message[]`
* `view:close() -> true`
* `view:closed_op() -> Op` yielding `reason`

`changed_op(last_seen)` requires `last_seen` to be an integer. If the view version already differs from `last_seen`, it is ready immediately.

Return shape:

```text
new_version, nil  -- changed
nil, reason       -- closed
```

`snapshot()` and `items()` return an array, not a map. Each entry is a `Message`:

```lua
{
  topic   = <Topic>,
  payload = <any>,
  origin  = <Origin>,
}
```

### Endpoint

* `ep:recv_op() -> Op` yielding `(Request|nil, err|string|nil)`
* `ep:recv() -> Request|nil, err|string|nil`
* `ep:iter() -> iterator<Request>`
* `ep:unbind() -> true`
* `ep:close() -> true`
* `ep:closed_op() -> Op` yielding `reason`
* `ep:why() -> any|nil`
* `ep:dropped() -> integer`
* `ep:kind() -> "endpoint"`
* `ep:topic() -> Topic`
* `ep:stats() -> table`

### Request

* `req.topic`
* `req.payload`
* `req.origin`
* `req:reply(value) -> boolean`
* `req:fail(err) -> boolean`
* `req:abandon(reason) -> boolean`
* `req:done() -> boolean`

A request may be completed once only.

## Usage

### Create a bus

```lua
local Bus = require 'bus'

local bus = Bus.new{
  q_length = 10,
  full     = 'drop_oldest',
  s_wild   = '+',
  m_wild   = '#',
}
```

With authorisation:

```lua
local bus = Bus.new{
  authoriser = my_authoriser,
}
```

### Connect

Connections are scope-bound by default:

```lua
local conn = bus:connect()
```

With a principal:

```lua
local conn = bus:connect{
  principal = my_principal,
}
```

With provenance decoration:

```lua
local conn = bus:connect{
  principal = my_principal,
  origin_factory = function ()
    return {
      kind       = 'fabric_import',
      link_id    = 'link-1',
      peer_node  = 'peer-a',
      peer_sid   = 'sid-42',
      generation = 7,
    }
  end,
}
```

### Derive a sibling connection

```lua
local child = conn:derive()
```

Override provenance on the derived connection:

```lua
local imported = conn:derive{
  origin_factory = {
    kind       = 'fabric_import',
    link_id    = 'link-1',
    peer_node  = 'peer-a',
    peer_sid   = 'sid-42',
    generation = 7,
  },
}
```

### Publish

```lua
conn:publish({ 'net', 'link' }, { ifname = 'eth0', up = true })
```

With extra provenance fields:

```lua
conn:publish({ 'net', 'link' }, { ifname = 'eth0', up = true }, {
  extra = { trace_id = 'abc123' },
})
```

Consume it:

```lua
local sub = conn:subscribe({ 'net', 'link' })
local msg, err = sub:recv()

if msg then
  print(msg.payload.ifname, msg.payload.up)
  print(msg.origin.kind)
else
  print('closed:', err)
end
```

### Retain and unretain

```lua
conn:retain({ 'fw', 'version' }, '1.2.3')
conn:unretain({ 'fw', 'version' })
```

### Subscribe

```lua
local sub = conn:subscribe({ 'fw', '#' })
```

Override queue settings:

```lua
local sub = conn:subscribe(
  { 'net', '+' },
  { queue_len = 50, full = 'reject_newest' }
)
```

Rendezvous subscription:

```lua
local sub = conn:subscribe(
  { 'events', 'transient' },
  { queue_len = 0, full = 'reject_newest' }
)
```

### Compose a timeout

Subscriptions, retained watches, and retained views do not have built-in timeouts. Compose them externally:

```lua
local fibers = require 'fibers'
local sleep  = require 'fibers.sleep'

local which, msg, err = fibers.perform(fibers.named_choice{
  msg      = sub:recv_op(),
  deadline = sleep.sleep_op(1.0):wrap(function ()
    return nil, 'timeout'
  end),
})
```

For a retained view, the changed arm itself may also report closure:

```lua
local which, new_version, err = fibers.perform(fibers.named_choice{
  changed  = view:changed_op(version),
  deadline = sleep.sleep_op(1.0):wrap(function ()
    return nil, 'timeout'
  end),
})

if which == 'changed' and new_version then
  version = new_version
elseif which == 'changed' then
  print('view closed:', err)
else
  print('timed out')
end
```

### Watch retained state

```lua
local watch = conn:watch_retained({ 'config', '#' }, {
  queue_len = 16,
  full      = 'drop_oldest',
  replay    = true,
})
```

Consume retained events:

```lua
local ev, err = watch:recv()
if ev then
  if ev.op == 'retain' then
    print('updated:', ev.payload)
  elseif ev.op == 'unretain' then
    print('removed:', table.concat(ev.topic, '/'))
  elseif ev.op == 'replay_done' then
    print('initial retained replay complete')
  end
else
  print('watch closed:', err)
end
```

### Use a retained view

```lua
local view = conn:retained_view({ 'config', '#' })

local version = view:version()
local snapshot = view:snapshot()

local msg = view:get({ 'config', 'network' })
if msg then
  print('network config:', msg.payload)
end

local new_version, err = fibers.perform(view:changed_op(version))
if new_version then
  print('retained view changed:', new_version)
else
  print('retained view closed:', err)
end
```

Iterate over a snapshot:

```lua
for _, msg in ipairs(view:snapshot()) do
  print(table.concat(msg.topic, '/'), msg.payload)
end
```

A typical loop:

```lua
local version = view:version()

while true do
  local which, new_version_or_status, err_or_reason = fibers.perform(fibers.named_choice{
    changed = view:changed_op(version),
    stop    = scope:cancel_op(),
  })

  if which == 'stop' then
    return
  end

  if new_version_or_status == nil then
    local close_reason = err_or_reason
    return nil, close_reason
  end

  version = new_version_or_status

  local snapshot = view:snapshot()
  -- Recompute from current retained truth.
end
```

Because `changed_op(...)` is close-aware, most loops do not need to race it separately against `view:closed_op()`. `closed_op()` remains useful when code only cares about closure.

### Bind an endpoint and handle requests

```lua
local ep = conn:bind({ 'rpc', 'echo' }, { queue_len = 1 })

fibers.spawn(function ()
  while true do
    local req, err = ep:recv()
    if not req then
      return
    end

    req:reply('echo:' .. tostring(req.payload))
  end
end)
```

Fail explicitly:

```lua
req:fail('not_supported')
```

### Call an endpoint

```lua
local value, err = conn:call({ 'rpc', 'echo' }, 'hello', { timeout = 1.0 })

if err == nil then
  print(value)
else
  print('call failed:', err)
end
```

With extra provenance fields:

```lua
local value, err = conn:call({ 'rpc', 'echo' }, 'hello', {
  timeout = 1.0,
  extra   = { trace_id = 'abc123' },
})
```

Point-to-point topics must be concrete. Wildcards are rejected, though literal wildcard tokens wrapped with `Bus.literal(...)` are allowed.

### Unsubscribe, unwatch, close, unbind, disconnect

```lua
sub:unsubscribe()
watch:unwatch()
view:close()
ep:unbind()
conn:disconnect()
```

Queued feed handles also support `close()` as a generic close operation.

All are intended to be idempotent and to wake blocked receivers promptly.

## Authorisation

The bus may be constructed with an optional authoriser.

Supported forms:

* `function(ctx) -> boolean|nil, reason?`
* table with `:allow(ctx)`
* table with `:authorize(ctx)`

The authoriser receives a context such as:

```lua
{
  bus       = bus,
  principal = principal,
  action    = action,
  topic     = topic,
  extra     = extra,
}
```

Actions are:

* `publish`
* `retain`
* `unretain`
* `subscribe`
* `watch_retained`
* `bind`
* `call`

In the current implementation, `retained_view(...)` is authorised using the same `watch_retained` action as retained lifecycle watches, because both are retained-state observation capabilities.

If authorisation fails, the attempted operation raises an error.

## Stats

`conn:stats()` returns a table such as:

```lua
{
  dropped          = ...,
  subscriptions    = ...,
  endpoints        = ...,
  retained_watches = ...,
  retained_views   = ...,
}
```

`bus:stats()` returns a table such as:

```lua
{
  connections      = ...,
  dropped          = ...,
  queue_len        = ...,
  full_policy      = ...,
  s_wild           = ...,
  m_wild           = ...,
  retained_watches = ...,
  retained_views   = ...,
  endpoints        = ...,
}
```

Retained views are observational objects and do not contribute to queue drop counts.

## Notes and limitations

* Delivery is best-effort and bounded; drops under load are expected.
* Retained replay to new subscriptions is bounded and best-effort.
* Retained-watch replay is bounded; if the bus cannot deliver the terminal `replay_done` marker, the watch is closed.
* Retained views represent current retained truth, not every lifecycle event.
* Retained views coalesce changes by design; use retained watches when every retain/unretain event matters.
* Retained view snapshots are arrays of `Message` objects, not maps keyed by internal topic strings.
* `changed_op(last_seen)` is close-aware and returns either a new version or a close reason.
* `full = "block"` is intentionally unsupported.
* Ordinary subscriptions observe published messages.
* Retained watches observe retained-state lifecycle.
* Retained views observe retained-state truth.
* Endpoints carry command requests only; they are not part of ordinary pub/sub.
* Origin metadata is part of bus semantics, not an application payload convention.
* `derive()` creates a sibling connection without exposing the bus object.

## Design summary

The bus intentionally exposes a small set of interaction styles.

### State/event plane

* `publish`
* `retain`
* `unretain`
* `subscribe`
* `watch_retained`
* `retained_view`

### Command plane

* `bind`
* `call`

That gives a small, teachable model:

* publish facts
* retain current truth
* observe current truth or retained lifecycle
* call owned actions

while keeping provenance available for observability, policy, and federation layers such as `fabric`.
