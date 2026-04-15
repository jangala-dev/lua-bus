# `lua-bus`

## Overview

The `bus` module provides a bounded, in-process messaging fabric built on `fibers` and trie-based topic matching.

It is intended for cooperative, single-threaded systems running under `fibers.run(...)`, where:

- delivery is **bounded**: every subscription, retained watch, and endpoint has a finite queue, which may be zero-length
- routing is **non-blocking**: a slow consumer does not stall publishers or callers

The bus exposes two public planes only:

- a **state/event plane** for publish/subscribe and retained state
- a **command plane** for concrete point-to-point request/reply

That gives a small, explicit programming model:

- **publish** facts
- **retain** current truth
- **call** owned actions

## Internal indexes

The bus uses three tries:

- a **pubsub trie** for ordinary subscriptions
  wildcards are allowed in stored subscription patterns; published topics are concrete queries

- a **retained trie** for retained state
  retained topics are stored as concrete keys; subscriptions and retained watches may query with wildcards

- a **retained-watch trie** for retained lifecycle feeds
  wildcards are allowed in stored watch patterns; retained writes and removals are concrete queries

Endpoints are stored separately in a concrete-topic registry.

## Core concepts

### Bus

The shared router, retained store, endpoint registry, and retained-watch registry.

### Connection

The capability handed to services. A connection is scope-bound by default: when the current scope exits, the connection is disconnected automatically.

### Subscription

A bounded mailbox receiving ordinary published `Message` values.

### RetainedWatch

A bounded mailbox receiving retained-state lifecycle `RetainedEvent` values.

### Endpoint

A bounded mailbox receiving concrete point-to-point `Request` values.

### Message

Delivered to ordinary subscriptions:

```lua
{
  topic   = <Topic>,
  payload = <any>,
  origin  = <Origin>,
}
```

### RetainedEvent

Delivered to retained watches:

```lua
{
  op      = 'retain' | 'unretain' | 'replay_done',
  topic   = <Topic|nil>,
  payload = <any|nil>,
  origin  = <Origin>,
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

  reply = function(self, value) ... end,
  fail  = function(self, err) ... end,
  done  = function(self) ... end,
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

## Assumptions and semantics

- Use from within `fibers.run(...)`.
- Delivery is always bounded.
- The bus never uses blocking queue policy.
- Slow consumers lose data according to their mailbox policy; they do not stall the system.
- Timeouts are not built into subscriptions or retained watches; compose them externally with `fibers.choice`, `fibers.named_choice`, and `fibers.sleep.sleep_op(...)`.
- `call(...)` supports timeout/deadline directly because bounded request/reply is part of the command plane.

## Topics, wildcards, and literals

A topic is a dense token array, for example:

```lua
{ 'net', 'link' }
```

Tokens must be strings or numbers.

### Wildcards

Subscription and retained-watch patterns may use:

- single-level wildcard: `+`
- multi-level wildcard: `#`

These tokens are configurable with `s_wild` and `m_wild`.

### Literal wildcard tokens

If the wildcard symbols must be treated as ordinary literal topic elements, wrap them with `bus.literal(...)`:

```lua
local Bus = require 'bus'

local topic = { 'cfg', Bus.literal('+') }
```

A literal wildcard token is treated as concrete for:

- endpoint binding
- point-to-point calls
- ordinary literal matching

## Delivery, queues, and full policy

Each subscription, retained watch, and endpoint mailbox has:

- `queue_len` — integer, `>= 0`
- `full` policy:
  - `"drop_oldest"` (default)
  - `"reject_newest"`

`"block"` is rejected. The bus is intentionally bounded and non-blocking.

### Queue length `0`

A queue length of `0` creates rendezvous-style delivery:

- delivery succeeds only if a receiver is waiting at send time
- otherwise the item is rejected by mailbox policy

This can be useful for highly transient traffic where stale queueing is undesirable.

### Drop accounting

Drops are tracked per handle and in aggregate:

- `sub:dropped()`
- `watch:dropped()`
- `ep:dropped()`
- `conn:dropped()`
- `bus:stats().dropped`

The count includes both:

- buffered evictions under `"drop_oldest"`, and
- admission failures under `"reject_newest"`

## State/event plane

The state/event plane consists of:

- `publish`
- `retain`
- `unretain`
- `subscribe`
- `watch_retained`

### Ordinary publish

An ordinary publish fans out to matching subscriptions only.

Publishing is best-effort per subscriber:

- the bus attempts one immediate enqueue into each matching subscription mailbox
- if that subscriber cannot accept the item, it is dropped for that subscriber

### Retained state

A retained write:

```lua
conn:retain({ 'fw', 'version' }, '1.2.3')
```

does two things:

1. it publishes the message to matching ordinary subscriptions
2. it stores the retained value under that concrete topic

A retained removal:

```lua
conn:unretain({ 'fw', 'version' })
```

removes the retained value. It does not publish an ordinary message.

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
  op     = 'unretain',
  topic  = ...,
  origin = ...,
}
```

On replay completion:

```lua
{
  op     = 'replay_done',
  topic  = nil,
  origin = { kind = 'bus', ... },
}
```

Retained watches are independent of ordinary subscriptions.

### Replay on retained watch creation

`watch_retained(...)` accepts `replay = true`, which emits synthetic `retain` events for the current retained values matching the pattern, followed by exactly one synthetic `replay_done` event.

This marker means the initial replay scan has completed. It does **not** imply global quiescence. Live retained updates may occur while replay is in progress and may be observed before or after `replay_done`, depending on timing.

If the bus cannot deliver `replay_done`, the watch is closed rather than silently dropping the marker.

## Command plane

The command plane consists of:

- `bind`
- `call`

A call targets exactly one concrete endpoint topic.

Endpoint handlers do not reply by publishing to reply topics. They receive a `Request` object and complete it directly with:

- `req:reply(value)`, or
- `req:fail(err)`

This keeps request/reply separate from ordinary pub/sub.

### Endpoint delivery

Bound endpoints are point-to-point and admission-signalled:

- if no endpoint is bound, the call fails with `no_route`
- if the endpoint queue is full, the call fails with `full`
- if the endpoint closes before replying, the caller sees `closed`
- if the deadline expires first, the caller sees `timeout`

Exact error values depend on the bus implementation, but these are the intended categories.

## Installation

Dependencies:

- `fibers`
- `trie` with pubsub and retained support
- `trie.literal`
- `uuid`

Load with:

```lua
local Bus = require 'bus'
```

## API summary

### Bus

- `Bus.new(params?) -> bus`
- `bus:connect(opts?) -> conn`
- `bus:stats() -> table`
- `Bus.literal(v) -> literal_token`

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
  principal?   = any,
  origin_base? = table|nil,
}
```

### Connection

State/event plane:

- `conn:publish(topic, payload[, opts]) -> true`
- `conn:retain(topic, payload[, opts]) -> true`
- `conn:unretain(topic[, opts]) -> true`
- `conn:subscribe(topic[, opts]) -> sub`
- `conn:unsubscribe(sub) -> true`
- `conn:watch_retained(topic[, opts]) -> watch`
- `conn:unwatch_retained(watch) -> true`

Command plane:

- `conn:bind(topic[, opts]) -> endpoint`
- `conn:unbind(endpoint) -> true`
- `conn:call_op(topic, payload[, opts]) -> Op`
- `conn:call(topic, payload[, opts]) -> value|nil, err|nil`

Lifecycle and stats:

- `conn:disconnect() -> true`
- `conn:is_disconnected() -> boolean`
- `conn:principal() -> any|nil`
- `conn:dropped() -> integer`
- `conn:stats() -> table`

### Subscription

- `sub:recv_op() -> Op` yielding `(Message|nil, err|string|nil)`
- `sub:recv() -> Message|nil, err|string|nil`
- `sub:unsubscribe() -> true`
- `sub:iter() -> iterator<Message>`
- `sub:payloads() -> iterator<any>`
- `sub:why() -> any|nil`
- `sub:dropped() -> integer`
- `sub:topic() -> Topic`
- `sub:stats() -> table`

### RetainedWatch

- `watch:recv_op() -> Op` yielding `(RetainedEvent|nil, err|string|nil)`
- `watch:recv() -> RetainedEvent|nil, err|string|nil`
- `watch:unwatch() -> true`
- `watch:iter() -> iterator<RetainedEvent>`
- `watch:why() -> any|nil`
- `watch:dropped() -> integer`
- `watch:topic() -> Topic`
- `watch:stats() -> table`

### Endpoint

- `ep:recv_op() -> Op` yielding `(Request|nil, err|string|nil)`
- `ep:recv() -> Request|nil, err|string|nil`
- `ep:iter() -> iterator<Request>`
- `ep:unbind() -> true`
- `ep:why() -> any|nil`
- `ep:dropped() -> integer`
- `ep:topic() -> Topic`

### Request

- `req.topic`
- `req.payload`
- `req.origin`
- `req:reply(value) -> boolean`
- `req:fail(err) -> boolean`
- `req:done() -> boolean`

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

### Publish

```lua
conn:publish({ 'net', 'link' }, { ifname = 'eth0', up = true })
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

Subscriptions and retained watches do not have built-in timeouts. Compose them externally:

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

Point-to-point topics must be concrete. Wildcards are rejected, though literal wildcard tokens wrapped with `Bus.literal(...)` are allowed.

### Unsubscribe, unwatch, unbind, disconnect

```lua
sub:unsubscribe()
watch:unwatch()
ep:unbind()
conn:disconnect()
```

All are intended to be idempotent and to wake blocked receivers promptly.

## Authorisation

The bus may be constructed with an optional authoriser.

Supported forms:

- `function(ctx) -> boolean|nil, reason?`
- table with `:allow(ctx)`
- table with `:authorize(ctx)`

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

Actions in the simplified API are:

- `publish`
- `retain`
- `unretain`
- `subscribe`
- `watch_retained`
- `bind`
- `call`

If authorisation fails, the attempted operation raises an error.

## Stats

`conn:stats()` returns a table such as:

```lua
{
  dropped          = ...,
  subscriptions    = ...,
  endpoints        = ...,
  retained_watches = ...,
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
  endpoints        = ...,
}
```

## Notes and limitations

- Delivery is best-effort and bounded; drops under load are expected.
- Retained replay to new subscriptions is bounded and best-effort.
- Retained-watch replay is bounded; if the bus cannot deliver the terminal `replay_done` marker, the watch is closed.
- `full = "block"` is intentionally unsupported.
- Ordinary subscriptions observe published messages.
- Retained watches observe retained-state lifecycle.
- Endpoints carry command requests only; they are not part of ordinary pub/sub.
- Origin metadata is part of bus semantics, not an application payload convention.

## Design summary

The bus intentionally exposes only two public interaction styles.

### State/event plane

- `publish`
- `retain`
- `unretain`
- `subscribe`
- `watch_retained`

### Command plane

- `bind`
- `call`

That gives a small, teachable model:

- publish facts
- retain current truth
- call owned actions

while keeping provenance available for observability, policy, and federation layers such as `fabric`.
