# `lua-bus`

## Overview

The `bus` module provides an in-process messaging bus built on `fibers` and trie-based topic matching.

It is intended for cooperative, single-threaded systems running under `fibers.run(...)`, where:

* delivery is **bounded** (every subscription, endpoint, and retained watch has a bounded queue, possibly zero-length), and
* publishing and routing are **non-blocking** (slow consumers do not stall publishers).

The bus uses three tries:

* a **pubsub trie** for ordinary subscriptions (wildcards allowed in stored keys; queries are literal),
* a **retained trie** for retained state (stored keys are literal; wildcards allowed in queries), and
* a **retained-watch trie** for observing retained-state lifecycle events (wildcards allowed in stored keys; queries are literal).

## Key ideas

* **Bus**: the shared router, retained store, endpoint registry, and retained-watch registry.
* **Connection**: the capability you pass to services; scope-bound by default.
* **Subscription**: a per-subscriber mailbox for ordinary publish fanout.
* **RetainedWatch**: a per-watcher mailbox for retained-state lifecycle events.
* **Endpoint**: a concrete point-to-point mailbox for lane B delivery.
* **Message**: `{ topic, payload, reply_to?, id? }` delivered to subscriptions and endpoints.
* **RetainedEvent**: `{ op, topic, payload?, reply_to?, id? }` delivered to retained watches.

## Assumptions and semantics

* Use only **inside fibers** (from within `fibers.run(...)`).
* Ordinary publish fanout is **best-effort**:

  * the bus attempts a single non-blocking enqueue per matching subscription,
  * if enqueue would block or is rejected by policy, the message is dropped for that subscription.
* Retained-watch delivery is also **best-effort** and bounded:

  * retained writes and retained removals are turned into retained events,
  * the bus attempts a single non-blocking enqueue per matching retained watch,
  * if enqueue would block or is rejected by policy, the event is dropped for that watch.
* Timeouts are not built in; compose them externally using `fibers.named_choice` / `fibers.choice` plus `fibers.sleep.sleep_op`.

## Topics, wildcards, and literals

Topics are token arrays (dense tables indexed `1..n`), for example:

```lua
{ 'net', 'link' }
````

Wildcards are supported in subscription and retained-watch patterns:

* single-level wildcard token: `+` (configurable via `s_wild`)
* multi-level wildcard token: `#` (configurable via `m_wild`)

### Literal tokens (escaping wildcards)

If you need to use the wildcard symbols as ordinary tokens, wrap them with `bus.literal(...)` (re-exported from `trie.literal`):

```lua
local Bus = require 'bus'

local topic = { 'cfg', Bus.literal('+') }  -- matches the literal "+" token
```

A literal token is treated as concrete for endpoint binding, point-to-point routing, and request/reply topics.

## Delivery, queues, and full policy

Each subscription, retained watch, and endpoint mailbox has:

* `queue_len` (number, **≥ 0**)
* `full` policy:

  * `"drop_oldest"` (default)
  * `"reject_newest"`
  * `"block"` is rejected (the bus must remain bounded and non-blocking)

`"drop_newest"` is deprecated and not supported; use `"reject_newest"`.

### Queue length = 0

A `queue_len` of `0` creates a rendezvous-style mailbox:

* delivery succeeds only if a receiver is waiting at send time,
* otherwise delivery would block, so the bus drops (or rejects) the item.

For subscriptions and retained watches, this is useful when you only want delivery if someone is actively waiting.

### Drop accounting

Drops are tracked per handle and can be queried:

* `sub:dropped()` — drops for that subscription
* `watch:dropped()` — drops for that retained watch
* `ep:dropped()` — drops for that endpoint
* `conn:dropped()` — sum of drops across owned subscriptions, retained watches, and endpoints

The counter aggregates both:

* buffered evictions under `"drop_oldest"`, and
* rejections under `"reject_newest"`.

## Retained state and retained watches

Retained messages are stored under concrete topics and replayed to future matching subscriptions.

A retained write:

```lua
conn:retain({ 'fw', 'version' }, '1.2.3')
```

does two things:

1. it publishes the message to ordinary subscribers, and
2. it stores the retained value for future replay.

A retained removal:

```lua
conn:unretain({ 'fw', 'version' })
```

removes the retained value. It does **not** publish an ordinary message.

### Watching retained-state lifecycle

If you need to observe retained writes and removals as events, use `watch_retained(...)`.

A retained watch receives `RetainedEvent` records:

* on retain:

  ```lua
  {
    op       = 'retain',
    topic    = ...,
    payload  = ...,
    reply_to = ...,
    id       = ...,
  }
  ```

* on unretain:

  ```lua
  {
    op    = 'unretain',
    topic = ...,
  }
  ```

Retained watches are independent of ordinary subscriptions.

### Replay on watch creation

`watch_retained(...)` accepts `replay = true` to emit synthetic `retain` events for the current retained items that match the pattern.

This is useful for consumers that need both:

* the current retained image, and
* subsequent retained changes.

## Installation

Dependencies:

* `fibers` (`fibers.op`, `fibers.performer`, `fibers.scope`, `fibers.mailbox`)
* `trie` (pubsub + retained; supports `trie.literal`)
* `uuid`

Load:

```lua
local Bus = require 'bus'
```

## API summary

### Bus

* `Bus.new(params?) -> bus`
* `bus:connect([opts]) -> conn`
* `bus:stats() -> table`
* `Bus.literal(v) -> literal_token` (or `require('bus').literal(v)`)

Constructor parameters:

* `q_length?: integer`
* `full?: "drop_oldest"|"reject_newest"`
* `s_wild?: string|number`
* `m_wild?: string|number`
* `authoriser?: function|table`

`bus:connect(opts?)` currently accepts:

* `principal?: any`

### Connection

* `conn:publish(topic, payload[, opts]) -> true`
* `conn:retain(topic, payload[, opts]) -> true`
* `conn:unretain(topic) -> true`
* `conn:subscribe(topic[, opts]) -> sub`
* `conn:unsubscribe(sub) -> true`
* `conn:watch_retained(topic[, opts]) -> watch`
* `conn:unwatch_retained(watch) -> true`
* `conn:disconnect() -> true`
* `conn:is_disconnected() -> boolean`
* `conn:principal() -> any|nil`
* `conn:dropped() -> number`
* `conn:stats() -> table`
* `conn:request_sub(topic, payload[, opts]) -> sub`
* `conn:request_once_op(topic, payload[, opts]) -> Op`

Lane B (opt-in):

* `conn:bind(topic[, opts]) -> endpoint`
* `conn:unbind(endpoint) -> true`
* `conn:publish_one_op(topic, payload[, opts]) -> Op`
* `conn:publish_one(topic, payload[, opts]) -> boolean, reason|nil`
* `conn:call_op(topic, payload[, opts]) -> Op`
* `conn:call(topic, payload[, opts]) -> reply|nil, err|nil`

### Subscription

* `sub:recv_op() -> Op` yielding `(Message|nil, err|string|nil)`
* `sub:recv() -> Message|nil, err|string|nil`
* `sub:unsubscribe() -> true`
* `sub:iter() -> iterator<Message>`
* `sub:payloads() -> iterator<any>`
* `sub:why() -> any|nil`
* `sub:dropped() -> number`
* `sub:topic() -> Topic`
* `sub:stats() -> table`

### RetainedWatch

* `watch:recv_op() -> Op` yielding `(RetainedEvent|nil, err|string|nil)`
* `watch:recv() -> RetainedEvent|nil, err|string|nil`
* `watch:unwatch() -> true`
* `watch:iter() -> iterator<RetainedEvent>`
* `watch:why() -> any|nil`
* `watch:dropped() -> number`
* `watch:topic() -> Topic`
* `watch:stats() -> table`

### Endpoint (lane B)

* `ep:recv_op() -> Op` yielding `(Message|nil, err|string|nil)`
* `ep:recv() -> Message|nil, err|string|nil`
* `ep:iter() -> iterator<Message>`
* `ep:payloads() -> iterator<any>`
* `ep:unbind() -> true`
* `ep:why() -> any|nil`
* `ep:dropped() -> number`
* `ep:topic() -> Topic`

## Usage

### Create a bus

```lua
local Bus = require 'bus'

local bus = Bus.new{
  q_length = 10,            -- default queue length
  full     = 'drop_oldest', -- default full policy
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

### Connect (scope-bound)

`bus:connect()` is bound to the current scope: on scope exit the connection is disconnected.

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

Publishing never blocks. If a subscriber cannot accept immediately, that subscriber drops (or rejects) the message.

### Retain and unretain

Retained messages are stored and replayed to matching future subscriptions:

```lua
conn:retain({ 'fw', 'version' }, '1.2.3')
```

Remove retained state:

```lua
conn:unretain({ 'fw', 'version' })
```

### Subscribe

Default behaviour uses the bus defaults (`q_length`, `full`):

```lua
local sub = conn:subscribe({ 'fw', '#' })
```

Override queue length and policy:

```lua
local sub = conn:subscribe({ 'net', '+' }, { queue_len = 50, full = 'reject_newest' })
```

Rendezvous subscription:

```lua
local sub = conn:subscribe({ 'events', 'transient' }, { queue_len = 0, full = 'reject_newest' })
```

### Receive (sync) and compose a timeout

`recv()` blocks until a message arrives or the subscription closes:

```lua
local msg, err = sub:recv()
if msg then
  print(msg.payload)
else
  print('closed:', err)
end
```

Timeouts are composed externally:

```lua
local fibers = require 'fibers'
local sleep  = require 'fibers.sleep'

local ev = fibers.named_choice{
  msg      = sub:recv_op(),
  deadline = sleep.sleep_op(1.0),
}

local which, msg, err = fibers.perform(ev)
if which == 'msg' then
  -- msg may be nil if the subscription closed
else
  -- deadline fired
end
```

### Watch retained-state changes

Create a retained watch:

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
    print('retained update:', ev.payload)
  elseif ev.op == 'unretain' then
    print('retained removal:', table.concat(ev.topic, '/'))
  end
else
  print('watch closed:', err)
end
```

Compose with a timeout:

```lua
local fibers = require 'fibers'
local sleep  = require 'fibers.sleep'

local evsel = fibers.named_choice{
  event    = watch:recv_op(),
  deadline = sleep.sleep_op(1.0),
}

local which, ev, err = fibers.perform(evsel)
if which == 'event' then
  -- ev may be nil if the watch closed
else
  -- deadline fired
end
```

### Unsubscribe, unwatch, and disconnect

```lua
sub:unsubscribe()     -- idempotent, wakes waiters
watch:unwatch()       -- idempotent, wakes waiters
conn:disconnect()     -- idempotent, closes all owned subs/watches/endpoints
```

## Request/reply

### Multi-reply: `request_sub`

Creates a fresh `reply_to` topic, subscribes to it first, then publishes the request.

```lua
local replies = conn:request_sub(
  { 'rpc', 'get_status' },
  { verbose = true },
  { queue_len = 10, full = 'drop_oldest' }
)

for msg in replies:iter() do
  print('reply:', msg.payload)
end
```

### Single reply: `request_once_op`

Returns an `Op` that yields the first reply message (or closes). It uses a temporary subscription with `queue_len = 1` and `'reject_newest'`, and always unsubscribes via `op.bracket`.

```lua
local fibers = require 'fibers'
local sleep  = require 'fibers.sleep'

local ev = fibers.named_choice{
  reply    = conn:request_once_op({ 'rpc', 'ping' }, { answer = 42 }),
  timeout  = sleep.sleep_op(1.0),
}

local which, msg, err = fibers.perform(ev)
if which == 'reply' and msg then
  print('reply:', msg.payload)
else
  print('no reply')
end
```

## Lane B: concrete point-to-point delivery

Lane B is opt-in and separate from ordinary pub/sub.

Use it when you want:

* exactly one concrete destination topic,
* bounded admission signalling,
* explicit request/reply style interactions.

Bind a concrete endpoint:

```lua
local ep = conn:bind({ 'rpc', 'echo' }, { queue_len = 1 })
```

Deliver to it directly:

```lua
local ok, reason = conn:publish_one({ 'rpc', 'echo' }, 'hello')
```

Point-to-point topics must be concrete. Wildcards are rejected, though literal wildcard tokens are allowed via `Bus.literal(...)`.

## Authorisation

The bus can be constructed with an optional `authoriser`.

Supported forms are:

* `function(ctx) -> boolean|nil, reason?`
* table with `:allow(ctx)`
* table with `:authorize(ctx)`

The authoriser is called with a context like:

```lua
{
  bus       = bus,
  principal = principal,
  action    = action,
  topic     = topic,
  extra     = extra,
}
```

Current actions include:

* `publish`
* `retain`
* `unretain`
* `subscribe`
* `watch_retained`
* `bind`
* `publish_one`
* `request`
* `call`

If authorisation fails, the operation raises an error.

## Stats

`conn:stats()` returns:

```lua
{
  dropped          = ...,
  subscriptions    = ...,
  endpoints        = ...,
  retained_watches = ...,
}
```

`bus:stats()` returns:

```lua
{
  connections      = ...,
  dropped          = ...,
  queue_len        = ...,
  full_policy      = ...,
  s_wild           = ...,
  m_wild           = ...,
  retained_watches = ...,
}
```

## Notes and limitations

* Delivery is best-effort; drops are expected under overload.
* Retained replay to new subscriptions is best-effort and bounded.
* Retained-watch replay and live retained events are also best-effort and bounded.
* `full = "block"` is intentionally not supported by the bus.
* Ordinary subscriptions observe published messages; retained watches observe retained-state lifecycle. They are related, but not the same mechanism.
