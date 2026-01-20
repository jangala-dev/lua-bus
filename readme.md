# `lua-bus`

## Overview

The `bus` module provides an in-process pub/sub messaging bus built on `fibers` and a trie-based topic matcher.

It is intended for cooperative, single-threaded systems running under `fibers.run(...)`, where:

* delivery is **bounded** (every subscription has a bounded queue, possibly zero-length), and
* publishing is **non-blocking** (publish never blocks; slow consumers do not stall publishers).

The bus uses two tries:

* a **pubsub trie** for wildcard subscriptions (wildcards allowed in stored keys; queries are literal), and
* a **retained trie** for retained state (stored keys are literal; wildcards allowed in queries).

## Key ideas

* **Bus**: the shared router and retained store.
* **Connection**: the publishing/subscription capability you pass to services; scope-bound by default.
* **Subscription**: a per-subscriber mailbox; exposes `Op`-based message retrieval.
* **Message**: `{ topic, payload, reply_to?, id? }` delivered to subscribers.

## Assumptions and semantics

* Use only **inside fibers** (from within `fibers.run(...)`).
* Publishing is **best-effort fanout**:

  * the bus attempts a single non-blocking enqueue per subscription,
  * if enqueue would block or is rejected by policy, the message is dropped for that subscription.
* Timeouts are not built in; compose them externally using `fibers.named_choice` / `fibers.choice` plus `fibers.sleep.sleep_op`.

## Topics, wildcards, and literals

Topics are token arrays (dense tables indexed `1..n`), e.g.

```lua
{ 'net', 'link' }
```

Wildcards are supported in subscription patterns:

* single-level wildcard token: `+` (configurable via `s_wild`)
* multi-level wildcard token: `#` (configurable via `m_wild`)

### Literal tokens (escaping wildcards)

If you need to use the wildcard symbols as ordinary tokens, wrap them with `bus.literal(...)` (re-exported from `trie.literal`):

```lua
local Bus = require 'bus'

local topic = { 'cfg', Bus.literal('+') }  -- matches the literal "+" token
```

A literal token is treated as concrete for endpoint binding and request/reply topics.

## Delivery, queues, and full policy

Each subscription has:

* `queue_len` (number, **≥ 0**)
* `full` policy:

  * `"drop_oldest"` (default)
  * `"reject_newest"`
  * `"block"` is rejected (this bus must remain bounded and non-blocking)

`"drop_newest"` is deprecated and not supported; use `"reject_newest"`.

### Queue length = 0 (rendezvous subscriptions)

A `queue_len` of `0` creates a rendezvous-style subscription:

* delivery succeeds only if a receiver is waiting at publish time,
* otherwise delivery would block, so the bus drops (or rejects) the message.

This is useful for “only if someone is listening right now” topics.

### Drop accounting

Drops are tracked per-subscription and can be queried:

* `sub:dropped()` — drops for that subscription
* `conn:dropped()` — sum of drops across subscriptions owned by the connection (computed at query time)

The counter aggregates both:

* buffered evictions under `"drop_oldest"`, and
* rejections under `"reject_newest"`.

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
* `bus:connect() -> conn`
* `bus:stats() -> table`
* `Bus.literal(v) -> literal_token` (or `require('bus').literal(v)`)

### Connection

* `conn:publish(topic, payload[, opts]) -> true`
* `conn:retain(topic, payload[, opts]) -> true`
* `conn:unretain(topic) -> true`
* `conn:subscribe(topic[, opts]) -> sub`
* `conn:unsubscribe(sub) -> true`
* `conn:disconnect() -> true`
* `conn:is_disconnected() -> boolean`
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
  q_length = 10,            -- default queue length for subscriptions
  full     = 'drop_oldest', -- default full policy
  s_wild   = '+',
  m_wild   = '#',
}
```

### Connect (scope-bound)

`bus:connect()` is bound to the current scope: on scope exit the connection is disconnected.

```lua
local conn = bus:connect()
```

### Publish

```lua
conn:publish({ 'net', 'link' }, { ifname = 'eth0', up = true })
```

Publishing never blocks. If a subscriber cannot accept immediately, that subscriber drops (or rejects) the message.

### Retain and unretain

Retained messages are stored and replayed to matching future subscriptions (best-effort, bounded):

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

Rendezvous subscription (bounded, non-blocking):

```lua
local sub = conn:subscribe({ 'events', 'transient' }, { queue_len = 0, full = 'reject_newest' })
```

### Receive (sync) and compose a timeout (recommended)

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

### Unsubscribe and disconnect

```lua
sub:unsubscribe()   -- idempotent, wakes waiters
conn:disconnect()   -- idempotent, closes all owned subs/endpoints
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

## Notes and limitations

* Delivery is best-effort; drops are expected under overload.
* Retained replay is also best-effort and bounded (a new subscription may drop retained replays if it cannot accept immediately).
* `full = "block"` is intentionally not supported by the bus.
