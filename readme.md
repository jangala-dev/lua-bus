# README: Bus Module

## Overview

The `bus` module provides an in-process pub/sub messaging bus built on `fibers` and a Trie-based topic matcher. It is intended for internal systems where components run as cooperative fibres under `fibers.run`, and where scope cancellation is treated as terminal.

The design separates the core concepts:

* **Message**: a topic-addressed payload with optional retained and request/reply metadata.
* **Subscription**: a per-subscriber queue with a close/unsubscribe lifecycle and `Op`-based message retrieval.
* **Connection**: an ownership boundary for subscriptions and the publishing API.
* **Bus**: the shared pub/sub router, using two tries:

  * a **pubsub trie** for wildcard subscriptions (wildcards in stored keys),
  * a **retained trie** for retained state (literal stored keys; wildcards allowed in queries).

The bus is designed for predictable behaviour under load: publishing is best-effort and does not block.

## Assumptions and semantics

* The module is used **only inside fibres**, i.e. from within `fibers.run(...)`.
* **Cancellation is terminal**: `fibers.perform` may raise when the current scope is cancelled. Code should rely on `scope:finally(...)` for cleanup rather than catching cancellation.
* Delivery is **best-effort**:

  * each subscription has a bounded queue,
  * if the queue is full, the message is dropped for that subscription.
* Subscriptions support wildcard topics using:

  * single-level wildcard: `+`
  * multi-level wildcard: `#`
    (both tokens are configurable at bus construction time).

## Features

* **Wildcard subscriptions**
  Subscribe using token arrays (strings/numbers). Wildcards are matched by the pubsub trie.

* **Retained messages**
  Messages marked `retained=true` are stored and replayed to new subscriptions whose patterns match. A retained message with `payload=nil` deletes the retained value for that topic.

* **Op-based message retrieval**
  Subscriptions expose `next_msg_op(timeout)` which composes with the wider `fibers.op` system (`choice`, timeouts, etc.). A synchronous wrapper `next_msg(timeout)` is also provided.

* **Scope-bound lifecycle by default**
  Connections are bound to the current scope; disconnect happens automatically on scope exit. Subscriptions are scope-bound by default (auto-unsubscribe on the current scope’s exit), with an opt-out.

* **Request/reply helper**
  A `reply_to` topic can be attached to a message. Helpers create a reply subscription (`request_sub`) or wait for the first reply (`request_once_op` / `request_once`).

## Installation

This module depends on:

* `fibers` (runtime, ops, sleep, channel, scope, cond)
* `trie` (pubsub + retained trie implementation)
* `uuid`

Load the module in Lua:

```lua
local Bus = require 'bus'
```

## Usage

### Initialisation

Create a new bus instance. You can configure queue length and wildcard tokens.

```lua
local Bus = require 'bus'

local bus = Bus.new({
  q_length = 10,   -- default per-subscription queue length
  s_wild   = '+',  -- single-level wildcard token
  m_wild   = '#',  -- multi-level wildcard token
})
```

### Establish a connection

`bus:connect()` returns a `Connection` that is automatically disconnected when the current scope exits.

```lua
local conn = bus:connect()
```

### Publishing

Construct messages with `Bus.new_msg(topic, payload, opts)` and publish through a connection.

```lua
local new_msg = Bus.new_msg

-- Publish a retained state update
conn:publish(new_msg({ 'fw', 'version' }, '1.2.3', { retained = true }))

-- Publish a transient event
conn:publish_topic({ 'net', 'link' }, { ifname = 'eth0', up = true })
```

Retained deletion uses `payload = nil`:

```lua
conn:publish(new_msg({ 'fw', 'version' }, nil, { retained = true }))
```

### Subscribing

Subscribe to a topic pattern. By default:

* retained messages are replayed immediately,
* the subscription is scope-bound (auto-unsubscribe when the current scope exits).

```lua
local sub = conn:subscribe({ 'fw', '#' })
```

You can control behaviour:

```lua
local sub = conn:subscribe({ 'net', '+' }, {
  replay_retained = true,   -- default: true
  queue_len       = 50,     -- override default queue length
  scope_bound     = true,   -- default: true
})
```

### Receiving messages

Use `next_msg(timeout)` for a blocking call, or `next_msg_op(timeout)` to compose with other ops.

```lua
local msg, err = sub:next_msg(1.0) -- seconds
if msg then
  print('topic:', table.concat(msg.topic, '/'), 'payload:', msg.payload)
elseif err then
  print('no message:', err) -- 'timeout', 'unsubscribed', 'disconnected', etc.
end
```

`next_msg_op` returns an `Op` that yields `(Message|nil, err|string|nil)`:

```lua
local fibers = require 'fibers'

local msg, err = fibers.perform(sub:next_msg_op(0.1))
```

### Unsubscribe and disconnect

Unsubscribing is idempotent and wakes any waiters.

```lua
sub:unsubscribe()
```

Disconnecting a connection unsubscribes and closes all of its subscriptions (idempotent):

```lua
conn:disconnect()
```

## Request/reply

### Multi-reply: subscribe to replies and publish request

`request_sub` ensures a `reply_to` topic exists, subscribes to it first (to avoid racing responders), then publishes.

```lua
local req = Bus.new_msg({ 'rpc', 'get_status' }, { verbose = true })

local replies = conn:request_sub(req, {
  queue_len = 10,
  scope_bound = true,
})

-- consume replies until you decide to stop
local msg, err = replies:next_msg(1.0)
```

### Single reply: wait for the first reply

`request_once_op` is suitable for use with `choice` and timeouts; it uses `op.bracket` to guarantee cleanup of the temporary subscription.

```lua
local req = Bus.new_msg({ 'rpc', 'ping' }, { answer = 42 })

local reply, err = conn:request_once(req, { timeout = 1.0 })
if reply then
  print('reply:', reply.payload)
else
  print('request failed:', err)
end
```

## Message structure

A `Message` has:

* `topic` (table): token array (strings/numbers)
* `payload` (any): user payload
* `retained` (boolean|nil): retained policy
* `reply_to` (table|nil): token array for replies
* `headers` (table): arbitrary metadata (default `{}`)
* `id` (any): caller-supplied or generated UUID

Construct via:

```lua
local msg = Bus.new_msg({ 'a', 'b' }, 'hello', { retained = true, headers = { x = 1 } })
```

## Delivery and backpressure policy

Publishing uses a best-effort enqueue:

* the bus attempts a single non-blocking `put_op` into each subscriber queue,
* if the queue is full, the message is dropped for that subscriber,
* publishing does not block the publisher fibre.

This is a deliberate choice for firmware control workloads where:

* retained topics cover “latest state”,
* event topics can tolerate drops under overload,
* and a slow consumer should not stall the system.

If your use case requires lossless delivery, build it explicitly on top (eg acknowledgements, per-topic flow control, or a “durable state” topic that is always retained).

## Notes for service integration

* Prefer to create one `Connection` per service scope via `bus:connect()`.
* Prefer scope-bound subscriptions (the default).
* Use `scope:finally(...)` for any service cleanup that must run even during cancellation.
* Treat cancellation as terminal: do not attempt to “complete protocols” after cancellation; instead ensure resources are released and subprocesses are shut down via finalisers.

## Known limitations / considerations

* Message delivery is best-effort; drops are silent by default.
* Retained messages are replayed as stored `Message` objects; treat messages as immutable in consumers.
* The module assumes all interactions occur within fibres; behaviour outside `fibers.run` is not defined.

## Possible future improvements (if needed)

* Optional instrumentation hooks (drops per subscription/topic, queue depth sampling).
* Alternative delivery modes (eg notify-on-drop, coalescing per topic, lossless per subscriber with backpressure).
* Convenience helpers for merging multiple subscriptions via `op.choice` or named selection.
* Optional “service RPC” conventions on top of `reply_to` (correlation headers, standard error replies).
