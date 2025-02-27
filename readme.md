# README: Bus Module

## Overview

The Bus module provides a lightweight, concurrent messaging system built on our updated Trie implementation and fibers framework. It offers efficient wildcard subscriptions, retained message delivery, and robust asynchronous message processing with advanced cancellation and timeout semantics. The design now cleanly separates message handling, subscription management, and connection logic while leveraging a flexible Trie-based topic matching engine.

## Features

- **Efficient Topic Matching:**  
  Uses an enhanced Trie for storing topics and retained messages. This Trie supports flexible key types (strings, tables, or custom types) and robust wildcard matching with single-level (`+`) and multi-level (`#`) wildcards.

- **Flexible Concurrency:**  
  Built on a fibers framework that provides lightweight green threads, allowing asynchronous message retrieval with support for timeouts and cancellation using operations (`op.choice`, etc.).

- **Asynchronous Message Delivery:**  
  Subscriptions offer both blocking and non-blocking message retrieval methods. Use methods like `next_msg_op`, `next_msg`, or context-aware variants to suit your concurrency model.

- **Retained Messages:**  
  Retained messages are stored and delivered to new subscribers immediately upon subscription, ensuring that subscribers receive the latest state without needing to republish.

- **Modular Connection Management:**  
  The module cleanly separates the concepts of Message, Subscription, Connection, and Bus, providing a clear and maintainable API for both publishers and subscribers.

- **Enhanced Error Handling:**  
  Improved error and timeout management in subscription operations ensures that your fibers remain responsive and robust even under high load.

## Installation

Ensure you have the required dependencies (fibers, fibers.queue, fibers.op, fibers.sleep, uuid, and our updated trie module). Then, include the Bus module in your Lua project.

```lua
local Bus = require 'bus'
```

## Usage

### Initialization

Create a new Bus instance by specifying configuration options such as queue length, wildcard tokens, and (if needed) a separator for string-based topics.

```lua
local Bus = require 'bus'
local bus = Bus.new({
    q_length = 10,       -- Optional; defaults to 10 if not provided
    s_wild = '+',        -- Single-level wildcard character
    m_wild = '#',        -- Multi-level wildcard character
})
```

### Establishing a Connection

Create a connection to the Bus. While the current version has removed built-in authentication for simplicity, you can integrate your own authentication layer before calling `connect`.

```lua
local connection = bus:connect()
if not connection then
    error("Connection failed!")
end
```

### Publishing Messages

Publish a message to a specific topic. Use `publish` to send a single message or `publish_multiple` to send nested payloads with hierarchical topics.

```lua
local new_msg = require 'bus'.new_msg

-- Publish a single message
connection:publish(new_msg({"foo", "bar", "fizz"}, "Hello World!", { retained = true }))

-- Publish multiple messages from a nested payload
connection:publish_multiple({"foo"}, {
    bar = {
        fizz = "Hello World!",
        buzz = "Another Message"
    }
}, { retained = false })
```

### Subscribing to Topics

Subscribe to topics with support for wildcards. New subscriptions automatically receive any retained messages that match the topic pattern.

```lua
-- Subscribe using a multi-level wildcard
local subscription = connection:subscribe({"foo", "#"})

-- Retrieve the next message with a 1-millisecond timeout
local msg, err = subscription:next_msg(1e-3)
if msg then
    print("Received:", msg.payload)
elseif err then
    print("Error:", err)
end
```

### Context-Aware Message Retrieval

For advanced scenarios, you can use fibers context-aware message retrieval to handle cancellation or deadlines.

```lua
local parent = context.background()
local ctx, cancel = context.with_cancel(parent)
local msg, err = subscription:next_msg_with_context(context)
if msg then
    print("Received with context:", msg.payload)
elseif err then
    print("Context error:", err)
end
```

### Cleanup and Disconnection

Unsubscribe from topics and disconnect connections cleanly when finished.

```lua
-- Unsubscribe from a topic
subscription:unsubscribe()

-- Disconnect the connection (automatically unsubscribes from all topics)
connection:disconnect()
```

## Technical Details

- **Trie-Based Topic Management:**  
  The updated Trie implementation supports tokenization of keys as strings or tables, flexible wildcard matching, and iterative traversal to efficiently match topic subscriptions.

- **Fibers Integration:**  
  Asynchronous operations use fibers to allow non-blocking waits and cancellation through `op.choice` and other constructs. This enables smooth, concurrent message processing.

- **Retained Message Handling:**  
  Messages marked as retained are stored in a separate Trie. When a new subscriber joins, all relevant retained messages are immediately enqueued.

- **Connection & Subscription Lifecycles:**  
  The Bus maintains connection-specific subscriptions for easier management. Disconnecting a connection automatically cleans up all associated subscriptions.

## Future Improvements

- **Logging Enhancements:**  
  Implement detailed logging for queue blocking events and failed operations.

- **Authentication:**  
  Include authentication mechanisms for a secure, production-ready solution.

- **Scalability Optimizations:**  
  Investigate further optimizations for managing large numbers of subscriptions and topic matches.

- **Federation Support:**  
  Explore mechanisms to interconnect multiple buses for distributed message handling.
