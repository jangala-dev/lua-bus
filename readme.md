# README: Bus Module

## Overview

The `Bus` module provides a lightweight messaging and subscription system, using tries to provide efficient wildcard subscriptions and retained message delivery. It is built on lua-fibers.

``` lua
-- Create a new Bus
local bus = Bus.new({s_wild='+', m_wild='#', sep = '/'})

-- Fiber 1: Publisher
Fiber.spawn(function()
    -- Create a connection and publish to a topic
    local conn = bus:connect({username='user', password ='pass'})
    conn:publish({topic="foo/bar/fizz", payload="Hello World!", retained=true})
end)

-- Fiber 2: Subscriber
Fiber.spawn(function()
    -- Create a connection and subscribe to a topic
    local conn = bus:connect({username='user', password='pass'})
    local sub = conn:subscribe("foo/#") -- using multi-level wildcard
    
    -- Listen for a message
    local msg, _ = sub:next_msg(1e-3) -- 1 millisecond timeout
    print("Fiber 2:", msg.payload)
end)
```

## Features

- **Efficient Storage and Retrieval**: The module utilizes trie-based structures from our `trie` module for effective message and topic storage.
  
- **Wildcard Capabilities**: The system supports single and multiple wildcard subscriptions, allowing for flexible topic matching.

- **Asynchronous Message Retrieval**: Subscriptions provide synchronous and asynchronous message consumption with timeouts.
  
- **Retained Messages**: The ability to keep certain messages for future delivery to subscribers.

- **Basic Authentication**: Basic username-password authentication before creating a connection.

## Usage

### Initialization

To create a new Bus:

```lua
local Bus = require 'your_module_path'
local bus = Bus.new({
    q_length = 10,               -- Optional, default is 10
    s_wild = '+',               -- Single wildcard character, replace with your choice
    m_wild = '#',               -- Multi-level wildcard character, replace with your choice
    sep = '/'                   -- Separator for the topics
})
```

### Connection

Create a new connection to the Bus:

```lua
local creds = {username = 'user', password = 'pass'}
local connection = bus:connect(creds)
if not connection then
    print("Authentication failed!")
end
```

### Publishing and Subscribing

Subscribe to a topic:

```lua
local subscription = connection:subscribe("sample/topic")
```

Retrieve the next message from a subscription:

```lua
local msg, err = subscription:next_msg(0.5) -- 0.5s timeout
if not msg then
    print("Timeout!")
end
```

Publish a message:

```lua
connection:publish({
    topic = "sample/topic",
    payload = "Hello, World!",
    retained = false
})
```

### Disconnecting and Cleanup

To unsubscribe from a topic:

```lua
subscription:unsubscribe()
```

To disconnect a connection:

```lua
connection:disconnect()
```

## Technical Details

- The module uses `fibers.queue`, `fibers.op`, and `fibers.sleep` for asynchronous operations.

- `Subscription:next_msg(timeout)` retrieves the next message from a subscription. If a timeout is provided, it will wait up to that duration for a message.

- Messages that carry the `retained` flag without a payload will result in deletion of the retained message with the same topic.

- The system has basic authentication, using a static credentials dictionary (`CREDS`). This should be expanded upon for a production environment.

## Future Improvements

- Proper logging of blocked queue operations.
- Refinement of the authentication system.
- Improving the efficiency of operations with large numbers of subscriptions.
  - [ ] Federation (enabling multiple buses to interconnect)
