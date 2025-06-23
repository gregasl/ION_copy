# REDIS-ION Bridge

A Java application that bridges between Redis and ION trading systems. This application receives order information from Redis, converts it to ION orders, and submits them using the ION framework.

## Features

- Receives order create and cancel messages from Redis
- Converts generic instrument IDs to native venue-specific IDs using ION's DepthListener
- Verifies trader login status using ION's CM_Login functionality
- Submits orders to ION trading venues
- Provides administrative commands through Redis
- Sends heartbeat messages to Redis for monitoring
- Configurable through properties file

## Architecture

The application follows a modular design with the following components:

- **RedisBridgeApplication**: Main application class that initializes and starts all components
- **ConfigurationManager**: Manages application configuration
- **RedisPublisher**: Publishes messages to Redis channels
- **RedisListener**: Listens for messages on Redis channels
- **OrderProcessor**: Processes order messages and submits them to ION
- **InstrumentConverter**: Converts generic instrument IDs to native venue-specific IDs
- **TraderLoginChecker**: Checks if traders are logged into specific venues
- **AdminCommandHandler**: Handles administrative commands
- **HeartbeatService**: Sends periodic heartbeat messages

## Prerequisites

- Java 8 or higher
- Redis server
- ION trading system with jmkv library

## Building

To build the application, run:

```bash
javac -cp "lib/*" src/com/iontrading/redisbridge/*.java src/com/iontrading/redisbridge/*/*.java
jar cfm build/redisbridge.jar src/com/iontrading/redisbridge/manifest.txt -C src .
```

## Running

To run the application:

```bash
java -jar build/redisbridge.jar --config path/to/config.properties
```

If no config path is specified, the application will use the default configuration at `src/com/iontrading/redisbridge/config/config.properties`.

## Configuration

The application is configured through a properties file. Here are the key configuration options:

- `app.id`: Application identifier
- `app.heartbeat.interval.seconds`: Interval for sending heartbeat messages
- `app.order.ttl.ms`: Time-to-live for orders
- `redis.host`: Redis server hostname
- `redis.port`: Redis server port
- `redis.order.channel`: Redis channel for order messages
- `redis.admin.channel`: Redis channel for admin messages
- `redis.heartbeat.channel`: Redis channel for heartbeat messages
- `venue.list`: Comma-separated list of venues
- `venue.trader.<VENUE>`: Trader ID for a specific venue

## Redis Message Formats

### Order Create Message

```json
{
  "messageType": "OrderCreate",
  "orderId": "client-order-123",
  "trader": "TRADER1",
  "instrumentId": "BOND123",
  "verb": "BUY",
  "marketSource": "FENICS",
  "price": 99.75,
  "quantity": 1000000
}
```

### Order Cancel Message

```json
{
  "messageType": "OrderCancel",
  "orderId": "client-cancel-123",
  "origId": "client-order-123",
  "trader": "TRADER1",
  "marketSource": "FENICS"
}
```

### Admin Command Message

```json
{
  "command": "STATUS",
  "parameters": {
    "detail": true
  }
}
```

## Admin Commands

The following administrative commands are supported:

- `STATUS`: Get the current status of the application
- `START`: Start the application if it's stopped
- `STOP`: Stop the application
- `CONFIG`: Get or update configuration

## Dependencies

- ION jmkv library
- Jedis (Redis client for Java)
- Jackson (JSON processing)
- SLF4J (Logging)

## License

Proprietary - ION Trading
