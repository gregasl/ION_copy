# FENICS Market Maker - Implementation Instructions

This document provides detailed instructions for working with the FENICS Market Maker system and the underlying ION MKV API. It serves as a technical companion to the README.md, with references to official ION documentation and code samples.

## Table of Contents
- [1. ION MKV API Fundamentals](#1-ion-mkv-api-fundamentals)
- [2. Market Making Implementation](#2-market-making-implementation)
- [3. Common Integration Patterns](#3-common-integration-patterns)
- [4. Development and Debugging](#4-development-and-debugging)
- [5. Production Best Practices](#5-production-best-practices)
- [6. API Documentation References](#6-api-documentation-references)

## 1. ION MKV API Fundamentals

### 1.1 API Architecture Overview

The ION MKV API (Java Market Knowledge Vehicle) is a high-performance market data and trading API used for building trading applications. Key architectural components include:

- **MKV Core**: Manages connections, authentication, and communication 
- **MKV Publish Manager**: Handles publishing of data to the platform
- **MKV Subscribe Manager**: Handles subscriptions to market data
- **Event Listeners**: Asynchronous event handling for market data updates

Reference: `API_JAVA-P-RNT-2_1.0.pdf` - Chapter 2: Architecture Overview

### 1.2 Connection and Authentication

The FENICS Market Maker connects to the ION platform using parameters defined in the `mkv.jinit` file:

```
mkv.component = MarketMaker
mkv.user = [username]
mkv.pwd = [password]
mkv.cshost = [host]
mkv.csport = [port]
```

Example implementation from our code base:

```java
MkvQoS qos = new MkvQoS();
qos.setArgs(args);
qos.setPublishListeners(new MkvPublishListener[] { om });

try {
    Mkv existingMkv = Mkv.getInstance();
    if (existingMkv == null) {
        Mkv.start(qos);
        LOGGER.info("Mkv started successfully.");
    } else {
        LOGGER.info("MKV API already started");
        existingMkv.getPublishManager().addPublishListener(om);
    }
} catch (Exception e) {
    LOGGER.warn("Failed to start Mkv", e);
}
```

Reference: `API_JAVA-P-RNT-3_3.4.pdf` - Chapter 3: Connection Management

### 1.3 Event Listeners

The ION MKV API is event-driven. Key listeners implemented in our system:

- **MkvPublishListener**: For publication events
- **MkvRecordListener**: For record updates
- **MkvChainListener**: For chain updates

Example from the `OrderManagement` class:

```java
public class OrderManagement implements MkvPublishListener, MkvRecordListener,
    MkvChainListener, IOrderManager {
    // Implementation...
}
```

Reference: `API_JAVA-P-TMT-1_1.4.pdf` - Chapter 4: Event Listeners

## 2. Market Making Implementation

### 2.1 Market Data Processing

The Market Maker subscribes to market data from different venues (DEALERWEB and BTEC) and processes this data to make quoting decisions.

Key classes involved:
- `DepthListener`: Subscribes to and processes market depth
- `BondConsolidatedData`: Consolidates market data from multiple sources
- `GCBestManager`: Manages best price calculation

Example depth subscription pattern:

```java
MkvPattern depthPattern = Mkv.getInstance().getPublishManager()
    .findPattern("VMO_REPO_US.DEPTH.*");
if (depthPattern != null) {
    depthPattern.addRecordListener(this);
    depthPattern.subscribe();
}
```

Reference: HTML sample - `SimplePatternSubscriber` demonstrates pattern subscriptions

### 2.2 Order Management

The Market Maker manages orders through the following key components:

- `MarketMaker`: Core quoting strategy implementation
- `MarketOrder`: Represents and manages individual orders
- `ActiveQuote`: Tracks active quotes in the market

Example order creation logic:

```java
// Create bid order
MarketOrder bidOrder = new MarketOrder();
bidOrder.setInstrument(instrument);
bidOrder.setSide("BID");
bidOrder.setPrice(bidPrice);
bidOrder.setSize(bidSize);
bidOrder.setOrderType(config.getOrderType());
bidOrder.setTimeInForce(config.getTimeInForce());

// Submit order
orderManager.submitOrder(bidOrder);
```

Reference: HTML sample - `AdvOrderManagement` demonstrates order management patterns

## 3. Common Integration Patterns

### 3.1 Publish-Subscribe Pattern

The system uses the publish-subscribe pattern for market data:

1. Subscribe to market data patterns
2. Process incoming updates
3. Make trading decisions
4. Publish orders

Example from our code:

```java
// Subscribe to market data
private void subscribeToMarketData() {
    try {
        MkvPattern pattern = Mkv.getInstance().getPublishManager()
            .createPattern("MARKET_DATA.*");
        pattern.addRecordListener(this);
        pattern.subscribe();
        LOGGER.info("Subscribed to market data pattern");
    } catch (MkvException e) {
        LOGGER.error("Failed to subscribe to market data", e);
    }
}
```

Reference: HTML sample - `SimplePublisher` and `SimpleSubscriber` demonstrate this pattern

### 3.2 Chain Processing

Chains are used to manage collections of related records (e.g., orders):

```java
private void processChain(MkvChain chain) {
    // Process each record in the chain
    Iterator<String> iter = chain.getItemsIterator();
    while (iter.hasNext()) {
        String recordName = iter.next();
        try {
            MkvRecord record = Mkv.getInstance().getPublishManager().findRecord(recordName);
            if (record != null) {
                processRecord(record);
            }
        } catch (MkvException e) {
            LOGGER.error("Error processing chain record: " + recordName, e);
        }
    }
}
```

Reference: HTML sample - `SimpleChainSubscriber` demonstrates chain processing

## 4. Development and Debugging

### 4.1 Logging Configuration

The system uses SLF4J with Logback for logging. Configure logging levels in the `ApplicationLogging` class:

```java
public static void configureProductionLogging() {
    // Set global default to ERROR (minimal logging)
    ApplicationLogging.setGlobalLogLevel("ERROR");
    
    // Only critical business events at WARN level
    ApplicationLogging.setLogLevels("WARN",
        "com.iontrading.automatedMarketMaking.OrderManagement"
    );
    
    // Keep order tracking at WARN for business monitoring
    ApplicationLogging.setLogLevels("WARN",
        "com.iontrading.automatedMarketMaking.MarketOrder"
    );
}
```

### 4.2 Debugging MKV Connections

Use the debug parameters in `mkv.jinit` to enable additional logging:

```
mkv.debug = 1       # Enable debug logging
mkv.debuglevel = 1  # Debug level (0-5, higher means more verbose)
```

### 4.3 Testing Market Data Subscriptions

To test market data subscriptions without placing orders:

1. Set `autoEnabled=false` in `MarketMakerConfig`
2. Start the application
3. Check logs for market data updates
4. Use the `getStatusSummary()` method to view market data status

Reference: HTML sample - `SimpleSubscriber` demonstrates subscription testing patterns

## 5. Production Best Practices

### 5.1 Connection Management

- Implement reconnection logic for network interruptions
- Monitor connection health with heartbeats
- Use the `MkvPlatformListener` to detect disconnections

Example pattern:

```java
private void setupConnectionMonitoring() {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduler.scheduleAtFixedRate(() -> {
        try {
            if (!Mkv.getInstance().isConnected()) {
                LOGGER.warn("MKV API disconnected, attempting reconnect");
                // Reconnection logic
            }
        } catch (Exception e) {
            LOGGER.error("Error in connection monitoring", e);
        }
    }, 30, 30, TimeUnit.SECONDS);
}
```

Reference: HTML sample - `PlatformListener` demonstrates connection monitoring

### 5.2 Error Recovery

Implement error recovery strategies:

1. Detect errors through exception handling and status checks
2. Log errors with sufficient context
3. Implement recovery actions (reconnect, resubscribe, etc.)
4. Use circuit breakers for persistent failures

Example from our code:

```java
private void recoverFromError() {
    try {
        LOGGER.info("Attempting to recover from error");
        
        // Cancel existing orders
        cancelAllOrders();
        
        // Re-subscribe to market data
        resubscribeToMarketData();
        
        // Resume normal operation
        resumeTrading();
        
        LOGGER.info("Recovery completed successfully");
    } catch (Exception e) {
        LOGGER.error("Recovery failed", e);
    }
}
```

### 5.3 Performance Optimization

- Use `AsyncLoggingManager` to prevent logging from impacting trading performance
- Use immutable objects like `MarketMakerConfig` to reduce GC pressure
- Pre-compute frequently accessed values
- Use concurrent data structures appropriately

## 6. API Documentation References

### 6.1 PDF Documentation

- `API_JAVA-P-RNT-2_1.0.pdf`: Core API runtime guide
- `API_JAVA-P-RNT-3_3.4.pdf`: Advanced runtime features
- `API_JAVA-P-TMT-1_1.4.pdf`: Trading API guide

### 6.2 HTML Samples and Documentation

The ION MKV API comes with HTML documentation and sample code available in the ION_Samples directory. Key samples relevant to market making:

- **SimplePublisher**: Basic publishing example
- **SimpleSubscriber**: Basic subscription example
- **SimpleChainPublisher/Subscriber**: Chain management
- **SimplePatternPublisher/Subscriber**: Pattern subscriptions
- **AdvOrderManagement**: Advanced order management
- **PlatformListener**: Connection monitoring

To utilize these samples:

1. Navigate to the ION_Samples directory
2. Examine the sample implementations
3. Adapt the patterns to the Market Maker context

### 6.3 Code Pattern Examples

For specific implementation examples related to market making, refer to these patterns in the sample code:

- **Market Data Processing**: See `SimplePatternSubscriber` example
- **Order Creation**: See `AdvOrderManagement` example
- **Chain Management**: See `SimpleChainSubscriber` example
- **Error Handling**: See `PlatformListener` example

## 7. Extending the Market Maker

When adding new features to the Market Maker, follow these steps:

1. Identify the relevant ION MKV API interfaces for your feature
2. Study the corresponding sample code in the ION_Samples directory
3. Design your implementation following established patterns
4. Implement proper error handling and logging
5. Test extensively before deployment

For example, to add a new market data source:

```java
private void addNewMarketDataSource(String sourceName, String pattern) {
    try {
        LOGGER.info("Adding new market data source: " + sourceName);
        
        // Create and subscribe to pattern
        MkvPattern newPattern = Mkv.getInstance().getPublishManager()
            .createPattern(pattern);
        newPattern.addRecordListener(this);
        newPattern.subscribe();
        
        LOGGER.info("Successfully subscribed to new market data source");
    } catch (MkvException e) {
        LOGGER.error("Failed to add new market data source", e);
    }
}
```

## 8. Troubleshooting Common Issues

### 8.1 Connection Issues

**Symptom**: Unable to connect to the ION platform
**Possible causes**:
- Incorrect `mkv.jinit` parameters
- Network connectivity problems
- Authentication failures

**Resolution**:
1. Verify `mkv.jinit` parameters
2. Check network connectivity to the host server
3. Verify credentials
4. Check firewall settings

Reference: `API_JAVA-P-RNT-2_1.0.pdf` - Connection Troubleshooting section

### 8.2 Subscription Issues

**Symptom**: Not receiving market data updates
**Possible causes**:
- Incorrect pattern subscription
- Permission issues
- Data not available

**Resolution**:
1. Verify pattern syntax
2. Check permissions for the user account
3. Confirm data availability on the server
4. Examine logs for subscription errors

Reference: HTML sample - `SimpleSubscriber` demonstrates proper subscription patterns

---

For additional assistance, consult the full ION MKV documentation or contact the ION support team.
