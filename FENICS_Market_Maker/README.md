# FENICS Automated Market Making System

## Overview

The FENICS Automated Market Making system is an advanced algorithmic trading platform built on ION's Java API (JMKV). It automatically monitors market data from DEALERWEB and BTEC through VMO_REPO_US, and places and manages orders on FENICS based on a sophisticated, configurable market making strategy.

## Core Features

- **Real-time Market Monitoring**: Monitors Bid/Ask prices from DEALERWEB and BTEC through VMO_REPO_US
- **Dynamic Order Management**: Places and updates orders on FENICS based on market movements 
- **Fallback Pricing**: Uses GC_BEST_BID as fallback when no direct bids are available
- **Auto-Hedging**: Optional automatic hedging of filled orders by trading on the venue that provided the price reference
- **Configurable Parameters**: Extensive configuration options for spreads, market hours, venues, and more
- **High Performance**: Optimized for low-latency trading with minimal GC pressure
- **Comprehensive Logging**: Configurable logging levels for production and development environments

## Quick Start

For those familiar with the ION platform, here's the fastest way to get started:

1. **Clone the repository**
   ```
   git clone [repository-url]
   cd FENICS_Market_Maker
   ```

2. **Build the project**
   ```powershell
   # Windows
   build_sample.bat automatedMarketMaking
   ```

3. **Configure connection parameters**
   ```
   # Edit RUN_OrderManagement/mkv.jinit with your credentials
   notepad RUN_OrderManagement/mkv.jinit
   ```

4. **Start the market maker**
   ```powershell
   go.bat automatedMarketMaking
   ```

5. **Monitor the output**
   The system will start and display initialization information in the terminal window.

## System Architecture

The system consists of several key components:

1. **OrderManagement**: Main component and entry point (`OrderManagement.main()`) that manages the overall system, registers listeners, subscribes to market data, and coordinates market making activities
2. **MarketMaker**: Implements the core market making strategy and logic for quoting and trading
3. **DepthListener**: High-performance listener that monitors market data feeds and depth updates from configured venues
4. **MarketMakerConfig**: Provides immutable configuration with optimized access patterns to eliminate GC pressure in critical paths
5. **BondEligibilityListener**: Determines which instruments are eligible for market making based on liquidity and other criteria
6. **AsyncLoggingManager**: Handles high-throughput logging without impacting trading performance
7. **ActiveQuote**: Manages the lifecycle of individual quotes and their state
8. **GCBestManager**: Handles best price calculation for bond pricing
9. **Instrument**: Represents tradable instruments with their attributes and market data

## Requirements

- Java JDK 1.8 (system is specifically compiled for Java 1.8 compatibility)
- ION Java API (jmkv168p9.jar or compatible version)
- Redis server (default host: cacheuat, port: 6379) for distributed order state management and inter-process communication
- Network access and sufficient permissions for DEALERWEB and BTEC market data feeds
- ION MKV connection credentials (configured in mkv.jinit file)

## Dependencies

The system uses the following external libraries:

- Jackson (JSON processing)
- Jedis (Redis client)
- SLF4J/Logback (Logging)
- JCTools (Concurrent data structures)

## File Structure

```
FENICS_Market_Maker/
├── API_*.pdf                  # API documentation files
├── build_sample.bat           # Windows build script
├── build_sample.sh            # Linux build script
├── buildsamples.xml           # ANT build configuration
├── go.bat                     # Quick start script for Windows
├── README.md                  # This documentation file
├── build/                     # Build output directory
│   ├── automatedMarketMaking.jar # Executable JAR file
│   └── classes/               # Compiled classes
├── lib/                       # External dependencies
│   ├── jedis-*.jar            # Redis client
│   ├── jackson-*.jar          # JSON processing
│   ├── jmkv*.jar              # ION Java API
│   └── slf4j-*.jar            # Logging framework
├── RUN_OrderManagement/       # Runtime directory
│   ├── mkv.jinit              # Configuration file for MKV API
│   ├── logs/                  # Log output directory
│   └── MkvDB/                 # MKV database files
└── src/                       # Source code
    └── com/iontrading/automatedMarketMaking/
        ├── ActiveQuote.java             # Quote lifecycle management
        ├── ApplicationLogging.java      # Logging configuration
        ├── AsyncLoggingManager.java     # Async logging system
        ├── Best.java                    # Best price tracking
        ├── BondConsolidatedData.java    # Consolidated bond data
        ├── BondEligibilityListener.java # Bond eligibility
        ├── DepthListener.java           # Market depth listener
        ├── GCBest.java                  # GC best price
        ├── GCBestManager.java           # GC best price manager
        ├── Instrument.java              # Instrument model
        ├── MarketDef.java               # Market definition
        ├── MarketMaker.java             # Market making logic
        ├── MarketMakerConfig.java       # Configuration
        ├── MarketOrder.java             # Order model
        └── OrderManagement.java         # Main system component
```

## Building the Project

### On Windows

```powershell
# Build the project
build_sample.bat automatedMarketMaking

# Quick build and run (alternative method)
go.bat automatedMarketMaking
```

### On Linux

```bash
# Build the project
./build_sample.sh automatedMarketMaking
```

The build process will:
1. Compile all necessary Java classes with target compatibility for Java 1.8
2. Generate class files in the `build/classes` directory
3. Create an executable JAR file at `build/automatedMarketMaking.jar` with the appropriate manifest

## Configuration

The system is configured through the `MarketMakerConfig` class. Key configuration options include:

- **autoEnabled**: Enable/disable automatic market making
- **quoteUpdateIntervalSeconds**: How frequently to update quotes
- **marketSource**: Data source for market prices
- **orderType**: Type of orders to place (e.g., LIMIT)
- **timeInForce**: Order time in force (e.g., GTC, IOC)
- **autoHedge**: Whether to automatically hedge filled orders
- **enforceMarketHours**: Whether to respect market hours for trading
- **targetVenues**: List of venues to monitor and trade on
- **minSize**: Minimum order size
- **defaultIntraMarketSpread**: Default spread to maintain between bid and ask

### Advanced Configuration

The `MarketMakerConfig` class uses a Builder pattern for configuration. For advanced customization, you can modify the `OrderManagement.java` file and adjust the configuration parameters:

```java
MarketMakerConfig config = new MarketMakerConfig.Builder()
    .autoEnabled(true)                       // Enable auto market making
    .quoteUpdateIntervalSeconds(2)           // How often to update quotes (seconds)
    .marketSource("VMO_REPO_US")             // Market data source
    .orderType("LIMIT")                      // Order type for submissions
    .timeInForce("GTC")                      // Time in Force setting
    .autoHedge(true)                         // Enable automatic hedging
    .enforceMarketHours(true)                // Respect market hours
    .regMarketOpenTime(LocalTime.of(7, 30))  // Regular market open time
    .regMarketCloseTime(LocalTime.of(17, 0)) // Regular market close time
    .cashMarketOpenTime(LocalTime.of(7, 0))  // Cash market open time
    .cashMarketCloseTime(LocalTime.of(17, 0))// Cash market close time
    .targetVenues(new HashSet<>(Arrays.asList(
        "DEALERWEB", "BTEC")))               // Market venues to monitor
    .defaultMarketMakingAllowed(true)        // Allow market making by default
    .minSize(1.0)                            // Minimum order size
    .defaultIntraMarketSpread(0.5)           // Default spread
    .build();
```

## Running the System

1. Ensure the Redis server is accessible at the configured endpoint (default: cacheuat:6379)
2. Configure the `RUN_OrderManagement/mkv.jinit` file with appropriate connection parameters:
   ```
   mkv.component = MarketMaker
   mkv.user = [your-username]
   mkv.pwd = [your-password]
   mkv.cshost = [connection-server-host]
   mkv.csport = [connection-server-port]
   mkv.listen = [listen-port]
   mkv.debug = [debug-level]
   ```
3. Navigate to the project root directory
4. Execute:

```powershell
# Method 1: Using the go.bat script (Windows)
go.bat automatedMarketMaking

# Method 2: Direct execution from the RUN_OrderManagement directory
cd RUN_OrderManagement
java -cp "../lib/*;../build/classes" com.iontrading.automatedMarketMaking.OrderManagement
```

The application will start, connect to the ION Platform, and begin monitoring configured instruments for market making opportunities.

## Market Making Strategy

The system implements the following strategy:

1. Monitors Bid/Ask from DEALERWEB and BTEC through VMO_REPO_US
2. Places orders on FENICS at Bid + Adjustment and Ask - Adjustment
3. If no bid is available, then bids at GC_BEST_BID + Adjustment
4. Continuously updates orders when the market moves to maintain competitive pricing
5. Optionally hedges filled orders by trading on the venue that provided the price reference

## Logging

The system provides different logging configurations:

- **Production Logging**: Minimal output with only critical business events
- **Development Logging**: More detailed output for debugging and development
- **Custom Logging**: Configurable through the ApplicationLogging class

## Redis Integration

The system uses Redis for:

- Distributed state management across multiple instances
- Publishing order events for external systems via pub/sub
- Maintaining consistent order state during restarts or failovers
- Inter-process communication between components
- Caching market data for quick access

Default Redis configuration:
- Host: cacheuat
- Port: 6379
- No authentication (configured for internal network access)

The `JedisPubSubListener` class handles Redis pub/sub events, allowing for real-time communication between components.

## Performance Considerations

- Uses immutable configuration objects for thread safety and reduced GC pressure
- Pre-computes frequent calculations like market hours checks to minimize real-time calculations
- Implements optimized collections (ConcurrentHashMap and other concurrent data structures) for high-throughput data processing
- Uses scheduled tasks with fixed rates to maintain consistent update timing
- Implements asynchronous logging to prevent I/O operations from blocking trading operations
- Uses atomic operations and lock-free algorithms where possible to reduce contention
- Caches reference data and pre-computed values to minimize recalculations
- Leverages JCTools high-performance concurrent data structures for critical components
- Optimizes hot paths to minimize object allocation during quote updates

## Safety Mechanisms

- Market hours enforcement to prevent trading outside regular hours
- Minimum size thresholds to prevent small orders
- Eligibility checking before placing orders
- Automated rate limiting

## Monitoring and Diagnostics

The system provides comprehensive monitoring and diagnostic capabilities:

- Runtime metrics available via log queries
- Order status and market data monitoring through logging
- Configurable log levels to adjust output detail
- Health status reporting
- Subscription monitoring to detect disconnections
- Pattern subscription monitoring for data quality

## Troubleshooting

### Common Issues

1. **Redis Connection Failures**
   - Ensure Redis server is running at the configured host/port
   - Check network connectivity to Redis server
   - Review logs for JedisConnectionException errors

2. **Market Data Issues**
   - Check eligibility criteria configuration
   - Verify market data subscriptions are active
   - Review logs for pattern subscription status

3. **Order Submission Problems**
   - Ensure proper permissions on the FENICS platform
   - Check order size meets minimum requirements
   - Verify market is open for the instruments being traded

### Diagnostic Commands

To view the current status of the market maker:
```
# Check log files in the RUN_OrderManagement/logs directory
# Filter for "STATUS" to see periodic status updates
```

## Support and Maintenance

For support, please contact:
- Technical support: trading-tech-support@example.com
- Business support: trading-desk@example.com

## License

This software is proprietary and confidential. Unauthorized copying, transfer, or use is strictly prohibited.

Copyright © 2025 ION Trading. All rights reserved.
