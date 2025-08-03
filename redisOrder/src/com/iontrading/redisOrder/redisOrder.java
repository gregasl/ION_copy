/*
 * Publisher - ION Market Data Publisher Implementation
 *
 * This class implements a market data publisher that:
 * 1. Connects to a Redis cache to receive market data updates
 * 2. Dynamically creates MKV schemas based on received data
 * 3. Publishes data through the ION MKV middleware
 * 

 * - Dynamic schema generation based on data content
 * - Efficient update processing through queued architecture
 * - Automatic schema versioning and compatibility checking
 * - Robust error handling and detailed logging
 * - Performance monitoring and statistics
 *
 * Data Flow:
 * Redis -> Message Queue -> Processor Thread -> MKV Publisher
 *
 * ION Trading U.K. Limited supplies this software code is for testing purposes
 * only. The scope of this software code is exclusively limited to the
 * demonstration of the structure of an application using the ION(tm) Common
 * Market and ION Trading U.K. Limited does not guarantee the correct behavior
 * of any deployed application using this software code.
 * This software code has not been thoroughly tested under all conditions.
 * ION, therefore, cannot guarantee or imply reliability, serviceability, or
 * function of this software.
 * Any use of this software outside of this defined scope is the sole
 * responsibility of the user.
 *
 * ION Trading ltd (2005)
 */

package com.iontrading.redisOrder;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvProperties;
import com.iontrading.mkv.MkvLog;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.enums.MkvShutdownMode;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Arrays;

public class redisOrder implements MkvPlatformListener {
    private MkvLog myLog;
    private int logLevel;
    private IONLogger logger;

    private static final String REDIS_QUEUE_NAME = "ASL_REPO_ORDERS_QUEUE";
    private static final String DEFAULT_REDIS_HOST = "cacheuat";
    private static final int DEFAULT_REDIS_PORT = 6379;
    private static final String HEARTBEAT_CHANNEL = "HEARTBEAT:ION:REDISORDER";
    private static final long HEARTBEAT_INTERVAL_SECONDS = 30;
    public final String hostname = System.getenv("COMPUTERNAME");

    public final boolean isProduction = hostname != null && hostname.equalsIgnoreCase("aslionapp01");

    // Keep only persistent storage and connections
    private final JedisPool jedisPool;
    private final Mkv mkv;
    private final ExecutorService processorPool;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();


    public redisOrder(String[] args) {
        // Create the initial configuration used to start the engine
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        
        try {
            // Start the engine and get back the instance of Mkv
            this.mkv = Mkv.start(qos);
            MkvProperties properties = mkv.getProperties();
            logLevel = properties.getIntProperty("DEBUG");
            // Initialize the log after starting Mkv
            this.myLog = mkv.getLogManager().createLogFile("REDISORDER");
            this.logger = new IONLogger(myLog, logLevel, "RedisOrder");

        } catch (MkvException e) {
            if (myLog != null) {
                logger.info("Failed to initialize redisOrder: " + e.getMessage());
            }
            throw new RuntimeException("Failed to initialize publisher", e);
        }

        // Initialize fields in constructor
        this.processorPool = Executors.newSingleThreadExecutor();
        
        // Initialize Redis connection in constructor
        String redisHost = DEFAULT_REDIS_HOST;
        int redisPort = DEFAULT_REDIS_PORT;
 
        logger.info("Connecting to Redis at " + redisHost + ":" + redisPort);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        this.jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
        
        // Test Redis connection
        try (Jedis jedis = jedisPool.getResource()) {
            String pong = jedis.ping();
            logger.info("Redis connection test successful: " + pong);
        } catch (Exception e) {
            logger.error("Failed to connect to Redis: " + e.getMessage());
            throw new RuntimeException("Failed to connect to Redis", e);
        }
        
        try {
            logger.info("=== Starting ION REDISORDER ===");
            logger.info("Initializing with args: " + Arrays.toString(args));
            logger.info("Created MkvQoS configuration");
            logger.info("Created processing queue and thread pool");
            logger.info("MKV engine started successfully");

            // Start processor thread first
            // logger.info("Starting processor thread...");
            // startProcessorThread();
            // Thread.sleep(1000); // Give processor thread time to initialize

            logger.info("Starting heartbeat...");
            startHeartbeat();

            // Then start Redis queue consumer
            logger.info("Starting Redis queue consumer...");
            
            // Create and start the simple order queue consumer
            OrderQueueConsumer consumer = new OrderQueueConsumer(
                jedisPool,
                running,
                REDIS_QUEUE_NAME,
                5  // 5 second timeout
            );
            consumer.start();

            logger.info("=== REDISORDER initialization complete ===");
            logger.info("REDISORDER components status:");
            logger.info("- Redis Connection: ACTIVE");
            logger.info("- MKV Engine: RUNNING");
            logger.info("- Update Processor: PROCESSING");
            logger.info("- Redis Queue Consumer: LISTENING");
            logger.info("Waiting for Redis messages on queue: " + REDIS_QUEUE_NAME);

        } catch (Exception e) {
            if (myLog != null) {
                logger.info("Failed to initialize redisOrder: " + e.getMessage());
            }
            throw new RuntimeException("Failed to initialize publisher", e);
        }
    }

    public static void main(String[] args) {
        redisOrder redisOrder = new redisOrder(args);
        if (redisOrder.logger == null) {
            redisOrder.logger = new IONLogger(redisOrder.getMkvLog(), redisOrder.getLogLevel(), "RedisOrder");
        }
        redisOrder.logger.info("Market Data Publisher started successfully");
    }
    
    /**
     * Implements the MkvPlatformListener.onMain method to handle platform events.
     * This is where we'll handle the shutdown request from the daemon.
     */
    @Override
    public void onMain(MkvPlatformEvent event) {
        if (event.intValue() == MkvPlatformEvent.SHUTDOWN_REQUEST_code) {
            logger.info("Received shutdown request from MKV platform");

            try {
                // Do the shutdown work synchronously in this method
                boolean isReady = shutdown();
            if (isReady) {
                // Signal that we're completely done
                Mkv.getInstance().shutdown(MkvShutdownMode.SYNC, 
                    "redisOrder shutdown complete");
                logger.info("Signaled SYNC shutdown to platform");
            } else {
                // We need more time, request async and let platform retry
                Mkv.getInstance().shutdown(MkvShutdownMode.ASYNC, 
                    "redisOrder still processing...");
                logger.info("Requested ASYNC shutdown - platform will retry");
            }
            } catch (MkvException e) {
                logger.info("Error during shutdown signaling: " + e.getMessage());
            }
        }
    }

    /**
     * Implements the MkvPlatformListener.onComponent method.
     * This is called when component state changes.
     */
    @Override
    public void onComponent(MkvComponent comp, boolean start) {
        logger.info("Component " + comp.getName() + " " + (start ? "started" : "stopped"));
    }

    /**
     * Implements the MkvPlatformListener.onConnect method.
     * This is called when the connection state changes.
     */
    @Override
    public void onConnect(String comp, boolean start) {
        logger.info("Connection to " + comp + " " + (start ? "established" : "lost"));
    }


    private void startHeartbeat() {
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (!running.get()) return;
            
            try (Jedis jedis = jedisPool.getResource()) {
            
                Instant instant = Instant.ofEpochMilli(System.currentTimeMillis());
                LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

                LocalDate date = dateTime.toLocalDate(); // For date only
                LocalTime time = dateTime.toLocalTime(); // For time only
                String heartbeatMessage = String.format("HEARTBEAT,%s,%s,%s,%s",
                    "ION-REDISORDER-" + mkv.getApplicationVersion(),
                    "Hostname: " + hostname,
                    "Date: " + date,
                    "Time: " + time
                );

                jedis.publish(HEARTBEAT_CHANNEL, heartbeatMessage);
                logger.info("Sent heartbeat: " + heartbeatMessage);
            } catch (Exception e) {
                logger.error("Failed to send heartbeat: " + e.getMessage());
            }
        }, 0, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);

        logger.info("Heartbeat started on channel: " + HEARTBEAT_CHANNEL);
    }

    public boolean shutdown() {
        logger.info("Shutting down redisOrder");
        running.set(false);
        
        // Shutdown heartbeat executor
        heartbeatExecutor.shutdown();
        try {
            heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            heartbeatExecutor.shutdownNow();
        }
        
        // Gracefully shutdown processor pool
        processorPool.shutdown();
        try {
            if (!processorPool.awaitTermination(60, TimeUnit.SECONDS)) {
                processorPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            processorPool.shutdownNow();
        }
        
        // Close Redis pool
        jedisPool.close();
        
        // Stop MKV using the instance method  
        mkv.stop();
        
        // Add monitoring shutdown
        monitor.shutdown();
        try {
            monitor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            monitor.shutdownNow();
        }

        logger.info("Publisher shutdown complete");
        return true;
    }
    
    /**
     * Gets the MkvLog instance for use by other components
     * @return The MkvLog instance
     */
    public MkvLog getMkvLog() {
        return myLog;
    }

    /**
     * Gets the current log level
     * @return The log level
     */
    public int getLogLevel() {
        return logLevel;
    }

}
