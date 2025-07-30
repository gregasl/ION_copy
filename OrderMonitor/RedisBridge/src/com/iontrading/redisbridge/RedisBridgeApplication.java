package com.iontrading.redisbridge;

import com.iontrading.redisbridge.admin.AdminCommandHandler;
import com.iontrading.redisbridge.config.ConfigurationManager;
import com.iontrading.redisbridge.heartbeat.HeartbeatService;
import com.iontrading.redisbridge.order.OrderProcessor;
import com.iontrading.redisbridge.redis.RedisListener;
import com.iontrading.redisbridge.redis.RedisPublisher;
import java.io.File;
import java.util.concurrent.CountDownLatch;

/**
 * Main application class for RedisBridge.
 * This class is responsible for initializing and managing the application.
 */
public class RedisBridgeApplication {
    
    // Singleton instance
    private static RedisBridgeApplication instance;
    
    // Reference to ConfigurationManager
    private final ConfigurationManager config;
    
    // Redis connection object (placeholder for actual Redis connection)
    private final Object redisConnection;
    
    // Reference to RedisPublisher
    private final RedisPublisher publisher;
    
    // Reference to AdminCommandHandler
    private final AdminCommandHandler adminHandler;
    
    // Reference to OrderProcessor
    private final OrderProcessor orderProcessor;
    
    // Reference to RedisListener
    private final RedisListener redisListener;
    
    // Reference to HeartbeatService
    private final HeartbeatService heartbeatService;
    
    // Running flag
    private volatile boolean running;
    
    // Shutdown latch
    private final CountDownLatch shutdownLatch;
    
    /**
     * Gets the singleton instance of RedisBridgeApplication.
     * 
     * @return The singleton instance
     */
    public static synchronized RedisBridgeApplication getInstance() {
        if (instance == null) {
            instance = new RedisBridgeApplication();
        }
        return instance;
    }
    
    /**
     * Private constructor to enforce singleton pattern.
     */
    private RedisBridgeApplication() {
        this.config = ConfigurationManager.getInstance();
        this.shutdownLatch = new CountDownLatch(1);
        this.running = false;
        
        // Create a placeholder for Redis connection
        this.redisConnection = new Object();
        
        // Initialize components
        this.publisher = RedisPublisher.getInstance(redisConnection);
        this.adminHandler = AdminCommandHandler.getInstance(publisher);
        this.orderProcessor = OrderProcessor.getInstance(publisher, adminHandler);
        this.redisListener = RedisListener.getInstance(redisConnection, orderProcessor, adminHandler);
        this.heartbeatService = HeartbeatService.getInstance(publisher, adminHandler);
    }
    
    /**
     * Starts the application.
     */
    public void start() {
        if (running) {
            return;
        }
        
        running = true;
        
        // Start components
        redisListener.start();
        heartbeatService.start();
        
        // Enable system
        adminHandler.setSystemEnabled(true);
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered");
            stop();
        }));
    }
    
    /**
     * Stops the application.
     */
    public void stop() {
        if (!running) {
            return;
        }
        
        running = false;
        
        System.out.println("Stopping RedisBridge application...");
        
        // Disable system
        adminHandler.setSystemEnabled(false);
        
        // Stop components
        heartbeatService.stop();
        redisListener.stop();
        
        // In a real implementation, we would close the Redis connection here
        
        System.out.println("RedisBridge application stopped");
        
        // Release shutdown latch
        shutdownLatch.countDown();
    }
    
    /**
     * Waits for the application to shut down.
     * 
     * @throws InterruptedException If the thread is interrupted
     */
    public void waitForShutdown() throws InterruptedException {
        shutdownLatch.await();
    }
    
    /**
     * Checks if the application is running.
     * 
     * @return true if the application is running, false otherwise
     */
    public boolean isRunning() {
        return running;
    }
    
    /**
     * Gets the Redis connection object.
     * 
     * @return The Redis connection object
     */
    public Object getRedisConnection() {
        return redisConnection;
    }
    
    /**
     * Gets the Redis publisher.
     * 
     * @return The Redis publisher
     */
    public RedisPublisher getPublisher() {
        return publisher;
    }
    
    /**
     * Gets the admin command handler.
     * 
     * @return The admin command handler
     */
    public AdminCommandHandler getAdminHandler() {
        return adminHandler;
    }
    
    /**
     * Gets the order processor.
     * 
     * @return The order processor
     */
    public OrderProcessor getOrderProcessor() {
        return orderProcessor;
    }
    
    /**
     * Gets the Redis listener.
     * 
     * @return The Redis listener
     */
    public RedisListener getRedisListener() {
        return redisListener;
    }
    
    /**
     * Gets the heartbeat service.
     * 
     * @return The heartbeat service
     */
    public HeartbeatService getHeartbeatService() {
        return heartbeatService;
    }
    
    /**
     * Main method.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        try {
            // Parse command line arguments
            String configPath = null;
            
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-config") && i + 1 < args.length) {
                    configPath = args[i + 1];
                    i++;
                }
            }
            
            // Load configuration
            if (configPath != null) {
                File configFile = new File(configPath);
                
                if (!configFile.exists()) {
                    System.err.println("Configuration file not found: " + configPath);
                    System.exit(1);
                }
                
                ConfigurationManager.getInstance().loadConfiguration(configPath);
            }
            
            // Get application instance
            RedisBridgeApplication app = RedisBridgeApplication.getInstance();
            
            // Start application
            app.start();
            
            // Wait for shutdown
            app.waitForShutdown();
        } catch (Exception e) {
            System.err.println("Error starting application: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
