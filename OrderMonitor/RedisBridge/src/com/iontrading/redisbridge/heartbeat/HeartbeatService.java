package com.iontrading.redisbridge.heartbeat;

import com.iontrading.redisbridge.admin.AdminCommandHandler;
import com.iontrading.redisbridge.config.ConfigurationManager;
import com.iontrading.redisbridge.redis.RedisPublisher;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Sends heartbeat messages.
 * This class is responsible for sending heartbeat messages to Redis.
 */
public class HeartbeatService {
    
    // Singleton instance
    private static HeartbeatService instance;
    
    // Reference to ConfigurationManager
    private final ConfigurationManager config;
    
    // Reference to RedisPublisher
    private final RedisPublisher publisher;
    
    // Reference to AdminCommandHandler
    private final AdminCommandHandler adminHandler;
    
    // Scheduled executor service for sending heartbeats
    private final ScheduledExecutorService executorService;
    
    // Running flag
    private volatile boolean running;
    
    // Start time
    private final long startTime;
    
    // Last heartbeat time
    private volatile long lastHeartbeatTime;
    
    /**
     * Gets the singleton instance of HeartbeatService.
     * 
     * @param publisher The Redis publisher
     * @param adminHandler The admin command handler
     * @return The singleton instance
     */
    public static synchronized HeartbeatService getInstance(RedisPublisher publisher, AdminCommandHandler adminHandler) {
        if (instance == null) {
            instance = new HeartbeatService(publisher, adminHandler);
        }
        return instance;
    }
    
    /**
     * Private constructor to enforce singleton pattern.
     * 
     * @param publisher The Redis publisher
     * @param adminHandler The admin command handler
     */
    private HeartbeatService(RedisPublisher publisher, AdminCommandHandler adminHandler) {
        this.config = ConfigurationManager.getInstance();
        this.publisher = publisher;
        this.adminHandler = adminHandler;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.running = false;
        this.startTime = System.currentTimeMillis();
        this.lastHeartbeatTime = 0;
    }
    
    /**
     * Starts the heartbeat service.
     */
    public void start() {
        if (running) {
            return;
        }
        
        running = true;
        
        // Schedule heartbeat task
        executorService.scheduleAtFixedRate(
            this::sendHeartbeat,
            0,
            30, // Default to 30 seconds if not configured
            TimeUnit.SECONDS
        );
    }
    
    /**
     * Stops the heartbeat service.
     */
    public void stop() {
        if (!running) {
            return;
        }
        
        running = false;
        
        // Shutdown executor service
        executorService.shutdown();
        
        System.out.println("Heartbeat service stopped");
    }
    
    /**
     * Checks if the heartbeat service is running.
     * 
     * @return true if the heartbeat service is running, false otherwise
     */
    public boolean isRunning() {
        return running;
    }
    
    /**
     * Gets the start time.
     * 
     * @return The start time
     */
    public long getStartTime() {
        return startTime;
    }
    
    /**
     * Gets the last heartbeat time.
     * 
     * @return The last heartbeat time
     */
    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }
    
    /**
     * Gets the uptime in milliseconds.
     * 
     * @return The uptime in milliseconds
     */
    public long getUptimeMillis() {
        return System.currentTimeMillis() - startTime;
    }
    
    /**
     * Gets the uptime as a formatted string.
     * 
     * @return The uptime as a formatted string
     */
    public String getUptimeFormatted() {
        long uptime = getUptimeMillis();
        
        long seconds = uptime / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        long days = hours / 24;
        
        seconds %= 60;
        minutes %= 60;
        hours %= 24;
        
        return String.format("%d days, %d hours, %d minutes, %d seconds", days, hours, minutes, seconds);
    }
    
    /**
     * Sends a heartbeat.
     */
    private void sendHeartbeat() {
        try {
            // Update last heartbeat time
            lastHeartbeatTime = System.currentTimeMillis();
            
            // Create status string
            String status = String.format(
                "Uptime: %s, System Enabled: %s",
                getUptimeFormatted(),
                adminHandler.isSystemEnabled() ? "Yes" : "No"
            );
            
            // Publish heartbeat
            publisher.publishHeartbeat(status);
            
        } catch (Exception e) {
            System.err.println("Error sending heartbeat: " + e.getMessage());
        }
    }
}
