package com.iontrading.redisbridge.admin;

import com.iontrading.redisbridge.config.ConfigurationManager;
import com.iontrading.redisbridge.redis.RedisPublisher;
import com.iontrading.redisbridge.util.LoggingUtils;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles admin commands.
 * This class is responsible for handling admin commands.
 */
public class AdminCommandHandler {
    
    // Singleton instance
    private static AdminCommandHandler instance;
    
    // Reference to ConfigurationManager
    private final ConfigurationManager config;
    
    // Reference to RedisPublisher
    private final RedisPublisher publisher;
    
    // System enabled flag
    private boolean systemEnabled;
    
    /**
     * Gets the singleton instance of AdminCommandHandler.
     * 
     * @param publisher The Redis publisher
     * @return The singleton instance
     */
    public static synchronized AdminCommandHandler getInstance(RedisPublisher publisher) {
        if (instance == null) {
            instance = new AdminCommandHandler(publisher);
        }
        return instance;
    }
    
    /**
     * Private constructor to enforce singleton pattern.
     * 
     * @param publisher The Redis publisher
     */
    private AdminCommandHandler(RedisPublisher publisher) {
        this.publisher = publisher;
        this.config = ConfigurationManager.getInstance();
        this.systemEnabled = config.isSystemEnabled();
    }
    
    /**
     * Handles a command.
     * 
     * @param command The command
     * @param params The command parameters
     * @return true if the command was handled, false otherwise
     */
    public boolean handleCommand(String command, Map<String, Object> params) {
        if (command == null) {
            return false;
        }
        
        switch (command.toLowerCase()) {
            case "status":
                return handleStatusCommand(params);
            case "enable":
                return handleEnableCommand(params);
            case "disable":
                return handleDisableCommand(params);
            case "shutdown":
                return handleShutdownCommand(params);
            default:
                System.err.println("Unknown command: " + command);
                return false;
        }
    }
    
    /**
     * Handles the status command.
     * 
     * @param params The command parameters
     * @return true if the command was handled, false otherwise
     */
    private boolean handleStatusCommand(Map<String, Object> params) {
        // Get status
        Map<String, Object> status = getStatus();
        
        // Publish status
        publisher.publishAdminMessage(getStatusJson(status));
        
        return true;
    }
    
    /**
     * Handles the enable command.
     * 
     * @param params The command parameters
     * @return true if the command was handled, false otherwise
     */
    private boolean handleEnableCommand(Map<String, Object> params) {
        // Enable system
        systemEnabled = true;
        
        // Get status
        Map<String, Object> status = getStatus();
        
        // Publish status
        publisher.publishAdminMessage(getStatusJson(status));
        
        return true;
    }
    
    /**
     * Handles the disable command.
     * 
     * @param params The command parameters
     * @return true if the command was handled, false otherwise
     */
    private boolean handleDisableCommand(Map<String, Object> params) {
        // Disable system
        systemEnabled = false;
        
        // Get status
        Map<String, Object> status = getStatus();
        
        // Publish status
        publisher.publishAdminMessage(getStatusJson(status));
        
        return true;
    }
    
    /**
     * Handles the shutdown command.
     * 
     * @param params The command parameters
     * @return true if the command was handled, false otherwise
     */
    private boolean handleShutdownCommand(Map<String, Object> params) {
        // Disable system
        systemEnabled = false;
        
        // Get status
        Map<String, Object> status = getStatus();
        
        // Publish status
        publisher.publishAdminMessage(getStatusJson(status));
        
        // Shutdown
        System.exit(0);
        
        return true;
    }
    
    /**
     * Gets the system status.
     * 
     * @return The system status
     */
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("type", "status");
        status.put("status", systemEnabled ? "enabled" : "disabled");
        status.put("timestamp", LoggingUtils.getCurrentTimestamp());
        
        return status;
    }
    
    /**
     * Gets the system status as a JSON string.
     * 
     * @param status The system status
     * @return The system status as a JSON string
     */
    private String getStatusJson(Map<String, Object> status) {
        Map<String, Object> response = new HashMap<>();
        response.put("type", "response");
        response.put("command", "status");
        response.put("status", status);
        response.put("timestamp", LoggingUtils.getCurrentTimestamp());
        
        return com.iontrading.redisbridge.util.JsonUtils.toJson(response);
    }
    
    /**
     * Checks if the system is enabled.
     * 
     * @return true if the system is enabled, false otherwise
     */
    public boolean isSystemEnabled() {
        return systemEnabled;
    }
    
    /**
     * Sets the system enabled flag.
     * 
     * @param systemEnabled The system enabled flag
     */
    public void setSystemEnabled(boolean systemEnabled) {
        this.systemEnabled = systemEnabled;
    }
}
