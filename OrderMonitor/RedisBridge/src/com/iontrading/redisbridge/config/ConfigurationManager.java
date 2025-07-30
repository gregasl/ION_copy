package com.iontrading.redisbridge.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Manages configuration.
 * This class is responsible for managing configuration.
 */
public class ConfigurationManager {
    
    // Singleton instance
    private static ConfigurationManager instance;
    
    // Properties
    private final Properties properties;
    
    // Default configuration file path
    private static final String DEFAULT_CONFIG_PATH = "src/com/iontrading/redisbridge/config/config.properties";
    
    // Redis host
    private String redisHost;
    
    // Redis port
    private int redisPort;
    
    // Redis password
    private String redisPassword;
    
    // Redis orders channel
    private String redisOrdersChannel;
    
    // Redis admin channel
    private String redisAdminChannel;
    
    // Redis heartbeat channel
    private String redisHeartbeatChannel;
    
    // System enabled flag
    private boolean systemEnabled;
    
    // Heartbeat interval
    private int heartbeatInterval;
    
    /**
     * Gets the singleton instance of ConfigurationManager.
     * 
     * @return The singleton instance
     */
    public static synchronized ConfigurationManager getInstance() {
        if (instance == null) {
            instance = new ConfigurationManager();
        }
        return instance;
    }
    
    /**
     * Private constructor to enforce singleton pattern.
     */
    private ConfigurationManager() {
        this.properties = new Properties();
        loadConfiguration(DEFAULT_CONFIG_PATH);
    }
    
    /**
     * Loads configuration from the specified file.
     * 
     * @param configPath The configuration file path
     */
    public void loadConfiguration(String configPath) {
        try (InputStream input = new FileInputStream(configPath)) {
            // Load properties
            properties.load(input);
            
            // Parse properties
            redisHost = properties.getProperty("redis.host", "localhost");
            redisPort = Integer.parseInt(properties.getProperty("redis.port", "6379"));
            redisPassword = properties.getProperty("redis.password", "");
            redisOrdersChannel = properties.getProperty("redis.orders.channel", "orders");
            redisAdminChannel = properties.getProperty("redis.admin.channel", "admin");
            redisHeartbeatChannel = properties.getProperty("redis.heartbeat.channel", "heartbeat");
            systemEnabled = Boolean.parseBoolean(properties.getProperty("system.enabled", "true"));
            heartbeatInterval = Integer.parseInt(properties.getProperty("heartbeat.interval", "5000"));
            
            System.out.println("Configuration loaded from " + configPath);
        } catch (IOException e) {
            System.err.println("Error loading configuration: " + e.getMessage());
        }
    }
    
    /**
     * Gets the Redis host.
     * 
     * @return The Redis host
     */
    public String getRedisHost() {
        return redisHost;
    }
    
    /**
     * Gets the Redis port.
     * 
     * @return The Redis port
     */
    public int getRedisPort() {
        return redisPort;
    }
    
    /**
     * Gets the Redis password.
     * 
     * @return The Redis password
     */
    public String getRedisPassword() {
        return redisPassword;
    }
    
    /**
     * Gets the Redis orders channel.
     * 
     * @return The Redis orders channel
     */
    public String getRedisOrdersChannel() {
        return redisOrdersChannel;
    }
    
    /**
     * Gets the Redis admin channel.
     * 
     * @return The Redis admin channel
     */
    public String getRedisAdminChannel() {
        return redisAdminChannel;
    }
    
    /**
     * Gets the Redis heartbeat channel.
     * 
     * @return The Redis heartbeat channel
     */
    public String getRedisHeartbeatChannel() {
        return redisHeartbeatChannel;
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
        properties.setProperty("system.enabled", String.valueOf(systemEnabled));
    }
    
    /**
     * Gets the heartbeat interval.
     * 
     * @return The heartbeat interval
     */
    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }
    
    /**
     * Gets a property.
     * 
     * @param key The property key
     * @return The property value, or null if not found
     */
    public String getProperty(String key) {
        return properties.getProperty(key);
    }
    
    /**
     * Gets a property with a default value.
     * 
     * @param key The property key
     * @param defaultValue The default value
     * @return The property value, or the default value if not found
     */
    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
    
    /**
     * Sets a property.
     * 
     * @param key The property key
     * @param value The property value
     */
    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }
}
