package com.iontrading.redisbridge.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides logging utilities.
 * This class is responsible for logging utilities.
 */
public class LoggingUtils {
    
    // Date format for timestamps
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    
    /**
     * Gets the current timestamp.
     * 
     * @return The current timestamp
     */
    public static String getCurrentTimestamp() {
        return DATE_FORMAT.format(new Date());
    }
    
    /**
     * Creates a log message.
     * 
     * @param level The log level
     * @param message The log message
     * @return The log message as a JSON string
     */
    public static String createLogMessage(String level, String message) {
        Map<String, Object> logMessage = new HashMap<>();
        logMessage.put("timestamp", getCurrentTimestamp());
        logMessage.put("level", level);
        logMessage.put("message", message);
        
        return JsonUtils.toJson(logMessage);
    }
    
    /**
     * Creates a log message with details.
     * 
     * @param level The log level
     * @param message The log message
     * @param details The log details
     * @return The log message as a JSON string
     */
    public static String createLogMessage(String level, String message, Map<String, Object> details) {
        Map<String, Object> logMessage = new HashMap<>();
        logMessage.put("timestamp", getCurrentTimestamp());
        logMessage.put("level", level);
        logMessage.put("message", message);
        logMessage.put("details", details);
        
        return JsonUtils.toJson(logMessage);
    }
    
    /**
     * Creates a log message with a single detail.
     * 
     * @param level The log level
     * @param message The log message
     * @param detailKey The detail key
     * @param detailValue The detail value
     * @return The log message as a JSON string
     */
    public static String createLogMessage(String level, String message, String detailKey, Object detailValue) {
        Map<String, Object> details = new HashMap<>();
        details.put(detailKey, detailValue);
        
        return createLogMessage(level, message, details);
    }
    
    /**
     * Creates a details map.
     * 
     * @param key The key
     * @param value The value
     * @return The details map
     */
    public static Map<String, Object> createDetailsMap(String key, Object value) {
        Map<String, Object> details = new HashMap<>();
        details.put(key, value);
        return details;
    }
    
    /**
     * Creates a details map.
     * 
     * @param key1 The first key
     * @param value1 The first value
     * @param key2 The second key
     * @param value2 The second value
     * @return The details map
     */
    public static Map<String, Object> createDetailsMap(String key1, Object value1, String key2, Object value2) {
        Map<String, Object> details = new HashMap<>();
        details.put(key1, value1);
        details.put(key2, value2);
        return details;
    }
    
    /**
     * Creates a details map.
     * 
     * @param key1 The first key
     * @param value1 The first value
     * @param key2 The second key
     * @param value2 The second value
     * @param key3 The third key
     * @param value3 The third value
     * @return The details map
     */
    public static Map<String, Object> createDetailsMap(String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        Map<String, Object> details = new HashMap<>();
        details.put(key1, value1);
        details.put(key2, value2);
        details.put(key3, value3);
        return details;
    }
    
    /**
     * Private constructor to prevent instantiation.
     */
    private LoggingUtils() {
        // Do nothing
    }
}
