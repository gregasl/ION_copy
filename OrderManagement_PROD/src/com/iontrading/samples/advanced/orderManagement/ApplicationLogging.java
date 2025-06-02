package com.iontrading.samples.advanced.orderManagement;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class provides a centralized way to initialize and configure
 * asynchronous logging for the entire application. All components
 * should use this class instead of setting up logging individually.
 */
public class ApplicationLogging {
    
    private static final Logger LOGGER = Logger.getLogger(ApplicationLogging.class.getName());
    private static boolean initialized = false;
    
    /**
     * Initialize logging for the entire application.
     * This method should be called once at application startup,
     * before any other logging operations.
     * 
     * @param applicationName Name of the application (used for log file naming)
     * @return true if initialization was successful, false otherwise
     */
    public static synchronized boolean initialize(String applicationName) {
        if (initialized) {
            System.out.println("Logging already initialized");
            return true;
        }
        
        try {
            System.out.println("### STARTUP: Initializing logging for " + applicationName);
            
            // Initialize the AsyncLoggingManager
            AsyncLoggingManager manager = AsyncLoggingManager.getInstance();
            manager.setupFileLogging(applicationName + ".log");
            
            // Set up machine-readable logging
            manager.setupMachineReadableLogging(applicationName);
            
            // Log initialization success
            logAsync(LOGGER, Level.INFO, "Asynchronous logging initialized for " + applicationName);
            
            // Wait up to 2 seconds for the first message to be processed
            System.out.println("### STARTUP: Waiting for logging system to process first message");
            if (!manager.waitForFirstMessageProcessed(2000)) {
                System.err.println("### WARNING: Logging system did not process first message within timeout");
            }
            
            // Register shutdown hook to ensure logs are flushed
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logAsync(LOGGER, Level.INFO, "Application shutting down, flushing logs");
                manager.shutdown();
            }));
            
            initialized = true;
            System.out.println("### STARTUP: Logging system fully initialized");
            return true;
            
        } catch (Exception e) {
            System.err.println("Failed to initialize logging: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * Log a message asynchronously.
     * This is the main method that all components should use for logging.
     * 
     * @param logger The logger to use
     * @param level The log level
     * @param message The message to log
     */
    public static void logAsync(Logger logger, Level level, String message) {
        if (!initialized) {
            // Fall back to synchronous logging if not initialized
            logger.log(level, message);
            return;
        }
        
        AsyncLoggingManager.getInstance().log(logger, level, message);
    }
    
    /**
     * Log a message with an exception asynchronously.
     * 
     * @param logger The logger to use
     * @param level The log level
     * @param message The message to log
     * @param thrown The exception to log
     */
    public static void logAsync(Logger logger, Level level, String message, Throwable thrown) {
        if (!initialized) {
            // Fall back to synchronous logging if not initialized
            logger.log(level, message, thrown);
            return;
        }
        
        AsyncLoggingManager.getInstance().log(logger, level, message, thrown);
    }
    
    /**
     * Set the log level for a specific logger or package
     * 
     * @param loggerName The name of the logger or package (e.g., "com.iontrading" or a specific class name)
     * @param level The desired logging level
     */
    public static void setLogLevel(String loggerName, Level level) {
        if (!initialized) {
            System.err.println("Logging not yet initialized. Call initialize() first.");
            return;
        }
        
        try {
            Logger logger = Logger.getLogger(loggerName);
            logger.setLevel(level);
            
            // Log the level change
            logAsync(LOGGER, Level.INFO, "Log level for " + loggerName + " set to " + level);
        } catch (Exception e) {
            System.err.println("Error setting log level: " + e.getMessage());
        }
    }
    
    /**
     * Convenience method to set log levels for multiple loggers
     * 
     * @param level The desired logging level
     * @param loggerNames List of logger or package names
     */
    public static void setLogLevels(Level level, String... loggerNames) {
        for (String loggerName : loggerNames) {
            setLogLevel(loggerName, level);
        }
    }
    
    /**
     * Set global logging level
     * 
     * @param level The desired global logging level
     */
    public static void setGlobalLogLevel(Level level) {
        if (!initialized) {
            System.err.println("Logging not yet initialized. Call initialize() first.");
            return;
        }
        
        try {
            // Set root logger level
            Logger rootLogger = Logger.getLogger("");
            rootLogger.setLevel(level);
            
            // Set application-specific package level
            Logger appLogger = Logger.getLogger("com.iontrading.samples.advanced");
            appLogger.setLevel(level);
            
            // Log the global level change
            logAsync(LOGGER, Level.INFO, "Global log level set to " + level);
        } catch (Exception e) {
            System.err.println("Error setting global log level: " + e.getMessage());
        }
    }
    

	/**
	 * Log order information in a machine-readable format (CSV).
	 * This is specifically for tracking order operations.
	 * 
	 * @param operation The operation type (e.g., SEND, FILL, CANCEL)
	 * @param source The market source
	 * @param trader The trader ID
	 * @param instrumentId The instrument ID
	 * @param direction Buy/Sell
	 * @param size Order size
	 * @param price Order price
	 * @param orderType Order type (e.g., Limit, Market)
	 * @param tif Time in force
	 * @param requestId Request ID
	 * @param orderId Order ID (can be null for new orders)
	 */
	public static void logOrderEvent(
	        String operation,
	        String source,
	        String trader,
	        String instrumentId,
	        String direction,
	        double size,
	        double price,
	        String orderType,
	        String tif,
	        int requestId,
	        String orderId) {
	    
	    if (!initialized) {
	        // Fall back to synchronous logging if not initialized
	        LOGGER.log(Level.INFO, String.format(
	            "ORDER,%s,%s,%s,%s,%s,%.2f,%.4f,%s,%s,%d,%s",
	            operation, source, trader, instrumentId, direction, 
	            size, price, orderType, tif, requestId, 
	            (orderId != null ? orderId : "")
	        ));
	        return;
	    }
	    
	    // Escape any commas in string fields
	    String safeInstrumentId = instrumentId.replace(",", "\\,");
	    String safeOrderId = (orderId != null) ? orderId.replace(",", "\\,") : "";
	    
	    // Format timestamp
	    String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(new java.util.Date());
	    
	    // Build CSV record
	    String csvRecord = String.format(
	        "%s,%s,%s,%s,%s,%s,%.2f,%.4f,%s,%s,%d,%s",
	        timestamp, operation, source, trader, safeInstrumentId, direction,
	        size, price, orderType, tif, requestId, safeOrderId
	    );
	    
	    // Log to machine-readable file
	    AsyncLoggingManager.getInstance().logMachineReadable(csvRecord);
	    
	    // Also log to normal log at FINE level for debugging
	    logAsync(LOGGER, Level.FINE, "Order event: " + csvRecord);
	}

	/**
	 * Simplified version for order updates where not all fields are changing
	 */
	public static void logOrderUpdate(
	        String operation,
	        int requestId,
	        String orderId,
	        String status) {
	    
	    if (!initialized) {
	        return;
	    }
	    
	    // Format timestamp
	    String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(new java.util.Date());
	    
	    // Build simplified CSV record with minimal fields
	    String csvRecord = String.format(
	        "%s,%s,,,,,,,,,,%d,%s,%s",
	        timestamp, operation, requestId, orderId, status
	    );
	    
	    // Log to machine-readable file
	    AsyncLoggingManager.getInstance().logMachineReadable(csvRecord);
	}
    
    /**
     * Get logging statistics for monitoring.
     * 
     * @return String containing current logging statistics
     */
    public static String getStatistics() {
        if (!initialized) {
            return "Logging not initialized";
        }
        
        AsyncLoggingManager manager = AsyncLoggingManager.getInstance();
        return String.format(
            "Logging stats: queued=%d, processed=%d, overflows=%d, queueSize=%d",
            manager.getMessagesQueued(),
            manager.getMessagesProcessed(),
            manager.getQueueOverflows(),
            manager.getCurrentQueueSize()
        );
    }
    
    /**
     * Log statistics about the logging system.
     * Useful for periodic monitoring.
     * 
     * @param level The log level to use
     */
    public static void logStatistics(Level level) {
        if (!initialized) {
            return;
        }
        
        logAsync(LOGGER, level, getStatistics());
    }
}