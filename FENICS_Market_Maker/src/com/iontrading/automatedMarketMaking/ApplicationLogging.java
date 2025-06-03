package com.iontrading.automatedMarketMaking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;
import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.classic.Level;
import ch.qos.logback.core.Appender;  

import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.File;

/**
 * Centralized logging initialization - completely programmatic configuration
 */
public class ApplicationLogging {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationLogging.class);
    private static boolean initialized = false;
    
    /**
     * Initialize SLF4J/Logback logging programmatically - no XML required
     */
    public static boolean initialize(String applicationName) {
        if (initialized) {
            return true;
        }
        
        try {
            // Bridge java.util.logging to SLF4J
            if (!SLF4JBridgeHandler.isInstalled()) {
                SLF4JBridgeHandler.removeHandlersForRootLogger();
                SLF4JBridgeHandler.install();
            }
            
            // Get the default log directory
            String logDir = System.getProperty("log.dir", "logs");
            
            // Ensure log directory exists
            File dir = new File(logDir);
            if (!dir.exists()) {
                boolean created = dir.mkdirs();
                if (!created) {
                    System.err.println("Failed to create log directory: " + dir.getAbsolutePath());
                    return false;
                }
            }
            
            // Configure logback programmatically
            configureProgrammaticLogging(applicationName, logDir);
            
            LOGGER.info("=== SLF4J/Logback logging initialized programmatically for {} ===", applicationName);
            LOGGER.info("Log directory: {}", dir.getAbsolutePath());
            
            initialized = true;
            return true;
            
        } catch (Exception e) {
            System.err.println("Failed to initialize SLF4J logging: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * Configure logback entirely in Java code
     */
    private static void configureProgrammaticLogging(String applicationName, String logDir) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        
        // Clear any existing configuration
        context.reset();
        
        String datePattern = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        
        // 1. Create Console Appender
        ConsoleAppender<ILoggingEvent> consoleAppender = createConsoleAppender(context);
        
        // 2. Create Main Log File Appender
        RollingFileAppender<ILoggingEvent> mainFileAppender = createMainFileAppender(context, 
            logDir + "/" + applicationName + "_" + datePattern + ".log",
            logDir + "/" + applicationName + "_%d{yyyy-MM-dd}.%i.log");

        // 4. Create Async Appenders for Performance
        AsyncAppender asyncMainAppender = createAsyncAppender(context, "ASYNC_MAIN", mainFileAppender);
        
        // 6. Configure Application Loggers
        ch.qos.logback.classic.Logger appLogger = context.getLogger("com.iontrading.automatedMarketMaking");
        appLogger.setLevel(Level.DEBUG);  // Now unambiguous
        
        // 7. Configure Root Logger
        ch.qos.logback.classic.Logger rootLogger = context.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.INFO);  // Now unambiguous
        rootLogger.addAppender(consoleAppender);
        rootLogger.addAppender(asyncMainAppender);
        
        // Start all appenders
        consoleAppender.start();
        mainFileAppender.start();
        asyncMainAppender.start();
        
        System.out.println("Programmatic logging configuration completed");
        System.out.println("Main log file: " + logDir + "/" + applicationName + "_" + datePattern + ".log");
    }
    
    /**
     * Create console appender
     */
    private static ConsoleAppender<ILoggingEvent> createConsoleAppender(LoggerContext context) {
        ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();
        consoleAppender.setContext(context);
        consoleAppender.setName("CONSOLE");
        
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern("%d{HH:mm:ss.SSS} [%thread] %-5level %c{1} - %msg%n");
        encoder.start();
        
        consoleAppender.setEncoder(encoder);
        return consoleAppender;
    }
    
    /**
     * Create main log file appender with rolling
     */
    private static RollingFileAppender<ILoggingEvent> createMainFileAppender(LoggerContext context, 
            String fileName, String fileNamePattern) {
        RollingFileAppender<ILoggingEvent> fileAppender = new RollingFileAppender<>();
        fileAppender.setContext(context);
        fileAppender.setName("MAIN_FILE");
        fileAppender.setFile(fileName);
        
        // Create encoder
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %c{1} - %msg%n");
        encoder.start();
        fileAppender.setEncoder(encoder);
        
        // Create rolling policy
        SizeAndTimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new SizeAndTimeBasedRollingPolicy<>();
        rollingPolicy.setContext(context);
        rollingPolicy.setParent(fileAppender);
        rollingPolicy.setFileNamePattern(fileNamePattern);
        rollingPolicy.setMaxFileSize(FileSize.valueOf("100MB"));
        rollingPolicy.setMaxHistory(30);
        rollingPolicy.setTotalSizeCap(FileSize.valueOf("3GB"));
        rollingPolicy.start();
        
        fileAppender.setRollingPolicy(rollingPolicy);
        return fileAppender;
    }

    /**
     * Create async appender for performance
     */
    private static AsyncAppender createAsyncAppender(LoggerContext context, String name, 
            Appender<ILoggingEvent> appender) {
        AsyncAppender asyncAppender = new AsyncAppender();
        asyncAppender.setContext(context);
        asyncAppender.setName(name);
        asyncAppender.setQueueSize(1024);
        asyncAppender.setDiscardingThreshold(0);
        asyncAppender.setIncludeCallerData(false);
        asyncAppender.setNeverBlock(true);
        asyncAppender.addAppender(appender);
        return asyncAppender;
    }
    
    /**
     * Test method to verify logging is working
     */
    public static void testLogging() {
        LOGGER.info("=== Testing Programmatic Logging Configuration ===");
        
        // Test regular logging at different levels
        LOGGER.error("TEST: Error level logging");
        LOGGER.warn("TEST: Warn level logging");
        LOGGER.info("TEST: Info level logging");
        LOGGER.debug("TEST: Debug level logging");
        
        LOGGER.info("=== Logging test complete - check logs directory ===");
    }
    
    /**
     * Get logging statistics
     */
    public static String getStatistics() {
        if (!initialized) {
            return "Logging not initialized";
        }
        
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        return String.format("SLF4J logging active, loggers: %d", loggerContext.getLoggerList().size());
    }
    
    /**
     * Set log level for a specific logger using SLF4J level names
     */
    public static void setLogLevel(String loggerName, String level) {
        try {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            ch.qos.logback.classic.Logger logger = loggerContext.getLogger(loggerName);
            logger.setLevel(Level.valueOf(level.toUpperCase()));
            
            LOGGER.info("Log level for {} set to {}", loggerName, level);
        } catch (Exception e) {
            LOGGER.error("Error setting log level for {}: {}", loggerName, e.getMessage());
        }
    }

    /**
     * Set log level for a specific logger - backward compatibility with java.util.logging.Level
     */
    public static void setLogLevel(String loggerName, java.util.logging.Level level) {
        String slf4jLevel = convertJulLevelToSLF4J(level);
        setLogLevel(loggerName, slf4jLevel);
    }

    /**
     * Set log levels for multiple loggers using SLF4J level names
     */
    public static void setLogLevels(String level, String... loggerNames) {
        for (String loggerName : loggerNames) {
            setLogLevel(loggerName, level);
        }
    }

    /**
     * Set log levels for multiple loggers - backward compatibility
     */
    public static void setLogLevels(java.util.logging.Level level, String... loggerNames) {
        String slf4jLevel = convertJulLevelToSLF4J(level);
        setLogLevels(slf4jLevel, loggerNames);
    }

    /**
     * Set global logging level using SLF4J level names
     */
    public static void setGlobalLogLevel(String level) {
        try {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            
            // Set root logger level
            ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
            rootLogger.setLevel(Level.valueOf(level.toUpperCase()));
            
            // Set application package level
            ch.qos.logback.classic.Logger appLogger = loggerContext.getLogger("com.iontrading.samples.advanced");
            appLogger.setLevel(Level.valueOf(level.toUpperCase()));
            
            LOGGER.info("Global log level set to {}", level);
        } catch (Exception e) {
            LOGGER.error("Error setting global log level: {}", e.getMessage());
        }
    }

    /**
     * Set global logging level - backward compatibility
     */
    public static void setGlobalLogLevel(java.util.logging.Level level) {
        String slf4jLevel = convertJulLevelToSLF4J(level);
        setGlobalLogLevel(slf4jLevel);
    }

    /**
     * Convert java.util.logging.Level to SLF4J level name
     */
    private static String convertJulLevelToSLF4J(java.util.logging.Level level) {
        if (level.intValue() >= java.util.logging.Level.SEVERE.intValue()) {
            return "ERROR";
        } else if (level.intValue() >= java.util.logging.Level.WARNING.intValue()) {
            return "WARN";
        } else if (level.intValue() >= java.util.logging.Level.INFO.intValue()) {
            return "INFO";
        } else if (level.intValue() >= java.util.logging.Level.FINE.intValue()) {
            return "DEBUG";
        } else {
            return "TRACE";
        }
    }

    /**
     * Async logging method - converts java.util.logging.Level to SLF4J
     */
    public static void logAsync(Logger logger, java.util.logging.Level level, String message) {
        if (!initialized) {
            System.out.println("Logging not initialized: " + message);
            return;
        }
        
        if (level.intValue() >= java.util.logging.Level.SEVERE.intValue()) {
            logger.error(message);
        } else if (level.intValue() >= java.util.logging.Level.WARNING.intValue()) {
            logger.warn(message);
        } else if (level.intValue() >= java.util.logging.Level.INFO.intValue()) {
            logger.info(message);
        } else if (level.intValue() >= java.util.logging.Level.FINE.intValue()) {
            logger.debug(message);
        } else {
            logger.trace(message);
        }
    }

    /**
     * Async logging method with exception
     */
    public static void logAsync(Logger logger, java.util.logging.Level level, String message, Throwable throwable) {
        if (!initialized) {
            System.out.println("Logging not initialized: " + message);
            if (throwable != null) {
                throwable.printStackTrace();
            }
            return;
        }
        
        if (level.intValue() >= java.util.logging.Level.SEVERE.intValue()) {
            logger.error(message, throwable);
        } else if (level.intValue() >= java.util.logging.Level.WARNING.intValue()) {
            logger.warn(message, throwable);
        } else if (level.intValue() >= java.util.logging.Level.INFO.intValue()) {
            logger.info(message, throwable);
        } else if (level.intValue() >= java.util.logging.Level.FINE.intValue()) {
            logger.debug(message, throwable);
        } else {
            logger.trace(message, throwable);
        }
    }

    /**
     * Log order update events
     */
    public static void logOrderUpdate(String orderId, int status, String message, String additionalInfo) {
        if (!initialized) {
            System.out.println("Logging not initialized. Order update: " + orderId + ", Status: " + status + ", " + message);
            return;
        }
        
        LOGGER.info("ORDER UPDATE - ID: {}, Status: {}, Message: {}, Additional: {}", 
                   orderId, status, message, additionalInfo);
    }
    
    /**
     * Log order events with all parameters
     */
    public static void logOrderEvent(String eventType, String orderId, String instrumentId, String side,
                                    String action, double price, double quantity, String status, 
                                    String exchange, int statusCode, String additionalInfo) {
        if (!initialized) {
            System.out.println("Logging not initialized. Order event: " + eventType + ", Order: " + orderId);
            return;
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("ORDER EVENT: ").append(eventType)
          .append(", ID: ").append(orderId)
          .append(", Instrument: ").append(instrumentId)
          .append(", Side: ").append(side)
          .append(", Action: ").append(action)
          .append(", Price: ").append(price)
          .append(", Qty: ").append(quantity)
          .append(", Status: ").append(status)
          .append(", Exchange: ").append(exchange)
          .append(", StatusCode: ").append(statusCode);
          
        if (additionalInfo != null) {
            sb.append(", Info: ").append(additionalInfo);
        }
        
        LOGGER.info(sb.toString());
    }
    
    /**
     * Log order events - overloaded version for null additional info
     */
    public static void logOrderEvent(String eventType, String orderId, String instrumentId, String side,
                                    String action, double price, double quantity, String status, 
                                    String exchange, int statusCode) {
        logOrderEvent(eventType, orderId, instrumentId, side, action, price, quantity, 
                      status, exchange, statusCode, null);
    }
}