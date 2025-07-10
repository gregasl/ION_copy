
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;
import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.classic.Level;
import ch.qos.logback.core.Appender;  

import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Centralized logging initialization - completely programmatic configuration
 * Enhanced with extensive diagnostic capabilities
 */
public class ApplicationLogging {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationLogging.class);
    private static boolean initialized = false;
    private static final Map<String, Long> methodEntryTimes = new ConcurrentHashMap<>();
    private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static volatile int errorCount = 0;
    private static volatile int warningCount = 0;
    
    // Debug flag - set to true for maximum verbosity
    private static volatile boolean DEBUG_MODE = false;
    
    /**
     * Initialize SLF4J/Logback logging programmatically - no XML required
     */
    public static boolean initialize(String applicationName) {
        logMethodEntry("initialize", "Starting logging initialization for application: " + applicationName);
        
        if (initialized) {
            LOGGER.info("Logging already initialized for {}", applicationName);
            return true;
        }
        
        try {
            LOGGER.info("Initializing logging for {}", applicationName);
            
            // Bridge java.util.logging to SLF4J
            if (!SLF4JBridgeHandler.isInstalled()) {
                LOGGER.info("Installing SLF4JBridgeHandler to redirect java.util.logging");
                SLF4JBridgeHandler.removeHandlersForRootLogger();
                SLF4JBridgeHandler.install();
                LOGGER.info("SLF4JBridgeHandler successfully installed");
            } else {
                LOGGER.info("SLF4JBridgeHandler already installed");
            }
            
            // Get the default log directory
            String logDir = System.getProperty("log.dir", "logs");
            LOGGER.info("Using log directory: {}", logDir);
            
            // Ensure log directory exists
            File dir = new File(logDir);
            if (!dir.exists()) {
                LOGGER.info("Log directory does not exist, creating: {}", dir.getAbsolutePath());
                boolean created = dir.mkdirs();
                if (!created) {
                    LOGGER.error("FAILED to create log directory: {}", dir.getAbsolutePath());
                    System.err.println("Failed to create log directory: " + dir.getAbsolutePath());
                    return false;
                }
                LOGGER.info("Log directory created successfully: {}", dir.getAbsolutePath());
            } else {
                LOGGER.info("Log directory already exists: {}", dir.getAbsolutePath());
            }
            
            // Configure logback programmatically
            LOGGER.info("Configuring Logback programmatically");
            configureProgrammaticLogging(applicationName, logDir);
            
            LOGGER.info("=== SLF4J/Logback logging initialized programmatically for {} ===", applicationName);
            LOGGER.info("Log directory: {}", dir.getAbsolutePath());
            
            logSystemInfo();
            
            initialized = true;
            logMethodExit("initialize", "Logging initialization completed successfully");
            return true;
            
        } catch (Exception e) {
            logException("initialize", "Failed to initialize SLF4J logging", e);
            System.err.println("Failed to initialize SLF4J logging: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * Configure logback entirely in Java code
     */
    private static void configureProgrammaticLogging(String applicationName, String logDir) {
        logMethodEntry("configureProgrammaticLogging", "Configuring Logback for app: " + applicationName + ", dir: " + logDir);
        
        try {
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            
            // Clear any existing configuration
            LOGGER.info("Clearing existing Logback configuration");
            context.reset();
            
            String datePattern = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            LOGGER.info("Using date pattern for log files: {}", datePattern);
            
            // 1. Create Console Appender
            LOGGER.info("Creating console appender");
            ConsoleAppender<ILoggingEvent> consoleAppender = createConsoleAppender(context);
            
            // 2. Create Main Log File Appender
            LOGGER.info("Creating main log file appender");
            String mainLogFilename = logDir + "/" + applicationName + "_" + datePattern + ".log";
            String mainLogPattern = logDir + "/" + applicationName + "_%d{yyyy-MM-dd}.%i.log";
            LOGGER.info("Main log file: {}", mainLogFilename);
            LOGGER.info("Main log pattern: {}", mainLogPattern);
            
            RollingFileAppender<ILoggingEvent> mainFileAppender = 
                createMainFileAppender(context, mainLogFilename, mainLogPattern);
    
            // 3. Create Error Log File Appender
            LOGGER.info("Creating error log file appender");
            String errorLogFilename = logDir + "/" + applicationName + "_ERROR_" + datePattern + ".log";
            String errorLogPattern = logDir + "/" + applicationName + "_ERROR_%d{yyyy-MM-dd}.%i.log";
            LOGGER.info("Error log file: {}", errorLogFilename);
            LOGGER.info("Error log pattern: {}", errorLogPattern);
            
            RollingFileAppender<ILoggingEvent> errorFileAppender = 
                createErrorFileAppender(context, errorLogFilename, errorLogPattern);
    
            // 4. Create Async Appenders for Performance
            LOGGER.info("Creating async appenders");
            AsyncAppender asyncMainAppender = createAsyncAppender(context, "ASYNC_MAIN", mainFileAppender);
            AsyncAppender asyncErrorAppender = createAsyncAppender(context, "ASYNC_ERROR", errorFileAppender);
            
            // 5. Configure Application Loggers
            LOGGER.info("Configuring application loggers");
            ch.qos.logback.classic.Logger appLogger = context.getLogger("com.iontrading.automatedMarketMaking");
            appLogger.setLevel(Level.DEBUG);
            appLogger.setAdditive(false); // Don't inherit appenders from root
            appLogger.addAppender(asyncMainAppender);
            appLogger.addAppender(asyncErrorAppender);
            appLogger.addAppender(consoleAppender);
            
            // 6. Configure Root Logger
            LOGGER.info("Configuring root logger");
            ch.qos.logback.classic.Logger rootLogger = context.getLogger(Logger.ROOT_LOGGER_NAME);
            rootLogger.setLevel(Level.INFO);
            rootLogger.addAppender(consoleAppender);
            rootLogger.addAppender(asyncMainAppender);
            rootLogger.addAppender(asyncErrorAppender);
            
            // Start all appenders
            LOGGER.info("Starting all appenders");
            consoleAppender.start();
            mainFileAppender.start();
            errorFileAppender.start();
            asyncMainAppender.start();
            asyncErrorAppender.start();
            
            System.out.println("Programmatic logging configuration completed");
            System.out.println("Main log file: " + mainLogFilename);
            System.out.println("Error log file: " + errorLogFilename);
            
            logMethodExit("configureProgrammaticLogging", "Logback configuration completed successfully");
        } catch (Exception e) {
            logException("configureProgrammaticLogging", "Failed to configure Logback programmatically", e);
        }
    }
    
    /**
     * Create console appender
     */
    private static ConsoleAppender<ILoggingEvent> createConsoleAppender(LoggerContext context) {
        logMethodEntry("createConsoleAppender", "Creating console appender");
        
        try {
            ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();
            consoleAppender.setContext(context);
            consoleAppender.setName("CONSOLE");
            
            PatternLayoutEncoder encoder = new PatternLayoutEncoder();
            encoder.setContext(context);
            encoder.setPattern("%d{HH:mm:ss.SSS} [%thread] %-5level %c{1} [%X{methodName}] - %msg%n");
            encoder.start();
            
            consoleAppender.setEncoder(encoder);
            
            logMethodExit("createConsoleAppender", "Console appender created successfully");
            return consoleAppender;
        } catch (Exception e) {
            logException("createConsoleAppender", "Failed to create console appender", e);
            throw new RuntimeException("Failed to create console appender", e);
        }
    }
    
    /**
     * Create main log file appender with rolling
     */
    private static RollingFileAppender<ILoggingEvent> createMainFileAppender(LoggerContext context, 
            String fileName, String fileNamePattern) {
        logMethodEntry("createMainFileAppender", "Creating main file appender: " + fileName);
        
        try {
            RollingFileAppender<ILoggingEvent> fileAppender = new RollingFileAppender<>();
            fileAppender.setContext(context);
            fileAppender.setName("MAIN_FILE");
            fileAppender.setFile(fileName);
            
            // Create encoder with enhanced pattern (includes method name from MDC)
            PatternLayoutEncoder encoder = new PatternLayoutEncoder();
            encoder.setContext(context);
            encoder.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %c{1} [%X{methodName}] - %msg%n");
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
            
            logMethodExit("createMainFileAppender", "Main file appender created successfully");
            return fileAppender;
        } catch (Exception e) {
            logException("createMainFileAppender", "Failed to create main file appender", e);
            throw new RuntimeException("Failed to create main file appender", e);
        }
    }

    /**
     * Create error log file appender with rolling
     */
    private static RollingFileAppender<ILoggingEvent> createErrorFileAppender(LoggerContext context, 
            String fileName, String fileNamePattern) {
        logMethodEntry("createErrorFileAppender", "Creating error file appender: " + fileName);
        
        try {
            RollingFileAppender<ILoggingEvent> fileAppender = new RollingFileAppender<>();
            fileAppender.setContext(context);
            fileAppender.setName("ERROR_FILE");
            fileAppender.setFile(fileName);
            
            // Create encoder with enhanced pattern
            PatternLayoutEncoder encoder = new PatternLayoutEncoder();
            encoder.setContext(context);
            encoder.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %c{1} [%X{methodName}] - %msg%n");
            encoder.start();
            fileAppender.setEncoder(encoder);
            
            // Create rolling policy
            SizeAndTimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new SizeAndTimeBasedRollingPolicy<>();
            rollingPolicy.setContext(context);
            rollingPolicy.setParent(fileAppender);
            rollingPolicy.setFileNamePattern(fileNamePattern);
            rollingPolicy.setMaxFileSize(FileSize.valueOf("50MB"));
            rollingPolicy.setMaxHistory(30);
            rollingPolicy.setTotalSizeCap(FileSize.valueOf("1GB"));
            rollingPolicy.start();
            
            fileAppender.setRollingPolicy(rollingPolicy);
            
            // Create filter to only include ERROR level
            ch.qos.logback.classic.filter.LevelFilter filter = new ch.qos.logback.classic.filter.LevelFilter();
            filter.setLevel(Level.ERROR);
            filter.setOnMatch(ch.qos.logback.core.spi.FilterReply.ACCEPT);
            filter.setOnMismatch(ch.qos.logback.core.spi.FilterReply.DENY);
            filter.start();
            
            fileAppender.addFilter(filter);
            
            logMethodExit("createErrorFileAppender", "Error file appender created successfully");
            return fileAppender;
        } catch (Exception e) {
            logException("createErrorFileAppender", "Failed to create error file appender", e);
            throw new RuntimeException("Failed to create error file appender", e);
        }
    }

    /**
     * Create async appender for performance
     */
    private static AsyncAppender createAsyncAppender(LoggerContext context, String name, 
            Appender<ILoggingEvent> appender) {
        logMethodEntry("createAsyncAppender", "Creating async appender: " + name);
        
        try {
            AsyncAppender asyncAppender = new AsyncAppender();
            asyncAppender.setContext(context);
            asyncAppender.setName(name);
            asyncAppender.setQueueSize(1024);
            asyncAppender.setDiscardingThreshold(0); // Don't discard any events
            asyncAppender.setIncludeCallerData(true); // Include caller data for better stack traces
            asyncAppender.setNeverBlock(false); // Block if queue is full to prevent data loss
            asyncAppender.addAppender(appender);
            
            logMethodExit("createAsyncAppender", "Async appender created successfully: " + name);
            return asyncAppender;
        } catch (Exception e) {
            logException("createAsyncAppender", "Failed to create async appender: " + name, e);
            throw new RuntimeException("Failed to create async appender: " + name, e);
        }
    }
    
    /**
     * Set debug mode on/off for extra verbose logging
     */
    public static void setDebugMode(boolean enabled) {
        DEBUG_MODE = enabled;
        LOGGER.info("Debug mode set to: {}", enabled);
        
        // Update log levels based on debug mode
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger appLogger = loggerContext.getLogger("com.iontrading.automatedMarketMaking");
        
        if (enabled) {
            appLogger.setLevel(Level.TRACE);
            LOGGER.info("Set application logger to TRACE level");
        } else {
            appLogger.setLevel(Level.DEBUG);
            LOGGER.info("Set application logger to DEBUG level");
        }
    }
    
    /**
     * Returns whether debug mode is currently enabled
     * @return true if debug mode is enabled, false otherwise
     */
    public static boolean getDebugMode() {
        return DEBUG_MODE;
    }
    
    /**
     * Test method to verify logging is working
     */
    public static void testLogging() {
        logMethodEntry("testLogging", "Starting logging test");
        
        LOGGER.info("=== Testing Programmatic Logging Configuration ===");
        
        // Test regular logging at different levels
        LOGGER.error("TEST: Error level logging");
        errorCount++;
        LOGGER.warn("TEST: Warn level logging");
        warningCount++;
        LOGGER.info("TEST: Info level logging");
        LOGGER.debug("TEST: Debug level logging");
        
        // Test exception logging
        try {
            throw new Exception("Test exception");
        } catch (Exception e) {
            logException("testLogging", "TEST: Exception logging test", e);
        }
        
        // Test with customer parameters
        LOGGER.info("TEST: Parameterized logging: {} and {}", "param1", "param2");
        
        LOGGER.info("=== Logging test complete - check logs directory ===");
        logMethodExit("testLogging", "Logging test completed");
    }
    
    /**
     * Get logging statistics
     */
    public static String getStatistics() {
        if (!initialized) {
            return "Logging not initialized";
        }
        
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        return String.format("SLF4J logging active, loggers: %d, errors: %d, warnings: %d", 
            loggerContext.getLoggerList().size(), errorCount, warningCount);
    }
    
    /**
     * Log system information for diagnostics
     */
    public static void logSystemInfo() {
        LOGGER.info("=== System Information ===");
        LOGGER.info("Java Version: {}", System.getProperty("java.version"));
        LOGGER.info("Java Vendor: {}", System.getProperty("java.vendor"));
        LOGGER.info("OS Name: {}", System.getProperty("os.name"));
        LOGGER.info("OS Version: {}", System.getProperty("os.version"));
        LOGGER.info("User Name: {}", System.getProperty("user.name"));
        LOGGER.info("User Home: {}", System.getProperty("user.home"));
        LOGGER.info("User Directory: {}", System.getProperty("user.dir"));
        LOGGER.info("Available Processors: {}", Runtime.getRuntime().availableProcessors());
        LOGGER.info("Max Memory: {} MB", Runtime.getRuntime().maxMemory() / (1024 * 1024));
        LOGGER.info("Total Memory: {} MB", Runtime.getRuntime().totalMemory() / (1024 * 1024));
        LOGGER.info("Free Memory: {} MB", Runtime.getRuntime().freeMemory() / (1024 * 1024));
        LOGGER.info("=== End System Information ===");
    }
    
    /**
     * Set log level for a specific logger using SLF4J level names
     */
    public static void setLogLevel(String loggerName, String level) {
        logMethodEntry("setLogLevel", "Setting log level for " + loggerName + " to " + level);
        
        try {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            ch.qos.logback.classic.Logger logger = loggerContext.getLogger(loggerName);
            Level oldLevel = logger.getLevel();
            logger.setLevel(Level.valueOf(level.toUpperCase()));
            
            LOGGER.info("Log level for {} changed from {} to {}", loggerName, oldLevel, level);
            logMethodExit("setLogLevel", "Log level set successfully");
        } catch (Exception e) {
            errorCount++;
            logException("setLogLevel", "Error setting log level for " + loggerName, e);
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
        logMethodEntry("setLogLevels", "Setting multiple log levels to " + level);
        
        for (String loggerName : loggerNames) {
            setLogLevel(loggerName, level);
        }
        
        logMethodExit("setLogLevels", "Multiple log levels set successfully");
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
        logMethodEntry("setGlobalLogLevel", "Setting global log level to " + level);
        
        try {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            
            // Set root logger level
            ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
            Level oldRootLevel = rootLogger.getLevel();
            rootLogger.setLevel(Level.valueOf(level.toUpperCase()));
            
            // Set application package level
            ch.qos.logback.classic.Logger appLogger = loggerContext.getLogger("com.iontrading.automatedMarketMaking");
            Level oldAppLevel = appLogger.getLevel();
            appLogger.setLevel(Level.valueOf(level.toUpperCase()));
            
            LOGGER.info("Global log level set to {} (root was: {}, app was: {})", 
                level, oldRootLevel, oldAppLevel);
            
            logMethodExit("setGlobalLogLevel", "Global log level set successfully");
        } catch (Exception e) {
            errorCount++;
            logException("setGlobalLogLevel", "Error setting global log level", e);
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
     * Record method entry with parameters for tracing
     */
    public static void logMethodEntry(String methodName, String details) {
        if (!initialized) {
            System.out.println(TIMESTAMP_FORMAT.format(new Date()) + " - ENTRY: " + methodName + " - " + details);
            return;
        }
        
        // Store method entry time for performance tracking
        methodEntryTimes.put(methodName + ":" + Thread.currentThread().getId(), System.currentTimeMillis());
        
        // Add method name to MDC for logging context
        org.slf4j.MDC.put("methodName", methodName);
        
        // Use trace to avoid flooding logs with method entries/exits in normal mode
        if (DEBUG_MODE) {
            LOGGER.debug("ENTRY: {} - {}", methodName, details);
        } else {
            LOGGER.trace("ENTRY: {} - {}", methodName, details);
        }
    }

    /**
     * Record method exit with result details for tracing
     */
    public static void logMethodExit(String methodName, String result) {
        if (!initialized) {
            System.out.println(TIMESTAMP_FORMAT.format(new Date()) + " - EXIT: " + methodName + " - " + result);
            return;
        }
        
        // Calculate method execution time if entry was logged
        String key = methodName + ":" + Thread.currentThread().getId();
        Long startTime = methodEntryTimes.remove(key);
        long duration = startTime != null ? System.currentTimeMillis() - startTime : -1;
        
        // Add method name to MDC for logging context
        org.slf4j.MDC.put("methodName", methodName);
        
        // Log at trace level to avoid flooding logs with method entries/exits in normal mode
        if (duration >= 0) {
            if (DEBUG_MODE) {
                LOGGER.debug("EXIT: {} - {} (duration: {} ms)", methodName, result, duration);
            } else {
                LOGGER.trace("EXIT: {} - {} (duration: {} ms)", methodName, result, duration);
            }
        } else {
            if (DEBUG_MODE) {
                LOGGER.debug("EXIT: {} - {}", methodName, result);
            } else {
                LOGGER.trace("EXIT: {} - {}", methodName, result);
            }
        }
        
        // Clear MDC
        org.slf4j.MDC.remove("methodName");
    }

    /**
     * Enhanced exception logging with stack trace and context
     */
    public static void logException(String methodName, String message, Throwable throwable) {
        errorCount++;
        
        if (!initialized) {
            System.err.println(TIMESTAMP_FORMAT.format(new Date()) + " - ERROR in " + methodName + ": " + message);
            if (throwable != null) {
                throwable.printStackTrace();
            }
            return;
        }
        
        // Add method name to MDC for logging context
        org.slf4j.MDC.put("methodName", methodName);
        
        // Extract detailed information about the exception
        String exceptionType = throwable != null ? throwable.getClass().getName() : "Unknown";
        String exceptionMessage = throwable != null ? throwable.getMessage() : "No message";
        String stackTrace = getStackTraceAsString(throwable);
        
        // Log the exception with full details
        LOGGER.error("Exception in {}: {} - Type: {}, Message: {}\nStack trace:\n{}", 
            methodName, message, exceptionType, exceptionMessage, stackTrace);
        
        // Clear MDC
        org.slf4j.MDC.remove("methodName");
    }

    /**
     * Convert stack trace to string for logging
     */
    private static String getStackTraceAsString(Throwable throwable) {
        if (throwable == null) {
            return "No stack trace available";
        }
        
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Async logging method - converts java.util.logging.Level to SLF4J
     */
    public static void logAsync(Logger logger, java.util.logging.Level level, String message) {
        if (!initialized) {
            System.out.println("Logging not initialized: " + message);
            return;
        }
        
        // Use the calling method name for context if available
        String callerMethod = getCallerMethodName();
        org.slf4j.MDC.put("methodName", callerMethod != null ? callerMethod : "unknown");
        
        try {
            if (level.intValue() >= java.util.logging.Level.SEVERE.intValue()) {
                errorCount++;
                logger.error("{}",message);
            } else if (level.intValue() >= java.util.logging.Level.WARNING.intValue()) {
                warningCount++;
                logger.warn("{}", message);
            } else if (level.intValue() >= java.util.logging.Level.INFO.intValue()) {
                logger.info("{}", message);
            } else if (level.intValue() >= java.util.logging.Level.FINE.intValue()) {
                logger.debug("{}", message);
            } else {
                logger.trace("{}", message);
            }
        } finally {
            org.slf4j.MDC.remove("methodName");
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
        
        // Use the calling method name for context if available
        String callerMethod = getCallerMethodName();
        org.slf4j.MDC.put("methodName", callerMethod != null ? callerMethod : "unknown");
        
        try {
            if (level.intValue() >= java.util.logging.Level.SEVERE.intValue()) {
                errorCount++;
                logger.error("{}", message, throwable);
            } else if (level.intValue() >= java.util.logging.Level.WARNING.intValue()) {
                warningCount++;
                logger.warn("{}", message, throwable);
            } else if (level.intValue() >= java.util.logging.Level.INFO.intValue()) {
                logger.info("{}", message, throwable);
            } else if (level.intValue() >= java.util.logging.Level.FINE.intValue()) {
                logger.debug("{}", message, throwable);
            } else {
                logger.trace("{}", message, throwable);
            }
        } finally {
            org.slf4j.MDC.remove("methodName");
        }
    }

    /**
     * Log order update events with enhanced contextual information
     */
    public static void logOrderUpdate(String orderId, int status, String message, String additionalInfo) {
        if (!initialized) {
            System.out.println(TIMESTAMP_FORMAT.format(new Date()) + " - Order update: " + orderId + 
                ", Status: " + status + ", " + message);
            return;
        }
        
        // Use the calling method name for context if available
        String callerMethod = getCallerMethodName();
        org.slf4j.MDC.put("methodName", callerMethod != null ? callerMethod : "OrderUpdate");
        
        try {
            LOGGER.info("ORDER UPDATE [{}] - ID: {}, Status: {}, Message: {}, Additional: {}", 
                TIMESTAMP_FORMAT.format(new Date()), orderId, status, message, additionalInfo);
            
            // Log detailed updates at debug level for additional diagnostics
            if (DEBUG_MODE) {
                LOGGER.debug("Order {} status changed to {} - details: {} - {}", 
                    orderId, status, message, additionalInfo);
            }
        } finally {
            org.slf4j.MDC.remove("methodName");
        }
    }
    
    /**
     * Log order events with all parameters and enhanced contextual information
     */
    public static void logOrderEvent(String eventType, String orderId, String instrumentId, String side,
                                    String action, double price, double quantity, String status, 
                                    String exchange, int statusCode, String additionalInfo) {
        if (!initialized) {
            System.out.println(TIMESTAMP_FORMAT.format(new Date()) + " - Order event: " + eventType + 
                ", Order: " + orderId + ", Instrument: " + instrumentId);
            return;
        }
        
        // Use the calling method name for context if available
        String callerMethod = getCallerMethodName();
        org.slf4j.MDC.put("methodName", callerMethod != null ? callerMethod : "OrderEvent");
        
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("ORDER EVENT [").append(TIMESTAMP_FORMAT.format(new Date())).append("]: ")
              .append(eventType)
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
            
            // Log additional context at debug level
            if (DEBUG_MODE) {
                Map<String, Object> eventContext = new HashMap<>();
                eventContext.put("orderId", orderId);
                eventContext.put("instrumentId", instrumentId);
                eventContext.put("side", side);
                eventContext.put("action", action);
                eventContext.put("price", price);
                eventContext.put("quantity", quantity);
                eventContext.put("status", status);
                eventContext.put("exchange", exchange);
                eventContext.put("statusCode", statusCode);
                eventContext.put("additionalInfo", additionalInfo);
                
                LOGGER.debug("Order event context: {}", eventContext);
            }
        } finally {
            org.slf4j.MDC.remove("methodName");
        }
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
    
    /**
     * Log market data updates for debugging purposes
     */
    public static void logMarketDataUpdate(String instrumentId, String venue, 
                                          Double bidPrice, Double askPrice, 
                                          String bidSource, String askSource) {
        if (!initialized || !DEBUG_MODE) {
            return;
        }
        
        // Use the calling method name for context if available
        String callerMethod = getCallerMethodName();
        org.slf4j.MDC.put("methodName", callerMethod != null ? callerMethod : "MarketDataUpdate");
        
        try {
            LOGGER.debug("MARKET DATA [{}]: Instrument={}, Venue={}, Bid={} ({}), Ask={} ({})", 
                TIMESTAMP_FORMAT.format(new Date()), instrumentId, venue, 
                bidPrice, bidSource, askPrice, askSource);
        } finally {
            org.slf4j.MDC.remove("methodName");
        }
    }
    
    /**
     * Log configuration changes for auditing
     */
    public static void logConfigChange(String component, String property, Object oldValue, Object newValue) {
        if (!initialized) {
            System.out.println(TIMESTAMP_FORMAT.format(new Date()) + " - Config change: " + 
                component + "." + property + " = " + newValue + " (was: " + oldValue + ")");
            return;
        }
        
        // Use the calling method name for context if available
        String callerMethod = getCallerMethodName();
        org.slf4j.MDC.put("methodName", callerMethod != null ? callerMethod : "ConfigChange");
        
        try {
            LOGGER.info("CONFIG CHANGE [{}]: {}.{} = {} (was: {})", 
                TIMESTAMP_FORMAT.format(new Date()), component, property, newValue, oldValue);
        } finally {
            org.slf4j.MDC.remove("methodName");
        }
    }
    
    /**
     * Get the caller method name for better context in logs
     */
    private static String getCallerMethodName() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        if (stackTrace.length >= 4) {
            // Skip getCallerMethodName, the immediate caller method, and get to the actual business logic method
            return stackTrace[3].getMethodName();
        }
        return null;
    }
    
    /**
     * Log a general diagnostic message with context
     */
    public static void logDiagnostic(String component, String message) {
        if (!initialized) {
            System.out.println(TIMESTAMP_FORMAT.format(new Date()) + " - DIAGNOSTIC [" + component + "]: " + message);
            return;
        }
        
        // Use the calling method name for context if available
        String callerMethod = getCallerMethodName();
        org.slf4j.MDC.put("methodName", callerMethod != null ? callerMethod : component);
        
        try {
            LOGGER.info("DIAGNOSTIC [{}]: {}", component, message);
        } finally {
            org.slf4j.MDC.remove("methodName");
        }
    }
    
    /**
     * Reset error and warning counters
     */
    public static void resetCounters() {
        errorCount = 0;
        warningCount = 0;
        LOGGER.info("Error and warning counters reset");
    }
    
    /**
     * Get current error count
     */
    public static int getErrorCount() {
        return errorCount;
    }
    
    /**
     * Get current warning count
     */
    public static int getWarningCount() {
        return warningCount;
    }
}