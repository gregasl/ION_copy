package com.iontrading.samples.advanced.orderManagement;

import java.io.IOException;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * AsyncLoggingManager provides high-performance asynchronous logging
 * for the trading system. It offloads logging operations to a dedicated
 * thread to prevent blocking in critical trading paths.
 */
public class AsyncLoggingManager {
    
    private static final String LOG_DIRECTORY = "logs";
    private static final int DEFAULT_QUEUE_SIZE = 500000; // Large queue to handle bursts
    private static final int DEFAULT_LOG_FILE_SIZE = 50 * 1024 * 1024; // 100 MB
    private static final int DEFAULT_LOG_FILE_COUNT = 10;
    private static final long SHUTDOWN_TIMEOUT_MS = 5000; // 5 seconds
    private FileHandler machineReadableHandler = null;
    private static final String MACHINE_LOG_SUFFIX = "_machine.log";
    private static final String CSV_HEADER = "timestamp,operation,source,trader,instrumentId,direction,size,price,orderType,tif,requestId,orderId\n";
    
    // Thread pool for async logging
    private final ExecutorService loggingExecutor;
    // Statistics for monitoring
    private final AtomicLong messagesQueued = new AtomicLong(0);
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final AtomicLong queueOverflows = new AtomicLong(0);
    
    // Singleton instance
    private static AsyncLoggingManager instance;
    
    /**
     * Get the singleton instance of AsyncLoggingManager
     */
    public static synchronized AsyncLoggingManager getInstance() {
        if (instance == null) {
            instance = new AsyncLoggingManager();
        }
        return instance;
    }
    
    /**
     * Private constructor to enforce singleton pattern
     */
    private AsyncLoggingManager() {
        // Create a thread pool with a single worker thread for logging
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(DEFAULT_QUEUE_SIZE);
        
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "AsyncLogging-Worker");
            t.setDaemon(false); // Make non-daemon so it doesn't exit prematurely
            return t;
        };
        
        // Custom rejection handler to track queue overflows
        RejectedExecutionHandler rejectionHandler = (runnable, executor) -> {
            queueOverflows.incrementAndGet();
            // Log to console that we're dropping a log message
            System.err.println("WARNING: Async logging queue full, dropping log message");
        };
        
        // Create a thread pool with a single worker, custom queue size, and rejection handler
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, // Core and max pool size of 1
                0L, TimeUnit.MILLISECONDS, // Keep alive time
                workQueue,
                threadFactory,
                rejectionHandler);
        
        loggingExecutor = executor;
        
        // Create logs directory if it doesn't exist
        File logDir = new File(LOG_DIRECTORY);
        if (!logDir.exists()) {
            logDir.mkdirs();
        }
        
        // Add shutdown hook to flush remaining logs
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }
    
    /**
     * Initialize file logging for the application
     * @param logFileName The name of the log file
     */
    public void setupFileLogging(String logFileName) {
        try {
            // Ensure log directory exists and is writable
            File logDir = new File(LOG_DIRECTORY);
            if (!logDir.exists()) {
                if (!logDir.mkdirs()) {
                    throw new IOException("Could not create log directory: " + logDir.getAbsolutePath());
                }
            }
            if (!logDir.isDirectory() || !logDir.canWrite()) {
                throw new IOException("Log directory is not writable: " + logDir.getAbsolutePath());
            }

            // Construct full log file path
            String logFilePath = new File(logDir, logFileName).getAbsolutePath();

            // Set up file handler with more error checking
            FileHandler fileHandler = null;
            try {
                fileHandler = new FileHandler(
                    logFilePath,
                    DEFAULT_LOG_FILE_SIZE,
                    DEFAULT_LOG_FILE_COUNT,
                    true
                );
            } catch (IOException e) {
                System.err.println("Failed to create file handler for log file: " + logFilePath);
                throw e;
            }

            ConsoleHandler consoleHandler = new ConsoleHandler();

            // Use custom formatter
            HighPerformanceFormatter formatter = new HighPerformanceFormatter();
            fileHandler.setFormatter(formatter);
            consoleHandler.setFormatter(formatter);

            // Configurable log levels
            Level fileLogLevel = Level.ALL;
            Level consoleLogLevel = Level.FINE; // You can adjust this

            fileHandler.setLevel(fileLogLevel);
            consoleHandler.setLevel(consoleLogLevel);

            // Configure root logger
            Logger rootLogger = Logger.getLogger("");
            
            // Remove existing handlers to prevent duplicate logging
            for (Handler handler : rootLogger.getHandlers()) {
                rootLogger.removeHandler(handler);
            }

            rootLogger.setLevel(Level.ALL);
            rootLogger.addHandler(fileHandler);
            rootLogger.addHandler(consoleHandler);

            // Log initial setup messages
            Logger setupLogger = Logger.getLogger(AsyncLoggingManager.class.getName());
            setupLogger.info("Logging initialized");
            setupLogger.info("Log file: " + logFilePath);
            setupLogger.info("Logging levels - File: " + fileLogLevel + ", Console: " + consoleLogLevel);

        } catch (Exception e) {
            System.err.println("Critical error setting up logging system");
            e.printStackTrace();
            
            // Fallback to system error stream if logging fails
            System.err.println("Logging setup failed: " + e.getMessage());
        }
    }
    
    /**
     * Waits for the logging system to process at least one message
     * or until timeout
     * @param timeoutMs timeout in milliseconds
     * @return true if a message was processed, false if timed out
     */
    public boolean waitForFirstMessageProcessed(long timeoutMs) {
        long startTime = System.currentTimeMillis();
        while (messagesProcessed.get() == 0) {
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                System.err.println("Timed out waiting for first log message to be processed");
                return false;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }
    
    /**
     * Silence system loggers that produce noise
     */
    private void silenceSystemLoggers() {
        String[] loggers = {
            "com.sun.jmx",
            "javax.management",
            "java.util.logging",
            "java.nio",
            "sun.net"
        };
        
        for (String loggerName : loggers) {
            Logger.getLogger(loggerName).setLevel(Level.OFF);
        }
    }
    
    /**
     * Configure the root logger with handlers
     */
    private void configureRootLogger(FileHandler fileHandler, ConsoleHandler consoleHandler) {
        Logger rootLogger = Logger.getLogger("");
        
        // Remove existing handlers
        for (Handler handler : rootLogger.getHandlers()) {
            rootLogger.removeHandler(handler);
        }
        
        // Set root logger level
        rootLogger.setLevel(Level.ALL);
        
        // Add our handlers
        rootLogger.addHandler(fileHandler);
        rootLogger.addHandler(consoleHandler);
        
        // Set application package to ALL level
        Logger.getLogger("com.iontrading.samples.advanced").setLevel(Level.ALL);
    }
    
    /**
     * Log a message asynchronously
     */
    public void log(Logger logger, Level level, String message) {
        // Skip if the level is not loggable
        if (!logger.isLoggable(level)) {
            return;
        }
        
        // Count the message as queued
        messagesQueued.incrementAndGet();
        
        // Create a log task and submit it to the executor
        loggingExecutor.execute(new LogTask(logger, level, message));
    }
    
    /**
     * Log a message with an exception asynchronously
     */
    public void log(Logger logger, Level level, String message, Throwable thrown) {
        // Skip if the level is not loggable
        if (!logger.isLoggable(level)) {
            return;
        }
        
        // Count the message as queued
        messagesQueued.incrementAndGet();
        
        // Create a log task and submit it to the executor
        loggingExecutor.execute(new LogTask(logger, level, message, thrown));
    }
    
    // Statistics getters
    public long getMessagesQueued() { return messagesQueued.get(); }
    public long getMessagesProcessed() { return messagesProcessed.get(); }
    public long getQueueOverflows() { return queueOverflows.get(); }
    
    /**
     * Get the current queue size (messages waiting to be processed)
     */
    public int getCurrentQueueSize() {
        if (loggingExecutor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) loggingExecutor).getQueue().size();
        }
        return -1; // Unknown
    }
    
    /**
     * Set up machine-readable logging with a dedicated file
     * 
     * @param logFileName Base name for the machine-readable log file
     */
    public void setupMachineReadableLogging(String logFileName) {
        try {
            // Ensure log directory exists
            File logDir = new File(LOG_DIRECTORY);
            if (!logDir.exists()) {
                if (!logDir.mkdirs()) {
                    throw new IOException("Could not create log directory: " + logDir.getAbsolutePath());
                }
            }
            
            // Construct full log file path with _machine suffix
            String machineLogPath = new File(logDir, logFileName + MACHINE_LOG_SUFFIX).getAbsolutePath();
            
            // Set up file handler for machine-readable logs
            machineReadableHandler = new FileHandler(
                machineLogPath,
                DEFAULT_LOG_FILE_SIZE,
                DEFAULT_LOG_FILE_COUNT,
                true  // Append to existing file
            );
            
            // Use simple formatter that outputs exactly what we pass in
            machineReadableHandler.setFormatter(new Formatter() {
                @Override
                public String format(LogRecord record) {
                    return record.getMessage() + System.lineSeparator();
                }
            });
            
            // Set level to INFO to ensure order logs are captured
            machineReadableHandler.setLevel(Level.INFO);
            
            // Create the CSV header if this is a new file
            File logFile = new File(machineLogPath);
            if (!logFile.exists() || logFile.length() == 0) {
                LogRecord headerRecord = new LogRecord(Level.INFO, CSV_HEADER);
                machineReadableHandler.publish(headerRecord);
                machineReadableHandler.flush();
            }
            
            Logger setupLogger = Logger.getLogger(AsyncLoggingManager.class.getName());
            setupLogger.info("Machine-readable logging initialized: " + machineLogPath);
            
        } catch (Exception e) {
            System.err.println("Critical error setting up machine-readable logging");
            e.printStackTrace();
        }
    }

    /**
     * Log a machine-readable record asynchronously (CSV format)
     */
    public void logMachineReadable(final String csvRecord) {
        if (machineReadableHandler == null) {
            // Fallback to standard logging if machine-readable logging not set up
            log(Logger.getLogger(AsyncLoggingManager.class.getName()), 
                Level.WARNING, 
                "Machine-readable logging not initialized. CSV record: " + csvRecord);
            return;
        }
        
        // Count the message as queued
        messagesQueued.incrementAndGet();
        
        // Submit to executor
        loggingExecutor.execute(() -> {
            try {
                // Create a log record with the CSV data
                LogRecord record = new LogRecord(Level.INFO, csvRecord);
                record.setLoggerName("machine.readable");
                
                // Publish directly to the machine-readable handler
                if (machineReadableHandler.isLoggable(record)) {
                    machineReadableHandler.publish(record);
                    machineReadableHandler.flush();
                }
                
                // Increment processed message count
                messagesProcessed.incrementAndGet();
                
            } catch (Exception e) {
                System.err.println("Error in machine-readable logging: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }
    
    /**
     * Shutdown the logging executor, waiting for queued logs to be processed
     */
    public void shutdown() {
        System.out.println("Shutting down AsyncLoggingManager...");
        
        // Close the machine-readable handler if it exists
        if (machineReadableHandler != null) {
            try {
                machineReadableHandler.flush();
                machineReadableHandler.close();
            } catch (Exception e) {
                System.err.println("Error closing machine-readable handler: " + e.getMessage());
            }
        }
        
        loggingExecutor.shutdown();
        try {
            // Wait for existing tasks to terminate
            if (!loggingExecutor.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                System.err.println("AsyncLoggingManager shutdown timed out. Some messages may be lost.");
                loggingExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            loggingExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        System.out.println("AsyncLoggingManager shutdown complete.");
    }
    
    /**
     * Task for asynchronous logging
     */
    private class LogTask implements Runnable {
        private final Logger logger;
        private final Level level;
        private final String message;
        private final Throwable thrown;
        private final long timestamp;
        private final StackTraceElement[] callerStack;

        public LogTask(Logger logger, Level level, String message) {
            this(logger, level, message, null);
        }

        public LogTask(Logger logger, Level level, String message, Throwable thrown) {
            this.logger = logger;
            this.level = level;
            this.message = message;
            this.thrown = thrown;
            this.timestamp = System.currentTimeMillis();
            
            // Capture caller stack trace for more accurate source information
            StackTraceElement[] fullStack = Thread.currentThread().getStackTrace();
            this.callerStack = extractCallerStack(fullStack);
        }

        /**
         * Extract the actual caller's stack trace, skipping internal logging classes
         */
        private StackTraceElement[] extractCallerStack(StackTraceElement[] fullStack) {
            for (int i = 1; i < fullStack.length; i++) {
                String className = fullStack[i].getClassName();
                if (!className.startsWith("java.util.logging") && 
                    !className.startsWith("com.iontrading.samples.advanced.orderManagement.AsyncLoggingManager") &&
                    !className.startsWith("java.lang.Thread")) {
                    // Found the first non-logging class in the stack
                    return new StackTraceElement[]{fullStack[i]};
                }
            }
            return new StackTraceElement[0];
        }

        @Override
        public void run() {
            try {
                // Ensure the log level is appropriate
                if (logger == null || !logger.isLoggable(level)) {
                    return;
                }

                // Create a detailed log record
                LogRecord record = new LogRecord(level, message);
                record.setLoggerName(logger.getName());
                record.setMillis(timestamp);
                record.setThrown(thrown);

                // Set source information from captured stack trace
                if (callerStack.length > 0) {
                    StackTraceElement callerFrame = callerStack[0];
                    record.setSourceClassName(callerFrame.getClassName());
                    record.setSourceMethodName(callerFrame.getMethodName());
                }

                // Publish to all relevant handlers
                publishToHandlers(record);

                // Increment processed message count
                messagesProcessed.incrementAndGet();

            } catch (Exception e) {
                // Fallback error handling
                System.err.println("Critical error in async logging task:");
                e.printStackTrace();
            }
        }

        /**
         * Publish log record to all relevant handlers
         */
        private void publishToHandlers(LogRecord record) {
            // Publish to logger's own handlers
            publishToLoggerHandlers(logger, record);

            // Publish to parent loggers
            Logger parent = logger.getParent();
            while (parent != null) {
                publishToLoggerHandlers(parent, record);
                parent = parent.getParent();
            }
        }

        /**
         * Publish log record to a specific logger's handlers
         */
        private void publishToLoggerHandlers(Logger targetLogger, LogRecord record) {
            if (targetLogger == null) return;

            for (Handler handler : targetLogger.getHandlers()) {
                try {
                    if (handler.isLoggable(record)) {
                        handler.publish(record);
                        handler.flush();
                    }
                } catch (Exception e) {
                    System.err.println("Error publishing to handler: " + handler);
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * High-performance formatter that minimizes string operations
     */
    private static class HighPerformanceFormatter extends Formatter {
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        
        @Override
        public String format(LogRecord record) {
            StringBuilder sb = new StringBuilder(256);
            
            // Format: timestamp [level] class: message
            sb.append(dateFormat.format(new Date(record.getMillis())));
            sb.append(" [");
            sb.append(record.getLevel().getName());
            sb.append("] ");
            sb.append(record.getSourceClassName());
            sb.append(": ");
            sb.append(formatMessage(record));
            sb.append(System.lineSeparator());
            
            // Add exception if present
            if (record.getThrown() != null) {
                try {
                    Throwable thrown = record.getThrown();
                    sb.append(thrown.toString());
                    sb.append(System.lineSeparator());
                    
                    // Add stack trace elements
                    for (StackTraceElement element : thrown.getStackTrace()) {
                        sb.append("    at ");
                        sb.append(element.toString());
                        sb.append(System.lineSeparator());
                    }
                } catch (Exception ex) {
                    sb.append("Error formatting exception: ");
                    sb.append(ex.toString());
                    sb.append(System.lineSeparator());
                }
            }
            
            return sb.toString();
        }
    }
}