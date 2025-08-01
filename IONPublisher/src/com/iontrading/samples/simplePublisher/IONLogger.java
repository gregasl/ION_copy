package com.iontrading.samples.simplePublisher;

import com.iontrading.mkv.MkvLog;

/**
 * Centralized logging utility for ION applications.
 * Provides thread-safe access to MkvLog with consistent formatting and log levels.
 */
public class IONLogger {
    private final MkvLog myLog;
    private final int logLevel;
    private final String contextName;

    /**
     * Create a new logger instance with specified log and level
     * 
     * @param log The MkvLog instance to use
     * @param level The logging level (0-ERROR, 1-WARNING, 2-INFO, 3-VERBOSE, 4-DEBUG)
     * @param context A name identifying the context of this logger (typically class name)
     */
    public IONLogger(MkvLog log, int level, String context) {
        this.myLog = log;
        this.logLevel = level;
        this.contextName = context;
    }

    /**
     * Log a message at ERROR level (0)
     * @param message The message to log
     */
    public void error(String message) {
        if (logLevel >= 0 && myLog != null) {
            myLog.add("[ERROR] [" + contextName + "] " + message);
        }
    }

    /**
     * Log a message at WARNING level (1)
     * @param message The message to log
     */
    public void warn(String message) {
        if (logLevel >= 1 && myLog != null) {
            myLog.add("[WARNING] [" + contextName + "] " + message);
        }
    }

    /**
     * Log a message at INFO level (2)
     * @param message The message to log
     */
    public void info(String message) {
        if (logLevel >= 2 && myLog != null) {
            myLog.add("[INFO] [" + contextName + "] " + message);
        }
    }

    /**
     * Log a message at VERBOSE level (3)
     * @param message The message to log
     */
    public void verbose(String message) {
        if (logLevel >= 3 && myLog != null) {
            myLog.add("[VERBOSE] [" + contextName + "] " + message);
        }
    }

    /**
     * Log a message at DEBUG level (4)
     * @param message The message to log
     */
    public void debug(String message) {
        if (logLevel >= 4 && myLog != null) {
            myLog.add("[DEBUG] [" + contextName + "] " + message);
        }
    }

    /**
     * Generic logging method matching your original implementation
     * @param message The message to log
     */
    public void log(String message) {
        switch (logLevel) {
            case 0:
                myLog.add("[ERROR] [" + contextName + "] " + message);
                break;
            case 1:
                myLog.add("[WARNING] [" + contextName + "] " + message);
                break;
            case 2:
                myLog.add("[INFO] [" + contextName + "] " + message);
                break;
            case 3:
                myLog.add("[VERBOSE] [" + contextName + "] " + message);
                break;
            case 4:
                myLog.add("[DEBUG] [" + contextName + "] " + message);
                break;
        }
    }

    /**
     * Factory method to create a logger from Publisher
     * @param simplePublisher The Publisher instance to get log from
     * @param context Context name for the logger
     * @return A new IONLogger instance
     */
    public static IONLogger fromPublisher(Publisher simplePublisher, String context) {
        return new IONLogger(
            simplePublisher.getMkvLog(),  // Add a getter for myLog in Publisher
            simplePublisher.getLogLevel(), // Add a getter for logLevel in Publisher
            context
        );
    }
}