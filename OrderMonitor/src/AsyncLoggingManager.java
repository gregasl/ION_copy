
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simplified AsyncLoggingManager for SLF4J compatibility
 * Most async functionality is now handled by Logback's AsyncAppender
 */
public class AsyncLoggingManager {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncLoggingManager.class);
    private static AsyncLoggingManager instance;
    private String logDirectory = "logs";
    
    // Statistics counters
    private volatile long messagesQueued = 0;
    private volatile long messagesProcessed = 0;
    private volatile long queueOverflows = 0;
    
    private AsyncLoggingManager() {
        this.logDirectory = System.getProperty("log.dir", "logs");
    }
    
    public static AsyncLoggingManager getInstance() {
        if (instance == null) {
            synchronized (AsyncLoggingManager.class) {
                if (instance == null) {
                    instance = new AsyncLoggingManager();
                }
            }
        }
        return instance;
    }
    
    /**
     * Set the log directory and reinitialize logging
     */
    public void setLogDirectory(String directory) {
        this.logDirectory = directory;
        System.setProperty("log.dir", directory);
        LOGGER.info("Log directory updated to: {}", directory);
        
        // Note: To fully change directory, you'd need to call ApplicationLogging.initialize() again
        // with a different application name to trigger reconfiguration
    }
    
    public String getLogDirectory() {
        return logDirectory;
    }
    
    /**
     * Setup file logging - now delegates to ApplicationLogging
     */
    public boolean setupFileLogging(String baseFileName) {
        LOGGER.info("File logging delegated to ApplicationLogging - files will be created automatically");
        return true;
    }
    
    public boolean setupMachineReadableLogging(String applicationName) {
        LOGGER.info("Machine-readable logging delegated to ApplicationLogging - files will be created automatically");
        return true;
    }
    
    // Add methods for backward compatibility
    public long getCurrentQueueSize() { return 0; } // Handled by AsyncAppender now
    public long getMessagesQueued() { return messagesQueued; }
    public long getMessagesProcessed() { return messagesProcessed; }
    public long getQueueOverflows() { return queueOverflows; }
}
