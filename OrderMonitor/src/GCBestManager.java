
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralized manager for GCBest data to ensure consistency across components.
 * Implements the Singleton pattern for global access.
 */
public class GCBestManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(GCBestManager.class);
    
    // Singleton instance
    private static volatile GCBestManager instance;
    
    // Thread-safe references to latest GC data
    private final AtomicReference<GCBest> gcBestCash = new AtomicReference<>();
    private final AtomicReference<GCBest> gcBestREG = new AtomicReference<>();
    private final AtomicReference<Double> cashGcRate = new AtomicReference<>(0.0);
    private final AtomicReference<Double> regGcRate = new AtomicReference<>(0.0);
    
    // Timestamp of last update
    private volatile long lastUpdateTimestamp = 0;
    
    private GCBestManager() {
        // Private constructor for singleton
    }
    
    /**
     * Get the singleton instance of GCBestManager
     */
    public static GCBestManager getInstance() {
        if (instance == null) {
            synchronized (GCBestManager.class) {
                if (instance == null) {
                    instance = new GCBestManager();
                }
            }
        }
        return instance;
    }
    
    /**
     * Update the GC Best data for cash
     * 
     * @param gcBest The new GCBest data
     * @param rate The new GC rate
     */
    public void updateCashGCBest(GCBest gcBest, double rate) {
        if (gcBest != null) {
            gcBestCash.set(gcBest);
            cashGcRate.set(rate);
            lastUpdateTimestamp = System.currentTimeMillis();
            LOGGER.debug("Updated Cash GCBest: {}, rate: {}", gcBest, rate);
        }
    }
    
    /**
     * Update the GC Best data for REG
     * 
     * @param gcBest The new GCBest data
     * @param rate The new GC rate
     */
    public void updateRegGCBest(GCBest gcBest, double rate) {
        if (gcBest != null) {
            gcBestREG.set(gcBest);
            regGcRate.set(rate);
            lastUpdateTimestamp = System.currentTimeMillis();
            LOGGER.debug("Updated REG GCBest: {}, rate: {}", gcBest, rate);
        }
    }
    
    /**
     * Get the latest Cash GCBest data
     */
    public GCBest getCashGCBest() {
        return gcBestCash.get();
    }
    
    /**
     * Get the latest REG GCBest data
     */
    public GCBest getRegGCBest() {
        return gcBestREG.get();
    }
    
    /**
     * Get the latest Cash GC rate
     */
    public double getCashGCRate() {
        return cashGcRate.get();
    }
    
    /**
     * Get the latest REG GC rate
     */
    public double getRegGCRate() {
        return regGcRate.get();
    }
    
    /**
     * Get the timestamp of the last update
     */
    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }
}