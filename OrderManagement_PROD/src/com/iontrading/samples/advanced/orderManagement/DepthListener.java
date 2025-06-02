/*
 * DepthListener
 *
 * The depth class listen for the best changes for a single instrument and notifies
 * the order manager the new prices.
 *
 */

package com.iontrading.samples.advanced.orderManagement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSubscribeProxy;

/**
 * DepthListener listens for market depth updates for a specific instrument
 * and notifies the order manager when best prices change.
 * 
 * It uses MkvSubscribeProxy to efficiently map record fields to Java bean properties.
 */
public class DepthListener implements MkvRecordListener {

	// Add logger for debugging
	private static final Logger LOGGER = Logger.getLogger(DepthListener.class.getName());
	/**
	 * Shared proxy instance used to map MKV record fields to Best bean properties.
	 * This is static as it can be shared across all instances for the same record type.
	 */
	private static MkvSubscribeProxy proxy;

    // Reference to the IOrderManager interface
    private final IOrderManager orderManager;
	
    // Store a map of record names to field maps
    private final Map<String, Map<String, Object>> recordDataMap = new HashMap<>();
    
    // Store the list of fields we're interested in
    private final Set<String> fieldsList = new HashSet<>(Arrays.asList(
    		"Id", "Ask0", "Ask1", "Ask2", "Ask3", "Ask4", "Ask5", "Ask6", "Ask7", "Ask8", "Ask9",
    		"AskSrc0", "AskSrc1", "AskSrc2", "AskSrc3", "AskSrc4", "AskSrc5", "AskSrc6", "AskSrc7", "AskSrc8", "AskSrc9", 
    		"AskAttribute0", "Bid0", "Bid1", "Bid2", "Bid3", "Bid4", "Bid5", "Bid6", "Bid7", "Bid8", "Bid9",
    		"BidSrc0", "BidSrc1", "BidSrc2", "BidSrc3", "BidSrc4", "BidSrc5", "BidSrc6", "BidSrc7", "BidSrc8", "BidSrc9", 
    		"BidAttribute0", "AskSize0", "BidSize0", "BidSize0_Min", "AskSize0_Min", "TrdValueLast"
    ));
    
	// Add activity tracking 
    private final AtomicLong lastUpdateTimestamp = new AtomicLong(0);
    private final AtomicLong updateCounter = new AtomicLong(0);
    private final AtomicLong consecutiveErrorCount = new AtomicLong(0);
    private final ScheduledExecutorService heartbeatScheduler = Executors.newScheduledThreadPool(1);
    private static final long HEARTBEAT_INTERVAL_MS = 5000; // 5 seconds
    private static final long DATA_SILENCE_WARNING_MS = 10000; // 10 seconds without data is concerning
    private static final long DATA_SILENCE_CRITICAL_MS = 30000; // 30 seconds without data is critical
    private final long startTime = System.currentTimeMillis();
    private static final String GC_TU10_C = "USD.CM_DEPTH.VMO_REPO_US.GC_TU10_C_Fixed";
    private static final String GC_TU10_REG = "USD.CM_DEPTH.VMO_REPO_US.GC_TU10_REG_Fixed";
    
    private GCBest cachedGCBestCash = null;
    private GCBest cachedGCBestREG = null;
    private double cachedCashGC = 0.0;
    private double cachedRegGC = 0.0;

	/**
	 * Initializes the MkvSubscribeProxy if it hasn't been initialized yet.
	 * The proxy maps MKV record field names to Java bean property names.
	 * 
	 * @param rec The MKV record to use for initialization
	 * @throws MkvException If the proxy initialization fails
	 */
	private static void initProxy(MkvRecord rec) throws MkvException {
		if (proxy == null) {
			ApplicationLogging.logAsync(LOGGER, Level.INFO, "Initializing MkvSubscribeProxy for DepthListener");
		    LOGGER.setLevel(Level.FINE);
			
			// Define the mapping from MKV field names to Best bean property names
			Properties p = new Properties();
			p.setProperty("Id", "Id");           // Map Id field to Id property
			p.setProperty("Ask0", "ask");           // Map Ask0 field to ask property
			p.setProperty("AskSrc0", "askSrc");           // Map Ask0 field to ask property
			p.setProperty("AskAttribute0", "askIsAON");           // Map Ask0 field to ask property
			p.setProperty("AskAttribute0", "askSrcCheck");           // Map Ask0 field to ask property
			p.setProperty("AskSize0", "askSize");   // Map AskSize0 field to askSize property
			p.setProperty("AskSize0_Min", "askSizeMin");           // Map AskSize0_Min field to askSizemin property
			p.setProperty("Bid0", "bid");           // Map Bid0 field to bid property
			p.setProperty("BidSrc0", "bidSrc");           // Map Ask0 field to ask property
			p.setProperty("BidAttribute0", "bidIsAON");           // Map Ask0 field to ask property
			p.setProperty("BidAttribute0", "bidSrcCheck");           // Map Ask0 field to ask property
			p.setProperty("BidSize0", "bidSize");   // Map BidSize0 field to bidSize property
			p.setProperty("BidSize0_Min", "bidSizeMin");   // Map BidSize0_Min field to bidSizemin property
			p.setProperty("TrdValueLast", "lastTradePrice");   // Map TrdValueLast field to lastTradePrice property

			ApplicationLogging.logAsync(LOGGER, Level.INFO, "Creating MkvSubscribeProxy for DepthListener");
						
			// Create the proxy with the Best class and the field mappings
			proxy = new MkvSubscribeProxy(Best.class, p);
			
			ApplicationLogging.logAsync(LOGGER, Level.INFO, "MkvSubscribeProxy created successfully");
		}
	}

	/**
	 * Creates a new DepthListener for the specified instrument.
	 * 
	 * @param instrId The instrument ID to listen for
	 * @param manager The order manager to notify of best changes
	 */
	public DepthListener(IOrderManager manager) {
		this.orderManager = manager;
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Creating DepthListener for pattern subscription");
        LOGGER.setLevel(Level.FINE);
		initializeHeartbeat();
	}


	/**
	 * This listener does not process partial updates, only full updates.
	 */
	public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
			boolean isSnapshot) {
		// Not interested in partial updates
	}

	/**
	 * Processes a full update for the depth record.
	 * Updates the Best object with the new prices and notifies the order manager.
	 */
	public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
			boolean isSnapshot) {
        try {
            // Update monitoring counters
            lastUpdateTimestamp.set(System.currentTimeMillis());
            
            String recordName = mkvRecord.getName();
            int cursor = mkvSupply.firstIndex();
            
            // Initialize record storage if it doesn't exist
            recordDataMap.putIfAbsent(recordName, new HashMap<>());
            Map<String, Object> recordData = recordDataMap.get(recordName);
            
            // Track if anything changed
            boolean changed = false;
           
            // Process updates
            while (cursor != -1) {
                String fieldName = mkvRecord.getMkvType().getFieldName(cursor);
                Object fieldValue = mkvSupply.getObject(cursor);
                
                // Only update if the field is in the subscribed fields list
                if (fieldsList.contains(fieldName)) {
                    Object oldValue = recordData.get(fieldName);
                    if (fieldValue != null && !fieldValue.equals(oldValue)) {
                        recordData.put(fieldName, fieldValue);
                        changed = true;
                    } else if (fieldValue == null && oldValue != null) {
                        recordData.put(fieldName, "null");
                        changed = true;
                    }
                }
                
                cursor = mkvSupply.nextIndex(cursor);
            }
            
            // If this is a GC record, update the cached GC data
            if (recordName.equals(GC_TU10_C)) {
                Map<String, Object> gcData = recordDataMap.get(GC_TU10_C);
                if (gcData != null) {
                    cachedGCBestCash = (GCBest) createAppropriateBean(GC_TU10_C, gcData);
                    cachedCashGC = getDoubleValue(gcData, "TrdValueLast", 0.0);
                }
            } else if (recordName.equals(GC_TU10_REG)) {
                Map<String, Object> gcData = recordDataMap.get(GC_TU10_REG);
                if (gcData != null) {
                    cachedGCBestREG = (GCBest) createAppropriateBean(GC_TU10_REG, gcData);
                    cachedRegGC = getDoubleValue(gcData, "TrdValueLast", 0.0);
                }
            }
            
            // Only proceed if data changed
            if (changed || isSnapshot) {
                Best best = createAppropriateBean(recordName, recordData);
                
                // Always use cached GC data
                orderManager.best(best, cachedCashGC, cachedRegGC, cachedGCBestCash, cachedGCBestREG);
                updateCounter.incrementAndGet();
            
            }
        } catch (Exception e) {
            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error processing depth update: " + e.getMessage());
            e.printStackTrace();
            consecutiveErrorCount.incrementAndGet();
        }
    }

    private Best createAppropriateBean(String recordName, Map<String, Object> recordData) {
        // Define which instruments should use SpecializedBest
        if (recordName.equals(GC_TU10_C) || recordName.equals(GC_TU10_REG)) {
            GCBest specialBest = new GCBest(recordName);
            
            // Set additional depth levels
            for (int i = 0; i < 10; i++) {
                specialBest.setAskPrice(i, getDoubleValue(recordData, "Ask" + i, 0.0));
                specialBest.setBidPrice(i, getDoubleValue(recordData, "Bid" + i, 0.0));
                specialBest.setAskSource(i, getStringValue(recordData, "AskSrc" + i, ""));
                specialBest.setBidSource(i, getStringValue(recordData, "BidSrc" + i, ""));
            }

            return specialBest;
        } else {
            // Use original Best for other instruments
            return createBestFromMap(recordName, recordData);
        }
    }

    /**
     * Helper method to create a Best object from the stored map data
     */
    private Best createBestFromMap(String recordName, Map<String, Object> recordData) {
        Best best = new Best(recordName);
        
        // Set ID if available
        String id = getStringValue(recordData, "Id", "");
        if (!id.isEmpty()) {
            best.setId(id);
        }
        
        // Set price fields with rounding already handled in the setter
        best.setAsk(getDoubleValue(recordData, "Ask0", 0.0));
        best.setBid(getDoubleValue(recordData, "Bid0", 0.0));
        
        // Set source fields
        best.setAskSrc(getStringValue(recordData, "AskSrc0", ""));
        best.setBidSrc(getStringValue(recordData, "BidSrc0", ""));

        best.setAskIsAON(getStringValue(recordData, "AskAttribute0", ""));
        best.setBidIsAON(getStringValue(recordData, "BidAttribute0", ""));

        best.setAskSrcCheck(getStringValue(recordData, "AskAttribute0", ""));
        best.setBidSrcCheck(getStringValue(recordData, "BidAttribute0", ""));
        
        // Set size fields
        best.setAskSize(getDoubleValue(recordData, "AskSize0", 0.0));
        best.setBidSize(getDoubleValue(recordData, "BidSize0", 0.0));
        best.setAskSizeMin(getDoubleValue(recordData, "AskSize0_Min", 0.0));
        best.setBidSizeMin(getDoubleValue(recordData, "BidSize0_Min", 0.0));
        
        // Set last fields
        best.setLastTradePrice(getDoubleValue(recordData, "TrdValueLast", 0.0));

        return best;
    }


    /**
     * Helper method to safely get a double value from the data map
     */
    private double getDoubleValue(Map<String, Object> data, String field, double defaultValue) {
        Object value = data.get(field);
        if (value == null) {
            return defaultValue;
        }
        
        try {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            } else if (value instanceof String) {
                return Double.parseDouble((String) value);
            }
        } catch (Exception e) {
            // Log the error but return default value
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                "Error converting field " + field + " value: " + value);
        }
        
        return defaultValue;
    }
    
    /**
     * Helper method to safely get a string value from the data map
     */
    private String getStringValue(Map<String, Object> data, String field, String defaultValue) {
        Object value = data.get(field);
        if (value == null) {
            return defaultValue;
        }
        
        return value.toString();
    }
    
    private void initializeHeartbeat() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                checkAndLogActivityStatus();
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                    "Error in DepthListener heartbeat: " + e.getMessage(), e);
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Shutting down DepthListener heartbeat");
            heartbeatScheduler.shutdown();
            try {
                if (!heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    heartbeatScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                heartbeatScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }));
    }
    
    private void checkAndLogActivityStatus() {
        long now = System.currentTimeMillis();
        long lastUpdate = lastUpdateTimestamp.get();
        
        if (lastUpdate == 0) {
            // No updates received yet
            ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                "DepthListener heartbeat: No market data updates received yet");
            return;
        }
        
        long silenceTime = now - lastUpdate;
        long updateCount = updateCounter.get();
        
        if (silenceTime > DATA_SILENCE_CRITICAL_MS) {
            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                "CRITICAL: No market data updates for " + (silenceTime/1000) + 
                " seconds. Last update at " + new Date(lastUpdate) + 
                ". Total updates received: " + updateCount);
            
            // Notify order management or system monitoring
            // This could trigger a reconnection or subscription refresh
        } else if (silenceTime > DATA_SILENCE_WARNING_MS) {
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                "WARNING: No market data updates for " + (silenceTime/1000) + 
                " seconds. Last update at " + new Date(lastUpdate) + 
                ". Total updates received: " + updateCount);
        } else {
            // Normal operation - periodic status log
            ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                "DepthListener heartbeat: Last update " + (silenceTime/1000) + 
                " seconds ago. Update rate: " + 
                String.format("%.2f", updateCount / ((now - startTime) / 1000.0)) + 
                " updates/sec. Total updates: " + updateCount);
            
            // Reset consecutive error count since we're receiving data
            consecutiveErrorCount.set(0);
        }
    }
	
	    // Public method to get health status for monitoring
    public boolean isReceivingData() {
        long silenceTime = System.currentTimeMillis() - lastUpdateTimestamp.get();
        return lastUpdateTimestamp.get() > 0 && silenceTime < DATA_SILENCE_WARNING_MS;
    }
    
    // Method to get detailed health information
    public Map<String, Object> getHealthStatus() {
        long now = System.currentTimeMillis();
        Map<String, Object> status = new HashMap<>();
        status.put("totalUpdates", updateCounter.get());
        status.put("lastUpdateTimestamp", lastUpdateTimestamp.get());
        status.put("timeSinceLastUpdateMs", lastUpdateTimestamp.get() > 0 ? now - lastUpdateTimestamp.get() : -1);
        status.put("consecutiveErrors", consecutiveErrorCount.get());
        status.put("isHealthy", isReceivingData());
        return status;
    }
}