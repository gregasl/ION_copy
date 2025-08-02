/*
 * DepthListener
 *
 * The depth class listens for the best changes for instruments and instrument mapping data,
 * and notifies the order manager of new prices.
 *
 */

package com.iontrading.automatedMarketMaking;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Date;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.MkvLog;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSubscribeProxy;


/**
 * DepthListener listens for market depth updates and instrument mapping data
 * for all instruments and notifies the order manager when best prices change.
 * 
 * It uses MkvSubscribeProxy to efficiently map record fields to Java bean properties.
 */
public class DepthListener implements MkvRecordListener {
    // Add logger for debugging
    private static MkvLog log = Mkv.getInstance().getLogManager().getLogFile("MarketMaker");
    private static IONLogger logger = new IONLogger(log, 2, "DepthListener");  
    
    /**
     * Shared proxy instances used to map MKV record fields to Java bean properties.
     * These are static as they can be shared across all instances for the same record types.
     */
    private static MkvSubscribeProxy depthProxy;
    private static MkvSubscribeProxy instrumentProxy;

    // Reference to the IOrderManager interface
    private final IOrderManager orderManager;
    
    // Store instrument mapping data
    private final Map<String, Instrument> instrumentData = new ConcurrentHashMap<>();

    // Store best prices for instruments
    private final Map<String, Best> bestCache = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, List<QueuedUpdate>> pendingDepthUpdates = new ConcurrentHashMap<>();
    private final Object queueLock = new Object();

    // Store a map of record names to field maps for depth data
    private final Map<String, Map<String, Object>> recordDataMap = new HashMap<>();
    
    // Store the list of fields we're interested in for depth data - use MarketDef
    private final Set<String> depthFieldsList = new HashSet<>(Arrays.asList(MarketDef.DEPTH_FIELDS));
    
    // Pattern identifiers - use MarketDef
    private static final String INSTRUMENT_PATTERN = MarketDef.INSTRUMENT_PATTERN;
    
    // Add instrumentation to track pattern subscriptions
    private boolean isInstrumentPatternSubscribed = false;
    private final AtomicLong instrumentPatternSubscribedTime = new AtomicLong(0);
    private final AtomicLong instrumentUpdatesReceived = new AtomicLong(0);

    // Add activity tracking 
    private final AtomicLong lastUpdateTimestamp = new AtomicLong(0);
    private final AtomicLong updateCounter = new AtomicLong(0);
    private final AtomicLong consecutiveErrorCount = new AtomicLong(0);
    private final Object healthLock = new Object();
    private final ScheduledExecutorService heartbeatScheduler = Executors.newScheduledThreadPool(1);
    private static final long HEARTBEAT_INTERVAL_MS = 5000; // 5 seconds
    private static final long DATA_SILENCE_WARNING_MS = 10000; // 10 seconds without data is concerning
    private static final long DATA_SILENCE_CRITICAL_MS = 30000; // 30 seconds without data is critical
    private final long startTime = System.currentTimeMillis();
    private static final String GC_TU10_C = MarketDef.GC_TU10_CASH;
    private static final String GC_TU10_REG = MarketDef.GC_TU10_REG;

    // List of electronic venues we want to prioritize
    private static final List<String> ELECTRONIC_VENUES = Arrays.asList(
        "BTEC_REPO_US", "DEALERWEB_REPO"
    );

    private static class QueuedUpdate {
        final MkvRecord record;
        final MkvSupply supply;
        final boolean isSnapshot;
        final long timestamp;
        
        QueuedUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
            this.record = record;
            this.supply = supply;
            this.isSnapshot = isSnapshot;
            this.timestamp = System.currentTimeMillis();
        }
    }

    /**
     * Initializes the MkvSubscribeProxy instances if they haven't been initialized yet.
     * The proxies map MKV record field names to Java bean property names.
     * 
     * @param rec The MKV record to use for initialization
     * @throws MkvException If the proxy initialization fails
     */
    private static void initProxies(MkvRecord rec) throws MkvException {

        if (instrumentProxy == null) {
            logger.info("Initializing MkvSubscribeProxy for DepthListener instrument data");

            // Define the mapping from MKV field names to Instrument bean property names
            Properties instrumentProps = new Properties();
            instrumentProps.setProperty("Id", "id");
            
            // Map Id0-Id15 fields
            for (int i = 0; i <= 15; i++) {
                instrumentProps.setProperty("Id" + i, "id" + i);
                instrumentProps.setProperty("Src" + i, "src" + i);
                instrumentProps.setProperty("Attribute" + i, "attribute" + i);
            }

            logger.info("Creating MkvSubscribeProxy for DepthListener instrument data");
            instrumentProxy = new MkvSubscribeProxy(Instrument.class, instrumentProps);
            logger.info("Instrument MkvSubscribeProxy created successfully");
        }

        if (depthProxy == null) {
            logger.info("Initializing MkvSubscribeProxy for DepthListener depth data");

            // Define the mapping from MKV field names to Best bean property names
            Properties depthProps = new Properties();
            depthProps.setProperty("Id", "Id");
            depthProps.setProperty("Ask0", "ask");
            depthProps.setProperty("Ask0Status", "askStatus");
            depthProps.setProperty("AskAttribute0", "askSrcCheck");
            depthProps.setProperty("AskSrc0", "askSrc");
            depthProps.setProperty("Ask0Status", "askIsAON");
            depthProps.setProperty("AskSize0", "askSize");
            depthProps.setProperty("AskSize0_Min", "askSizeMin");
            depthProps.setProperty("Bid0", "bid");
            depthProps.setProperty("BidAttribute0", "bidSrcCheck");
            depthProps.setProperty("BidSrc0", "bidSrc");
            depthProps.setProperty("Bid0Status", "bidStatus");
            depthProps.setProperty("Bid0Status", "bidIsAON");
            depthProps.setProperty("BidSize0", "bidSize");
            depthProps.setProperty("BidSize0_Min", "bidSizeMin");
            depthProps.setProperty("TrdValueLast", "lastTradePrice");

            logger.info("Creating MkvSubscribeProxy for DepthListener depth data");
            depthProxy = new MkvSubscribeProxy(Best.class, depthProps);
            logger.info("Depth MkvSubscribeProxy created successfully");
        }
    }

    /**
     * Creates a new DepthListener for pattern subscription.
     * 
     * @param manager The order manager to notify of best changes
     */
    public DepthListener(IOrderManager manager) {
        this.orderManager = manager;
        logger.info("Creating DepthListener for pattern subscription");
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
     * Processes a full update for depth or instrument records.
     * Updates the appropriate object with the new data and notifies the order manager if needed.
     */
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
            boolean isSnapshot) {
        try {
            // Update monitoring counters
            lastUpdateTimestamp.set(System.currentTimeMillis());
            
            String recordName = mkvRecord.getName();
            
            // Process instrument updates immediately
            if (recordName.contains("CM_INSTRUMENT")) {
                processInstrumentUpdate(mkvRecord, mkvSupply, isSnapshot);
                
                // After processing an instrument, check if we have any pending depth updates
                // for the same instrument that can now be processed
                String instrumentId = extractInstrumentId(recordName);
                if (instrumentId != null) {
                    processQueuedUpdatesForInstrument(instrumentId);
                }
            } 
            // Queue depth updates if the corresponding instrument may not be loaded yet
            else if (recordName.contains("CM_DEPTH")) {
                String depthInstrumentId = extractInstrumentIdFromDepth(recordName);
                
                // If we already have the instrument data, process immediately
                if (depthInstrumentId != null && instrumentData.containsKey(depthInstrumentId)) {
                    processDepthUpdate(mkvRecord, mkvSupply, isSnapshot);
                } 
                // Otherwise queue it for processing after instrument data arrives
                else {
                    queueDepthUpdate(depthInstrumentId, mkvRecord, mkvRecord.cloneSupply(), isSnapshot);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing update: " + e.getMessage() + " " + e);
            consecutiveErrorCount.incrementAndGet();
        }
    }
    
    /**
     * Processes depth updates for market data.
     */
    private void processDepthUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
        try {
            String recordName = mkvRecord.getName();
            String depthInstrumentId = extractInstrumentIdFromDepth(recordName);
            if (depthInstrumentId != null) {
                synchronized (instrumentData) {
                    if (!instrumentData.containsKey(depthInstrumentId)) {
                        // Race condition: instrument was removed, re-queue the update
                        queueDepthUpdate(depthInstrumentId, mkvRecord, mkvSupply, isSnapshot);
                        return;
                    }
                }
            }
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
                if (depthFieldsList.contains(fieldName)) {
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
            if (recordName.equals(GC_TU10_C) || recordName.equals(GC_TU10_REG)) {
                GCBest gcBest = (GCBest) createAppropriateBean(recordName, recordData);
                if (recordName.equals(GC_TU10_C)) {
                    Map<String, Object> gcData = recordDataMap.get(GC_TU10_C);
                    if (gcData != null) {
                        double cashGC = getDoubleValue(gcData, "TrdValueLast", 0.0);
                        GCBestManager.getInstance().updateCashGCBest(gcBest, cashGC);
                    }
                } else if (recordName.equals(GC_TU10_REG)) {
                    Map<String, Object> gcData = recordDataMap.get(GC_TU10_REG);
                    if (gcData != null) {
                        double regGC = getDoubleValue(gcData, "TrdValueLast", 0.0);
                        GCBestManager.getInstance().updateRegGCBest(gcBest, regGC);
                    }
                }
            }

            // Only proceed if data changed
            if (changed) {
                Best best = createAppropriateBean(recordName, recordData);
                logger.info(depthInstrumentId + " - Processing depth update for " + recordName +
                    ": " + best.toString() + " (isSnapshot=" + isSnapshot + ")");
                // Always use cached GC data
                orderManager.best(best, 
                    GCBestManager.getInstance().getCashGCRate(), 
                    GCBestManager.getInstance().getRegGCRate(), 
                    GCBestManager.getInstance().getCashGCBest(), 
                    GCBestManager.getInstance().getRegGCBest());
                updateCounter.incrementAndGet();
            }
            
        } catch (Exception e) {
            logger.error("Error processing depth update: " + e.getMessage() + " " + e);
            logger.error("Error details " +  e);
        }
    }
    
    /**
     * Processes instrument updates for mapping data.
     */
    private void processInstrumentUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
        try {
            String recordName = mkvRecord.getName();
            String instrumentId = extractInstrumentId(recordName);
            if (instrumentId == null || instrumentId.trim().isEmpty()) {
                logger.warn("Skipping update - no instrument ID in record: " + recordName);
                return;
            }

            // Single consolidated log instead of Steps 1-4
            logger.debug("Processing \"" + (isSnapshot ? "SNAPSHOT" : "UPDATE") + "\" for instrument " + instrumentId);

            Instrument instrument = null;

            // Steps 5-6 consolidated
            boolean isNewInstrument = !instrumentData.containsKey(instrumentId);
            if (isNewInstrument) {
                instrument = instrumentData.computeIfAbsent(instrumentId, 
                    k -> {
                        return new Instrument(instrumentId);
                    });
            } else {
                instrument = instrumentData.get(instrumentId);
            }

            // Steps 7-11 consolidated
            if (instrumentProxy != null) {
                try {
                    initProxies(mkvRecord);
                    updateInstrumentFields(instrument, mkvRecord, mkvSupply);
                    instrument.buildSourceMappings();
                } catch (Exception e) {
                    logger.error("Error processing instrument \"" + instrumentId + "\" with proxy " + e);
                }
            } else if (isNewInstrument) {
                // Fallback to manual processing if proxy is unavailable
                processInstrumentManually(mkvRecord, mkvSupply, instrument);
            }

        } catch (Exception e) {
            logger.error("Fatal error in processInstrumentUpdate " + e);
        }
    }
    
    /**
     * Manual processing fallback for instrument data when proxy fails.
     */
    private void processInstrumentManually(MkvRecord mkvRecord, MkvSupply mkvSupply, Instrument instrument) {
        try {
                
            int cursor = mkvSupply.firstIndex();
            int fieldCount = 0;
            int processedCount = 0;
            
            while (cursor != -1) {
                try {
                    String fieldName = mkvRecord.getMkvType().getFieldName(cursor);
                    Object fieldValue = mkvSupply.getObject(cursor);
                    fieldCount++;

                    if (fieldValue != null) {
                        processedCount++;

                        updateInstrumentProperty(instrument, fieldName, fieldValue);
                    } else {
                        logger.debug("Skipping null field in manual processing: " + fieldName);
                    }
                } catch (Exception e) {
                    logger.warn("Error processing field manually for instrument: " + instrument.getInstrumentId() + " " + e);
                }
                cursor = mkvSupply.nextIndex(cursor);
            }

            logger.info("processInstrumentManually completed for " + instrument.getInstrumentId() + ": processed " + processedCount + " out of " + fieldCount + " fields");

            // Build source mappings after manual processing
            instrument.buildSourceMappings();
            
        } catch (Exception e) {
            logger.error("Error in manual instrument processing " + e);
        }
    }
    
    /**
     * Extracts instrument ID from record name or other means.
     */
    private String extractInstrumentId(String recordName) {
        // Extract the instrument ID from the record name
        // Example: "USD.CM_INSTRUMENT.VMO_REPO_US.SOME_INSTRUMENT_ID" -> "SOME_INSTRUMENT_ID"
        if (recordName != null && recordName.contains(INSTRUMENT_PATTERN)) {
            String[] parts = recordName.split("\\.");
            if (parts.length > 3) {
                return parts[parts.length - 1]; // Last part is the instrument ID
            }
        }
        
        // Log the full record name when we can't extract an ID
        if (recordName != null) {
            logger.info("Unable to extract instrument ID from record name: " + recordName);
        }

        return null;
    }

    private void queueDepthUpdate(String instrumentId, MkvRecord record, MkvSupply supply, boolean isSnapshot) {
    if (instrumentId == null) {
        // If we can't determine the instrument ID, just process it immediately
        processDepthUpdate(record, supply, isSnapshot);
        return;
    }
    
    // Add to the queue for this instrument
    pendingDepthUpdates.computeIfAbsent(instrumentId, k -> new ArrayList<>())
        .add(new QueuedUpdate(record, supply, isSnapshot));

    logger.debug("Queued depth update for instrument " + instrumentId + " - waiting for instrument data. Queue size: " +
        pendingDepthUpdates.getOrDefault(instrumentId, Collections.emptyList()).size());
}

private String extractInstrumentIdFromDepth(String recordName) {
    // Extract instrument ID from depth record name
    // Example: "USD.CM_DEPTH.VMO_REPO_US.SOME_INSTRUMENT_ID" -> "SOME_INSTRUMENT_ID"
    if (recordName != null && recordName.contains("CM_DEPTH")) {
        String[] parts = recordName.split("\\.");
        if (parts.length > 3) {
            return parts[parts.length - 1]; // Last part is the instrument ID
        }
    }
    return null;
}

private void processQueuedUpdatesForInstrument(String instrumentId) {
    
    synchronized (queueLock) {
        List<QueuedUpdate> updates = pendingDepthUpdates.get(instrumentId);
        if (updates == null || updates.isEmpty()) {
            return;
        }

        // Process all queued updates for this instrument
        List<QueuedUpdate> updatesCopy = new ArrayList<>(updates);
        updates.clear();

        logger.info("Processing " + updatesCopy.size() + " queued depth updates for instrument: " + instrumentId);

        for (QueuedUpdate update : updatesCopy) {
            try {
                processDepthUpdate(update.record, update.supply, update.isSnapshot);
            } catch (Exception e) {
                logger.error("Error processing queued depth update: " + e.getMessage() + " " + e);
            }
        }
    }
}

// Add a method to periodically clean up old queued updates
private void cleanupQueuedUpdates() {
    long now = System.currentTimeMillis();
    long maxAgeMs = 60000; // 1 minute
    
    synchronized (queueLock) {
        Iterator<Map.Entry<String, List<QueuedUpdate>>> iter = pendingDepthUpdates.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, List<QueuedUpdate>> entry = iter.next();
            List<QueuedUpdate> updates = entry.getValue();
            
            // Remove old updates
            updates.removeIf(update -> (now - update.timestamp) > maxAgeMs);
            
            // Remove empty lists
            if (updates.isEmpty()) {
                iter.remove();
            }
        }
    }
}
private Best createAppropriateBean(String recordName, Map<String, Object> recordData) {
    // Get or create the Best instance from cache
    Best best = bestCache.computeIfAbsent(recordName, key -> {
        if (key.equals(GC_TU10_C) || key.equals(GC_TU10_REG)) {
            // Log only when creating a new instance
            logger.info("Created new GCBest instance for instrument: " + key);
            return new GCBest(key);
        } else {
            // Log only when creating a new instance
            logger.info("Created new Best instance for instrument: " + key);
            return new Best(key);
        }
    });
    
    // Update the existing instance
    if (best instanceof GCBest && (recordName.equals(GC_TU10_C) || recordName.equals(GC_TU10_REG))) {
        GCBest specialBest = (GCBest) best;
        
        // Set additional depth levels
        for (int i = 0; i < 10; i++) {
            specialBest.setAskPrice(i, getDoubleValue(recordData, "Ask" + i, 0.0));
            specialBest.setBidPrice(i, getDoubleValue(recordData, "Bid" + i, 0.0));
            specialBest.setAskSource(i, getStringValue(recordData, "AskSrc" + i, ""));
            specialBest.setBidSource(i, getStringValue(recordData, "BidSrc" + i, ""));
        }
        
        return specialBest;
    } else {
        // Update regular Best instance
        updateBestFromMap(best, recordData);
        return best;
    }
}

/**
 * Helper method to update an existing Best object from the stored map data
 */
private void updateBestFromMap(Best best, Map<String, Object> recordData) {
    // Set ID if available
    String id = getStringValue(recordData, "Id", "");
    if (!id.isEmpty()) {
        best.setId(id);
    }

        // For GCBest, we use the original logic with only level 0
    if (best instanceof GCBest) {
        GCBest gcBest = (GCBest) best;

        // Set price fields with rounding already handled in the setter
        gcBest.setAsk(getDoubleValue(recordData, "Ask0", 0.0));
        gcBest.setBid(getDoubleValue(recordData, "Bid0", 0.0));

        // Set source fields
        gcBest.setAskSrc(getStringValue(recordData, "AskSrc0", ""));
        gcBest.setBidSrc(getStringValue(recordData, "BidSrc0", ""));
        int askStatus = getIntValue(recordData, "Ask0Status", 0);
        int bidStatus = getIntValue(recordData, "Bid0Status", 0);
        gcBest.setAskStatus(askStatus);
        gcBest.setBidStatus(bidStatus);

        // Set AON flags using the status bitmasks
        gcBest.setAskIsAON(askStatus);
        gcBest.setBidIsAON(bidStatus);

        gcBest.setAskSrcCheck(getStringValue(recordData, "AskAttribute0", ""));
        gcBest.setBidSrcCheck(getStringValue(recordData, "BidAttribute0", ""));

        // Set size fields
        gcBest.setAskSize(getDoubleValue(recordData, "AskSize0", 0.0));
        gcBest.setBidSize(getDoubleValue(recordData, "BidSize0", 0.0));
        gcBest.setAskSizeMin(getDoubleValue(recordData, "AskSize0_Min", 0.0));
        gcBest.setBidSizeMin(getDoubleValue(recordData, "BidSize0_Min", 0.0));

    } else {
        // Regular Best instance - set all fields
    
        int askLevel = findFirstElectronicVenueLevel(recordData, true);
        int bidLevel = findFirstElectronicVenueLevel(recordData, false);
        // Use level 0 as fallback if no electronic venue found
        askLevel = (askLevel == -1) ? 0 : askLevel;
        bidLevel = (bidLevel == -1) ? 0 : bidLevel;

        logger.debug("Using ask level " + askLevel + " and bid level " + bidLevel + " for " + best.getId());

        // Set price fields with rounding already handled in the setter
        best.setAsk(getDoubleValue(recordData, "Ask" + askLevel, 0.0));
        best.setBid(getDoubleValue(recordData, "Bid" + bidLevel, 0.0));

        logger.debug(id + " - Set ask price: " + best.getAsk() + 
            ", bid price: " + best.getBid() + 
            " (ask level: " + askLevel + ", bid level: " + bidLevel + ")");

        best.setAskSrcCheck(getStringValue(recordData, "AskAttribute" + askLevel, ""));
        best.setBidSrcCheck(getStringValue(recordData, "BidAttribute" + bidLevel, ""));

        logger.debug(id + " - Set ask source check: " + best.getAskSrcCheck() + 
            ", bid source check: " + best.getBidSrcCheck() + 
            " (ask level: " + askLevel + ", bid level: " + bidLevel + ")");

        // Set source fields
        best.setAskSrc(getStringValue(recordData, "AskSrc" + askLevel, ""));
        best.setBidSrc(getStringValue(recordData, "BidSrc" + bidLevel, ""));
        int askStatus = getIntValue(recordData, "Ask" + askLevel + "Status", 0);
        int bidStatus = getIntValue(recordData, "Bid" + bidLevel + "Status", 0);
        best.setAskStatus(askStatus);
        best.setBidStatus(bidStatus);

        logger.debug(id + " - Set bid status: " + best.getBidStatus() + 
            " (ask level: " + askLevel + ", bid level: " + bidLevel + ")");

        // Set AON flags using the status bitmasks
        best.setAskIsAON(askStatus);
        best.setBidIsAON(bidStatus);

        logger.debug(id + " - Set ask is AON: " + best.getAskIsAON() + 
            ", bid is AON: " + best.getBidIsAON() + 
            " (ask level: " + askLevel + ", bid level: " + bidLevel + ")");

        best.setAskSrcCheck(getStringValue(recordData, "AskAttribute" + askLevel, ""));
        best.setBidSrcCheck(getStringValue(recordData, "BidAttribute" + bidLevel, ""));
        
        logger.debug(id + " - Set ask source check: " + best.getAskSrc() + 
            ", bid source check: " + best.getBidSrc() + 
            " (ask level: " + askLevel + ", bid level: " + bidLevel + ")");

        // Set size fields
        best.setAskSize(getDoubleValue(recordData, "AskSize" + askLevel, 0.0));
        best.setBidSize(getDoubleValue(recordData, "BidSize" + bidLevel, 0.0));

        logger.debug(id + " - Set bid size: " + best.getBidSize() + 
            ", ask size: " + best.getAskSize() +
            " (ask level: " + askLevel + ", bid level: " + bidLevel + ")");

        best.setAskSizeMin(getDoubleValue(recordData, "AskSize" + askLevel + "_Min", 0.0));
        best.setBidSizeMin(getDoubleValue(recordData, "BidSize" + bidLevel + "_Min", 0.0));

        logger.debug(id + " - Set ask size min: " + best.getAskSizeMin() + 
            ", bid size min: " + best.getBidSizeMin() + 
            " (ask level: " + askLevel + ", bid level: " + bidLevel + ")");

        // Set last fields
        best.setLastTradePrice(getDoubleValue(recordData, "TrdValueLast", 0.0));
    }
    best.setLastTradePrice(getDoubleValue(recordData, "TrdValueLast", 0.0));
}

    /**
     * Find the first level with an electronic venue source
     * @param recordData The map containing all depth data
     * @param isAsk True to search for ask levels, false for bid levels
     * @return The first level (0-9) with an electronic venue, or -1 if none found
     */
    private int findFirstElectronicVenueLevel(Map<String, Object> recordData, boolean isAsk) {
        // Check levels 0-9 for electronic venues
        String prefix = isAsk ? "AskSrc" : "BidSrc";
        
        for (int i = 0; i < 10; i++) {
            String source = getStringValue(recordData, prefix + i, "");
            if (isElectronicVenue(source)) {
                double price = getDoubleValue(recordData, (isAsk ? "Ask" : "Bid") + i, 0.0);
                int status = getIntValue(recordData, (isAsk ? "Ask" : "Bid") + i + "Status", 0);

                // Skip AON levels when looking for electronic venues
                if (isAON(status)) { continue; }
                // Make sure there's a valid price at this level
                if (price > 0.0) {
                    return i;
                }
            }
        }
        return -1; // No electronic venue found
    }

    private boolean isAON(int status) {
        // Directly use the AON bit-checking logic instead of creating an object
        return (status & Best.PRICE_AON) != 0;
    }

    /**
     * Check if the source is an electronic venue
     */
    private boolean isElectronicVenue(String source) {
        return source != null && !source.isEmpty() && ELECTRONIC_VENUES.contains(source);
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
            logger.warn("Error converting field \"" + field + "\" value: " + value);
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
    
    /**
     * Helper method to safely get an integer value from the data map
     */
    private int getIntValue(Map<String, Object> data, String field, int defaultValue) {
        Object value = data.get(field);
        if (value == null) {
            return defaultValue;
        }
        
        try {
            if (value instanceof Number) {
                return ((Number) value).intValue();
            } else if (value instanceof String) {
                // Handle both decimal and hex string formats
                String strValue = (String) value;
                if (strValue.startsWith("0x") || strValue.startsWith("0X")) {
                    return Integer.parseInt(strValue.substring(2), 16);
                } else {
                    return Integer.parseInt(strValue);
                }
            }
        } catch (Exception e) {
            logger.warn("Error converting field \"" + field + "\" value: " + value);
        }
        return defaultValue;
    }

/**
 * Gets the minimum quantity for a specific source.
 * Delegates to the appropriate Instrument object.
 * 
 * @param instrumentId The instrument identifier
 * @param sourceId The source identifier (e.g., "DEALERWEB_REPO", "BTEC_REPO_US", "FENICS_USREPO")
 * @return The minimum quantity as a double, or -1 if not found or invalid
 */
public double getMinimumQuantityBySource(String instrumentId, String sourceId) {
    logger.debug("getMinimumQuantityBySource called with instrumentId=" + instrumentId + ", sourceId=" + sourceId);

    if (instrumentId == null || sourceId == null || instrumentId.isEmpty() || sourceId.isEmpty()) {
        logger.warn("getMinimumQuantityBySource received null/empty parameter: instrumentId=" + instrumentId + ", sourceId=" + sourceId);
        return -1;
    }

    Instrument instrument = instrumentData.get(instrumentId);
    if (instrument == null) {
        logger.warn("No instrument data found for: " + instrumentId + " (total instruments loaded: " + instrumentData.size() + ")");

        // Add diagnostic info about available instruments for this source
        int matchCount = 0;
        StringBuilder matchingInstruments = new StringBuilder();
        for (Map.Entry<String, Instrument> entry : instrumentData.entrySet()) {
            String key = entry.getKey();
            if (key.contains(instrumentId) || instrumentId.contains(key)) {
                matchCount++;
                if (matchCount <= 5) { // Limit to 5 similar matches to avoid huge logs
                    matchingInstruments.append(key).append(", ");
                }
            }
        }
        
        if (matchCount > 0) {
            logger.info("Found " + matchCount + " similar instrument IDs for minimum quantity lookup: " +
                (matchCount > 5 ? matchingInstruments.toString() + "..." : matchingInstruments.toString()));
        }

        return -1;
    }

    // If we found the instrument, delegate to it and log the result
    double result = instrument.getMinimumQuantityBySource(sourceId);
    logger.debug("getMinimumQuantityBySource result for " + instrumentId + " with source " + sourceId + ": " + result);

    if (result <= 0) {
        logger.warn("Instrument " + instrumentId + " found but returned no valid minimum quantity for source: " + sourceId);
    }

    return result;
}

/**
 * Gets the minimum quantity for a specific source with a fallback default.
 * This is a convenience method that provides a fallback value if no specific minimum is found.
 * 
 * @param instrumentId The instrument identifier
 * @param sourceId The source identifier
 * @param defaultMinimum The default minimum to return if source-specific minimum is not found
 * @return The minimum quantity for the source, or defaultMinimum if not found
 */
public double getMinimumQuantityBySource(String instrumentId, String sourceId, double defaultMinimum) {
    double sourceMinimum = getMinimumQuantityBySource(instrumentId, sourceId);
    if (sourceMinimum <= 0) {
        logger.debug("No specific minimum found for instrument " + instrumentId + " source " + sourceId + ", using default: " + defaultMinimum);
        return defaultMinimum;
    }
    return sourceMinimum;
}

/**
 * Gets all minimum quantities mapped by source for a specific instrument.
 * Useful for configuration validation and debugging.
 * 
 * @param instrumentId The instrument identifier
 * @return Map of source -> minimum quantity, or empty map if instrument not found
 */
public Map<String, Double> getAllMinimumQuantitiesBySource(String instrumentId) {
    if (instrumentId == null || instrumentId.isEmpty()) {
        logger.warn("getAllMinimumQuantitiesBySource received null/empty instrumentId");
        return new HashMap<>();
    }

    Instrument instrument = instrumentData.get(instrumentId);
    if (instrument == null) {
        logger.warn("No instrument data found for: " + instrumentId + " when getting all minimum quantities");
        return new HashMap<>();
    }

    return instrument.getAllMinimumQuantitiesBySource();
}

    /**
     * Gets the native instrument ID for a source.
     * Delegates to the appropriate Instrument object.
     * 
     * @param instrumentId The instrument identifier
     * @param sourceId The source identifier
     * @param isAON Whether to look for AON variant
     * @return The native instrument ID or null if not found
     */
public String getInstrumentFieldBySourceString(String instrumentId, String sourceId, Boolean isAON) {
    logger.debug("getInstrumentFieldBySourceString called with instrumentId=" + instrumentId + ", sourceId=" + sourceId + ", isAON=" + isAON);

    if (instrumentId == null || sourceId == null || instrumentId.isEmpty() || sourceId.isEmpty()) {
        logger.warn("getInstrumentFieldBySourceString received null/empty parameter: instrumentId=" + instrumentId + ", sourceId=" + sourceId);
        return null;
    }

    Instrument instrument = instrumentData.get(instrumentId);
    if (instrument == null) {
        logger.warn("No instrument data found for: " + instrumentId + " (total instruments loaded: " + instrumentData.size() + ")");

        // Add diagnostic info about available instruments for this source
        int matchCount = 0;
        StringBuilder matchingInstruments = new StringBuilder();
        for (Map.Entry<String, Instrument> entry : instrumentData.entrySet()) {
            String key = entry.getKey();
            if (key.contains(instrumentId) || instrumentId.contains(key)) {
                matchCount++;
                if (matchCount <= 5) { // Limit to 5 similar matches to avoid huge logs
                    matchingInstruments.append(key).append(", ");
                }
            }
        }
        
        if (matchCount > 0) {
            logger.info("Found " + matchCount + " similar instrument IDs: " +
               (matchCount > 5 ? matchingInstruments.toString() + "..." : matchingInstruments.toString()));
        }

        return null;
    }

    // If we found the instrument, delegate to it and log the result
    String result = instrument.getInstrumentFieldBySourceString(sourceId, isAON);
    logger.debug("getInstrumentFieldBySourceString result for \"" + instrumentId + "\" with source \"" + sourceId + "\" (isAON=" + isAON + "): " + result);

    if (result == null) {
        logger.warn("Instrument " + instrumentId + " found but returned null mapping for source: " + sourceId + " (isAON=" + isAON + ")");
    }

    return result;
}

    /**
     * Get total number of instruments loaded
     */
    public int getInstrumentCount() {
        return instrumentData.size();
    }
    

    private void initializeHeartbeat() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                checkAndLogActivityStatus();
            } catch (Exception e) {
                logger.error("Error in DepthListener heartbeat: " + e.getMessage() + " " + e);
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down DepthListener heartbeat");
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
            logger.info("DepthListener heartbeat: No market data updates received yet. Instruments loaded: " + instrumentData.size() +
                ", Instrument pattern subscribed: " + isInstrumentPatternSubscribed + ", Instrument updates received: " + instrumentUpdatesReceived.get());
            return;
        }

        cleanupQueuedUpdates();

        // Report queue sizes in heartbeat
        int totalQueuedUpdates = pendingDepthUpdates.values().stream()
            .mapToInt(List::size)
            .sum();

        if (totalQueuedUpdates > 0) {
            logger.info("Currently " + totalQueuedUpdates + " depth updates queued for " + pendingDepthUpdates.size() + " instruments");
        }

        long silenceTime = now - lastUpdate;
        long updateCount = updateCounter.get();
        
        if (silenceTime > DATA_SILENCE_CRITICAL_MS) {
            logger.error("CRITICAL: No market data updates for " + (silenceTime / 1000) + " seconds. Last update at " + new Date(lastUpdate) + ". Total updates received: " + updateCount + ". Instruments loaded: " + instrumentData.size() + ". Instrument pattern subscribed: " + isInstrumentPatternSubscribed + ". Instrument updates: " + instrumentUpdatesReceived.get());
        } else if (silenceTime > DATA_SILENCE_WARNING_MS) {
            logger.warn("WARNING: No market data updates for " + (silenceTime / 1000) + " seconds. Last update at " + new Date(lastUpdate) + ". Total updates received: " + updateCount + ". Instruments loaded: " + instrumentData.size() + ". Instrument pattern subscribed: " + isInstrumentPatternSubscribed + ". Instrument updates: " + instrumentUpdatesReceived.get());
        } else {
            // Normal operation - periodic status log
            logger.info("DepthListener heartbeat: Last update " + (silenceTime / 1000) + " seconds ago. Update rate: " + String.format("%.2f", updateCount / ((now - startTime) / 1000.0)) + " updates/sec. Total updates: " + updateCount + ". Instruments loaded: " + instrumentData.size() + ". Instrument pattern subscribed: " + isInstrumentPatternSubscribed + ". Instrument updates: " + instrumentUpdatesReceived.get());

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
        status.put("lastUpdateTimestamp", formatTimestamp(lastUpdateTimestamp.get()));
        status.put("timeSinceLastUpdateMs", lastUpdateTimestamp.get() > 0 ? now - lastUpdateTimestamp.get() : -1);
        status.put("consecutiveErrors", consecutiveErrorCount.get());
        status.put("isHealthy", isReceivingData());
        status.put("instrumentsLoaded", instrumentData.size());
        status.put("instrumentPatternSubscribed", isInstrumentPatternSubscribed);
        status.put("instrumentUpdatesReceived", instrumentUpdatesReceived.get());
        status.put("timeSinceInstrumentSubscriptionMs", 
            instrumentPatternSubscribedTime.get() > 0 ? now - instrumentPatternSubscribedTime.get() : -1);
        return status;
    }
    
    /**
     * Helper method to update instrument fields using MkvRecord and MkvSupply
     */
    private void updateInstrumentFields(Instrument instrument, MkvRecord mkvRecord, MkvSupply mkvSupply) {
        try {
            logger.info("Starting updateInstrumentFields for " + instrument.getInstrumentId());

            int cursor = mkvSupply.firstIndex();
            int fieldCount = 0;
            int processedCount = 0;
            
            while (cursor != -1) {
                String fieldName = mkvRecord.getMkvType().getFieldName(cursor);
                Object fieldValue = mkvSupply.getObject(cursor);
                fieldCount++;

                logger.info("Field " + fieldCount + ": " + fieldName + " = " + fieldValue + " (null=" + (fieldValue == null) + ")");

                // Process ALL non-null fields for instruments, not just those in the list
                if (fieldValue != null) {
                    processedCount++;
                    logger.info("Processing field " + processedCount + ": " + fieldName + " = " + fieldValue);

                    // Update the instrument property
                    updateInstrumentProperty(instrument, fieldName, fieldValue);
                } else {
                    logger.info("Skipping null field: " + fieldName);
                }

                cursor = mkvSupply.nextIndex(cursor);
            }

            logger.info("updateInstrumentFields completed for " + instrument.getInstrumentId() + ": processed " + processedCount + " out of " + fieldCount + " fields");

        } catch (Exception e) {
            logger.error("Error updating instrument fields: " + e.getMessage() + " " + e);
        }
    }

    /**
     * Updates an instrument property using reflection
     */
    private void updateInstrumentProperty(Instrument instrument, String fieldName, Object value) {
        try {
            // Handle special fields
            if ("Id".equals(fieldName)) {
                instrument.setId(value.toString());
            } 
            // Handle indexed fields (Id0-Id15)
            else if (fieldName.startsWith("Id") && fieldName.length() > 2) {
                // Extract numeric index (Id0 -> 0)
                String indexStr = fieldName.substring(2);
                try {
                    int index = Integer.parseInt(indexStr);
                    
                    // Call the setter directly - more reliable than reflection
                    switch (index) {
                        case 0: instrument.setId0(value.toString()); break;
                        case 1: instrument.setId1(value.toString()); break;
                        case 2: instrument.setId2(value.toString()); break;
                        case 3: instrument.setId3(value.toString()); break;
                        case 4: instrument.setId4(value.toString()); break;
                        case 5: instrument.setId5(value.toString()); break;
                        case 6: instrument.setId6(value.toString()); break;
                        case 7: instrument.setId7(value.toString()); break;
                        case 8: instrument.setId8(value.toString()); break;
                        case 9: instrument.setId9(value.toString()); break;
                        case 10: instrument.setId10(value.toString()); break;
                        case 11: instrument.setId11(value.toString()); break;
                        case 12: instrument.setId12(value.toString()); break;
                        case 13: instrument.setId13(value.toString()); break;
                        case 14: instrument.setId14(value.toString()); break;
                        case 15: instrument.setId15(value.toString()); break;
                        default: 
                            logger.warn("Invalid Id index: " + index);
                            return;
                    }

                } catch (NumberFormatException e) {
                    logger.warn("Invalid id index format: " + fieldName);
                }
            }
            // Handle source fields (Src0-Src15) 
            else if (fieldName.startsWith("Src") && fieldName.length() > 3) {
                // Extract numeric index (Src0 -> 0)
                String indexStr = fieldName.substring(3);
                try {
                    int index = Integer.parseInt(indexStr);
                    
                    // Call the setter directly - more reliable than reflection
                    switch (index) {
                        case 0: instrument.setSrc0(value.toString()); break;
                        case 1: instrument.setSrc1(value.toString()); break;
                        case 2: instrument.setSrc2(value.toString()); break;
                        case 3: instrument.setSrc3(value.toString()); break;
                        case 4: instrument.setSrc4(value.toString()); break;
                        case 5: instrument.setSrc5(value.toString()); break;
                        case 6: instrument.setSrc6(value.toString()); break;
                        case 7: instrument.setSrc7(value.toString()); break;
                        case 8: instrument.setSrc8(value.toString()); break;
                        case 9: instrument.setSrc9(value.toString()); break;
                        case 10: instrument.setSrc10(value.toString()); break;
                        case 11: instrument.setSrc11(value.toString()); break;
                        case 12: instrument.setSrc12(value.toString()); break;
                        case 13: instrument.setSrc13(value.toString()); break;
                        case 14: instrument.setSrc14(value.toString()); break;
                        case 15: instrument.setSrc15(value.toString()); break;
                        default: 
                            logger.warn("Invalid Src index: " + index);
                            return;
                    }

                } catch (NumberFormatException e) {
                    logger.warn("Invalid source index format: " + fieldName);
                }
            }
            // Handle attribute fields (Attribute0-Attribute15)
            else if (fieldName.startsWith("Attribute") && fieldName.length() > 9) {
                // Extract numeric index (Attribute0 -> 0)
                String indexStr = fieldName.substring(9);
                try {
                    int index = Integer.parseInt(indexStr);
                    
                    // Call the setter directly - more reliable than reflection
                    switch (index) {
                        case 0: instrument.setAttribute0(value.toString()); break;
                        case 1: instrument.setAttribute1(value.toString()); break;
                        case 2: instrument.setAttribute2(value.toString()); break;
                        case 3: instrument.setAttribute3(value.toString()); break;
                        case 4: instrument.setAttribute4(value.toString()); break;
                        case 5: instrument.setAttribute5(value.toString()); break;
                        case 6: instrument.setAttribute6(value.toString()); break;
                        case 7: instrument.setAttribute7(value.toString()); break;
                        case 8: instrument.setAttribute8(value.toString()); break;
                        case 9: instrument.setAttribute9(value.toString()); break;
                        case 10: instrument.setAttribute10(value.toString()); break;
                        case 11: instrument.setAttribute11(value.toString()); break;
                        case 12: instrument.setAttribute12(value.toString()); break;
                        case 13: instrument.setAttribute13(value.toString()); break;
                        case 14: instrument.setAttribute14(value.toString()); break;
                        case 15: instrument.setAttribute15(value.toString()); break;
                        default:
                            logger.warn("Invalid Attribute index: " + index);
                            return;
                    }

                    // Check if attribute value ends with "AON" and set the corresponding is#AON flag
                    String attributeValue = value.toString();
                    if (attributeValue != null && attributeValue.endsWith("AON")) {
                        // Set the corresponding AON flag to true
                        switch (index) {
                            case 0: instrument.setIs0AON(Boolean.TRUE); break;
                            case 1: instrument.setIs1AON(Boolean.TRUE); break;
                            case 2: instrument.setIs2AON(Boolean.TRUE); break;
                            case 3: instrument.setIs3AON(Boolean.TRUE); break;
                            case 4: instrument.setIs4AON(Boolean.TRUE); break;
                            case 5: instrument.setIs5AON(Boolean.TRUE); break;
                            case 6: instrument.setIs6AON(Boolean.TRUE); break;
                            case 7: instrument.setIs7AON(Boolean.TRUE); break;
                            case 8: instrument.setIs8AON(Boolean.TRUE); break;
                            case 9: instrument.setIs9AON(Boolean.TRUE); break;
                            case 10: instrument.setIs10AON(Boolean.TRUE); break;
                            case 11: instrument.setIs11AON(Boolean.TRUE); break;
                            case 12: instrument.setIs12AON(Boolean.TRUE); break;
                            case 13: instrument.setIs13AON(Boolean.TRUE); break;
                            case 14: instrument.setIs14AON(Boolean.TRUE); break;
                            case 15: instrument.setIs15AON(Boolean.TRUE); break;
                        }
                        logger.info("Set AON flag for index " + index + " to TRUE due to attribute value: " + attributeValue);
                    } else if (attributeValue != null) {
                        // Set the corresponding AON flag to false if it exists
                        // This ensures we properly handle changes where AON is removed
                        switch (index) {
                            case 0: instrument.setIs0AON(Boolean.FALSE); break;
                            case 1: instrument.setIs1AON(Boolean.FALSE); break;
                            case 2: instrument.setIs2AON(Boolean.FALSE); break;
                            case 3: instrument.setIs3AON(Boolean.FALSE); break;
                            case 4: instrument.setIs4AON(Boolean.FALSE); break;
                            case 5: instrument.setIs5AON(Boolean.FALSE); break;
                            case 6: instrument.setIs6AON(Boolean.FALSE); break;
                            case 7: instrument.setIs7AON(Boolean.FALSE); break;
                            case 8: instrument.setIs8AON(Boolean.FALSE); break;
                            case 9: instrument.setIs9AON(Boolean.FALSE); break;
                            case 10: instrument.setIs10AON(Boolean.FALSE); break;
                            case 11: instrument.setIs11AON(Boolean.FALSE); break;
                            case 12: instrument.setIs12AON(Boolean.FALSE); break;
                            case 13: instrument.setIs13AON(Boolean.FALSE); break;
                            case 14: instrument.setIs14AON(Boolean.FALSE); break;
                            case 15: instrument.setIs15AON(Boolean.FALSE); break;
                        }
                    }

                } catch (NumberFormatException e) {
                    logger.warn("Invalid attribute index format: " + fieldName);
                }
            }
        } catch (Exception e) {
            logger.warn("Error updating property " + fieldName + ": " + e.getMessage() + " " + e);
        }
    }

    private String formatTimestamp(long timestampMs) {
        if (timestampMs <= 0) {
            return "N/A";
        }
        
        LocalDateTime dateTime = LocalDateTime.ofInstant(
            java.time.Instant.ofEpochMilli(timestampMs), 
            java.time.ZoneId.systemDefault());
        
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        return dateTime.format(formatter);
        }
    

    public void cleanup() {
        synchronized (queueLock) {
            pendingDepthUpdates.clear();
        }
        bestCache.clear();
        recordDataMap.clear();
        if (heartbeatScheduler != null && !heartbeatScheduler.isShutdown()) {
            heartbeatScheduler.shutdown();
            try {
                if (!heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    heartbeatScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                heartbeatScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    /**
     * Updates the instrument pattern subscription status tracking
     * 
     * @param subscribed The new subscription status
     */
    public void setInstrumentPatternSubscribed(boolean subscribed) {
        synchronized (healthLock) {
            boolean oldValue = isInstrumentPatternSubscribed;
            isInstrumentPatternSubscribed = subscribed;

            if (oldValue != subscribed) {
                logger.info("Instrument pattern subscription status changed: " + oldValue + " -> " + subscribed);
            }
        }
    }
}
