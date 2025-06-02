package com.iontrading.samples.advanced.orderManagement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Date;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvPattern;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;

/**
 * Enhanced VenueTraderListener that proactively manages trader data freshness
 * and provides reliable self-match prevention capabilities.
 */
public class VenueTraderListener implements MkvRecordListener {
    
    private static final Logger LOGGER = Logger.getLogger(VenueTraderListener.class.getName());
    
    // Venues to monitor
    private static final Set<String> VENUES = new HashSet<>(Arrays.asList(
            "BTEC_REPO_US", 
            "FENICS_USREPO", 
            "DEALERWEB_REPO"));
    
    // Enhanced field set - subscribe to more fields to detect any activity
    private static final String[] TRADER_FIELDS = {
            "Id", "Ask0Traders", "Bid0Traders", 
            "Ask0", "Bid0", "AskSize0", "BidSize0", // Price/size changes indicate activity
    };
    
    // Data freshness configuration
    private static final long FRESH_DATA_THRESHOLD_MS = 30000; // 30 seconds
    private static final long STALE_DATA_WARNING_MS = 60000;   // 1 minute
    private static final long DATA_EXPIRY_MS = 300000;         // 5 minutes
    
    /**
     * Enhanced data structure to track both values and metadata
     */
    private static class TraderData {
        private final String value;
        private final LocalTime timestamp;
        private final boolean isExplicitNull; // Distinguish between null and missing
        
        public TraderData(String value, LocalTime timestamp, boolean isExplicitNull) {
            this.value = value;
            this.timestamp = timestamp;
            this.isExplicitNull = isExplicitNull;
        }
        
        public String getValue() { return value; }
        public LocalTime getTimestamp() { return timestamp; }
        public boolean isExplicitNull() { return isExplicitNull; }
        
        public boolean isFresh() {
            if (timestamp == null) return false;
            return ChronoUnit.MILLIS.between(timestamp, LocalTime.now()) < FRESH_DATA_THRESHOLD_MS;
        }
        
        public boolean isStale() {
            if (timestamp == null) return true;
            return ChronoUnit.MILLIS.between(timestamp, LocalTime.now()) > STALE_DATA_WARNING_MS;
        }
        
        public boolean isExpired() {
            if (timestamp == null) return true;
            return ChronoUnit.MILLIS.between(timestamp, LocalTime.now()) > DATA_EXPIRY_MS;
        }
    }
    
    // Enhanced data structures
    private final Map<String, Map<String, Map<String, TraderData>>> venueData = new ConcurrentHashMap<>();
    private final Map<String, Boolean> venueSubscribed = new ConcurrentHashMap<>();
    private final Object subscriptionLock = new Object();
    
    // Track any update activity per instrument (not just trader fields)
    private final Map<String, Map<String, LocalTime>> lastActivityByInstrument = new ConcurrentHashMap<>();
    
    // Statistics and monitoring
    private final AtomicLong updateCounter = new AtomicLong(0);
    private final AtomicLong traderFieldUpdateCounter = new AtomicLong(0);
    private final AtomicLong lastUpdateTimestamp = new AtomicLong(0);
    private final AtomicLong consecutiveErrorCount = new AtomicLong(0);
    private final ScheduledExecutorService monitor = Executors.newScheduledThreadPool(2);
    
    // Enhanced monitoring
    private final Map<String, Set<String>> knownInstruments = new ConcurrentHashMap<>();
    private final long startTime = System.currentTimeMillis();
    
    public VenueTraderListener() {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
            "Initializing Enhanced VenueTraderListener for venues: " + VENUES);
        
        // Initialize data structures
        for (String venue : VENUES) {
            venueData.put(venue, new ConcurrentHashMap<>());
            venueSubscribed.put(venue, false);
            lastActivityByInstrument.put(venue, new ConcurrentHashMap<>());
            knownInstruments.put(venue, ConcurrentHashMap.newKeySet());
        }
        
        subscribeToVenues();
        startEnhancedMonitoring();
    }
    
    @Override
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
        String recordName = null;
        String venue = null;
        String instrumentId = null;
        
        try {
            // Enhanced null checks and logging at method entry
            if (mkvRecord == null) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "onFullUpdate called with null mkvRecord");
                return;
            }
            
            if (mkvSupply == null) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "onFullUpdate called with null mkvSupply for record: " + mkvRecord.getName());
                return;
            }
            
            recordName = mkvRecord.getName();        
            if (recordName == null || recordName.isEmpty()) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, "Record has null or empty name");
                return;
            }
            
            venue = extractVenue(recordName);
            if (venue == null) {
                if (LOGGER.isLoggable(Level.FINE)) {
                    ApplicationLogging.logAsync(LOGGER, Level.FINE, "No venue extracted from record: " + recordName);
                }
                return;
            }
            
            String askTraders = null;
            String bidTraders = null;
            String ask0PriceStr = null;    
            String bid0PriceStr = null;    
            String ask0SizeStr = null;    
            String bid0SizeStr = null;    
            LocalTime updateTime = LocalTime.now();
            boolean hasTraderFieldUpdates = false;
            
            // Enhanced cursor loop with detailed logging
            int cursor = -1;
            int fieldCount = 0;
            int errorCount = 0;
            
            try {
                cursor = mkvSupply.firstIndex();
                if (LOGGER.isLoggable(Level.FINE)) {
                    ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                        "Starting field processing for record: " + recordName + ", first cursor: " + cursor);
                }
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                    "Failed to get first index for record: " + recordName + ", error: " + e.getMessage(), e);
                return;
            }
            
            while (cursor != -1) {
                String fieldName = null;
                Object fieldValue = null;
                String fieldValueStr = null;
                fieldCount++;
                
                try {
                    // Get field name with error handling
                    try {
                        fieldName = mkvRecord.getMkvType().getFieldName(cursor);
                    } catch (Exception e) {
                        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                            "Failed to get field name at cursor " + cursor + " for record " + recordName + 
                            ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
                        throw e; // Re-throw to be caught by outer catch
                    }
                    
                    // Get field value using getObject() - the key fix!
                    try {
                        fieldValue = mkvSupply.getObject(cursor);
                        
                        // Convert to string safely
                        if (fieldValue != null) {
                            fieldValueStr = fieldValue.toString();
                        }
                        
                    } catch (Exception e) {
                        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                            "Failed to get field value for field '" + fieldName + "' at cursor " + cursor + 
                            " for record " + recordName + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
                        throw e; // Re-throw to be caught by outer catch
                    }
                    
                    // Process known fields - using fieldValueStr for string operations
                    if ("Id".equals(fieldName)) {
                        instrumentId = fieldValueStr;
                        if (LOGGER.isLoggable(Level.FINEST)) {
                            ApplicationLogging.logAsync(LOGGER, Level.FINEST, 
                                "Found instrumentId: " + instrumentId + " for record: " + recordName);
                        }
                    } else if ("Ask0Traders".equals(fieldName)) {
                        askTraders = fieldValueStr;
                        hasTraderFieldUpdates = true;
                    } else if ("Bid0Traders".equals(fieldName)) {
                        bidTraders = fieldValueStr;
                        hasTraderFieldUpdates = true;
                    } else if ("Ask0".equals(fieldName)) {          
                        ask0PriceStr = fieldValueStr;
                    } else if ("Bid0".equals(fieldName)) {          
                        bid0PriceStr = fieldValueStr;
                    } else if ("AskSize0".equals(fieldName)) {          
                        ask0SizeStr = fieldValueStr;
                    } else if ("BidSize0".equals(fieldName)) {          
                        bid0SizeStr = fieldValueStr;
                    }

                } catch (Exception fieldException) {
                    errorCount++;
                    
                    // Comprehensive field-level error logging
                    StringBuilder errorMsg = new StringBuilder();
                    errorMsg.append("Field processing error #").append(errorCount).append(" in record '").append(recordName).append("':");
                    errorMsg.append("\n  - Venue: ").append(venue);
                    errorMsg.append("\n  - Field count processed: ").append(fieldCount);
                    errorMsg.append("\n  - Current cursor: ").append(cursor);
                    errorMsg.append("\n  - Field name: ").append(fieldName != null ? "'" + fieldName + "'" : "NULL/FAILED_TO_GET");
                    errorMsg.append("\n  - Field value (Object): ").append(fieldValue != null ? fieldValue.toString() : "NULL/FAILED_TO_GET");
                    errorMsg.append("\n  - Field value (String): ").append(fieldValueStr != null ? "'" + fieldValueStr + "'" : "NULL/FAILED_TO_GET");
                    errorMsg.append("\n  - Is snapshot: ").append(isSnapshot);
                    errorMsg.append("\n  - Exception type: ").append(fieldException.getClass().getName());
                    errorMsg.append("\n  - Exception message: ").append(fieldException.getMessage());
                    
                    // Add cause chain if present
                    Throwable cause = fieldException.getCause();
                    int causeLevel = 1;
                    while (cause != null && causeLevel <= 3) {
                        errorMsg.append("\n  - Cause ").append(causeLevel).append(": ")
                               .append(cause.getClass().getSimpleName()).append(" - ").append(cause.getMessage());
                        cause = cause.getCause();
                        causeLevel++;
                    }
                    
                    ApplicationLogging.logAsync(LOGGER, Level.WARNING, errorMsg.toString());
                    
                    // Log stack trace for critical exceptions
                    if (fieldException instanceof NullPointerException || 
                        fieldException instanceof IndexOutOfBoundsException ||
                        fieldException instanceof IllegalStateException) {
                        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                            "Stack trace for critical exception:", fieldException);
                    }
                }
                
                // Enhanced cursor advancement with detailed error handling
                try {
                    cursor = mkvSupply.nextIndex(cursor);
                    
                    if (LOGGER.isLoggable(Level.FINEST)) {
                        ApplicationLogging.logAsync(LOGGER, Level.FINEST, 
                            "Advanced cursor to " + cursor + " for record " + recordName);
                    }
                    
                } catch (Exception cursorException) {
                    ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                        "Critical error advancing cursor for record " + recordName + 
                        " after processing " + fieldCount + " fields: " + 
                        cursorException.getClass().getSimpleName() + " - " + cursorException.getMessage(), 
                        cursorException);
                    break;
                }
            }
            
            // Log summary of field processing
            if (LOGGER.isLoggable(Level.FINE)) {
                ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                    "Field processing complete for record " + recordName + 
                    ": processed " + fieldCount + " fields, " + errorCount + " errors, instrumentId=" + instrumentId);
            }

            if (instrumentId == null || instrumentId.isEmpty()) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "No instrumentId found for record " + recordName + " (venue=" + venue + 
                    "), processed " + fieldCount + " fields with " + errorCount + " errors");
                return;
            }
            
            // Record activity for this instrument regardless of trader field changes
            try {
                recordInstrumentActivity(venue, instrumentId, updateTime);
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Error recording instrument activity for " + venue + "/" + instrumentId + ": " + e.getMessage(), e);
            }
            
            // Store trader data with enhanced metadata - wrap each call individually
            try {
                storeEnhancedTraderData(venue, instrumentId, "Ask0Traders", askTraders, updateTime, true);
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Error storing Ask0Traders data for " + venue + "/" + instrumentId + ": " + e.getMessage(), e);
            }
            
            try {
                storeEnhancedTraderData(venue, instrumentId, "Bid0Traders", bidTraders, updateTime, true);
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Error storing Bid0Traders data for " + venue + "/" + instrumentId + ": " + e.getMessage(), e);
            }
            
            try {
                storeEnhancedTraderData(venue, instrumentId, "Ask0", ask0PriceStr, updateTime, true);
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Error storing Ask0 data for " + venue + "/" + instrumentId + ": " + e.getMessage(), e);
            }
            
            try {
                storeEnhancedTraderData(venue, instrumentId, "Bid0", bid0PriceStr, updateTime, true);
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Error storing Bid0 data for " + venue + "/" + instrumentId + ": " + e.getMessage(), e);
            }
            
            try {
                storeEnhancedTraderData(venue, instrumentId, "AskSize0", ask0SizeStr, updateTime, true);
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Error storing AskSize0 data for " + venue + "/" + instrumentId + ": " + e.getMessage(), e);
            }
            
            try {
                storeEnhancedTraderData(venue, instrumentId, "BidSize0", bid0SizeStr, updateTime, true);
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Error storing BidSize0 data for " + venue + "/" + instrumentId + ": " + e.getMessage(), e);
            }
            
            // Update counters with error handling
            try {
                updateCounter.incrementAndGet();
                lastUpdateTimestamp.set(System.currentTimeMillis());
                consecutiveErrorCount.set(0);
                
                if (hasTraderFieldUpdates) {
                    traderFieldUpdateCounter.incrementAndGet();
                }
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Error updating counters for " + venue + "/" + instrumentId + ": " + e.getMessage(), e);
            }
            
            if (LOGGER.isLoggable(Level.FINE)) {
                ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                    "Enhanced update completed for venue " + venue + ", instrument " + instrumentId + 
                    " (traderUpdate=" + hasTraderFieldUpdates + ", fieldCount=" + fieldCount + 
                    ", errorCount=" + errorCount + "): askTraders=" + askTraders + 
                    ", bidTraders=" + bidTraders);
            }
            
        } catch (Exception e) {
            consecutiveErrorCount.incrementAndGet();
            
            // Comprehensive outer exception logging
            StringBuilder errorMsg = new StringBuilder();
            errorMsg.append("CRITICAL ERROR in onFullUpdate:");
            errorMsg.append("\n  - Record name: ").append(recordName != null ? recordName : "NULL/UNKNOWN");
            errorMsg.append("\n  - Venue: ").append(venue != null ? venue : "NULL/UNKNOWN");
            errorMsg.append("\n  - Instrument ID: ").append(instrumentId != null ? instrumentId : "NULL/UNKNOWN");
            errorMsg.append("\n  - Is snapshot: ").append(isSnapshot);
            errorMsg.append("\n  - Exception type: ").append(e.getClass().getName());
            errorMsg.append("\n  - Exception message: ").append(e.getMessage());
            errorMsg.append("\n  - Thread: ").append(Thread.currentThread().getName());
            errorMsg.append("\n  - Consecutive error count: ").append(consecutiveErrorCount.get());
            
            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, errorMsg.toString(), e);
        }
    }

    @Override
    public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
    	// only update full updates
    }

    
    /**
     * Record activity for an instrument to track data freshness
     */
    private void recordInstrumentActivity(String venue, String instrumentId, LocalTime time) {
        lastActivityByInstrument.get(venue).put(instrumentId, time);
        knownInstruments.get(venue).add(instrumentId);
    }
    
    /**
     * Store trader data with enhanced metadata
     */
    private void storeEnhancedTraderData(String venue, String instrumentId, String fieldName, 
                                       String value, LocalTime time, boolean wasExplicitUpdate) {
        Map<String, Map<String, TraderData>> venueMap = venueData.get(venue);
        if (venueMap == null) return;
        
        Map<String, TraderData> instrumentMap = venueMap.computeIfAbsent(
            instrumentId, k -> new ConcurrentHashMap<>()
        );
        
        TraderData traderData = new TraderData(value, time, wasExplicitUpdate && value == null);
        instrumentMap.put(fieldName, traderData);
    }
    
    /**
     * Enhanced trader info retrieval with data freshness validation
     */
    public String getTraderInfo(String venue, String instrumentId, String fieldName) {
        if (venue == null || instrumentId == null || fieldName == null) {
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                "Invalid parameters in getTraderInfo: venue=" + venue + 
                ", instrumentId=" + instrumentId + ", fieldName=" + fieldName);
            return null;
        }
        
        try {
            // Check venue subscription
            if (!isVenueSubscribed(venue)) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Venue " + venue + " is not subscribed");
                return null;
            }
            
            // Get the trader data
            Map<String, Map<String, TraderData>> venueMap = venueData.get(venue);
            if (venueMap == null) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "No data found for venue: " + venue);
                return null;
            }
            
            Map<String, TraderData> instrumentMap = venueMap.get(instrumentId);
            if (instrumentMap == null) {
                // Check if we've seen this instrument recently
                LocalTime lastActivity = lastActivityByInstrument.get(venue).get(instrumentId);
                if (lastActivity != null) {
                    long timeSinceActivity = ChronoUnit.MILLIS.between(lastActivity, LocalTime.now());
                    if (timeSinceActivity < FRESH_DATA_THRESHOLD_MS) {
                        // Instrument is active but has no trader data - this means no traders
                        ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                            "Active instrument with no trader data (no traders present): " + 
                            venue + "/" + instrumentId + "/" + fieldName);
                        return ""; // Empty string indicates no traders (vs null indicating no data)
                    }
                }
                
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "No data found for instrument: " + instrumentId + " in venue: " + venue);
                return null;
            }
            
            TraderData traderData = instrumentMap.get(fieldName);
            if (traderData == null) {
                // Check instrument activity
                LocalTime lastActivity = lastActivityByInstrument.get(venue).get(instrumentId);
                if (lastActivity != null && 
                    ChronoUnit.MILLIS.between(lastActivity, LocalTime.now()) < FRESH_DATA_THRESHOLD_MS) {
                    // Recent activity but no specific trader field data
                    ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                        "Recent instrument activity but no " + fieldName + " data for " + 
                        venue + "/" + instrumentId + " (likely no traders)");
                    return ""; // No traders present
                }
                
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "No " + fieldName + " data found for instrument: " + instrumentId + 
                    " in venue: " + venue);
                return null;
            }
            
            // Check data freshness
            if (traderData.isExpired()) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Trader data is expired for " + venue + "/" + instrumentId + "/" + fieldName + 
                    " (age: " + ChronoUnit.MILLIS.between(traderData.getTimestamp(), LocalTime.now()) + "ms)");
                return null; // Don't use expired data for trading decisions
            } else if (traderData.isStale()) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Trader data is stale for " + venue + "/" + instrumentId + "/" + fieldName + 
                    " (age: " + ChronoUnit.MILLIS.between(traderData.getTimestamp(), LocalTime.now()) + "ms)");
                // Continue to use stale data but warn about it
            }
            
            String value = traderData.getValue();
            
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Retrieved trader info for " + venue + "/" + instrumentId + "/" + fieldName + 
                ": '" + value + "' (age: " + 
                ChronoUnit.MILLIS.between(traderData.getTimestamp(), LocalTime.now()) + "ms)");
            
            return value;
            
        } catch (Exception e) {
            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                "Error retrieving enhanced trader info: " + e.getMessage(), e);
            return null;
        }
    }
        /**
     * Enhanced method to get trader info as double (for prices)
     */
    public Double getTraderInfoAsDouble(String venue, String instrumentId, String fieldName) {
        String stringValue = getTraderInfo(venue, instrumentId, fieldName);
        if (stringValue == null) {
            return null;
        }
        
        try {
            return Double.parseDouble(stringValue.trim());
        } catch (NumberFormatException e) {
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                "Error converting trader info to double: venue=" + venue + 
                ", instrument=" + instrumentId + ", field=" + fieldName + 
                ", value='" + stringValue + "'");
            return null;
        }
    }
    /**
     * Get data quality information for a specific venue/instrument/field
     */
    public Map<String, Object> getDataQuality(String venue, String instrumentId, String fieldName) {
        Map<String, Object> quality = new HashMap<>();
        
        try {
            // Basic info
            quality.put("venue", venue);
            quality.put("instrumentId", instrumentId);
            quality.put("fieldName", fieldName);
            quality.put("venueSubscribed", isVenueSubscribed(venue));
            
            // Last activity time
            LocalTime lastActivity = lastActivityByInstrument.get(venue).get(instrumentId);
            quality.put("lastInstrumentActivity", lastActivity);
            quality.put("instrumentActivityAge", lastActivity != null ? 
                ChronoUnit.MILLIS.between(lastActivity, LocalTime.now()) : -1);
            
            // Trader data info
            Map<String, Map<String, TraderData>> venueMap = venueData.get(venue);
            if (venueMap != null) {
                Map<String, TraderData> instrumentMap = venueMap.get(instrumentId);
                if (instrumentMap != null) {
                    TraderData traderData = instrumentMap.get(fieldName);
                    if (traderData != null) {
                        quality.put("hasTraderData", true);
                        quality.put("traderDataValue", traderData.getValue());
                        quality.put("traderDataTimestamp", traderData.getTimestamp());
                        quality.put("traderDataAge", ChronoUnit.MILLIS.between(
                            traderData.getTimestamp(), LocalTime.now()));
                        quality.put("isFresh", traderData.isFresh());
                        quality.put("isStale", traderData.isStale());
                        quality.put("isExpired", traderData.isExpired());
                        quality.put("wasExplicitNull", traderData.isExplicitNull());
                    } else {
                        quality.put("hasTraderData", false);
                    }
                } else {
                    quality.put("hasInstrumentData", false);
                }
            } else {
                quality.put("hasVenueData", false);
            }
            
        } catch (Exception e) {
            quality.put("error", e.getMessage());
        }
        
        return quality;
    }
    
    /**
     * Enhanced monitoring with proactive data quality checks
     */
    private void startEnhancedMonitoring() {
        // Primary monitoring task
        monitor.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                long lastUpdate = lastUpdateTimestamp.get();
                long totalUpdates = updateCounter.get();
                long traderUpdates = traderFieldUpdateCounter.get();
                
                ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                    "Enhanced VenueTrader Status: " +
                    "totalUpdates=" + totalUpdates + 
                    ", traderFieldUpdates=" + traderUpdates + 
                    ", lastUpdate=" + (lastUpdate > 0 ? (now - lastUpdate) / 1000 : -1) + "s ago");
                
                // Per-venue detailed status
                for (String venue : VENUES) {
                    boolean subscribed = isVenueSubscribed(venue);
                    int instrumentCount = knownInstruments.get(venue).size();
                    int dataCount = venueData.get(venue).size();
                    
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                        "Venue " + venue + ": subscribed=" + subscribed + 
                        ", knownInstruments=" + instrumentCount + 
                        ", instrumentsWithTraderData=" + dataCount);
                }
                
                // Check for excessive errors
                if (consecutiveErrorCount.get() > 10) {
                    ApplicationLogging.logAsync(LOGGER, Level.SEVERE,
                        "High error count (" + consecutiveErrorCount.get() + 
                        "), attempting venue resubscription");
                    resubscribeAllVenues();
                    consecutiveErrorCount.set(0);
                }
                
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                    "Error in enhanced monitoring: " + e.getMessage(), e);
            }
        }, 15, 15, TimeUnit.SECONDS);
        
        // Data quality monitoring task
        monitor.scheduleAtFixedRate(() -> {
            try {
                checkDataQuality();
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                    "Error in data quality check: " + e.getMessage(), e);
            }
        }, 30, 60, TimeUnit.SECONDS); // Check quality every minute
    }
    
    /**
     * Check data quality across all venues and warn about stale data
     */
    private void checkDataQuality() {
        for (String venue : VENUES) {
            if (!isVenueSubscribed(venue)) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Venue " + venue + " is not subscribed");
                continue;
            }
            
            Set<String> instruments = knownInstruments.get(venue);
            int staleCount = 0;
            int expiredCount = 0;
            
            for (String instrumentId : instruments) {
                LocalTime lastActivity = lastActivityByInstrument.get(venue).get(instrumentId);
                if (lastActivity != null) {
                    long age = ChronoUnit.MILLIS.between(lastActivity, LocalTime.now());
                    if (age > DATA_EXPIRY_MS) {
                        expiredCount++;
                    } else if (age > STALE_DATA_WARNING_MS) {
                        staleCount++;
                    }
                }
            }
            
            if (expiredCount > 0 || staleCount > 0) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Data quality issues for venue " + venue + ": " +
                    "stale=" + staleCount + ", expired=" + expiredCount + 
                    " out of " + instruments.size() + " instruments");
            }
        }
    }
    
    /**
     * Extract the venue from the record name
     */   
    private String extractVenue(String recordName) {
        for (String venue : VENUES) {
            if (recordName.contains("." + venue + ".")) {
                return venue;
            }
        }
        return null;
    }
    
    private void subscribeToVenues() {
        for (String venue : VENUES) {
            subscribeToVenue(venue);
        }
        setupPatternSubscriptionMonitor();
    }
    
    private void subscribeToVenue(String venue) {
        // Implementation similar to original but with enhanced fields
        final String patternName = "USD.CM_DEPTH." + venue + ".";
        
        try {
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            MkvObject obj = pm.getMkvObject(patternName);
            
            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                synchronized (subscriptionLock) {
                    if (!venueSubscribed.get(venue)) {
                        ((MkvPattern) obj).subscribe(TRADER_FIELDS, this);
                        venueSubscribed.put(venue, true);
                        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                            "Enhanced subscription successful for venue " + venue);
                    }
                }
            } else {
                // Set up listener as in original implementation
                ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                    "Setting up listener for venue " + venue);
            }
        } catch (Exception e) {
            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                "Error in enhanced venue subscription for " + venue + ": " + e.getMessage(), e);
        }
    }
    
    private void setupPatternSubscriptionMonitor() {
        monitor.scheduleAtFixedRate(() -> {
            // Similar to original but with enhanced monitoring
            for (String venue : VENUES) {
                try {
                    // Check subscription health
                    if (!isVenueSubscribed(venue)) {
                        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                            "Venue " + venue + " subscription lost, attempting resubscribe");
                        subscribeToVenue(venue);
                    }
                } catch (Exception e) {
                    ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                        "Error in subscription monitor for venue " + venue + ": " + e.getMessage(), e);
                }
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
    
    private void resubscribeAllVenues() {
        for (String venue : VENUES) {
            venueSubscribed.put(venue, false);
            subscribeToVenue(venue);
        }
    }
    
    public boolean isVenueSubscribed(String venue) {
        Boolean status = venueSubscribed.get(venue);
        return status != null && status;
    }
    
    public LocalTime getTraderInfoTimestamp(String venue, String instrumentId, String fieldName) {
        try {
            Map<String, Map<String, TraderData>> venueMap = venueData.get(venue);
            if (venueMap != null) {
                Map<String, TraderData> instrumentMap = venueMap.get(instrumentId);
                if (instrumentMap != null) {
                    TraderData traderData = instrumentMap.get(fieldName);
                    if (traderData != null) {
                        return traderData.getTimestamp();
                    }
                }
            }
        } catch (Exception e) {
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                "Error getting trader info timestamp: " + e.getMessage());
        }
        return null;
    }
    /**
     * Helper method to safely convert a price string to double
     */
    private double safeStringToDouble(String priceStr, double defaultValue) {
        if (priceStr == null || priceStr.trim().isEmpty()) {
            return defaultValue;
        }
        
        try {
            return Double.parseDouble(priceStr.trim());
        } catch (NumberFormatException e) {
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                "Error converting price string to double: '" + priceStr + "', using default: " + defaultValue);
            return defaultValue;
        }
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
                String strValue = ((String) value).trim();
                if (strValue.isEmpty()) {
                    return defaultValue;
                }
                return Double.parseDouble(strValue);
            }
        } catch (Exception e) {
            // Log the error but return default value
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                "Error converting field " + field + " value: " + value + ", error: " + e.getMessage());
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
        
        String strValue = value.toString();
        return strValue.trim().isEmpty() ? defaultValue : strValue.trim();
    }
}