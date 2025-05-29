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
 * VenueTraderListener subscribes to venue-specific depth information to track
 * trader information at the best price levels.
 * 
 * This class monitors multiple venues simultaneously and provides access to
 * trader information via a lookup method.
 */
public class VenueTraderListener implements MkvRecordListener {
    
    // Add logger for debugging
    private static final Logger LOGGER = Logger.getLogger(VenueTraderListener.class.getName());
    
    // Venues to monitor
    private static final Set<String> VENUES = new HashSet<>(Arrays.asList(
            "BTEC_REPO_US", 
            "FENICS_USREPO", 
            "DEALERWEB_REPO"));
    
    // Fields to subscribe to for each venue
    private static final String[] TRADER_FIELDS = {
            "Id", "Ask0Traders", "Bid0Traders"
    };
    
    // Data structure to store venue/instrument/trader information
    // Map structure: <Venue, <InstrumentId, <FieldName, Value>>>
    private final Map<String, Map<String, Map<String, String>>> venueData = new ConcurrentHashMap<>();
    
    // Track subscription status for each venue with synchronization protection
    private final Map<String, Boolean> venueSubscribed = new ConcurrentHashMap<>();
    private final Object subscriptionLock = new Object();
    
    // Statistics for monitoring
    private final AtomicLong updateCounter = new AtomicLong(0);
    private final AtomicLong lastUpdateTimestamp = new AtomicLong(0);
    private final AtomicLong consecutiveErrorCount = new AtomicLong(0);
    private final ScheduledExecutorService monitor = Executors.newScheduledThreadPool(1);
    
    // Monitoring constants
    private static final long HEARTBEAT_INTERVAL_MS = 5000; // 5 seconds for monitoring
    private final long startTime = System.currentTimeMillis();
    
    /**
     * Create a new VenueTraderListener and initialize subscriptions to all venues.
     */
    public VenueTraderListener() {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Initializing VenueTraderListener for venues: " + VENUES);
        
        // Initialize data structures for all venues
        for (String venue : VENUES) {
            venueData.put(venue, new ConcurrentHashMap<>());
            venueSubscribed.put(venue, false);
        }
        
        // Start the subscription process for all venues
        subscribeToVenues();
        
        // Set up monitoring thread with improved health checks
        startMonitoring();
    }
    
    /**
     * Subscribe to market data for all configured venues
     */
    private void subscribeToVenues() {
        for (String venue : VENUES) {
            try {
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Attempting to subscribe to venue: " + venue);
                subscribeToVenue(venue);
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                    "Error subscribing to venue " + venue + ": " + e.getMessage(), e);
            }
        }
        
        // Set up the pattern subscription monitor similar to OrderManagement
        setupPatternSubscriptionMonitor();
    }
    
    /**
     * Subscribe to a specific venue using the pattern format with improved error handling
     * 
     * @param venue The venue name to subscribe to
     */
    private void subscribeToVenue(String venue) {
        // Construct the pattern name for this venue
        final String patternName = "USD.CM_DEPTH." + venue + ".";
        
        try {
            ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                "Subscribing to venue pattern: " + patternName);
            
            // Get the publish manager to access the pattern
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            MkvObject obj = pm.getMkvObject(patternName);
            
            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                // Pattern exists, subscribe immediately
                synchronized (subscriptionLock) {
                    // Double-check inside synchronization to avoid race conditions
                    if (!venueSubscribed.get(venue)) {
                        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                            "Found pattern for venue " + venue + ", subscribing");
                        
                        // Set up subscription
                        ((MkvPattern) obj).subscribe(TRADER_FIELDS, this);
                        
                        // Mark as subscribed
                        venueSubscribed.put(venue, true);
                        
                        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                            "Successfully subscribed to venue " + venue);
                    }
                }
            } else {
                // Pattern doesn't exist yet, set up a listener to wait for it
                ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                    "Pattern for venue " + venue + " not found, setting up listener");
                
                // Create a publication listener for this venue
                final String capturedVenue = venue; // Capture for the inner class
                MkvPublishListener venueListener = new MkvPublishListener() {
                    @Override
                    public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
                        // Only proceed if we're not already subscribed
                        if (venueSubscribed.get(capturedVenue)) {
                            return;
                        }
                        
                        // Check if this is our pattern being published
                        if (pub_unpub && mkvObject.getName().equals(patternName) &&
                            mkvObject.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                            
                            trySubscribeAndRemoveListener(mkvObject, pm, this, capturedVenue);
                        }
                    }
                    
                    @Override
                    public void onPublishIdle(String component, boolean start) {
                        // Only proceed if we're not already subscribed
                        if (venueSubscribed.get(capturedVenue)) {
                            return;
                        }
                        
                        // Try looking for the pattern again at idle time
                        MkvObject obj = pm.getMkvObject(patternName);
                        
                        if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                            trySubscribeAndRemoveListener(obj, pm, this, capturedVenue);
                        }
                    }
                    
                    @Override
                    public void onSubscribe(MkvObject mkvObject) {
                        // Not needed
                    }
                };
                
                // Add the listener
                pm.addPublishListener(venueListener);
                
                ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                    "Set up publication listener for venue " + venue);
            }
        } catch (Exception e) {
            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                "Error in subscription process for venue " + venue + ": " + e.getMessage(), e);
        }
    }
    
    /**
     * Helper method to handle subscription and listener removal safely
     */
    private void trySubscribeAndRemoveListener(MkvObject mkvObject, MkvPublishManager pm, 
                                             MkvPublishListener listener, String venue) {
        synchronized (subscriptionLock) {
            // Check again inside synchronization to avoid race conditions
            if (venueSubscribed.get(venue)) {
                return;
            }
            
            try {
                ApplicationLogging.logAsync(LOGGER, Level.INFO,
                    "Pattern found for venue " + venue + ", subscribing");
                    
                ((MkvPattern) mkvObject).subscribe(TRADER_FIELDS, this);
                venueSubscribed.put(venue, true);
                
                ApplicationLogging.logAsync(LOGGER, Level.INFO,
                    "Successfully subscribed to pattern for venue " + venue);
                    
                // Remove the listener now that we've subscribed
                pm.removePublishListener(listener);
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE,
                    "Error subscribing to pattern for venue " + venue + ": " + e.getMessage(), e);
            }
        }
    }
    
    /**
     * Set up a monitor to check pattern subscription status periodically
     */
    private void setupPatternSubscriptionMonitor() {
        // Schedule a task to check pattern subscription status regularly
        monitor.scheduleAtFixedRate(() -> {
            try {
                MkvPublishManager pm = Mkv.getInstance().getPublishManager();
                
                // Check each venue's subscription status
                for (String venue : VENUES) {
                    final String patternName = "USD.CM_DEPTH." + venue + ".";
                    
                    // Skip if already marked as subscribed
                    if (!venueSubscribed.get(venue)) {
                        continue;
                    }
                    
                    MkvObject obj = pm.getMkvObject(patternName);
                    
                    if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                        MkvPattern pattern = (MkvPattern) obj;
                        
                        // Check if our listener is still subscribed to the pattern
                        boolean currentlySubscribed = pattern.isSubscribed(this, TRADER_FIELDS);
                        
                        ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                            "Pattern subscription check for venue " + venue + ": subscribed=" + currentlySubscribed + 
                            ", our state=" + venueSubscribed.get(venue));
                        
                        // If we think we're subscribed but actually aren't, resubscribe
                        if (venueSubscribed.get(venue) && !currentlySubscribed) {
                            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                                "Pattern subscription lost for venue " + venue + ", resubscribing");
                                
                            synchronized (subscriptionLock) {
                                pattern.subscribe(TRADER_FIELDS, this);
                                
                                ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                    "Successfully resubscribed to pattern for venue " + venue);
                            }
                        }
                        
                        // Update our tracking state to match reality
                        venueSubscribed.put(venue, currentlySubscribed);
                    } else {
                        // Pattern doesn't exist anymore, need to reset our state
                        if (venueSubscribed.get(venue)) {
                            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                                "Pattern not found for venue " + venue + " but we thought we were subscribed");
                            venueSubscribed.put(venue, false);
                        }
                        
                        // Try to find and subscribe to the pattern again
                        subscribeToVenue(venue);
                    }
                }
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                    "Error in pattern subscription monitor: " + e.getMessage(), e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
    
    /**
     * Not processing partial updates, only full updates
     */
    @Override
    public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
        // Not needed for this implementation
    }
    
    /**
     * Process full updates for depth records from all venues with improved error handling
     */
    @Override
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
        try {
            // Get the record name
            String recordName = mkvRecord.getName();
            
            // Determine which venue this record is from
            String venue = extractVenue(recordName);
            
            // Skip if not one of our monitored venues
            if (venue == null) {
                return;
            }
            
            // Extract the instrument ID from the record data
            String instrumentId = null;
            String askTraders = null;
            String bidTraders = null;
            
            // Process all fields
            int cursor = mkvSupply.firstIndex();
            while (cursor != -1) {
                String fieldName = mkvRecord.getMkvType().getFieldName(cursor);
                
                // Extract the values we're interested in
                if ("Id".equals(fieldName)) {
                    instrumentId = mkvSupply.getString(cursor);
                } else if ("Ask0Traders".equals(fieldName)) {
                    askTraders = mkvSupply.getString(cursor);
                } else if ("Bid0Traders".equals(fieldName)) {
                    bidTraders = mkvSupply.getString(cursor);
                }
                
                cursor = mkvSupply.nextIndex(cursor);
            }
            
            // Skip if we couldn't extract an instrument ID
            if (instrumentId == null || instrumentId.isEmpty()) {
                if (LOGGER.isLoggable(Level.FINE)) {
                    ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                        "Skipping record with no instrument ID: " + recordName);
                }
                return;
            }
            
            // Store the data
            storeVenueData(venue, instrumentId, "Ask0Traders", askTraders);
            storeVenueData(venue, instrumentId, "Bid0Traders", bidTraders);
            
            // Update monitoring counters
            updateCounter.incrementAndGet();
            lastUpdateTimestamp.set(System.currentTimeMillis());
            consecutiveErrorCount.set(0); // Reset error count on successful update
            
            if (LOGGER.isLoggable(Level.FINE)) {
                ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                    "Processed trader update for venue " + venue + ", instrument " + instrumentId + 
                    ": askTraders=" + askTraders + ", bidTraders=" + bidTraders);
            }
        } catch (Exception e) {
            consecutiveErrorCount.incrementAndGet();
            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                "Error processing trader update: " + e.getMessage() + 
                ", consecutiveErrors=" + consecutiveErrorCount.get(), e);
        }
    }
    
    /**
     * Extract the venue name from a record name
     * 
     * @param recordName The full record name
     * @return The venue name or null if not found
     */
    private String extractVenue(String recordName) {
        for (String venue : VENUES) {
            if (recordName.contains("." + venue + ".")) {
                return venue;
            }
        }
        return null;
    }
    
    /**
     * Store venue data in the internal data structure
     * 
     * @param venue The venue name
     * @param instrumentId The instrument ID
     * @param fieldName The field name
     * @param value The field value
     */
    private void storeVenueData(String venue, String instrumentId, String fieldName, String value) {
        // Get the map for this venue
        Map<String, Map<String, String>> venueMap = venueData.get(venue);
        
        // If we don't have data for this venue, log an error and return
        if (venueMap == null) {
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                "No data map found for venue: " + venue);
            return;
        }
        
        // Get or create the map for this instrument
        Map<String, String> instrumentMap = venueMap.computeIfAbsent(
            instrumentId, k -> new ConcurrentHashMap<>()
        );
        
        // Store the value, or remove it if null
        if (value != null) {
            instrumentMap.put(fieldName, value);
        } else {
            instrumentMap.remove(fieldName);
        }
    }
    
    /**
     * Retrieve trader information from a specific venue, instrument, and field
     * with improved error handling and verbose logging
     * 
     * @param venue The venue name (BTEC_REPO_US, FENICS_USREPO, or DEALERWEB_REPO)
     * @param instrumentId The instrument ID
     * @param fieldName The field name (Ask0Traders or Bid0Traders)
     * @return The trader information or null if not found
     */
    public String getTraderInfo(String venue, String instrumentId, String fieldName) {
        // Validate inputs
        if (venue == null || instrumentId == null || fieldName == null) {
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                "Invalid parameters in getTraderInfo: venue=" + venue + 
                ", instrumentId=" + instrumentId + ", fieldName=" + fieldName);
            return null;
        }
        
        try {
            // Log that we're attempting to get trader info
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Attempting to get trader info for venue=" + venue + 
                ", instrumentId=" + instrumentId + ", fieldName=" + fieldName);
                
            // Check if we've subscribed to this venue
            if (!isVenueSubscribed(venue)) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "Venue " + venue + " is not subscribed, attempting to resubscribe");
                subscribeToVenue(venue);
                return null;
            }
            
            // Check if we have data for this venue
            Map<String, Map<String, String>> venueMap = venueData.get(venue);
            if (venueMap == null) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "No data found for venue: " + venue);
                return null;
            }
            
            // Check if we have data for this instrument
            Map<String, String> instrumentMap = venueMap.get(instrumentId);
            if (instrumentMap == null) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "No data found for instrument: " + instrumentId + " in venue: " + venue);
                return null;
            }
            
            // Return the field value
            String value = instrumentMap.get(fieldName);
            
            if (value == null) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "No " + fieldName + " data found for instrument: " + instrumentId + 
                    " in venue: " + venue);
            } else {
                ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                    "Found trader info for venue " + venue + ", instrument " + instrumentId + 
                    ", field " + fieldName + ": " + value);
            }
            
            return value;
        } catch (Exception e) {
            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                "Error retrieving trader info: " + e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Get all instrument IDs available for a specific venue
     * 
     * @param venue The venue name
     * @return Set of instrument IDs or empty set if none found
     */
    public Set<String> getInstrumentsForVenue(String venue) {
        Map<String, Map<String, String>> venueMap = venueData.get(venue);
        if (venueMap == null) {
            return new HashSet<>();
        }
        return new HashSet<>(venueMap.keySet());
    }
    
    /**
     * Get subscription status for a venue
     * 
     * @param venue The venue name
     * @return true if subscribed, false otherwise
     */
    public boolean isVenueSubscribed(String venue) {
        Boolean status = venueSubscribed.get(venue);
        return status != null && status;
    }
    
    /**
     * Get total number of instruments with trader data across all venues
     */
    public int getTotalInstrumentCount() {
        int count = 0;
        for (String venue : VENUES) {
            Map<String, Map<String, String>> venueMap = venueData.get(venue);
            if (venueMap != null) {
                count += venueMap.size();
            }
        }
        return count;
    }
    
    /**
     * Start the monitoring thread for health checking
     */
    private void startMonitoring() {
        monitor.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                long lastUpdate = lastUpdateTimestamp.get();
                long updateCount = updateCounter.get();
                long silenceTime = lastUpdate > 0 ? now - lastUpdate : -1;
                int instrumentCount = getTotalInstrumentCount();
                
                // Log basic statistics
                if (lastUpdate == 0) {
                    // No updates received yet
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                        "VenueTraderListener heartbeat: No updates received yet");
                } else {
                    // Normal operation
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                        "VenueTraderListener heartbeat: Last update " + (silenceTime/1000) + 
                        " seconds ago. Update rate: " + 
                        String.format("%.2f", updateCount / ((now - startTime) / 1000.0)) + 
                        " updates/sec. Total updates: " + updateCount);
                }
                
                // Log subscription status for each venue
                for (String venue : VENUES) {
                    boolean subscribed = isVenueSubscribed(venue);
                    int venueInstrumentCount = venueData.get(venue).size();
                    
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                        "Venue " + venue + ": subscribed=" + subscribed + 
                        ", instruments=" + venueInstrumentCount);
                }
                
                // Check for excessive errors
                if (consecutiveErrorCount.get() > 5) {
                    ApplicationLogging.logAsync(LOGGER, Level.SEVERE,
                        "High number of consecutive errors: " + consecutiveErrorCount.get() +
                        ". Attempting to resubscribe to all venues.");
                        
                    // Reset error count
                    consecutiveErrorCount.set(0);
                        
                    // Attempt to resubscribe to all venues
                    for (String venue : VENUES) {
                        venueSubscribed.put(venue, false);
                        subscribeToVenue(venue);
                    }
                }
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                    "Error in monitoring thread: " + e.getMessage(), e);
            }
        }, 10, 10, TimeUnit.SECONDS); // Run more frequently - every 10 seconds
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Shutting down VenueTraderListener monitor");
            monitor.shutdown();
            try {
                if (!monitor.awaitTermination(5, TimeUnit.SECONDS)) {
                    monitor.shutdownNow();
                }
            } catch (InterruptedException e) {
                monitor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }));
    }
    
    /**
     * Method to get detailed health information
     */
    public Map<String, Object> getHealthStatus() {
        long now = System.currentTimeMillis();
        Map<String, Object> status = new HashMap<>();
        status.put("totalUpdates", updateCounter.get());
        status.put("totalInstruments", getTotalInstrumentCount());
        status.put("lastUpdateTimestamp", lastUpdateTimestamp.get());
        status.put("timeSinceLastUpdateMs", lastUpdateTimestamp.get() > 0 ? now - lastUpdateTimestamp.get() : -1);
        status.put("consecutiveErrors", consecutiveErrorCount.get());
        
        // Add venue-specific information
        Map<String, Object> venueStatus = new HashMap<>();
        for (String venue : VENUES) {
            Map<String, Object> stats = new HashMap<>();
            stats.put("subscribed", isVenueSubscribed(venue));
            stats.put("instrumentCount", venueData.get(venue).size());
            venueStatus.put(venue, stats);
        }
        status.put("venueStatus", venueStatus);
        return status;
    }
}
