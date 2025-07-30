package com.iontrading.redisbridge.ion;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Listens for depth updates.
 * This class is responsible for listening for depth updates.
 */
public class DepthListener {
    
    // Singleton instance
    private static DepthListener instance;
    
    // Map of venue to instrument ID to venue-specific ID
    private final Map<String, Map<String, String>> venueToInstrumentToVenueSpecific;
    
    // Map of venue to venue-specific ID to instrument ID
    private final Map<String, Map<String, String>> venueToVenueSpecificToInstrument;
    
    // Map of instrument ID to best
    private final Map<String, Best> instrumentToBest;
    
    // Queue of updates
    private final ConcurrentLinkedQueue<QueuedUpdate> updateQueue;
    
    // Update thread
    private final Thread updateThread;
    
    // Running flag
    private volatile boolean running;
    
    /**
     * Gets the singleton instance of DepthListener.
     * 
     * @return The singleton instance
     */
    public static synchronized DepthListener getInstance() {
        if (instance == null) {
            instance = new DepthListener();
        }
        return instance;
    }
    
    /**
     * Private constructor to enforce singleton pattern.
     */
    private DepthListener() {
        this.venueToInstrumentToVenueSpecific = new ConcurrentHashMap<>();
        this.venueToVenueSpecificToInstrument = new ConcurrentHashMap<>();
        this.instrumentToBest = new ConcurrentHashMap<>();
        this.updateQueue = new ConcurrentLinkedQueue<>();
        this.running = true;
        
        // Start update thread
        this.updateThread = new Thread(this::processUpdates);
        this.updateThread.setDaemon(true);
        this.updateThread.start();
    }
    
    /**
     * Adds a venue-specific ID mapping.
     * 
     * @param venue The venue
     * @param instrumentId The instrument ID
     * @param venueSpecificId The venue-specific ID
     */
    public void addVenueSpecificId(String venue, String instrumentId, String venueSpecificId) {
        if (venue == null || instrumentId == null || venueSpecificId == null) {
            return;
        }
        
        // Add venue to instrument to venue-specific mapping
        Map<String, String> instrumentToVenueSpecific = venueToInstrumentToVenueSpecific.computeIfAbsent(venue, k -> new ConcurrentHashMap<>());
        instrumentToVenueSpecific.put(instrumentId, venueSpecificId);
        
        // Add venue to venue-specific to instrument mapping
        Map<String, String> venueSpecificToInstrument = venueToVenueSpecificToInstrument.computeIfAbsent(venue, k -> new ConcurrentHashMap<>());
        venueSpecificToInstrument.put(venueSpecificId, instrumentId);
    }
    
    /**
     * Removes a venue-specific ID mapping.
     * 
     * @param venue The venue
     * @param instrumentId The instrument ID
     */
    public void removeVenueSpecificId(String venue, String instrumentId) {
        if (venue == null || instrumentId == null) {
            return;
        }
        
        // Remove venue to instrument to venue-specific mapping
        Map<String, String> instrumentToVenueSpecific = venueToInstrumentToVenueSpecific.get(venue);
        if (instrumentToVenueSpecific == null) {
            return;
        }
        
        String venueSpecificId = instrumentToVenueSpecific.remove(instrumentId);
        
        if (venueSpecificId == null) {
            return;
        }
        
        // Remove venue to venue-specific to instrument mapping
        Map<String, String> venueSpecificToInstrument = venueToVenueSpecificToInstrument.get(venue);
        if (venueSpecificToInstrument != null) {
            venueSpecificToInstrument.remove(venueSpecificId);
        }
    }
    
    /**
     * Gets the venue-specific ID for an instrument ID and venue.
     * 
     * @param venue The venue
     * @param instrumentId The instrument ID
     * @return The venue-specific ID, or null if not found
     */
    public String getVenueSpecificId(String venue, String instrumentId) {
        if (venue == null || instrumentId == null) {
            return null;
        }
        
        Map<String, String> instrumentToVenueSpecific = venueToInstrumentToVenueSpecific.get(venue);
        
        if (instrumentToVenueSpecific == null) {
            return null;
        }
        
        return instrumentToVenueSpecific.get(instrumentId);
    }
    
    /**
     * Gets the instrument ID for a venue-specific ID and venue.
     * 
     * @param venue The venue
     * @param venueSpecificId The venue-specific ID
     * @return The instrument ID, or null if not found
     */
    public String getInstrumentId(String venue, String venueSpecificId) {
        if (venue == null || venueSpecificId == null) {
            return null;
        }
        
        Map<String, String> venueSpecificToInstrument = venueToVenueSpecificToInstrument.get(venue);
        
        if (venueSpecificToInstrument == null) {
            return null;
        }
        
        return venueSpecificToInstrument.get(venueSpecificId);
    }
    
    /**
     * Gets the best for an instrument ID.
     * 
     * @param instrumentId The instrument ID
     * @return The best, or null if not found
     */
    public Best getBest(String instrumentId) {
        if (instrumentId == null) {
            return null;
        }
        
        return instrumentToBest.get(instrumentId);
    }
    
    /**
     * Updates the best for an instrument ID.
     * 
     * @param instrumentId The instrument ID
     * @param bidPrice The bid price
     * @param bidSize The bid size
     * @param askPrice The ask price
     * @param askSize The ask size
     * @param flags The flags
     */
    public void updateBest(String instrumentId, double bidPrice, double bidSize, double askPrice, double askSize, int flags) {
        if (instrumentId == null) {
            return;
        }
        
        // Queue update
        updateQueue.add(new QueuedUpdate(instrumentId, bidPrice, bidSize, askPrice, askSize, flags));
    }
    
    /**
     * Processes updates.
     */
    private void processUpdates() {
        while (running) {
            try {
                // Process updates
                QueuedUpdate update = updateQueue.poll();
                
                if (update == null) {
                    // No updates, sleep
                    Thread.sleep(10);
                    continue;
                }
                
                // Get best
                Best best = instrumentToBest.computeIfAbsent(update.instrumentId, k -> new Best());
                
                // Update best
                best.setBidPrice(update.bidPrice);
                best.setBidSize(update.bidSize);
                best.setAskPrice(update.askPrice);
                best.setAskSize(update.askSize);
                best.setFlags(update.flags);
                best.setTimestamp(System.currentTimeMillis());
            } catch (InterruptedException e) {
                // Interrupted, exit
                break;
            } catch (Exception e) {
                // Error, log and continue
                System.err.println("Error processing depth update: " + e.getMessage());
            }
        }
    }
    
    /**
     * Stops the depth listener.
     */
    public void stop() {
        running = false;
        updateThread.interrupt();
    }
    
    /**
     * Clears all mappings.
     */
    public void clearMappings() {
        venueToInstrumentToVenueSpecific.clear();
        venueToVenueSpecificToInstrument.clear();
        instrumentToBest.clear();
        updateQueue.clear();
    }
    
    /**
     * Queued update.
     */
    private static class QueuedUpdate {
        private final String instrumentId;
        private final double bidPrice;
        private final double bidSize;
        private final double askPrice;
        private final double askSize;
        private final int flags;
        
        /**
         * Creates a new QueuedUpdate.
         * 
         * @param instrumentId The instrument ID
         * @param bidPrice The bid price
         * @param bidSize The bid size
         * @param askPrice The ask price
         * @param askSize The ask size
         * @param flags The flags
         */
        public QueuedUpdate(String instrumentId, double bidPrice, double bidSize, double askPrice, double askSize, int flags) {
            this.instrumentId = instrumentId;
            this.bidPrice = bidPrice;
            this.bidSize = bidSize;
            this.askPrice = askPrice;
            this.askSize = askSize;
            this.flags = flags;
        }
    }
}
