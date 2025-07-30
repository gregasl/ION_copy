package com.iontrading.redisbridge.instrument;

import com.iontrading.redisbridge.ion.DepthListener;
import java.util.HashMap;
import java.util.Map;

/**
 * Converts instruments.
 * This class is responsible for converting instruments.
 */
public class InstrumentConverter {
    
    // Singleton instance
    private static InstrumentConverter instance;
    
    // Reference to DepthListener
    private final DepthListener depthListener;
    
    // Map of venue to instrument ID to venue-specific ID
    private final Map<String, Map<String, String>> venueToInstrumentToVenueSpecific;
    
    // Map of venue to venue-specific ID to instrument ID
    private final Map<String, Map<String, String>> venueToVenueSpecificToInstrument;
    
    /**
     * Gets the singleton instance of InstrumentConverter.
     * 
     * @return The singleton instance
     */
    public static synchronized InstrumentConverter getInstance() {
        if (instance == null) {
            instance = new InstrumentConverter();
        }
        return instance;
    }
    
    /**
     * Private constructor to enforce singleton pattern.
     */
    private InstrumentConverter() {
        this.depthListener = DepthListener.getInstance();
        this.venueToInstrumentToVenueSpecific = new HashMap<>();
        this.venueToVenueSpecificToInstrument = new HashMap<>();
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
        Map<String, String> instrumentToVenueSpecific = venueToInstrumentToVenueSpecific.computeIfAbsent(venue, k -> new HashMap<>());
        instrumentToVenueSpecific.put(instrumentId, venueSpecificId);
        
        // Add venue to venue-specific to instrument mapping
        Map<String, String> venueSpecificToInstrument = venueToVenueSpecificToInstrument.computeIfAbsent(venue, k -> new HashMap<>());
        venueSpecificToInstrument.put(venueSpecificId, instrumentId);
        
        // Add to depth listener
        depthListener.addVenueSpecificId(venue, instrumentId, venueSpecificId);
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
        
        // Remove from depth listener
        depthListener.removeVenueSpecificId(venue, instrumentId);
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
     * Converts a generic ID to a venue-specific ID.
     * 
     * @param genericId The generic ID
     * @param venue The venue
     * @return The venue-specific ID, or the generic ID if not found
     */
    public String convertToVenueSpecificId(String genericId, String venue) {
        if (genericId == null || venue == null) {
            return genericId;
        }
        
        String venueSpecificId = getVenueSpecificId(venue, genericId);
        
        if (venueSpecificId == null) {
            return genericId;
        }
        
        return venueSpecificId;
    }
    
    /**
     * Converts a venue-specific ID to a generic ID.
     * 
     * @param venueSpecificId The venue-specific ID
     * @param venue The venue
     * @return The generic ID, or the venue-specific ID if not found
     */
    public String convertToGenericId(String venueSpecificId, String venue) {
        if (venueSpecificId == null || venue == null) {
            return venueSpecificId;
        }
        
        String genericId = getInstrumentId(venue, venueSpecificId);
        
        if (genericId == null) {
            return venueSpecificId;
        }
        
        return genericId;
    }
    
    /**
     * Clears all mappings.
     */
    public void clearMappings() {
        venueToInstrumentToVenueSpecific.clear();
        venueToVenueSpecificToInstrument.clear();
        depthListener.clearMappings();
    }
    
    /**
     * Gets the depth listener.
     * 
     * @return The depth listener
     */
    public DepthListener getDepthListener() {
        return depthListener;
    }
}
