package com.iontrading.redisbridge.ion;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents an instrument.
 * This class contains information about an instrument.
 */
public class Instrument {
    
    // Instrument ID
    private String instrumentId;
    
    // Symbol
    private String symbol;
    
    // Description
    private String description;
    
    // Type
    private String type;
    
    // Venue
    private String venue;
    
    // Currency
    private String currency;
    
    // Tick size
    private double tickSize;
    
    // Lot size
    private double lotSize;
    
    // Minimum quantity
    private double minimumQuantity;
    
    // Maximum quantity
    private double maximumQuantity;
    
    // Enabled flag
    private boolean enabled;
    
    // Additional properties
    private final Map<String, Object> properties;
    
    /**
     * Creates a new Instrument.
     */
    public Instrument() {
        this.properties = new HashMap<>();
        this.type = MarketDef.INSTRUMENT_TYPE_EQUITY;
        this.venue = MarketDef.VENUE_EXCHANGE;
        this.currency = MarketDef.CURRENCY_USD;
        this.tickSize = 0.01;
        this.lotSize = 1;
        this.minimumQuantity = 1;
        this.maximumQuantity = Double.MAX_VALUE;
        this.enabled = true;
    }
    
    /**
     * Creates a new Instrument with the specified instrument ID.
     * 
     * @param instrumentId The instrument ID
     */
    public Instrument(String instrumentId) {
        this();
        this.instrumentId = instrumentId;
    }
    
    /**
     * Creates a new Instrument with the specified instrument ID and symbol.
     * 
     * @param instrumentId The instrument ID
     * @param symbol The symbol
     */
    public Instrument(String instrumentId, String symbol) {
        this(instrumentId);
        this.symbol = symbol;
    }
    
    /**
     * Gets the instrument ID.
     * 
     * @return The instrument ID
     */
    public String getInstrumentId() {
        return instrumentId;
    }
    
    /**
     * Sets the instrument ID.
     * 
     * @param instrumentId The instrument ID
     */
    public void setInstrumentId(String instrumentId) {
        this.instrumentId = instrumentId;
    }
    
    /**
     * Gets the symbol.
     * 
     * @return The symbol
     */
    public String getSymbol() {
        return symbol;
    }
    
    /**
     * Sets the symbol.
     * 
     * @param symbol The symbol
     */
    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }
    
    /**
     * Gets the description.
     * 
     * @return The description
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Sets the description.
     * 
     * @param description The description
     */
    public void setDescription(String description) {
        this.description = description;
    }
    
    /**
     * Gets the type.
     * 
     * @return The type
     */
    public String getType() {
        return type;
    }
    
    /**
     * Sets the type.
     * 
     * @param type The type
     */
    public void setType(String type) {
        this.type = type;
    }
    
    /**
     * Gets the venue.
     * 
     * @return The venue
     */
    public String getVenue() {
        return venue;
    }
    
    /**
     * Sets the venue.
     * 
     * @param venue The venue
     */
    public void setVenue(String venue) {
        this.venue = venue;
    }
    
    /**
     * Gets the currency.
     * 
     * @return The currency
     */
    public String getCurrency() {
        return currency;
    }
    
    /**
     * Sets the currency.
     * 
     * @param currency The currency
     */
    public void setCurrency(String currency) {
        this.currency = currency;
    }
    
    /**
     * Gets the tick size.
     * 
     * @return The tick size
     */
    public double getTickSize() {
        return tickSize;
    }
    
    /**
     * Sets the tick size.
     * 
     * @param tickSize The tick size
     */
    public void setTickSize(double tickSize) {
        this.tickSize = tickSize;
    }
    
    /**
     * Gets the lot size.
     * 
     * @return The lot size
     */
    public double getLotSize() {
        return lotSize;
    }
    
    /**
     * Sets the lot size.
     * 
     * @param lotSize The lot size
     */
    public void setLotSize(double lotSize) {
        this.lotSize = lotSize;
    }
    
    /**
     * Gets the minimum quantity.
     * 
     * @return The minimum quantity
     */
    public double getMinimumQuantity() {
        return minimumQuantity;
    }
    
    /**
     * Sets the minimum quantity.
     * 
     * @param minimumQuantity The minimum quantity
     */
    public void setMinimumQuantity(double minimumQuantity) {
        this.minimumQuantity = minimumQuantity;
    }
    
    /**
     * Gets the maximum quantity.
     * 
     * @return The maximum quantity
     */
    public double getMaximumQuantity() {
        return maximumQuantity;
    }
    
    /**
     * Sets the maximum quantity.
     * 
     * @param maximumQuantity The maximum quantity
     */
    public void setMaximumQuantity(double maximumQuantity) {
        this.maximumQuantity = maximumQuantity;
    }
    
    /**
     * Checks if the instrument is enabled.
     * 
     * @return true if the instrument is enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }
    
    /**
     * Sets the enabled flag.
     * 
     * @param enabled The enabled flag
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    /**
     * Gets a property.
     * 
     * @param key The property key
     * @return The property value, or null if not found
     */
    public Object getProperty(String key) {
        return properties.get(key);
    }
    
    /**
     * Sets a property.
     * 
     * @param key The property key
     * @param value The property value
     */
    public void setProperty(String key, Object value) {
        properties.put(key, value);
    }
    
    /**
     * Gets all properties.
     * 
     * @return The properties
     */
    public Map<String, Object> getProperties() {
        return new HashMap<>(properties);
    }
    
    /**
     * Converts a generic ID to a venue-specific ID.
     * 
     * @param genericId The generic ID
     * @param venue The venue
     * @param depthListener The depth listener
     * @return The venue-specific ID, or the generic ID if not found
     */
    public static String convertToVenueSpecificId(String genericId, String venue, DepthListener depthListener) {
        if (genericId == null || venue == null || depthListener == null) {
            return genericId;
        }
        
        String venueSpecificId = depthListener.getVenueSpecificId(venue, genericId);
        
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
     * @param depthListener The depth listener
     * @return The generic ID, or the venue-specific ID if not found
     */
    public static String convertToGenericId(String venueSpecificId, String venue, DepthListener depthListener) {
        if (venueSpecificId == null || venue == null || depthListener == null) {
            return venueSpecificId;
        }
        
        String genericId = depthListener.getInstrumentId(venue, venueSpecificId);
        
        if (genericId == null) {
            return venueSpecificId;
        }
        
        return genericId;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Instrument{");
        sb.append("instrumentId='").append(instrumentId).append('\'');
        sb.append(", symbol='").append(symbol).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", venue='").append(venue).append('\'');
        sb.append(", currency='").append(currency).append('\'');
        sb.append(", tickSize=").append(tickSize);
        sb.append(", lotSize=").append(lotSize);
        sb.append(", minimumQuantity=").append(minimumQuantity);
        sb.append(", maximumQuantity=").append(maximumQuantity);
        sb.append(", enabled=").append(enabled);
        sb.append('}');
        return sb.toString();
    }
}
