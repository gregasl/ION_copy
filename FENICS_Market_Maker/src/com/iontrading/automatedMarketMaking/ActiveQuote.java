package com.iontrading.automatedMarketMaking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ActiveQuote represents a two-sided quote for a specific instrument.
 * It tracks the bid and ask orders along with their reference sources and status.
 * This class handles both the market making logic and order status tracking.
 */
public class ActiveQuote {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveQuote.class);

    private final String Id;
    private MarketOrder bidOrder;
    private MarketOrder askOrder;
    private String bidReferenceSource;
    private String askReferenceSource;
    private double bidPrice;
    private double askPrice;
    private int bidUpdateFailureCount = 0;
    private int askUpdateFailureCount = 0;
    private long bidBackoffUntilTime = 0;
    private long askBackoffUntilTime = 0;

    // Source type flags
    private boolean isGcBasedBid = false;
    private boolean isGcBasedAsk = false;
    private boolean isMarketBasedBid = false;
    private boolean isMarketBasedAsk = false;
    
    // Order status flags
    private boolean isBidActive = false;
    private boolean isAskActive = false;
    
    // Creation timestamps
    private long bidCreationTime = 0;
    private long askCreationTime = 0;

    /**
     * Creates a new ActiveQuote for an instrument
     * 
     * @param Id The instrument identifier
     */
    public ActiveQuote(String Id) {
        this.Id = Id;
    }
    
    /**
     * @return The instrument ID this quote is for
     */
    public String getId() { 
        return Id;
    }
   
    /**
     * @return The current bid order or null if none exists
     */
    public MarketOrder getBidOrder() { 
        return bidOrder;
    }
    
    /**
     * @return The current ask order or null if none exists
     */
    public MarketOrder getAskOrder() {
        return askOrder;
    }
    
    /**
     * Updates the bid order and its associated metadata
     * 
     * @param order The new bid order
     * @param referenceSource The data source used for pricing
     * @param price The price of the order
     */
    public void setBidOrder(MarketOrder order, String referenceSource, double price) {
        this.bidOrder = order;
        this.bidReferenceSource = referenceSource;
        this.bidPrice = price;
        
        // If order is null, mark as inactive
        if (order == null) {
            this.isBidActive = false;
        } else {
            // Otherwise mark as active and set creation time
            this.isBidActive = true;
            this.bidCreationTime = System.currentTimeMillis();
        }
        
        // Update source type flags
        this.isGcBasedBid = referenceSource != null && referenceSource.startsWith("GC_");
        this.isMarketBasedBid = referenceSource != null && !isGcBasedBid && 
                            !referenceSource.equals("DEFAULT");
    }
    
    /**
     * Updates the ask order and its associated metadata
     * 
     * @param order The new ask order
     * @param referenceSource The data source used for pricing
     * @param price The price of the order
     */
    public void setAskOrder(MarketOrder order, String referenceSource, double price) {
        this.askOrder = order;
        this.askReferenceSource = referenceSource;
        this.askPrice = price;
        
        // If order is null, mark as inactive
        if (order == null) {
            this.isAskActive = false;
        } else {
            // Otherwise mark as active and set creation time
            this.isAskActive = true;
            this.askCreationTime = System.currentTimeMillis();
        }
        
        // Update source type flags
        this.isGcBasedAsk = referenceSource != null && referenceSource.startsWith("GC_");
        this.isMarketBasedAsk = referenceSource != null && !isGcBasedAsk && 
                            !referenceSource.equals("DEFAULT");
    }
    
    /**
     * Set bid activity status
     * 
     * @param isActive Whether the bid is now active
     */
    public void setBidActive(boolean isActive) {
        this.isBidActive = isActive;
        if (!isActive && LOGGER.isInfoEnabled()) {
            LOGGER.info("Bid for {} deactivated by status change", Id);
        }
    }
    
    /**
     * Set ask activity status
     * 
     * @param isActive Whether the ask is now active
     */
    public void setAskActive(boolean isActive) {
        this.isAskActive = isActive;
        if (!isActive && LOGGER.isInfoEnabled()) {
            LOGGER.info("Ask for {} deactivated by status change", Id);
        }
    }
    
    /**
     * Sets whether the bid is GC-based
     * 
     * @param isGcBased Whether the bid is based on GC data
     */
    public void setGcBasedBid(boolean isGcBased) {
        this.isGcBasedBid = isGcBased;
    }

    /**
     * Sets whether the ask is GC-based
     * 
     * @param isGcBased Whether the ask is based on GC data
     */
    public void setGcBasedAsk(boolean isGcBased) {
        this.isGcBasedAsk = isGcBased;
    }

    /**
     * Sets whether the bid is market-based
     * 
     * @param isMarketBased Whether the bid is based on market data
     */
    public void setMarketBasedBid(boolean isMarketBased) {
        this.isMarketBasedBid = isMarketBased;
    }

    /**
     * Sets whether the ask is market-based
     * 
     * @param isMarketBased Whether the ask is based on market data
     */
    public void setMarketBasedAsk(boolean isMarketBased) {
        this.isMarketBasedAsk = isMarketBased;
    }

    /**
     * @return The reference source for bid pricing
     */
    public String getBidReferenceSource() { 
        return bidReferenceSource; 
    }
    
    /**
     * @return The reference source for ask pricing
     */
    public String getAskReferenceSource() { 
        return askReferenceSource; 
    }
    
    /**
     * @return The current bid price
     */
    public double getBidPrice() { 
        return bidPrice; 
    }
    
    /**
     * @return The current ask price
     */
    public double getAskPrice() { 
        return askPrice; 
    }
    
    /**
     * @return Whether the bid price is based on GC data
     */
    public boolean isGcBasedBid() { 
        return isGcBasedBid; 
    }
    
    /**
     * @return Whether the ask price is based on GC data
     */
    public boolean isGcBasedAsk() { 
        return isGcBasedAsk; 
    }
    
    /**
     * @return Whether the bid price is based on market data
     */
    public boolean isMarketBasedBid() { 
        return isMarketBasedBid; 
    }
    
    /**
     * @return Whether the ask price is based on market data
     */
    public boolean isMarketBasedAsk() { 
        return isMarketBasedAsk; 
    }
    
    /**
     * @return The number of consecutive bid update failures
     */
    public int getBidUpdateFailureCount() { 
        return bidUpdateFailureCount; 
    }
    
    /**
     * @return The number of consecutive ask update failures
     */
    public int getAskUpdateFailureCount() { 
        return askUpdateFailureCount; 
    }
    
    /**
     * @return The timestamp until which bid updates are backed off
     */
    public long getBidBackoffUntilTime() { 
        return bidBackoffUntilTime; 
    }
    
    /**
     * @return The timestamp until which ask updates are backed off
     */
    public long getAskBackoffUntilTime() { 
        return askBackoffUntilTime; 
    }
    
    /**
     * @return Whether the bid is currently active
     */
    public boolean isBidActive() {
        return isBidActive && bidOrder != null && !bidOrder.isDead();
    }
    
    /**
     * @return Whether the ask is currently active
     */
    public boolean isAskActive() {
        return isAskActive && askOrder != null && !askOrder.isDead();
    }
    
    /**
     * @return The age of the bid order in milliseconds, or 0 if none
     */
    public long getBidAge() {
        return bidCreationTime > 0 ? System.currentTimeMillis() - bidCreationTime : 0;
    }
    
    /**
     * @return The age of the ask order in milliseconds, or 0 if none
     */
    public long getAskAge() {
        return askCreationTime > 0 ? System.currentTimeMillis() - askCreationTime : 0;
    }
    
    /**
     * Increment the bid failure counter and calculate backoff time
     */
    public void incrementBidFailure() {
        bidUpdateFailureCount++;
        // Calculate exponential backoff time (in milliseconds)
        long backoffMs = Math.min(300_000, (long)(1000 * Math.pow(2, bidUpdateFailureCount)));
        bidBackoffUntilTime = System.currentTimeMillis() + backoffMs;
        
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Bid failure count for {} incremented to {}, backing off for {}ms", 
                Id, bidUpdateFailureCount, backoffMs);
        }
    }
    
    /**
     * Increment the ask failure counter and calculate backoff time
     */
    public void incrementAskFailure() {
        askUpdateFailureCount++;
        // Calculate exponential backoff time (in milliseconds)
        long backoffMs = Math.min(300_000, (long)(1000 * Math.pow(2, askUpdateFailureCount)));
        askBackoffUntilTime = System.currentTimeMillis() + backoffMs;
        
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Ask failure count for {} incremented to {}, backing off for {}ms", 
                Id, askUpdateFailureCount, backoffMs);
        }
    }
    
    /**
     * Reset the bid failure counter and backoff time
     */
    public void resetBidFailure() {
        bidUpdateFailureCount = 0;
        bidBackoffUntilTime = 0;
        
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Bid failure count for {} reset", Id);
        }
    }
    
    /**
     * Reset the ask failure counter and backoff time
     */
    public void resetAskFailure() {
        askUpdateFailureCount = 0;
        askBackoffUntilTime = 0;
        
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Ask failure count for {} reset", Id);
        }
    }
    
    /**
     * @return Whether the bid is currently in backoff
     */
    public boolean isBidInBackoff() {
        return System.currentTimeMillis() < bidBackoffUntilTime;
    }
    
    /**
     * @return Whether the ask is currently in backoff
     */
    public boolean isAskInBackoff() {
        return System.currentTimeMillis() < askBackoffUntilTime;
    }

    /**
     * @return String representation of ActiveQuote for debugging
     */
    @Override
    public String toString() {
        return "ActiveQuote[instrument=" + Id + 
            ", bidActive=" + isBidActive() + 
            ", askActive=" + isAskActive() + 
            ", bidPrice=" + bidPrice + 
            ", askPrice=" + askPrice + 
            ", bidSource=" + bidReferenceSource + 
            ", askSource=" + askReferenceSource + "]";
    }
    
    /**
     * Creates a status summary map for monitoring and reporting
     * 
     * @return A map containing the current quote status
     */
    public java.util.Map<String, Object> getStatusSummary() {
        java.util.Map<String, Object> status = new java.util.HashMap<>();
        status.put("Id", Id);
        status.put("bidActive", isBidActive());
        status.put("askActive", isAskActive());
        status.put("bidPrice", bidPrice);
        status.put("askPrice", askPrice);
        status.put("bidSource", bidReferenceSource);
        status.put("askSource", askReferenceSource);
        status.put("bidGcBased", isGcBasedBid);
        status.put("askGcBased", isGcBasedAsk);
        status.put("bidAge", getBidAge());
        status.put("askAge", getAskAge());
        status.put("bidBackoffRemaining", Math.max(0, bidBackoffUntilTime - System.currentTimeMillis()));
        status.put("askBackoffRemaining", Math.max(0, askBackoffUntilTime - System.currentTimeMillis()));
        return status;
    }
}