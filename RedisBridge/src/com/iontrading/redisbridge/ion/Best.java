package com.iontrading.redisbridge.ion;

/**
 * Represents the best bid and ask for an instrument.
 * This class contains information about the best bid and ask for an instrument.
 */
public class Best {
    
    // Bid price
    private double bidPrice;
    
    // Bid size
    private double bidSize;
    
    // Ask price
    private double askPrice;
    
    // Ask size
    private double askSize;
    
    // Flags
    private int flags;
    
    // Timestamp
    private long timestamp;
    
    /**
     * Creates a new Best.
     */
    public Best() {
        this.bidPrice = 0;
        this.bidSize = 0;
        this.askPrice = 0;
        this.askSize = 0;
        this.flags = 0;
        this.timestamp = System.currentTimeMillis();
    }
    
    /**
     * Creates a new Best with the specified bid and ask.
     * 
     * @param bidPrice The bid price
     * @param bidSize The bid size
     * @param askPrice The ask price
     * @param askSize The ask size
     */
    public Best(double bidPrice, double bidSize, double askPrice, double askSize) {
        this.bidPrice = bidPrice;
        this.bidSize = bidSize;
        this.askPrice = askPrice;
        this.askSize = askSize;
        this.flags = MarketDef.PRICE_TRADEABLE;
        this.timestamp = System.currentTimeMillis();
    }
    
    /**
     * Creates a new Best with the specified bid and ask and flags.
     * 
     * @param bidPrice The bid price
     * @param bidSize The bid size
     * @param askPrice The ask price
     * @param askSize The ask size
     * @param flags The flags
     */
    public Best(double bidPrice, double bidSize, double askPrice, double askSize, int flags) {
        this.bidPrice = bidPrice;
        this.bidSize = bidSize;
        this.askPrice = askPrice;
        this.askSize = askSize;
        this.flags = flags;
        this.timestamp = System.currentTimeMillis();
    }
    
    /**
     * Gets the bid price.
     * 
     * @return The bid price
     */
    public double getBidPrice() {
        return bidPrice;
    }
    
    /**
     * Sets the bid price.
     * 
     * @param bidPrice The bid price
     */
    public void setBidPrice(double bidPrice) {
        this.bidPrice = bidPrice;
    }
    
    /**
     * Gets the bid size.
     * 
     * @return The bid size
     */
    public double getBidSize() {
        return bidSize;
    }
    
    /**
     * Sets the bid size.
     * 
     * @param bidSize The bid size
     */
    public void setBidSize(double bidSize) {
        this.bidSize = bidSize;
    }
    
    /**
     * Gets the ask price.
     * 
     * @return The ask price
     */
    public double getAskPrice() {
        return askPrice;
    }
    
    /**
     * Sets the ask price.
     * 
     * @param askPrice The ask price
     */
    public void setAskPrice(double askPrice) {
        this.askPrice = askPrice;
    }
    
    /**
     * Gets the ask size.
     * 
     * @return The ask size
     */
    public double getAskSize() {
        return askSize;
    }
    
    /**
     * Sets the ask size.
     * 
     * @param askSize The ask size
     */
    public void setAskSize(double askSize) {
        this.askSize = askSize;
    }
    
    /**
     * Gets the flags.
     * 
     * @return The flags
     */
    public int getFlags() {
        return flags;
    }
    
    /**
     * Sets the flags.
     * 
     * @param flags The flags
     */
    public void setFlags(int flags) {
        this.flags = flags;
    }
    
    /**
     * Gets the timestamp.
     * 
     * @return The timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }
    
    /**
     * Sets the timestamp.
     * 
     * @param timestamp The timestamp
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    /**
     * Gets the mid price.
     * 
     * @return The mid price
     */
    public double getMidPrice() {
        if (bidPrice <= 0 || askPrice <= 0) {
            return 0;
        }
        
        return (bidPrice + askPrice) / 2;
    }
    
    /**
     * Gets the spread.
     * 
     * @return The spread
     */
    public double getSpread() {
        if (bidPrice <= 0 || askPrice <= 0) {
            return 0;
        }
        
        return askPrice - bidPrice;
    }
    
    /**
     * Gets the spread percentage.
     * 
     * @return The spread percentage
     */
    public double getSpreadPercentage() {
        if (bidPrice <= 0 || askPrice <= 0) {
            return 0;
        }
        
        return (askPrice - bidPrice) / getMidPrice() * 100;
    }
    
    /**
     * Checks if the best is tradeable.
     * 
     * @return true if the best is tradeable, false otherwise
     */
    public boolean isTradeable() {
        return (flags & MarketDef.PRICE_TRADEABLE) != 0;
    }
    
    /**
     * Checks if the best is indicative.
     * 
     * @return true if the best is indicative, false otherwise
     */
    public boolean isIndicative() {
        return (flags & MarketDef.PRICE_INDICATIVE) != 0;
    }
    
    /**
     * Checks if the best is from a market maker.
     * 
     * @return true if the best is from a market maker, false otherwise
     */
    public boolean isMarketMaker() {
        return (flags & MarketDef.PRICE_MARKET_MAKER) != 0;
    }
    
    /**
     * Checks if the best is from an exchange.
     * 
     * @return true if the best is from an exchange, false otherwise
     */
    public boolean isExchange() {
        return (flags & MarketDef.PRICE_EXCHANGE) != 0;
    }
    
    /**
     * Checks if the best is firm.
     * 
     * @return true if the best is firm, false otherwise
     */
    public boolean isFirm() {
        return (flags & MarketDef.PRICE_FIRM) != 0;
    }
    
    /**
     * Checks if the best is executable.
     * 
     * @return true if the best is executable, false otherwise
     */
    public boolean isExecutable() {
        return (flags & MarketDef.PRICE_EXECUTABLE) != 0;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Best{");
        sb.append("bidPrice=").append(bidPrice);
        sb.append(", bidSize=").append(bidSize);
        sb.append(", askPrice=").append(askPrice);
        sb.append(", askSize=").append(askSize);
        sb.append(", flags=").append(flags);
        sb.append(", timestamp=").append(timestamp);
        sb.append('}');
        return sb.toString();
    }
}
