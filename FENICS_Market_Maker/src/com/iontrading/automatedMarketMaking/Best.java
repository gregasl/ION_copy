/*
 * Best
 *
 * Java bean representing the best for an instrument. 
 *
 */

package com.iontrading.automatedMarketMaking;

/**
 * Best is a data container class representing the best bid and ask prices
 * for a specific instrument.
 * 
 * This is a simple Java bean with getters and setters to hold the
 * current market best prices and sizes.
 */
public class Best {

	 // Price attribute bitmask constants
    public static final int PRICE_NOT_TRADABLE = 0x0001;
    public static final int PRICE_CREDIT_UNKNOWN = 0x0002;
    public static final int PRICE_IMPLIED = 0x0004;
    public static final int PRICE_INDICATIVE = 0x0008;
    public static final int PRICE_AON = 0x0010;
    public static final int PRICE_MINE = 0x0020;  // Price belongs to the bank
    public static final int PRICE_OCO = 0x0040;
    public static final int PRICE_VOICE = 0x0080;
    public static final int PRICE_CCP = 0x0100;
    public static final int PRICE_BILATERAL = 0x0200;
    public static final int PRICE_NOT_ETRADABLE = 0x0400;

    private static final String FENICS_USREPO = MarketDef.FENICS_USREPO;
    private static final String BGC = MarketDef.BGC;

    /**
     * The instrument ID this Best object represents.
     * This is immutable once set in the constructor.
     */
    private final String instrumentId;
    
    /**
     * The CUSIP
     */
    private String Id;
    
    private double ask;

    private String askSrc;
    
    private int askStatus;
    
    private Boolean askIsAON;
    
    private String askSrcCheck;

    private double askSize;
        
    private double askSizeMin;
    
    private double bid;
    
    private int bidStatus;

    private String bidSrc;    
    
    private Boolean bidIsAON;
    
    private String bidSrcCheck;

    private double bidSize;

    private double bidSizeMin;
    
    private double lastTradePrice;
    
    /**
     * Creates a new Best instance for the specified instrument.
     * 
     * @param instrumentId The ID of the instrument this Best represents
     */
    public Best(String instrumentId) {
        this.instrumentId = instrumentId;
        // Prices and sizes are initialized to 0.0 by default
    }

    /**
     * @return The instrument ID this Best represents
     */
    public String getInstrumentId() {
        return instrumentId;
    }
    
    /**
     * @return The instrument ID this Best represents
     */
    public String getId() {
        return Id;
    }
    
    /**
     * Sets the CUSIP ID for this instrument
     * 
     * @param Id The CUSIP ID
     */
    public void setId(String Id) {

        this.Id = Id;
    }
        
    /**
     * @return The best ask (sell) price in the market
     */
    public double getAsk() {
        return ask;
    }

    /**
     * Sets the best ask source in the market.
     * 
     * @param askSrc The new ask source
     */
    public void setAskSrc(String askSrc) {

        this.askSrc = askSrc;
    }
    
    /**
     * @return The best ask source in the market
     */
    public String getAskSrc() {
        if (askSrc == null) {

            return null;
        }
        
        if (askSrc.equals(FENICS_USREPO)) {
            return askSrcCheck;
        } else {
            return askSrc;
        }
    }

    public void setAskStatus(int AskStatus) {
    	this.askStatus = AskStatus;
    }

    public int getAskStatus() {
    	return askStatus;
    }
    
    public void setAskIsAON(int askStatus) {
        boolean isAon = isAON(askStatus);

        this.askIsAON = isAon;
    }
    
    /**
     * @return Whether the ask is All-or-None
     */
    public Boolean getAskIsAON() {
        return askIsAON;
    }
    
    /**
     * Sets the ask source check value based on attribute value
     * 
     * @param AskAttribute0 The attribute value that determines the source check
     */
    public void setAskSrcCheck(String AskAttribute0) {

        if (AskAttribute0 == null) {
            this.askSrcCheck = null;
            return;
        }

        if (AskAttribute0.equals(FENICS_USREPO)) {
            this.askSrcCheck = FENICS_USREPO;
        } else if (AskAttribute0.equals(BGC)) {
            this.askSrcCheck = BGC;
        } else {
            this.askSrcCheck = null;
        }
        
    }

    /**
     * @return The ask source check value
     */
    public String getAskSrcCheck() {
        return askSrcCheck;
    }
    
    /**
     * Sets the best ask (sell) price in the market.
     * 
     * @param ask The new ask price
     */
    public void setAsk(double ask) {
        double roundedAsk = Math.round(ask * 100.0) / 100.0;
        this.ask = roundedAsk;
    }

    /**
     * @return The quantity available at the best ask price
     */
    public double getAskSize() {
        return askSize;
    }

    /**
     * Sets the quantity available at the best ask price.
     * 
     * @param askSize The new ask size
     */
    public void setAskSize(double askSize) {

        this.askSize = askSize;
    }

    /**
     * @return The minimum quantity available at the best ask price
     */
    public double getAskSizeMin() {
        return askSizeMin;
    }

    /**
     * Sets the min quantity available at the best ask price.
     * 
     * @param askSizeMin The new minimum ask size
     */
    public void setAskSizeMin(double askSizeMin) {

        this.askSizeMin = askSizeMin;
    }
    
    /**
     * @return The best bid (buy) price in the market
     */
    public double getBid() {
        return bid;
    }

    /**
     * Sets the best bid (buy) price in the market.
     * 
     * @param bid The new bid price
     */
    public void setBid(double bid) {
        double roundedBid = Math.round(bid * 100.0) / 100.0;

        this.bid = roundedBid;
    }

    public void setBidIsAON(int bidStatus) {
        boolean isAon = isAON(bidStatus);

        this.bidIsAON = isAon;
    }
    
    /**
     * @return Whether the bid is All-or-None
     */
    public Boolean getBidIsAON() {
        return bidIsAON;
    }
    
    /**
     * Sets the bid source check value based on attribute value
     * 
     * @param BidAttribute0 The attribute value that determines the source check
     */
    public void setBidSrcCheck(String BidAttribute0) {
 

        if (BidAttribute0 == null) {
            this.bidSrcCheck = null;
            return;
        }

        if (BidAttribute0.equals(FENICS_USREPO)) {
            this.bidSrcCheck = FENICS_USREPO;
        } else if (BidAttribute0.equals(BGC)) {
            this.bidSrcCheck = BGC;
        } else {
            this.bidSrcCheck = null;
        }

    }

    /**
     * @return The bid source check value
     */
    public String getBidSrcCheck() {
        return bidSrcCheck;
    }
    
    public void setBidStatus(int BidStatus) {
    	this.bidStatus = BidStatus;
    }

    public int getBidStatus() {
    	return bidStatus;
    }
    
    /**
     * Sets the best bid source in the market.
     * 
     * @param bidSrc The new bid source
     */
    public void setBidSrc(String bidSrc) {

        this.bidSrc = bidSrc;
    }
    
    /**
     * @return The best bid source in the market
     */
    public String getBidSrc() {
        if (bidSrc == null) {

            return null;
        }
        
        if (bidSrc.equals(FENICS_USREPO)) {

            return bidSrcCheck;
        } else {
            return bidSrc;
        }
    }
    
    /**
     * @return The quantity available at the best bid price
     */
    public double getBidSize() {
        return bidSize;
    }

    /**
     * Sets the quantity available at the best bid price.
     */
    public void setBidSize(double bidSize) {

        this.bidSize = bidSize;
    }
    
    /**
     * Sets the min quantity available at the best bid price.
     * 
     * @param bidSizeMin The new minimum bid size
     */
    public void setBidSizeMin(double bidSizeMin) {

        this.bidSizeMin = bidSizeMin;
    }

    /**
     * @return The min quantity available at the best bid price
     */
    public double getBidSizeMin() {
        return bidSizeMin;
    }
    
    /**
     * Sets the last trade price for this instrument
     * 
     * @param lastTradePrice The last trade price
     */
    public void setLastTradePrice(double lastTradePrice) {

        this.lastTradePrice = lastTradePrice;
    }
    
    /**
     * @return The last trade price for this instrument
     */
    public double getLastTradePrice() {
        return lastTradePrice;
    }
    
    /**
     * Check if a specific bit is set in the bitmask
     */
    private boolean isBitSet(int bitmask, int bit) {
        return (bitmask & bit) != 0;
    }

    /**
     * Check if the price belongs to the bank (Mine flag)
     */
    public boolean isMinePrice(int bitmask) {
        return isBitSet(bitmask, PRICE_MINE);
    }
    
    /**
     * Check if the price belongs to the bank (Mine flag)
     */
    public boolean isAON(int bitmask) {
        return isBitSet(bitmask, PRICE_AON);
    }

    /**
     * Returns a string representation of this Best object,
     * showing the instrument ID and prices.
     */
    @Override
    public String toString() {
        String representation = "Best{instrumentId='" + instrumentId + "', " +
               "bid=" + bid + "/" + bidSize + ", " +
               "ask=" + ask + "/" + askSize + "}";

        return representation;
    }
}