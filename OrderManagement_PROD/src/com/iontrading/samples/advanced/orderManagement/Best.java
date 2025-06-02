/*
 * Best
 *
 * Java bean representing the best for an instrument. 
 *
 */

package com.iontrading.samples.advanced.orderManagement;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Best is a data container class representing the best bid and ask prices
 * for a specific instrument.
 * 
 * This is a simple Java bean with getters and setters to hold the
 * current market best prices and sizes.
 */
public class Best {
    
    private static final Logger LOGGER = Logger.getLogger(Best.class.getName());
    private static final String FENICS_USREPO = "FENICS_USREPO";
    private static final String BGC = "BGC";

    /**
     * The instrument ID this Best object represents.
     * This is immutable once set in the constructor.
     */
    private final String instrumentId;
    
    /**
     * The CUSIP
     */
    private String Id;
    
    /**
     * The best ask (sell) price in the market.
     */
    private double ask;

    /**
     * The best ask source in the market.
     */
    private String askSrc;
    
    private Boolean askIsAON;
    
    private String askSrcCheck;

    /**
     * The quantity available at the best ask price.
     */
    private double askSize;
        
    /**
     * The min quantity available at the best ask price.
     */
    private double askSizeMin;
    
    /**
     * The best bid (buy) price in the market.
     */
    private double bid;
    
    /**
     * The best bid source in the market.
     */
    private String bidSrc;    
    
    private Boolean bidIsAON;
    
    private String bidSrcCheck;
    
    /**
     * The quantity available at the best bid price.
     */
    private double bidSize;

    /**
     * The min quantity available at the best bid price.
     */
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
        if (LOGGER.isLoggable(Level.INFO)) {
            ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                "Created new Best instance for instrument: " + instrumentId);
        }
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
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting ID for instrument " + instrumentId + ": " + Id);
        }
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
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting askSrc for instrument " + instrumentId + ": " + askSrc);
        }
        this.askSrc = askSrc;
    }
    
    /**
     * @return The best ask source in the market
     */
    public String getAskSrc() {
        if (askSrc == null) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "getAskSrc called with null askSrc for instrument: " + instrumentId);
            }
            return null;
        }
        
        if (askSrc.equals(FENICS_USREPO)) {
            if (LOGGER.isLoggable(Level.FINE)) {
                ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                    "FENICS_USREPO source detected for ask, returning askSrcCheck: " + askSrcCheck + 
                    " for instrument: " + instrumentId);
            }
            return askSrcCheck;
        } else {
            return askSrc;
        }
    }

    /**
     * Sets whether the ask is All-or-None based on attribute value
     * 
     * @param AskAttribute0 The attribute value that determines AON status
     */
    public void setAskIsAON(String AskAttribute0) {
        boolean isAon = AskAttribute0 != null && AskAttribute0.endsWith("AON");
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting askIsAON for instrument " + instrumentId + " to " + isAon + 
                " based on attribute: " + AskAttribute0);
        }
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
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting askSrcCheck for instrument " + instrumentId + 
                " based on attribute: " + AskAttribute0);
        }
        
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
        
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Set askSrcCheck for instrument " + instrumentId + " to: " + this.askSrcCheck);
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
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting ask price for instrument " + instrumentId + ": " + 
                roundedAsk + " (original: " + ask + ")");
        }
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
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting askSize for instrument " + instrumentId + ": " + askSize);
        }
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
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting askSizeMin for instrument " + instrumentId + ": " + askSizeMin);
        }
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
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting bid price for instrument " + instrumentId + ": " + 
                roundedBid + " (original: " + bid + ")");
        }
        this.bid = roundedBid;
    }

    /**
     * Sets whether the bid is All-or-None based on attribute value
     * 
     * @param BidAttribute0 The attribute value that determines AON status
     */
    public void setBidIsAON(String BidAttribute0) {
        boolean isAon = BidAttribute0 != null && BidAttribute0.endsWith("AON");
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting bidIsAON for instrument " + instrumentId + " to " + isAon + 
                " based on attribute: " + BidAttribute0);
        }
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
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting bidSrcCheck for instrument " + instrumentId + 
                " based on attribute: " + BidAttribute0);
        }
        
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
        
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Set bidSrcCheck for instrument " + instrumentId + " to: " + this.bidSrcCheck);
        }
    }
    
    /**
     * @return The bid source check value
     */
    public String getBidSrcCheck() {
        return bidSrcCheck;
    }
    
    /**
     * Sets the best bid source in the market.
     * 
     * @param bidSrc The new bid source
     */
    public void setBidSrc(String bidSrc) {
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting bidSrc for instrument " + instrumentId + ": " + bidSrc);
        }
        this.bidSrc = bidSrc;
    }
    
    /**
     * @return The best bid source in the market
     */
    public String getBidSrc() {
        if (bidSrc == null) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                    "getBidSrc called with null bidSrc for instrument: " + instrumentId);
            }
            return null;
        }
        
        if (bidSrc.equals(FENICS_USREPO)) {
            if (LOGGER.isLoggable(Level.FINE)) {
                ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                    "FENICS_USREPO source detected for bid, returning bidSrcCheck: " + bidSrcCheck + 
                    " for instrument: " + instrumentId);
            }
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
     * 
     * @param bidSize The new bid size
     */
    public void setBidSize(double bidSize) {
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting bidSize for instrument " + instrumentId + ": " + bidSize);
        }
        this.bidSize = bidSize;
    }
    
    /**
     * Sets the min quantity available at the best bid price.
     * 
     * @param bidSizeMin The new minimum bid size
     */
    public void setBidSizeMin(double bidSizeMin) {
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting bidSizeMin for instrument " + instrumentId + ": " + bidSizeMin);
        }
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
        if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                "Setting lastTradePrice for instrument " + instrumentId + ": " + lastTradePrice);
        }
        this.lastTradePrice = lastTradePrice;
    }
    
    /**
     * @return The last trade price for this instrument
     */
    public double getLastTradePrice() {
        return lastTradePrice;
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
               
        if (LOGGER.isLoggable(Level.FINER)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINER, 
                "toString called for instrument " + instrumentId + ", returning: " + representation);
        }
        
        return representation;
    }
}