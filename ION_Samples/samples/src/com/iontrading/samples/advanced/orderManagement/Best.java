/*
 * Best
 *
 * Java bean representing the best for an instrument. 
 *
 * ION Trading U.K. Limited supplies this software code is for testing purposes
 * only. The scope of this software code is exclusively limited to the
 * demonstration of the structure of an application using the ION(tm) Common
 * Market and ION Trading U.K. Limited does not guarantee the correct behavior
 * of any deployed application using this software code.
 * This software code has not been thoroughly tested under all conditions.
 * ION, therefore, cannot guarantee or imply reliability, serviceability, or
 * function of this software.
 * Any use of this software outside of this defined scope is the sole
 * responsibility of the user.
 *
 * ION Trading ltd (2005)
 */

package com.iontrading.samples.advanced.orderManagement;

public class Best {
    
    private final String instrumentId;
    
    private double ask;
    private double askSize;
    private double bid;
    private double bidSize;
    
    /** Creates a new instance of Depth */
    public Best(String instrumentId) {
    	 this.instrumentId = instrumentId;
    }

    public String getInstrumentId() {
        return instrumentId;
    }

    public double getAsk() {
        return ask;
    }

    public void setAsk(double ask) {
        this.ask = ask;
    }

    public double getAskSize() {
        return askSize;
    }

    public void setAskSize(double askSize) {
        this.askSize = askSize;
    }

    public double getBid() {
        return bid;
    }

    public void setBid(double bid) {
        this.bid = bid;
    }

    public double getBidSize() {
        return bidSize;
    }

    public void setBidSize(double bidSize) {
        this.bidSize = bidSize;
    }
    
}
