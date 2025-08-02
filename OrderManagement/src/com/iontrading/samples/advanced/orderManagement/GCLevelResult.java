package com.iontrading.samples.advanced.orderManagement;

public class GCLevelResult {
    private final Double bidPrice;
    private final Double askPrice;
    
    public GCLevelResult(Double bidPrice, Double askPrice) {
        this.bidPrice = bidPrice;
        this.askPrice = askPrice;
    }
    
    public Double getBidPrice() {
        return bidPrice;
    }
    
    public Double getAskPrice() {
        return askPrice;
    }
    
    @Override
    public String toString() {
        return String.format("GCLevel{bid=%.2f, ask=%.2f}", 
            bidPrice != null ? bidPrice : 0.0,
            askPrice != null ? askPrice : 0.0);
    }
}