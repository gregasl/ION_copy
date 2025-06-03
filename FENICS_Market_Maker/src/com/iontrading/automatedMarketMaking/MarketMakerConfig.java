package com.iontrading.automatedMarketMaking;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Configuration for the MarketMaker component
 */
public class MarketMakerConfig {
    // Default spread in basis points
    private int defaultSpreadBps = 5;
    
    // Default size for quotes
    private int defaultSize = 100;
    
    // Flag to enable automated market making
    private boolean autoEnabled = false;
    
    // Interval for quote updates in seconds
    private int quoteUpdateIntervalSeconds = 30;
    private String marketSource = "FENICS_USREPO";
    private String[] targetVenues = {"BTEC_REPO_US", "DEALERWEB_REPO"};

    // Trading parameters
    private double minSize = 25.0;
    private double bidAdjustment = 0.01;
    private double askAdjustment = 0.01;
    private double minSpread = 0.02;
    private double defaultSpread = 0.05;
    private double minPrice = 50.0;
    private double maxPrice = 200.0;


    private String orderType = "Limit";
    private String timeInForce = "FAS"; // Good Till Cancel
    private boolean autoHedge = false;
    private boolean cashOnly = true;
    private boolean defaultMarketMakingAllowed = true;

    /**
     * Creates a new MarketMakerConfig with default values
     */
    public MarketMakerConfig() {
        // Use default values
    }
    
    /**
     * Creates a new MarketMakerConfig with specified values
     * 
     * @param defaultSpreadBps Default spread in basis points
     * @param defaultSize Default size for quotes
     * @param autoEnabled Flag to enable automated market making
     * @param quoteUpdateIntervalSeconds Interval for quote updates in seconds
     */
    public MarketMakerConfig(int defaultSpreadBps, int defaultSize, 
                           boolean autoEnabled, int quoteUpdateIntervalSeconds) {
        this.defaultSpreadBps = defaultSpreadBps;
        this.defaultSize = defaultSize;
        this.autoEnabled = autoEnabled;
        this.quoteUpdateIntervalSeconds = quoteUpdateIntervalSeconds;
    }
    
    public int getDefaultSpreadBps() {
        return defaultSpreadBps;
    }
    
    public void setDefaultSpreadBps(int defaultSpreadBps) {
        this.defaultSpreadBps = defaultSpreadBps;
    }
    
    public int getDefaultSize() {
        return defaultSize;
    }
    
    public void setDefaultSize(int defaultSize) {
        this.defaultSize = defaultSize;
    }
    
    public boolean isAutoEnabled() {
        return autoEnabled;
    }
    
    public void setAutoEnabled(boolean autoEnabled) {
        this.autoEnabled = autoEnabled;
    }
    
    public int getQuoteUpdateIntervalSeconds() {
        return quoteUpdateIntervalSeconds;
    }
    
    public void setQuoteUpdateIntervalSeconds(int quoteUpdateIntervalSeconds) {
        this.quoteUpdateIntervalSeconds = quoteUpdateIntervalSeconds;
    }

        // Add all the missing getters and setters
    public String getMarketSource() { return marketSource; }
    public void setMarketSource(String marketSource) { this.marketSource = marketSource; }

    public String[] getTargetVenues() { return targetVenues; }
    public void setTargetVenues(String[] targetVenues) { this.targetVenues = targetVenues; }

    public double getMinSize() { return minSize; }
    public void setMinSize(double minSize) { this.minSize = minSize; }

    public double getBidAdjustment() { return bidAdjustment; }
    public void setBidAdjustment(double bidAdjustment) { this.bidAdjustment = bidAdjustment; }

    public double getAskAdjustment() { return askAdjustment; }
    public void setAskAdjustment(double askAdjustment) { this.askAdjustment = askAdjustment; }

    public double getMinSpread() { return minSpread; }
    public void setMinSpread(double minSpread) { this.minSpread = minSpread; }

    public double getDefaultSpread() { return defaultSpread; }
    public void setDefaultSpread(double defaultSpread) { this.defaultSpread = defaultSpread; }

    public double getMinPrice() { return minPrice; }
    public void setMinPrice(double minPrice) { this.minPrice = minPrice; }

    public double getMaxPrice() { return maxPrice; }
    public void setMaxPrice(double maxPrice) { this.maxPrice = maxPrice; }

    public String getOrderType() { return orderType; }
    public void setOrderType(String orderType) { this.orderType = orderType; }

    public String getTimeInForce() { return timeInForce; }
    public void setTimeInForce(String timeInForce) { this.timeInForce = timeInForce; }

    public boolean isAutoHedge() { return autoHedge; }
    public void setAutoHedge(boolean autoHedge) { this.autoHedge = autoHedge; }

    public boolean isCashOnly() { return cashOnly; }
    public void setCashOnly(boolean cashOnly) { this.cashOnly = cashOnly; }

    public boolean isDefaultMarketMakingAllowed() { return defaultMarketMakingAllowed; }
    public void setDefaultMarketMakingAllowed(boolean defaultMarketMakingAllowed) { 
        this.defaultMarketMakingAllowed = defaultMarketMakingAllowed; 
    }

public Set<String> getTargetVenuesSet() {
    return new HashSet<>(Arrays.asList(targetVenues));
}
    
    @Override
    public String toString() {
        return "MarketMakerConfig{" +
            "defaultSpreadBps=" + defaultSpreadBps +
            ", defaultSize=" + defaultSize +
            ", autoEnabled=" + autoEnabled +
            ", quoteUpdateIntervalSeconds=" + quoteUpdateIntervalSeconds +
            '}';
    }
}