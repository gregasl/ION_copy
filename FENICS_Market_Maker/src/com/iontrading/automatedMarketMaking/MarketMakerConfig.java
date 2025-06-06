package com.iontrading.automatedMarketMaking;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Configuration for the MarketMaker component
 */
public class MarketMakerConfig {

    // Flag to enable automated market making
    private boolean autoEnabled = false;
    
    private LocalTime cashMarketOpenTime = LocalTime.of(7, 0); // 7:00 AM
    private LocalTime cashMarketCloseTime = LocalTime.of(23, 55); // 11:55 PM
    private LocalTime regMarketOpenTime = LocalTime.of(9, 0); // 9:00 AM
    private LocalTime regMarketCloseTime = LocalTime.of(16, 30); // 4:30 PM
    private boolean enforceMarketHours = true;

    // Interval for quote updates in seconds
    private int quoteUpdateIntervalSeconds = 30;
    private String marketSource = "FENICS_USREPO";
    private String[] targetVenues = {"BTEC_REPO_US", "DEALERWEB_REPO"};

    // Trading parameters
    private double minSize = 25.0;


    private String orderType = "Limit";
    private String timeInForce = "FAS"; // Good Till Cancel
    private boolean autoHedge = false;
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
    public MarketMakerConfig(boolean autoEnabled, int quoteUpdateIntervalSeconds) {
        this.autoEnabled = autoEnabled;
        this.quoteUpdateIntervalSeconds = quoteUpdateIntervalSeconds;
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

    public String getOrderType() { return orderType; }
    public void setOrderType(String orderType) { this.orderType = orderType; }

    public String getTimeInForce() { return timeInForce; }
    public void setTimeInForce(String timeInForce) { this.timeInForce = timeInForce; }

    public boolean isAutoHedge() { return autoHedge; }
    public void setAutoHedge(boolean autoHedge) { this.autoHedge = autoHedge; }

    public boolean isDefaultMarketMakingAllowed() { return defaultMarketMakingAllowed; }
    public void setDefaultMarketMakingAllowed(boolean defaultMarketMakingAllowed) { 
        this.defaultMarketMakingAllowed = defaultMarketMakingAllowed; 
    }

    public LocalTime getCashMarketOpenTime() {
        return cashMarketOpenTime;
    }

    public void setCashMarketOpenTime(LocalTime cashMarketOpenTime) {
        this.cashMarketOpenTime = cashMarketOpenTime;
    }

    public LocalTime getCashMarketCloseTime() {
        return cashMarketCloseTime;
    }

    public void setCashMarketCloseTime(LocalTime cashMarketCloseTime) {
        this.cashMarketCloseTime = cashMarketCloseTime;
    }

    public LocalTime getRegMarketOpenTime() {
        return regMarketOpenTime;
    }

    public void setRegMarketOpenTime(LocalTime regMarketOpenTime) {
        this.regMarketOpenTime = regMarketOpenTime;
    }

    public LocalTime getRegMarketCloseTime() {
        return regMarketCloseTime;
    }

    public void setRegMarketCloseTime(LocalTime regMarketCloseTime) {
        this.regMarketCloseTime = regMarketCloseTime;
    }

    public boolean isEnforceMarketHours() {
        return enforceMarketHours;
    }

    public void setEnforceMarketHours(boolean enforceMarketHours) {
        this.enforceMarketHours = enforceMarketHours;
    }


    public Set<String> getTargetVenuesSet() {
        return new HashSet<>(Arrays.asList(targetVenues));
    }
    
    @Override
    public String toString() {
        return "MarketMakerConfig{" +
            ", autoEnabled=" + autoEnabled +
            ", quoteUpdateIntervalSeconds=" + quoteUpdateIntervalSeconds +
            '}';
    }
}