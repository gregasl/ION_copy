package com.iontrading.automatedMarketMaking;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Immutable configuration for the MarketMaker component
 * Optimized for high-frequency access with pre-computed values
 */
public final class MarketMakerConfig {
    
    // Core configuration - all final for immutability
    private final boolean autoEnabled;
    private final int quoteUpdateIntervalMillis; // Pre-converted to millis
    private final String marketSource;
    private final String orderType;
    private final String timeInForce;
    private final boolean autoHedge;
    private final boolean defaultMarketMakingAllowed;
    private final double minSize;
    private final double defaultIntraMarketSpread;

    // Market hours - pre-computed for fast access
    private final int regMarketOpenMinutes;
    private final int regMarketCloseMinutes;
    private final int cashMarketOpenMinutes;
    private final int cashMarketCloseMinutes;
    private final boolean enforceMarketHours;
    
    // Target venues - pre-computed immutable set
    private final Set<String> targetVenuesSet;
    private final String[] targetVenuesArray; // For backward compatibility
    
    // Performance optimization - cache current market status
    private final boolean isDuringRegHours;
    private final boolean isDuringCashHours;
    private volatile boolean isDuringRegHoursCache;
    private volatile boolean isDuringCashHoursCache;
    private volatile long lastMarketHoursCheck = 0;
    private static final long MARKET_HOURS_CACHE_MS = 30000; // 30 seconds

    private MarketMakerConfig(Builder builder) {
        // Core settings
        this.autoEnabled = builder.autoEnabled;
        this.quoteUpdateIntervalMillis = builder.quoteUpdateIntervalSeconds * 1000;
        this.marketSource = builder.marketSource;
        this.orderType = builder.orderType;
        this.timeInForce = builder.timeInForce;
        this.autoHedge = builder.autoHedge;
        this.defaultMarketMakingAllowed = builder.defaultMarketMakingAllowed;
        this.minSize = builder.minSize;
        this.defaultIntraMarketSpread = builder.defaultIntraMarketSpread;
        this.enforceMarketHours = builder.enforceMarketHours;
        
        // Pre-compute venue collections for performance
        this.targetVenuesSet = Collections.unmodifiableSet(new HashSet<>(builder.targetVenues));
        this.targetVenuesArray = builder.targetVenues.toArray(new String[0]);
        
        // Pre-compute time values to avoid LocalTime operations in hot paths
        this.regMarketOpenMinutes = builder.regMarketOpenTime.getHour() * 60 + builder.regMarketOpenTime.getMinute();
        this.regMarketCloseMinutes = builder.regMarketCloseTime.getHour() * 60 + builder.regMarketCloseTime.getMinute();
        this.cashMarketOpenMinutes = builder.cashMarketOpenTime.getHour() * 60 + builder.cashMarketOpenTime.getMinute();
        this.cashMarketCloseMinutes = builder.cashMarketCloseTime.getHour() * 60 + builder.cashMarketCloseTime.getMinute();
        
        // Pre-compute current market status for fast access
        int currentMinutes = LocalTime.now().getHour() * 60 + LocalTime.now().getMinute();
        this.isDuringRegHours = enforceMarketHours ? 
            (currentMinutes >= regMarketOpenMinutes && currentMinutes <= regMarketCloseMinutes) : true;
        this.isDuringCashHours = enforceMarketHours ? 
            (currentMinutes >= cashMarketOpenMinutes && currentMinutes <= cashMarketCloseMinutes) : true;
        this.isDuringRegHoursCache = this.isDuringRegHours;
        this.isDuringCashHoursCache = this.isDuringCashHours;
    }
    
    // Hot path method - optimized for speed
    public boolean isMarketOpen(String termCode) {
        if (!enforceMarketHours) return true;
        return "REG".equals(termCode) ? isDuringRegHours : isDuringCashHours;
    }
    
    // Getters - all immutable, no setters
    public boolean isAutoEnabled() { return autoEnabled; }
    public int getQuoteUpdateIntervalMillis() { return quoteUpdateIntervalMillis; }
    public int getQuoteUpdateIntervalSeconds() { return quoteUpdateIntervalMillis / 1000; }
    public String getMarketSource() { return marketSource; }
    public String getOrderType() { return orderType; }
    public String getTimeInForce() { return timeInForce; }
    public boolean isAutoHedge() { return autoHedge; }
    public boolean isDefaultMarketMakingAllowed() { return defaultMarketMakingAllowed; }
    public double getMinSize() { return minSize; }
    public double getDefaultIntraMarketSpread() { return defaultIntraMarketSpread; }
    public boolean isEnforceMarketHours() { return enforceMarketHours; }
    public int getCashMarketOpenMinutes() { return cashMarketOpenMinutes; }
    public int getCashMarketCloseMinutes() { return cashMarketCloseMinutes; }
    public int getRegMarketOpenMinutes() { return regMarketOpenMinutes; }
    public int getRegMarketCloseMinutes() { return regMarketCloseMinutes; }


    public LocalTime getCashMarketOpenTime() {
        return LocalTime.of(cashMarketOpenMinutes / 60, cashMarketOpenMinutes % 60);
    }

    public LocalTime getCashMarketCloseTime() {
        return LocalTime.of(cashMarketCloseMinutes / 60, cashMarketCloseMinutes % 60);
    }

    public LocalTime getRegMarketOpenTime() {
        return LocalTime.of(regMarketOpenMinutes / 60, regMarketOpenMinutes % 60);
    }

    public LocalTime getRegMarketCloseTime() {
        return LocalTime.of(regMarketCloseMinutes / 60, regMarketCloseMinutes % 60);
    }

    // Optimized venue access - no object creation
    public Set<String> getTargetVenuesSet() { return targetVenuesSet; }
    public String[] getTargetVenues() { return targetVenuesArray.clone(); } // Defensive copy
    
    // Market hours access
    public boolean isDuringRegHours() {
        long now = System.currentTimeMillis();
        if (now - lastMarketHoursCheck > MARKET_HOURS_CACHE_MS) {
            synchronized(this) {
                if (now - lastMarketHoursCheck > MARKET_HOURS_CACHE_MS) {
                    // Recalculate market hours
                    int currentMinutes = LocalTime.now().getHour() * 60 + LocalTime.now().getMinute();
                    this.isDuringRegHoursCache = !enforceMarketHours || 
                        (currentMinutes >= regMarketOpenMinutes && currentMinutes <= regMarketCloseMinutes);
                    this.isDuringCashHoursCache = !enforceMarketHours || 
                        (currentMinutes >= cashMarketOpenMinutes && currentMinutes <= cashMarketCloseMinutes);
                    lastMarketHoursCheck = now;
                }
            }
        }
        return isDuringRegHoursCache;
    }

    public boolean isDuringCashHours() {
        isDuringRegHours(); // Trigger cache refresh if needed
        return isDuringCashHoursCache;
    }    

    // Builder pattern for construction
    public static class Builder {
        private boolean autoEnabled = false;
        private int quoteUpdateIntervalSeconds = 30;
        private String marketSource = "FENICS_USREPO";
        private String orderType = "Limit";
        private String timeInForce = "FAS";
        private boolean autoHedge = false;
        private boolean defaultMarketMakingAllowed = true;
        private double defaultIntraMarketSpread = 0.01;
        private double minSize = 25.0;
        private boolean enforceMarketHours = true;
        
        private LocalTime regMarketOpenTime = LocalTime.of(9, 0); // 9:00 AM EST
        private LocalTime regMarketCloseTime = LocalTime.of(16, 30); // 4:30 PM EST
        private LocalTime cashMarketOpenTime = LocalTime.of(7, 0); // 7:00 AM EST
        private LocalTime cashMarketCloseTime = LocalTime.of(11, 55); // 11:55 AM EST
        
        private Set<String> targetVenues = new HashSet<>(Arrays.asList("BTEC_REPO_US", "DEALERWEB_REPO"));
        
        public LocalTime getCashMarketOpenTime() {
            return cashMarketOpenTime;
        }

        public LocalTime getCashMarketCloseTime() {
            return cashMarketCloseTime;
        }

        public LocalTime getRegMarketOpenTime() {
            return regMarketOpenTime;
        }

        public LocalTime getRegMarketCloseTime() {
            return regMarketCloseTime;
        }

        public Builder setAutoEnabled(boolean autoEnabled) {
            this.autoEnabled = autoEnabled;
            return this;
        }
        
        public Builder setQuoteUpdateIntervalSeconds(int seconds) {
            this.quoteUpdateIntervalSeconds = seconds;
            return this;
        }
        
        public Builder setMarketSource(String marketSource) {
            this.marketSource = marketSource;
            return this;
        }
        
        public Builder setOrderType(String orderType) {
            this.orderType = orderType;
            return this;
        }
        
        public Builder setTimeInForce(String timeInForce) {
            this.timeInForce = timeInForce;
            return this;
        }
        
        public Builder setAutoHedge(boolean autoHedge) {
            this.autoHedge = autoHedge;
            return this;
        }
        
        public Builder setDefaultMarketMakingAllowed(boolean allowed) {
            this.defaultMarketMakingAllowed = allowed;
            return this;
        }
        
        public Builder setMinSize(double minSize) {
            this.minSize = minSize;
            return this;
        }
        
        public Builder setEnforceMarketHours(boolean enforce) {
            this.enforceMarketHours = enforce;
            return this;
        }
        
        public Builder setRegMarketHours(LocalTime open, LocalTime close) {
            this.regMarketOpenTime = open;
            this.regMarketCloseTime = close;
            return this;
        }
        
        public Builder setCashMarketHours(LocalTime open, LocalTime close) {
            this.cashMarketOpenTime = open;
            this.cashMarketCloseTime = close;
            return this;
        }
        
        public Builder setTargetVenues(String... venues) {
            this.targetVenues = new HashSet<>(Arrays.asList(venues));
            return this;
        }
        
        public Builder addTargetVenue(String venue) {
            this.targetVenues.add(venue);
            return this;
        }

        public MarketMakerConfig build() {

            if (quoteUpdateIntervalSeconds <= 0) {
                throw new IllegalArgumentException("Quote update interval must be positive");
            }
            if (minSize <= 0) {
                throw new IllegalArgumentException("Minimum size must be positive");
            }
            if (marketSource == null || marketSource.trim().isEmpty()) {
                throw new IllegalArgumentException("Market source cannot be null or empty");
            }
            if (targetVenues == null || targetVenues.isEmpty()) {
                throw new IllegalArgumentException("At least one target venue must be specified");
            }
            return new MarketMakerConfig(this);
        }
    }
    
    // Factory methods for common configurations
    public static MarketMakerConfig createDefault() {
        return new Builder().build();
    }
    
    public static MarketMakerConfig createTestConfig() {
        return new Builder()
            .setAutoEnabled(true)
            .setEnforceMarketHours(false)
            .setQuoteUpdateIntervalSeconds(5)
            .build();
    }
    
    // Removed duplicate build() method that was causing compilation errors

    @Override
    public String toString() {
        return "MarketMakerConfig{" +
            "autoEnabled=" + autoEnabled +
            ", quoteUpdateIntervalSeconds=" + getQuoteUpdateIntervalSeconds() +
            ", marketSource='" + getMarketSource() + '\'' +
            ", orderType='" + getOrderType() + '\'' +
            ", timeInForce='" + getTimeInForce() + '\'' +
            ", minSize=" + getMinSize() +
            ", enforceMarketHours=" + isEnforceMarketHours() +
            ", targetVenues=" + getTargetVenues().length + " venues" +
            '}';
    }
}