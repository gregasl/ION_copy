package com.iontrading.redisbridge.trader;

import com.iontrading.redisbridge.ion.OrderManagement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Checks if traders are logged in.
 * This class is responsible for checking if traders are logged in.
 */
public class TraderLoginChecker {
    
    // Singleton instance
    private static TraderLoginChecker instance;
    
    // Reference to OrderManagement
    private final OrderManagement orderManagement;
    
    // Map of trader ID to last login time
    private final Map<String, Long> traderLastLoginTime;
    
    /**
     * Gets the singleton instance of TraderLoginChecker.
     * 
     * @return The singleton instance
     */
    public static synchronized TraderLoginChecker getInstance() {
        if (instance == null) {
            instance = new TraderLoginChecker();
        }
        return instance;
    }
    
    /**
     * Private constructor to enforce singleton pattern.
     */
    private TraderLoginChecker() {
        this.orderManagement = OrderManagement.getInstance();
        this.traderLastLoginTime = new ConcurrentHashMap<>();
    }
    
    /**
     * Logs in a trader.
     * 
     * @param traderId The trader ID
     * @return true if the trader was logged in, false otherwise
     */
    public boolean loginTrader(String traderId) {
        if (traderId == null) {
            return false;
        }
        
        // Login trader
        boolean success = orderManagement.loginTrader(traderId);
        
        if (success) {
            // Update last login time
            traderLastLoginTime.put(traderId, System.currentTimeMillis());
        }
        
        return success;
    }
    
    /**
     * Logs out a trader.
     * 
     * @param traderId The trader ID
     * @return true if the trader was logged out, false otherwise
     */
    public boolean logoutTrader(String traderId) {
        if (traderId == null) {
            return false;
        }
        
        // Logout trader
        boolean success = orderManagement.logoutTrader(traderId);
        
        if (success) {
            // Remove last login time
            traderLastLoginTime.remove(traderId);
        }
        
        return success;
    }
    
    /**
     * Checks if a trader is logged in.
     * 
     * @param traderId The trader ID
     * @return true if the trader is logged in, false otherwise
     */
    public boolean isTraderLoggedIn(String traderId) {
        if (traderId == null) {
            return false;
        }
        
        return orderManagement.isTraderLoggedIn(traderId);
    }
    
    /**
     * Gets the last login time for a trader.
     * 
     * @param traderId The trader ID
     * @return The last login time, or 0 if the trader has never logged in
     */
    public long getLastLoginTime(String traderId) {
        if (traderId == null) {
            return 0;
        }
        
        Long lastLoginTime = traderLastLoginTime.get(traderId);
        
        if (lastLoginTime == null) {
            return 0;
        }
        
        return lastLoginTime;
    }
    
    /**
     * Clears all traders.
     */
    public void clearTraders() {
        orderManagement.clearTraders();
        traderLastLoginTime.clear();
    }
}
