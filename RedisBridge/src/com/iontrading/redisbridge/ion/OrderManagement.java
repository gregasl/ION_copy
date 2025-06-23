package com.iontrading.redisbridge.ion;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages orders.
 * This class is responsible for managing orders.
 */
public class OrderManagement {
    
    // Singleton instance
    private static OrderManagement instance;
    
    // Map of order ID to order
    private final Map<String, MarketOrder> orders;
    
    // Map of client order ID to order ID
    private final Map<String, String> clientOrderIdToOrderId;
    
    // Map of trader ID to logged in flag
    private final Map<String, Boolean> traderLoggedIn;
    
    /**
     * Gets the singleton instance of OrderManagement.
     * 
     * @return The singleton instance
     */
    public static synchronized OrderManagement getInstance() {
        if (instance == null) {
            instance = new OrderManagement();
        }
        return instance;
    }
    
    /**
     * Private constructor to enforce singleton pattern.
     */
    private OrderManagement() {
        this.orders = new ConcurrentHashMap<>();
        this.clientOrderIdToOrderId = new ConcurrentHashMap<>();
        this.traderLoggedIn = new ConcurrentHashMap<>();
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
        
        traderLoggedIn.put(traderId, true);
        return true;
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
        
        traderLoggedIn.put(traderId, false);
        return true;
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
        
        Boolean loggedIn = traderLoggedIn.get(traderId);
        return loggedIn != null && loggedIn;
    }
    
    /**
     * Submits an order.
     * 
     * @param order The order
     * @return The order ID, or null if the order could not be submitted
     */
    public String submitOrder(MarketOrder order) {
        if (order == null) {
            return null;
        }
        
        // Generate order ID
        String orderId = UUID.randomUUID().toString();
        order.setOrderId(orderId);
        
        // Set creation time
        order.setCreationTime(System.currentTimeMillis());
        
        // Set update time
        order.setUpdateTime(order.getCreationTime());
        
        // Set status
        order.setStatus(MarketDef.ORDER_STATUS_NEW);
        
        // Add order
        orders.put(orderId, order);
        
        // Add client order ID mapping
        if (order.getClientOrderId() != null) {
            clientOrderIdToOrderId.put(order.getClientOrderId(), orderId);
        }
        
        return orderId;
    }
    
    /**
     * Cancels an order.
     * 
     * @param orderId The order ID
     * @return true if the order was canceled, false otherwise
     */
    public boolean cancelOrder(String orderId) {
        if (orderId == null) {
            return false;
        }
        
        MarketOrder order = orders.get(orderId);
        
        if (order == null) {
            return false;
        }
        
        // Check if order is active
        if (!order.isActive()) {
            return false;
        }
        
        // Set status
        order.setStatus(MarketDef.ORDER_STATUS_CANCELED);
        
        // Set update time
        order.setUpdateTime(System.currentTimeMillis());
        
        return true;
    }
    
    /**
     * Gets an order.
     * 
     * @param orderId The order ID
     * @return The order, or null if not found
     */
    public MarketOrder getOrder(String orderId) {
        if (orderId == null) {
            return null;
        }
        
        return orders.get(orderId);
    }
    
    /**
     * Gets all orders.
     * 
     * @return The orders
     */
    public Map<String, MarketOrder> getOrders() {
        return new HashMap<>(orders);
    }
    
    /**
     * Gets an order by client order ID.
     * 
     * @param clientOrderId The client order ID
     * @return The order, or null if not found
     */
    public MarketOrder getOrderByClientOrderId(String clientOrderId) {
        if (clientOrderId == null) {
            return null;
        }
        
        String orderId = clientOrderIdToOrderId.get(clientOrderId);
        
        if (orderId == null) {
            return null;
        }
        
        return getOrder(orderId);
    }
    
    /**
     * Clears all orders.
     */
    public void clearOrders() {
        orders.clear();
        clientOrderIdToOrderId.clear();
    }
    
    /**
     * Clears all traders.
     */
    public void clearTraders() {
        traderLoggedIn.clear();
    }
    
    /**
     * Clears all data.
     */
    public void clearAll() {
        clearOrders();
        clearTraders();
    }
}
