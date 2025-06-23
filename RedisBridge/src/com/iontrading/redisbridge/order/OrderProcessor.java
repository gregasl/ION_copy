package com.iontrading.redisbridge.order;

import com.iontrading.redisbridge.admin.AdminCommandHandler;
import com.iontrading.redisbridge.config.ConfigurationManager;
import com.iontrading.redisbridge.instrument.InstrumentConverter;
import com.iontrading.redisbridge.ion.MarketDef;
import com.iontrading.redisbridge.ion.MarketOrder;
import com.iontrading.redisbridge.ion.OrderManagement;
import com.iontrading.redisbridge.redis.RedisPublisher;
import com.iontrading.redisbridge.trader.TraderLoginChecker;
import com.iontrading.redisbridge.util.LoggingUtils;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Processes orders.
 * This class is responsible for processing orders.
 */
public class OrderProcessor {
    
    // Singleton instance
    private static OrderProcessor instance;
    
    // Reference to ConfigurationManager
    private final ConfigurationManager config;
    
    // Reference to RedisPublisher
    private final RedisPublisher publisher;
    
    // Reference to AdminCommandHandler
    private final AdminCommandHandler adminHandler;
    
    // Reference to InstrumentConverter
    private final InstrumentConverter instrumentConverter;
    
    // Reference to TraderLoginChecker
    private final TraderLoginChecker loginChecker;
    
    // Reference to OrderManagement
    private final OrderManagement orderManagement;
    
    // Map of client order ID to order info
    private final Map<String, OrderInfo> orderInfoByClientId;
    
    // Map of internal order ID to order info
    private final Map<String, OrderInfo> orderInfoByInternalId;
    
    /**
     * Gets the singleton instance of OrderProcessor.
     * 
     * @param publisher The Redis publisher
     * @param adminHandler The admin command handler
     * @return The singleton instance
     */
    public static synchronized OrderProcessor getInstance(RedisPublisher publisher, AdminCommandHandler adminHandler) {
        if (instance == null) {
            instance = new OrderProcessor(publisher, adminHandler);
        }
        return instance;
    }
    
    /**
     * Private constructor to enforce singleton pattern.
     * 
     * @param publisher The Redis publisher
     * @param adminHandler The admin command handler
     */
    private OrderProcessor(RedisPublisher publisher, AdminCommandHandler adminHandler) {
        this.publisher = publisher;
        this.adminHandler = adminHandler;
        this.config = ConfigurationManager.getInstance();
        this.orderInfoByClientId = new ConcurrentHashMap<>();
        this.orderInfoByInternalId = new ConcurrentHashMap<>();
        this.instrumentConverter = InstrumentConverter.getInstance();
        this.loginChecker = TraderLoginChecker.getInstance();
        this.orderManagement = OrderManagement.getInstance();
    }
    
    /**
     * Processes a new order.
     * 
     * @param orderMessage The order message
     */
    public void processNewOrder(Map<String, Object> orderMessage) {
        String clientOrderId = (String) orderMessage.get("clientOrderId");
        String traderId = (String) orderMessage.get("traderId");
        String instrumentId = (String) orderMessage.get("instrumentId");
        String verb = (String) orderMessage.get("verb");
        Double quantity = (Double) orderMessage.get("quantity");
        Double price = (Double) orderMessage.get("price");
        
        // Check if system is enabled
        if (!adminHandler.isSystemEnabled()) {
            publisher.publishOrderResponse(
                clientOrderId,
                false,
                "System is disabled",
                LoggingUtils.createDetailsMap("reason", "System is disabled")
            );
            return;
        }
        
        // Validate required fields
        if (clientOrderId == null || traderId == null || instrumentId == null || 
            verb == null || quantity == null) {
            publisher.publishOrderResponse(
                clientOrderId,
                false,
                "Missing required fields",
                LoggingUtils.createDetailsMap("reason", "Missing required fields")
            );
            return;
        }
        
        // Check if trader is logged in
        if (!loginChecker.isTraderLoggedIn(traderId)) {
            publisher.publishOrderResponse(
                clientOrderId,
                false,
                "Trader not logged in",
                LoggingUtils.createDetailsMap("reason", "Trader not logged in")
            );
            return;
        }
        
        // For now, we'll assume all instrument IDs are valid
        // In a real implementation, we would validate the instrument ID
        // using instrumentConverter
        
        
        try {
            // Create market order
            MarketOrder order = new MarketOrder();
            order.setClientOrderId(clientOrderId);
            order.setTraderId(traderId);
            order.setInstrumentId(instrumentId);
            order.setVerb(verb.equalsIgnoreCase("BUY") ? MarketOrder.BUY : MarketOrder.SELL);
            order.setQuantity(quantity);
            
            if (price != null) {
                order.setPrice(price);
                order.setOrderType(MarketDef.ORDER_TYPE_LIMIT);
            } else {
                order.setOrderType(MarketDef.ORDER_TYPE_MARKET);
            }
            
            // Submit order
            String internalOrderId = orderManagement.submitOrder(order);
            
            if (internalOrderId != null) {
                // Store order info
                OrderInfo orderInfo = new OrderInfo(clientOrderId, internalOrderId, order);
                orderInfoByClientId.put(clientOrderId, orderInfo);
                orderInfoByInternalId.put(internalOrderId, orderInfo);
                
                // Publish response
                publisher.publishOrderResponse(
                    clientOrderId,
                    true,
                    "Order submitted",
                    LoggingUtils.createDetailsMap(
                        "internalOrderId", internalOrderId,
                        "status", order.getStatus()
                    )
                );
            } else {
                // Publish error
                publisher.publishOrderResponse(
                    clientOrderId,
                    false,
                    "Order submission failed",
                    LoggingUtils.createDetailsMap("reason", "Order submission failed")
                );
            }
        } catch (Exception e) {
            // Publish error
            publisher.publishOrderResponse(
                clientOrderId,
                false,
                "Error: " + e.getMessage(),
                LoggingUtils.createDetailsMap("reason", "Error: " + e.getMessage())
            );
        }
    }
    
    /**
     * Processes a cancel order.
     * 
     * @param orderMessage The order message
     */
    public void processCancelOrder(Map<String, Object> orderMessage) {
        String clientOrderId = (String) orderMessage.get("clientOrderId");
        String originalClientOrderId = (String) orderMessage.get("originalClientOrderId");
        String traderId = (String) orderMessage.get("traderId");
        
        // Check if system is enabled
        if (!adminHandler.isSystemEnabled()) {
            publisher.publishOrderResponse(
                clientOrderId,
                false,
                "System is disabled",
                LoggingUtils.createDetailsMap("reason", "System is disabled")
            );
            return;
        }
        
        // Validate required fields
        if (clientOrderId == null || originalClientOrderId == null || traderId == null) {
            publisher.publishOrderResponse(
                clientOrderId,
                false,
                "Missing required fields",
                LoggingUtils.createDetailsMap("reason", "Missing required fields")
            );
            return;
        }
        
        // Check if trader is logged in
        if (!loginChecker.isTraderLoggedIn(traderId)) {
            publisher.publishOrderResponse(
                clientOrderId,
                false,
                "Trader not logged in",
                LoggingUtils.createDetailsMap("reason", "Trader not logged in")
            );
            return;
        }
        
        // Get original order
        OrderInfo originalOrderInfo = orderInfoByClientId.get(originalClientOrderId);
        
        if (originalOrderInfo == null) {
            publisher.publishOrderResponse(
                clientOrderId,
                false,
                "Original order not found",
                LoggingUtils.createDetailsMap("reason", "Original order not found")
            );
            return;
        }
        
        try {
            // Cancel order
            boolean success = orderManagement.cancelOrder(originalOrderInfo.getInternalOrderId());
            
            if (success) {
                // Publish response
                publisher.publishOrderResponse(
                    clientOrderId,
                    true,
                    "Order canceled",
                    LoggingUtils.createDetailsMap(
                        "originalClientOrderId", originalClientOrderId,
                        "internalOrderId", originalOrderInfo.getInternalOrderId(),
                        "status", MarketDef.ORDER_STATUS_CANCELED
                    )
                );
            } else {
                // Publish error
                publisher.publishOrderResponse(
                    clientOrderId,
                    false,
                    "Order cancellation failed",
                    LoggingUtils.createDetailsMap("reason", "Order cancellation failed")
                );
            }
        } catch (Exception e) {
            // Publish error
            publisher.publishOrderResponse(
                clientOrderId,
                false,
                "Error: " + e.getMessage(),
                LoggingUtils.createDetailsMap("reason", "Error: " + e.getMessage())
            );
        }
    }
    
    /**
     * Gets an order by client order ID.
     * 
     * @param clientOrderId The client order ID
     * @return The order info, or null if not found
     */
    public OrderInfo getOrderByClientId(String clientOrderId) {
        return orderInfoByClientId.get(clientOrderId);
    }
    
    /**
     * Gets an order by internal order ID.
     * 
     * @param internalOrderId The internal order ID
     * @return The order info, or null if not found
     */
    public OrderInfo getOrderByInternalId(String internalOrderId) {
        return orderInfoByInternalId.get(internalOrderId);
    }
    
    /**
     * Clears all orders.
     */
    public void clearOrders() {
        orderInfoByClientId.clear();
        orderInfoByInternalId.clear();
    }
    
    /**
     * Generates a unique order ID.
     * 
     * @return The order ID
     */
    public String generateOrderId() {
        return UUID.randomUUID().toString();
    }
    
    /**
     * Order info.
     * This class contains information about an order.
     */
    public static class OrderInfo {
        
        // Client order ID
        private final String clientOrderId;
        
        // Internal order ID
        private final String internalOrderId;
        
        // Market order
        private final MarketOrder order;
        
        /**
         * Creates a new OrderInfo.
         * 
         * @param clientOrderId The client order ID
         * @param internalOrderId The internal order ID
         * @param order The market order
         */
        public OrderInfo(String clientOrderId, String internalOrderId, MarketOrder order) {
            this.clientOrderId = clientOrderId;
            this.internalOrderId = internalOrderId;
            this.order = order;
        }
        
        /**
         * Gets the client order ID.
         * 
         * @return The client order ID
         */
        public String getClientOrderId() {
            return clientOrderId;
        }
        
        /**
         * Gets the internal order ID.
         * 
         * @return The internal order ID
         */
        public String getInternalOrderId() {
            return internalOrderId;
        }
        
        /**
         * Gets the market order.
         * 
         * @return The market order
         */
        public MarketOrder getOrder() {
            return order;
        }
    }
}
