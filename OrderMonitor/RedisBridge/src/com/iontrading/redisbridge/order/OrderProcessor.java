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
import com.iontrading.redisbridge.util.JsonUtils;
import java.util.Map;
import java.util.HashMap;
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
    
    // 交易场所到交易员的映射
    private final Map<String, String> venueToTraderMap = new HashMap<>();
    
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
        
        // 初始化交易场所到交易员的映射
        initializeVenueToTraderMap();
    }
    
    /**
     * 初始化交易场所到交易员的映射
     */
    private void initializeVenueToTraderMap() {
        venueToTraderMap.put("BTEC_REPO_US", "TEST2");
        venueToTraderMap.put("DEALERWEB_REPO", "asldevtrd1");
        venueToTraderMap.put("FENICS_USREPO", "frosasl1");
    }
    
    /**
     * 生成专业的 requestId
     */
    private String generateRequestId() {
        return "Py" + System.currentTimeMillis();
    }
    
    /**
     * 根据市场源获取交易员ID
     */
    private String getTraderForMarketSource(String marketSource) {
        String trader = venueToTraderMap.get(marketSource);
        if (trader == null) {
            return null;
        }
        return trader;
    }
    
    /**
     * Processes a new order.
     * 支持您的消息格式：CommandType, marketSource, instrumentId, side, quantity, price
     * 
     * @param orderMessage The order message
     */
    public void processNewOrder(Map<String, Object> orderMessage) {
        String requestId = generateRequestId();
        
        String commandType = (String) orderMessage.get("CommandType");
        String marketSource = (String) orderMessage.get("marketSource");
        String instrumentId = (String) orderMessage.get("instrumentId");
        String side = (String) orderMessage.get("side");
        
        // 数值类型安全转换
        Number qtyNum = (Number) orderMessage.get("quantity");
        Number priceNum = (Number) orderMessage.get("price");
        Double quantity = qtyNum != null ? qtyNum.doubleValue() : null;
        Double price = priceNum != null ? priceNum.doubleValue() : null;
        
        // 自动确定交易员
        String traderId = (String) orderMessage.get("traderId");
        if (traderId == null || traderId.isEmpty()) {
            traderId = getTraderForMarketSource(marketSource);
            if (traderId == null) {
                publishErrorResponse(requestId, "No trader configured for market source: " + marketSource);
                return;
            }
        }
        
        // 自动确定订单类型
        String orderType = (String) orderMessage.get("orderType");
        if (orderType == null) {
            orderType = (price != null) ? "Limit" : "Market";
            System.out.println("Auto-determined order type: " + orderType);
        }
        
        // 自动设置 Time In Force
        String timeInForce = (String) orderMessage.get("timeInForce");
        if (timeInForce == null) {
            timeInForce = "Day";
            System.out.println("Auto-set time in force: " + timeInForce);
        }
        
        // 验证系统状态
        if (!adminHandler.isSystemEnabled()) {
            publishErrorResponse(requestId, "System is disabled");
            return;
        }
        
        // 验证必需字段
        if (marketSource == null || instrumentId == null || side == null || quantity == null) {
            publishErrorResponse(requestId, "Missing required fields: marketSource, instrumentId, side, quantity");
            return;
        }
        
        try {
            // 创建 MarketOrder 对象
            MarketOrder order = new MarketOrder();
            order.setClientOrderId(requestId);
            order.setTraderId(traderId);
            order.setInstrumentId(instrumentId);
            
            // 设置买卖方向 - 注意 OrderManagement 期望 "Buy"/"Sell" 首字母大写
            String verb = side.substring(0, 1).toUpperCase() + side.substring(1).toLowerCase();
            order.setVerb(verb.equalsIgnoreCase("BUY") ? MarketOrder.BUY : MarketOrder.SELL);
            
            order.setQuantity(quantity);
            
            if (price != null) {
                order.setPrice(price);
                order.setOrderType(MarketDef.ORDER_TYPE_LIMIT);
            } else {
                order.setOrderType(MarketDef.ORDER_TYPE_MARKET);
            }
            
            // 如果 MarketOrder 有这些方法，取消注释
            // order.setMarketSource(marketSource);
            // order.setTimeInForce(timeInForce);
            
            // 使用 submitOrder 方法而不是 addOrder
            String ionOrderId = orderManagement.submitOrder(order);
            
            if (ionOrderId != null) {
                // 更新订单ID
                order.setOrderId(ionOrderId);
                
                // 保存订单信息
                OrderInfo orderInfo = new OrderInfo(requestId, ionOrderId, order);
                orderInfoByClientId.put(requestId, orderInfo);
                orderInfoByInternalId.put(ionOrderId, orderInfo);
                
                // 发布成功响应
                publishSuccessResponse(requestId, ionOrderId, order.getStatus());
            } else {
                publishErrorResponse(requestId, "Order submission failed - OrderManagement returned null");
            }
            
        } catch (Exception e) {
            System.err.println("Error processing order: " + e.getMessage());
            e.printStackTrace();
            publishErrorResponse(requestId, "Error: " + e.getMessage());
        }
    }
    
    /**
     * 发布成功响应
     */
    private void publishSuccessResponse(String requestId, String ionOrderId, String status) {
        Map<String, Object> response = new HashMap<>();
        response.put("type", "response");
        response.put("requestId", requestId);
        response.put("ionOrderId", ionOrderId);
        response.put("success", true);
        response.put("status", status != null ? status : "SUBMITTED");
        response.put("message", "Order submitted successfully");
        response.put("timestamp", LoggingUtils.getCurrentTimestamp());
        
        String json = JsonUtils.toJson(response);
        publisher.publishOrderMessage(json);
        System.out.println("Published success response: " + json);
    }
    
    /**
     * 发布错误响应
     */
    private void publishErrorResponse(String requestId, String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("type", "error");
        response.put("requestId", requestId);
        response.put("success", false);
        response.put("message", message);
        response.put("timestamp", LoggingUtils.getCurrentTimestamp());
        
        String json = JsonUtils.toJson(response);
        publisher.publishOrderMessage(json);
        System.err.println("Published error response: " + json);
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