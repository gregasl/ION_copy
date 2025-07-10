// package com.iontrading.automatedMarketMaking;

// import java.util.*;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.Executors;
// import java.util.concurrent.ScheduledExecutorService;
// import java.util.concurrent.TimeUnit;
// import java.util.concurrent.atomic.AtomicBoolean;
// import java.util.concurrent.atomic.AtomicLong;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.iontrading.mkv.Mkv;
// import com.iontrading.mkv.MkvChain;
// import com.iontrading.mkv.MkvComponent;
// import com.iontrading.mkv.MkvObject;
// import com.iontrading.mkv.MkvPublishManager;
// import com.iontrading.mkv.MkvRecord;
// import com.iontrading.mkv.MkvSupply;
// import com.iontrading.mkv.MkvType;
// import com.iontrading.mkv.enums.MkvFieldType;
// import com.iontrading.mkv.enums.MkvChainAction;
// import com.iontrading.mkv.enums.MkvObjectType;
// import com.iontrading.mkv.enums.MkvPlatformEvent;
// import com.iontrading.mkv.events.MkvChainListener;
// import com.iontrading.mkv.events.MkvFunctionCallEvent;
// import com.iontrading.mkv.events.MkvFunctionCallListener;
// import com.iontrading.mkv.events.MkvPlatformListener;
// import com.iontrading.mkv.events.MkvPublishListener;
// import com.iontrading.mkv.events.MkvRecordListener;
// import com.iontrading.mkv.qos.MkvQoS;
// import com.iontrading.mkv.exceptions.*;

// import redis.clients.jedis.Jedis;
// import redis.clients.jedis.JedisPool;
// import redis.clients.jedis.JedisPubSub;

// /**
//  * ION Wrapper - A comprehensive bridge between Python and ION Platform
//  * 
//  * This wrapper provides:
//  * 1. Full order lifecycle management (create, update, cancel, monitor)
//  * 2. Bi-directional communication with Python via Redis
//  * 3. Complete order state tracking and caching
//  * 4. Automatic parameter resolution for different venues
//  * 5. Real-time order status notifications
//  */
// public class OrderManagementWrapper implements MkvPublishListener, MkvChainListener, 
//                                               MkvRecordListener, MkvPlatformListener,
//                                               MkvFunctionCallListener, IOrderManager {
    
//     private static final Logger LOGGER = LoggerFactory.getLogger(OrderManagementWrapper.class);
    
//     // Redis configuration
//     private static final String REDIS_HOST = System.getProperty("redis.host", "cacheuat");
//     private static final int REDIS_PORT = Integer.parseInt(System.getProperty("redis.port", "6379"));
//     private static final String CONTROL_CHANNEL = "ion:wrapper:control";
//     private static final String STATUS_CHANNEL = "ion:wrapper:status";
//     private static final String ORDER_UPDATE_CHANNEL = "ion:wrapper:orders";
    
//     // JSON mapper for Redis messages
//     private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
//     // Redis connection pool
//     private JedisPool jedisPool;
    
//     // Active MarketOrder objects tracked by reqId
//     private final Map<Integer, MarketOrder> activeOrders = new ConcurrentHashMap<>();
    
//     // Order information cache - comprehensive order data
//     // Key: orderId, Value: OrderInfo object
//     private final Map<String, OrderInfo> orderCache = new ConcurrentHashMap<>();
    
//     // Request ID to Order ID mapping for tracking responses
//     private final Map<Integer, String> reqIdToOrderIdMap = new ConcurrentHashMap<>();
    
//     // Order ID to Request ID mapping
//     private final Map<String, Integer> orderIdToReqIdMap = new ConcurrentHashMap<>();
    
//     // Venue to trader mapping (from boss's code)
//     private final Map<String, String> venueToTraderMap = new HashMap<>();
    
//     // Application ID for identifying orders from this wrapper
//     private final String applicationId = "Python_ION_Wrapper";
    
//     // Ready flag
//     private volatile boolean isReady = false;
    
//     // Health monitoring
//     private final ScheduledExecutorService healthCheckExecutor = Executors.newScheduledThreadPool(1);
//     private final AtomicLong lastActivityTime = new AtomicLong(System.currentTimeMillis());
//     private final AtomicLong orderUpdateCount = new AtomicLong(0);
//     private final AtomicLong commandProcessedCount = new AtomicLong(0);
    
//     // Control thread for Redis listener
//     private Thread controlListenerThread;
//     private final AtomicBoolean isRunning = new AtomicBoolean(true);
    
//     /**
//      * Order state enumeration
//      */
//     private enum OrderState {
//         PENDING("Pending"),          // Order sent, awaiting confirmation
//         ACTIVE("Active"),            // Order is active in the market
//         PARTIALLY_FILLED("PartFill"), // Order is partially filled
//         FILLED("Filled"),            // Order is completely filled
//         CANCELLED("Cancelled"),      // Order was cancelled
//         REJECTED("Rejected"),        // Order was rejected
//         EXPIRED("Expired"),          // Order expired
//         ERROR("Error");              // Error state
        
//         private final String displayName;
        
//         OrderState(String displayName) {
//             this.displayName = displayName;
//         }
        
//         public String getDisplayName() {
//             return displayName;
//         }
//     }
    
//     /**
//      * Comprehensive order information class
//      */
//     private static class OrderInfo {
//         // Core identifiers
//         String orderId;
//         String origId;
//         String compNameOrigin;
//         String trader;
//         int reqId;
        
//         // Order details
//         String instrumentId;
//         String verb;
//         double quantity;
//         double price;
//         String type;
//         String tif;
        
//         // State tracking
//         OrderState state = OrderState.PENDING;
//         double filledQty = 0;
//         double remainingQty;
//         double avgFillPrice = 0;
        
//         // Timestamps
//         long creationTime = System.currentTimeMillis();
//         long lastUpdateTime = System.currentTimeMillis();
        
//         // Error information
//         int errorCode = 0;
//         String errorMessage = "";
        
//         public Map<String, Object> toMap() {
//             Map<String, Object> map = new HashMap<>();
//             map.put("orderId", orderId);
//             map.put("origId", origId);
//             map.put("compNameOrigin", compNameOrigin);
//             map.put("trader", trader);
//             map.put("reqId", reqId);
//             map.put("instrumentId", instrumentId);
//             map.put("verb", verb);
//             map.put("quantity", quantity);
//             map.put("price", price);
//             map.put("type", type);
//             map.put("tif", tif);
//             map.put("state", state.getDisplayName());
//             map.put("filledQty", filledQty);
//             map.put("remainingQty", remainingQty);
//             map.put("avgFillPrice", avgFillPrice);
//             map.put("creationTime", creationTime);
//             map.put("lastUpdateTime", lastUpdateTime);
//             map.put("errorCode", errorCode);
//             map.put("errorMessage", errorMessage);
//             return map;
//         }
//     }
    
//     /**
//      * Constructor - initializes the wrapper
//      */
//     public OrderManagementWrapper() {
//         LOGGER.info("Initializing ION Wrapper v2.0");
        
//         // Initialize venue to trader mapping
//         initializeTraderMap();
        
//         // Initialize Redis connection
//         initializeRedis();
        
//         // Start health monitoring
//         startHealthMonitoring();
//     }
    
//     /**
//      * Initialize venue to trader mapping
//      */
//     private void initializeTraderMap() {
//         // Production mappings
//         venueToTraderMap.put("BTEC_REPO_US", "TEST2");
//         venueToTraderMap.put("DEALERWEB_REPO", "asldevtrd1");
//         venueToTraderMap.put("FENICS_USREPO", "frosasl1");
        
//         // Add more mappings as needed
//         venueToTraderMap.put("TRADEWEB", "twbasl1");
//         venueToTraderMap.put("BGC", "bgcasl1");
        
//         LOGGER.info("Venue to trader mapping initialized with {} entries", venueToTraderMap.size());
//     }
    
//     /**
//      * Initialize Redis connection and control listener
//      */
//     private void initializeRedis() {
//         try {
//             jedisPool = new JedisPool(REDIS_HOST, REDIS_PORT);
            
//             // Test connection
//             try (Jedis jedis = jedisPool.getResource()) {
//                 String pong = jedis.ping();
//                 LOGGER.info("Redis connection established at {}:{} - {}", REDIS_HOST, REDIS_PORT, pong);
//             }
            
//             // Start control channel listener
//             startControlListener();
            
//         } catch (Exception e) {
//             LOGGER.error("Failed to initialize Redis", e);
//             throw new RuntimeException("Redis initialization failed", e);
//         }
//     }
    
//     /**
//      * Start listening for control commands from Python
//      */
//     private void startControlListener() {
//         controlListenerThread = new Thread(() -> {
//             while (isRunning.get()) {
//                 try (Jedis jedis = jedisPool.getResource()) {
//                     LOGGER.info("Starting control listener on channel: {}", CONTROL_CHANNEL);
                    
//                     jedis.subscribe(new JedisPubSub() {
//                         @Override
//                         public void onMessage(String channel, String message) {
//                             lastActivityTime.set(System.currentTimeMillis());
//                             handleControlMessage(message);
//                         }
                        
//                         @Override
//                         public void onSubscribe(String channel, int subscribedChannels) {
//                             LOGGER.info("Subscribed to control channel: {}", channel);
//                             publishStatus("LISTENER_READY", "Control listener is active", null);
//                         }
                        
//                         @Override
//                         public void onUnsubscribe(String channel, int subscribedChannels) {
//                             LOGGER.info("Unsubscribed from control channel: {}", channel);
//                         }
//                     }, CONTROL_CHANNEL);
                    
//                 } catch (Exception e) {
//                     LOGGER.error("Control listener error, will retry in 5 seconds", e);
//                     try {
//                         Thread.sleep(5000);
//                     } catch (InterruptedException ie) {
//                         Thread.currentThread().interrupt();
//                         break;
//                     }
//                 }
//             }
//         }, "Redis-Control-Listener");
        
//         controlListenerThread.setDaemon(true);
//         controlListenerThread.start();
//     }
    
//     /**
//      * Handle control messages from Python
//      */
//     private void handleControlMessage(String message) {
//         LOGGER.info("Received control message: {}", message);
//         commandProcessedCount.incrementAndGet();
        
//         try {
//             Map<String, Object> command = OBJECT_MAPPER.readValue(message, Map.class);
//             String action = (String) command.get("action");
//             String requestId = (String) command.get("requestId"); // Optional request tracking
            
//             if (!isReady && !"PING".equals(action)) {
//                 publishStatus("NOT_READY", "Wrapper is still initializing", requestId);
//                 return;
//             }
            
//             switch (action.toUpperCase()) {
//                 case "CREATE_ORDER":
//                     handleCreateOrder(command, requestId);
//                     break;
                    
//                 case "UPDATE_ORDER":
//                     handleUpdateOrder(command, requestId);
//                     break;
                    
//                 case "CANCEL_ORDER":
//                     handleCancelOrder(command, requestId);
//                     break;
                    
//                 case "GET_ORDER_INFO":
//                     handleGetOrderInfo(command, requestId);
//                     break;
                    
//                 case "GET_ALL_ORDERS":
//                     handleGetAllOrders(command, requestId);
//                     break;
                    
//                 case "PING":
//                     handlePing(requestId);
//                     break;
                    
//                 case "GET_STATS":
//                     handleGetStats(requestId);
//                     break;
                    
//                 default:
//                     LOGGER.warn("Unknown action: {}", action);
//                     publishStatus("ERROR", "Unknown action: " + action, requestId);
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("Error handling control message", e);
//             publishStatus("ERROR", "Failed to process command: " + e.getMessage(), null);
//         }
//     }
    
//     /**
//      * Handle create order command
//      */
//     private void handleCreateOrder(Map<String, Object> command, String requestId) {
//         try {
//             String marketSource = (String) command.get("marketSource");
//             String traderId = (String) command.get("traderId");
//             String instrId = (String) command.get("instrumentId");
//             String verb = (String) command.get("verb");
//             Double qty = ((Number) command.get("quantity")).doubleValue();
//             Double price = ((Number) command.get("price")).doubleValue();
//             String type = (String) command.getOrDefault("type", "Limit");
//             String tif = (String) command.getOrDefault("tif", "Day");
            
//             // Validate required parameters
//             if (marketSource == null || instrId == null || verb == null) {
//                 publishStatus("CREATE_FAILED", "Missing required parameters", requestId);
//                 return;
//             }
            
//             // If traderId not provided, try to get it from venue mapping
//             if (traderId == null) {
//                 traderId = venueToTraderMap.get(marketSource);
//                 if (traderId == null) {
//                     publishStatus("CREATE_FAILED", 
//                         "No trader mapping found for venue: " + marketSource, requestId);
//                     return;
//                 }
//             }
            
//             LOGGER.info("Creating order: {} {} {} @ {} on {} with trader {}", 
//                         verb, qty, instrId, price, marketSource, traderId);
            
//             // Create the order
//             MarketOrder order = addOrder(marketSource, traderId, instrId, verb, qty, price, type, tif);
            
//             if (order != null) {
//                 // Create initial order info
//                 OrderInfo info = new OrderInfo();
//                 info.reqId = order.getMyReqId();
//                 info.instrumentId = instrId;
//                 info.verb = verb;
//                 info.quantity = qty;
//                 info.price = price;
//                 info.type = type;
//                 info.tif = tif;
//                 info.remainingQty = qty;
//                 info.compNameOrigin = marketSource;
//                 info.trader = traderId;
                
//                 // Store in temporary map until we get orderId
//                 reqIdToOrderIdMap.put(order.getMyReqId(), "PENDING_" + order.getMyReqId());
                
//                 publishStatus("ORDER_SENT", 
//                     String.format("Order sent with reqId: %d", order.getMyReqId()), requestId);
//             } else {
//                 publishStatus("CREATE_FAILED", "Failed to create order", requestId);
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("Error creating order", e);
//             publishStatus("CREATE_ERROR", e.getMessage(), requestId);
//         }
//     }
    
//     /**
//      * Handle update order command
//      */
//     private void handleUpdateOrder(Map<String, Object> command, String requestId) {
//         try {
//             String orderId = (String) command.get("orderId");
//             Double newPrice = command.containsKey("price") ? 
//                 ((Number) command.get("price")).doubleValue() : null;
//             Double newQty = command.containsKey("quantity") ? 
//                 ((Number) command.get("quantity")).doubleValue() : null;
            
//             // Get order info from cache
//             OrderInfo orderInfo = orderCache.get(orderId);
//             if (orderInfo == null) {
//                 publishStatus("UPDATE_FAILED", 
//                     String.format("Order %s not found in cache", orderId), requestId);
//                 return;
//             }
            
//             // Use stored parameters for update
//             String verb = orderInfo.verb;
//             double qty = newQty != null ? newQty : orderInfo.remainingQty;
//             double price = newPrice != null ? newPrice : orderInfo.price;
            
//             LOGGER.info("Updating order {} to {} @ {} on {}", 
//                         orderId, qty, price, orderInfo.compNameOrigin);
            
//             // Create reqId for update
//             int updateReqId = activeOrders.size() + 1000; // Simple unique ID generation
            
//             // Call update using correct parameters - matching the boss's code signature
//             MarketOrder updateOrder = MarketOrder.orderUpdate(
//                 orderInfo.compNameOrigin,   // MarketSource
//                 orderInfo.trader,           // TraderId
//                 orderInfo.orderId,          // orderId (not origId for update)
//                 updateReqId + "",           // reqId as string
//                 verb,                       // verb
//                 qty,                        // quantity
//                 price,                      // price
//                 this                        // orderManager
//             );
            
//             if (updateOrder != null) {
//                 // Update local cache
//                 orderInfo.price = price;
//                 orderInfo.quantity = qty;
//                 orderInfo.lastUpdateTime = System.currentTimeMillis();
                
//                 publishStatus("UPDATE_SENT", 
//                     String.format("Update request sent for order %s", orderId), requestId);
//             } else {
//                 publishStatus("UPDATE_FAILED", 
//                     String.format("Failed to send update for order %s", orderId), requestId);
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("Error updating order", e);
//             publishStatus("UPDATE_ERROR", e.getMessage(), requestId);
//         }
//     }
    
//     /**
//      * Handle cancel order command
//      */
//     private void handleCancelOrder(Map<String, Object> command, String requestId) {
//         try {
//             String orderId = (String) command.get("orderId");
            
//             // Get order info from cache
//             OrderInfo orderInfo = orderCache.get(orderId);
//             if (orderInfo == null) {
//                 publishStatus("CANCEL_FAILED", 
//                     String.format("Order %s not found in cache", orderId), requestId);
//                 return;
//             }
            
//             // Check if order is in cancellable state
//             if (orderInfo.state == OrderState.FILLED || 
//                 orderInfo.state == OrderState.CANCELLED ||
//                 orderInfo.state == OrderState.EXPIRED) {
//                 publishStatus("CANCEL_FAILED", 
//                     String.format("Order %s is in state %s and cannot be cancelled", 
//                         orderId, orderInfo.state.getDisplayName()), requestId);
//                 return;
//             }
            
//             LOGGER.info("Cancelling order: orderId={}, origId={}, venue={}, trader={}", 
//                         orderId, orderInfo.origId, orderInfo.compNameOrigin, orderInfo.trader);
            
//             // Use the correct parameters for cancel
//             MarketOrder cancelOrder = MarketOrder.orderCancel(
//                 orderInfo.compNameOrigin,  // marketSource
//                 orderInfo.trader,          // traderId
//                 orderInfo.origId,          // Use origId NOT orderId for cancel!
//                 this
//             );
            
//             if (cancelOrder != null) {
//                 publishStatus("CANCEL_SENT", 
//                     String.format("Cancel request sent for order %s", orderId), requestId);
//             } else {
//                 publishStatus("CANCEL_FAILED", 
//                     String.format("Failed to send cancel for order %s", orderId), requestId);
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("Error cancelling order", e);
//             publishStatus("CANCEL_ERROR", e.getMessage(), requestId);
//         }
//     }
    
//     /**
//      * Handle get order info command
//      */
//     private void handleGetOrderInfo(Map<String, Object> command, String requestId) {
//         String orderId = (String) command.get("orderId");
        
//         OrderInfo orderInfo = orderCache.get(orderId);
//         if (orderInfo != null) {
//             publishOrderUpdate(orderInfo, "ORDER_INFO", requestId);
//         } else {
//             publishStatus("ORDER_NOT_FOUND", 
//                 String.format("Order %s not found in cache", orderId), requestId);
//         }
//     }
    
//     /**
//      * Handle get all orders command
//      */
//     private void handleGetAllOrders(Map<String, Object> command, String requestId) {
//         String filterState = (String) command.get("state");
//         String filterVenue = (String) command.get("marketSource");
//         String filterInstrument = (String) command.get("instrumentId");
        
//         List<Map<String, Object>> orders = new ArrayList<>();
        
//         for (OrderInfo orderInfo : orderCache.values()) {
//             // Apply filters
//             if (filterState != null && !orderInfo.state.getDisplayName().equals(filterState)) {
//                 continue;
//             }
//             if (filterVenue != null && !orderInfo.compNameOrigin.equals(filterVenue)) {
//                 continue;
//             }
//             if (filterInstrument != null && !orderInfo.instrumentId.equals(filterInstrument)) {
//                 continue;
//             }
            
//             orders.add(orderInfo.toMap());
//         }
        
//         Map<String, Object> response = new HashMap<>();
//         response.put("requestId", requestId);
//         response.put("orders", orders);
//         response.put("count", orders.size());
//         response.put("timestamp", System.currentTimeMillis());
        
//         publishToRedis(ORDER_UPDATE_CHANNEL, response);
//     }
    
//     /**
//      * Handle ping command
//      */
//     private void handlePing(String requestId) {
//         Map<String, Object> pong = new HashMap<>();
//         pong.put("status", "PONG");
//         pong.put("ready", isReady);
//         pong.put("uptime", System.currentTimeMillis() - lastActivityTime.get());
//         pong.put("orderCount", orderCache.size());
//         pong.put("activeOrders", activeOrders.size());
        
//         publishStatus("PONG", "Wrapper is alive", requestId);
//     }
    
//     /**
//      * Handle get statistics command
//      */
//     private void handleGetStats(String requestId) {
//         Map<String, Object> stats = new HashMap<>();
//         stats.put("totalOrders", orderCache.size());
//         stats.put("activeOrders", countOrdersByState(OrderState.ACTIVE));
//         stats.put("pendingOrders", countOrdersByState(OrderState.PENDING));
//         stats.put("filledOrders", countOrdersByState(OrderState.FILLED));
//         stats.put("cancelledOrders", countOrdersByState(OrderState.CANCELLED));
//         stats.put("orderUpdates", orderUpdateCount.get());
//         stats.put("commandsProcessed", commandProcessedCount.get());
//         stats.put("uptime", System.currentTimeMillis() - lastActivityTime.get());
        
//         Map<String, Object> response = new HashMap<>();
//         response.put("requestId", requestId);
//         response.put("stats", stats);
//         response.put("timestamp", System.currentTimeMillis());
        
//         publishToRedis(STATUS_CHANNEL, response);
//     }
    
//     private long countOrdersByState(OrderState state) {
//         return orderCache.values().stream()
//             .filter(o -> o.state == state)
//             .count();
//     }
    
//     /**
//      * Start health monitoring
//      */
//     private void startHealthMonitoring() {
//         healthCheckExecutor.scheduleAtFixedRate(() -> {
//             try {
//                 long lastActivity = System.currentTimeMillis() - lastActivityTime.get();
                
//                 // Publish health status
//                 Map<String, Object> health = new HashMap<>();
//                 health.put("status", "HEALTH_CHECK");
//                 health.put("lastActivity", lastActivity);
//                 health.put("orderCount", orderCache.size());
//                 health.put("isReady", isReady);
//                 health.put("orderUpdates", orderUpdateCount.get());
                
//                 publishToRedis(STATUS_CHANNEL, health);
                
//                 // Log warning if no activity for a while
//                 if (lastActivity > 300000) { // 5 minutes
//                     LOGGER.warn("No activity for {} ms", lastActivity);
//                 }
                
//             } catch (Exception e) {
//                 LOGGER.error("Health check error", e);
//             }
//         }, 30, 30, TimeUnit.SECONDS);
//     }
    
//     // ========== MkvPlatformListener Implementation ==========
    
//     @Override
//     public void onMain(MkvPlatformEvent event) {
//         switch (event.intValue()) {
//             case MkvPlatformEvent.START_code:
//                 LOGGER.info("MKV platform START event received");
//                 break;
                
//             case MkvPlatformEvent.STOP_code:
//                 LOGGER.info("MKV platform STOP event received");
//                 cleanup();
//                 break;
                
//             case MkvPlatformEvent.REGISTER_IDLE_code:
//                 LOGGER.info("MKV platform REGISTER_IDLE event received");
//                 break;
                
//             case MkvPlatformEvent.REGISTER_code:
//                 LOGGER.info("MKV platform REGISTER event received");
//                 break;
//         }
//     }
    
//     @Override
//     public void onComponent(MkvComponent comp, boolean registered) {
//         LOGGER.info("Component event: {} - registered: {}", comp.getName(), registered);
//     }
    
//     @Override
//     public void onConnect(String comp, boolean connected) {
//         LOGGER.info("Connect event: {} - connected: {}", comp, connected);
//     }
    
//     // ========== MkvPublishListener Implementation ==========
    
//     @Override
//     public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
//         if (dwl) {
//             return;
//         }
        
//         if (pub_unpub) {
//             // Comment out this line to reduce noise - too many objects published
//             // LOGGER.debug("New object published: {} ({})", 
//             //             mkvObject.getName(), mkvObject.getMkvObjectType());
                        
//             // Check if it's the CM_Order chain
//             if (mkvObject.getMkvObjectType().equals(MkvObjectType.CHAIN) &&
//                 mkvObject.getName().equals("CM_Order")) {
                    
//                 try {
//                     MkvChain chain = (MkvChain) mkvObject;
//                     chain.subscribe(this);
//                     LOGGER.info("Subscribed to CM_Order chain");
//                 } catch (Exception e) {
//                     LOGGER.error("Error subscribing to CM_Order chain", e);
//                 }
//             }
//         }
//     }
    
//     @Override
//     public void onPublishIdle(String component, boolean start) {
//         if (start) {
//             LOGGER.info("Publish idle - subscribing to CM_Order chain");
//             subscribeToOrderChain();
            
//             // Mark as ready and publish status
//             isReady = true;
//             publishStatus("READY", "ION Wrapper initialized and ready to accept commands", null);
//         }
//     }
    
//     @Override
//     public void onSubscribe(MkvObject mkvObject) {
//         // Not needed
//     }
    
//     /**
//      * Subscribe to CM_Order chain
//      */
//     private void subscribeToOrderChain() {
//         try {
//             MkvPublishManager pm = Mkv.getInstance().getPublishManager();
//             MkvChain orderChain = pm.getMkvChain("CM_Order");
            
//             if (orderChain != null) {
//                 if (!orderChain.isSubscribed()) {
//                     orderChain.subscribe(this);
//                     LOGGER.info("成功订阅CM_Order链");
//                 } else {
//                     LOGGER.info("已经订阅了CM_Order链");
//                 }
//             } else {
//                 LOGGER.info("CM_Order链未找到 - 尝试创建并发布");
//                 // 尝试创建并发布CM_Order链
//                 try {
//                     // 首先创建CM_Order类型
//                     String[] orderFieldNames = {
//                         "OrderId", "OrigId", "CompNameOrigin", "Trader", "InstrumentId", 
//                         "VerbStr", "Quantity", "Price", "StatusStr", "QuantityFilled", 
//                         "AvgPrice", "RequestId", "ActiveStr"
//                     };
//                     MkvFieldType[] orderFieldTypes = {
//                         MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.STR,
//                         MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.REAL, MkvFieldType.REAL,
//                         MkvFieldType.STR, MkvFieldType.REAL, MkvFieldType.REAL, MkvFieldType.STR,
//                         MkvFieldType.STR
//                     };
                    
//                     MkvType orderType = new MkvType("CM_Order", orderFieldNames, orderFieldTypes);
//                     orderType.publish();
//                     LOGGER.info("CM_Order类型发布成功");
                    
//                     // 然后创建并发布链
//                     orderChain = new MkvChain("CM_Order", "CM_Order");
//                     orderChain.publish();
//                     LOGGER.info("CM_Order链发布成功");
                    
//                     // 订阅链
//                     orderChain.subscribe(this);
//                     LOGGER.info("成功订阅自己发布的CM_Order链");
                    
//                 } catch (Exception createEx) {
//                     LOGGER.error("创建CM_Order链失败: {}", createEx.getMessage());
                    
//                     // 如果创建失败，再次尝试获取已存在的链
//                     try {
//                         orderChain = pm.getMkvChain("CM_Order");
//                         if (orderChain != null) {
//                             if (!orderChain.isSubscribed()) {
//                                 orderChain.subscribe(this);
//                                 LOGGER.info("成功订阅已存在的CM_Order链");
//                             } else {
//                                 LOGGER.info("已经订阅了已存在的CM_Order链");
//                             }
//                         } else {
//                             LOGGER.warn("CM_Order链未找到 - 将等待其他组件发布");
//                         }
//                     } catch (Exception retryEx) {
//                         LOGGER.error("重试获取CM_Order链失败: {}", retryEx.getMessage());
//                     }
//                 }
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("订阅订单链时出错: {}", e.getMessage());
//         }
//     }
    
//     // ========== MkvChainListener Implementation ==========
    
//     @Override
//     public void onSupply(MkvChain chain, String recordName, int pos, MkvChainAction action) {
//         switch(action.intValue()) {
//             case MkvChainAction.SET_code:
//                 LOGGER.debug("Chain SET received");
//                 break;
                
//             case MkvChainAction.INSERT_code:
//             case MkvChainAction.APPEND_code:
//                 LOGGER.debug("Order record added: {}", recordName);
//                 subscribeToOrderRecord(recordName);
//                 break;
                
//             case MkvChainAction.DELETE_code:
//                 LOGGER.debug("Order record deleted: {}", recordName);
//                 handleOrderRecordDeleted(recordName);
//                 break;
                
//             case MkvChainAction.IDLE_code:
//                 LOGGER.debug("Chain IDLE - subscribing to all records");
//                 for(int i = 0; i < chain.size(); i++) {
//                     String recName = (String)chain.get(i);
//                     subscribeToOrderRecord(recName);
//                 }
//                 break;
//         }
//     }
    
//     /**
//      * Subscribe to an individual order record
//      */
//     private void subscribeToOrderRecord(String recordName) {
//         try {
//             MkvPublishManager pm = Mkv.getInstance().getPublishManager();
//             MkvRecord record = pm.getMkvRecord(recordName);
            
//             if (record != null && !record.isSubscribed()) {
//                 record.subscribe(this);
//                 LOGGER.debug("Subscribed to order record: {}", recordName);
//             }
//         } catch (Exception e) {
//             LOGGER.error("Error subscribing to order record: {}", recordName, e);
//         }
//     }
    
//     /**
//      * Handle order record deletion
//      */
//     private void handleOrderRecordDeleted(String recordName) {
//         // Extract orderId from record name if possible
//         // Record names might be in format: CM_Order.VENUE.ORDERID
//         String[] parts = recordName.split("\\.");
//         if (parts.length >= 3) {
//             String orderId = parts[parts.length - 1];
//             OrderInfo orderInfo = orderCache.get(orderId);
//             if (orderInfo != null) {
//                 orderInfo.state = OrderState.EXPIRED;
//                 orderInfo.lastUpdateTime = System.currentTimeMillis();
//                 publishOrderUpdate(orderInfo, "ORDER_DELETED", null);
//             }
//         }
//     }
    
//     // ========== MkvRecordListener Implementation ==========
    
//     @Override
//     public void onFullUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
//         handleOrderUpdate(record, supply, true);
//     }
    
//     @Override
//     public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
//         handleOrderUpdate(record, supply, false);
//     }
    
//     /**
//      * Handle order record updates from CM_Order
//      */
//     private void handleOrderUpdate(MkvRecord record, MkvSupply supply, boolean isFullUpdate) {
//         try {
//             String recordName = record.getName();
//             LOGGER.debug("Order update received: {} (full={})", recordName, isFullUpdate);
//             orderUpdateCount.incrementAndGet();
            
//             // Extract order fields
//             String orderId = getStringValue(record, "OrderId");
//             if (orderId == null || orderId.isEmpty()) {
//                 return; // Skip if no orderId
//             }
            
//             // Get or create order info
//             OrderInfo orderInfo = orderCache.computeIfAbsent(orderId, k -> new OrderInfo());
            
//             // Update core identifiers
//             orderInfo.orderId = orderId;
//             orderInfo.origId = getStringValue(record, "OrigId");
//             orderInfo.compNameOrigin = getStringValue(record, "CompNameOrigin");
//             orderInfo.trader = getStringValue(record, "Trader");
            
//             // Update order details
//             String instrument = getStringValue(record, "InstrumentId");
//             if (instrument != null) orderInfo.instrumentId = instrument;
            
//             String verb = getStringValue(record, "VerbStr");
//             if (verb != null) orderInfo.verb = verb;
            
//             Double quantity = getDoubleValue(record, "Quantity");
//             if (quantity != null) orderInfo.quantity = quantity;
            
//             Double price = getDoubleValue(record, "Price");
//             if (price != null) orderInfo.price = price;
            
//             // Update state
//             String status = getStringValue(record, "StatusStr");
//             if (status != null) {
//                 orderInfo.state = mapOrderStatus(status);
//             }
            
//             // Update fill information
//             Double qtyFilled = getDoubleValue(record, "QuantityFilled");
//             if (qtyFilled != null) {
//                 orderInfo.filledQty = qtyFilled;
//                 orderInfo.remainingQty = orderInfo.quantity - qtyFilled;
//             }
            
//             Double avgPrice = getDoubleValue(record, "AvgPrice");
//             if (avgPrice != null && avgPrice > 0) {
//                 orderInfo.avgFillPrice = avgPrice;
//             }
            
//             // Get request ID
//             String requestId = getStringValue(record, "RequestId");
//             if (requestId != null) {
//                 try {
//                     int reqId = Integer.parseInt(requestId);
//                     orderInfo.reqId = reqId;
                    
//                     // Update mappings
//                     orderIdToReqIdMap.put(orderId, reqId);
//                     reqIdToOrderIdMap.put(reqId, orderId);
                    
//                     // Map to IOrderManager if this is our order
//                     mapOrderIdToReqId(orderId, reqId);
//                 } catch (NumberFormatException e) {
//                     LOGGER.debug("Invalid requestId format: {}", requestId);
//                 }
//             }
            
//             // Update timestamps
//             orderInfo.lastUpdateTime = System.currentTimeMillis();
            
//             // Check if order is dead
//             if (isOrderDead(orderInfo)) {
//                 // Notify IOrderManager
//                 MarketOrder marketOrder = activeOrders.get(orderInfo.reqId);
//                 if (marketOrder != null) {
//                     orderDead(marketOrder);
//                 }
//             }
            
//             // Publish update to Redis
//             publishOrderUpdate(orderInfo, "ORDER_UPDATE", null);
            
//             LOGGER.info("Order updated: {} - {} {} {} @ {} [{}] filled={}/{}", 
//                         orderId, orderInfo.verb, orderInfo.instrumentId, 
//                         orderInfo.quantity, orderInfo.price, 
//                         orderInfo.state.getDisplayName(),
//                         orderInfo.filledQty, orderInfo.quantity);
            
//         } catch (Exception e) {
//             LOGGER.error("Error processing order update", e);
//         }
//     }
    
//     /**
//      * Helper method to get string value from record
//      */
//     private String getStringValue(MkvRecord record, String fieldName) {
//         try {
//             return record.getValue(fieldName).getString();
//         } catch (Exception e) {
//             return null;
//         }
//     }
    
//     /**
//      * Helper method to get double value from record
//      */
//     private Double getDoubleValue(MkvRecord record, String fieldName) {
//         try {
//             return record.getValue(fieldName).getReal();
//         } catch (Exception e) {
//             return null;
//         }
//     }
    
//     /**
//      * Map ION status string to OrderState
//      */
//     private OrderState mapOrderStatus(String status) {
//         if (status == null) return OrderState.PENDING;
        
//         switch (status.toUpperCase()) {
//             case "NEW":
//             case "ACTIVE":
//             case "WORKING":
//                 return OrderState.ACTIVE;
                
//             case "PARTIALLY_FILLED":
//             case "PARTFILL":
//                 return OrderState.PARTIALLY_FILLED;
                
//             case "FILLED":
//             case "COMPLETE":
//                 return OrderState.FILLED;
                
//             case "CANCELLED":
//             case "CANCELED":
//                 return OrderState.CANCELLED;
                
//             case "REJECTED":
//                 return OrderState.REJECTED;
                
//             case "EXPIRED":
//                 return OrderState.EXPIRED;
                
//             default:
//                 LOGGER.debug("Unknown order status: {}", status);
//                 return OrderState.PENDING;
//         }
//     }
    
//     /**
//      * Check if order is dead (cannot trade anymore)
//      */
//     private boolean isOrderDead(OrderInfo orderInfo) {
//         return orderInfo.state == OrderState.FILLED ||
//                orderInfo.state == OrderState.CANCELLED ||
//                orderInfo.state == OrderState.REJECTED ||
//                orderInfo.state == OrderState.EXPIRED ||
//                orderInfo.state == OrderState.ERROR;
//     }
    
//     // ========== MkvFunctionCallListener Implementation ==========
    
//     @Override
//     public void onResult(MkvFunctionCallEvent event, MkvSupply result) {
//         LOGGER.debug("Function call result event: {}", event);
        
//         // This is called when we receive a successful response from order creation/cancel/update
//         try {
//             // Extract result values if needed
//             if (result != null) {
//                 LOGGER.debug("Function call succeeded with result");
//             }
            
//             handleFunctionCallEvent(event, 0, null);
//         } catch (Exception e) {
//             LOGGER.error("Error handling function result", e);
//         }
//     }
    
//     @Override
//     public void onError(MkvFunctionCallEvent event, byte errCode, String errStr) {
//         LOGGER.error("Function call error: errCode={}, errStr={}", errCode, errStr);
        
//         // Handle error response
//         handleFunctionCallEvent(event, errCode, errStr);
//     }
    
//     /**
//      * Handle function call event (both success and error)
//      */
//     private void handleFunctionCallEvent(MkvFunctionCallEvent event, int errorCode, String errorMessage) {
//         try {
//             // The event contains information about which function was called
//             // We need to track which order this relates to
            
//             LOGGER.debug("Processing function call event - errorCode: {}", errorCode);
            
//             // In the boss's code, the MarketOrder object handles its own response
//             // For the wrapper, we mainly need to track errors
            
//             if (errorCode != 0 && errorMessage != null) {
//                 // Publish error status
//                 publishStatus("FUNCTION_ERROR", 
//                     String.format("Function call failed: %s (code: %d)", errorMessage, errorCode), 
//                     null);
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("Error handling function call event", e);
//         }
//     }
    
//     // ========== IOrderManager Implementation ==========
    
//     @Override
//     public void orderDead(MarketOrder order) {
//         LOGGER.info("Order dead: reqId={}", order.getMyReqId());
        
//         // Remove from active orders
//         activeOrders.remove(order.getMyReqId());
        
//         // Update order state in cache
//         String orderId = reqIdToOrderIdMap.get(order.getMyReqId());
//         if (orderId != null) {
//             OrderInfo orderInfo = orderCache.get(orderId);
//             if (orderInfo != null && !isOrderDead(orderInfo)) {
//                 // Only update if not already in a dead state
//                 orderInfo.state = OrderState.EXPIRED;
//                 orderInfo.lastUpdateTime = System.currentTimeMillis();
//                 publishOrderUpdate(orderInfo, "ORDER_DEAD", null);
//             }
//         }
        
//         publishStatus("ORDER_DEAD", 
//             String.format("Order with reqId %d is dead", order.getMyReqId()), null);
//     }
    
//     @Override
//     public MarketOrder addOrder(String marketSource, String traderId, String instrId, 
//                                 String verb, double qty, double price, String type, String tif) {
//         try {
//             MarketOrder order = MarketOrder.orderCreate(marketSource, traderId, instrId, 
//                                                        verb, qty, price, type, tif, this);
            
//             if (order != null) {
//                 LOGGER.info("Order created successfully: reqId={}", order.getMyReqId());
//                 activeOrders.put(order.getMyReqId(), order);
//             }
            
//             return order;
//         } catch (Exception e) {
//             LOGGER.error("Error creating order", e);
//             publishStatus("ORDER_CREATE_ERROR", e.getMessage(), null);
//             return null;
//         }
//     }
    
//     @Override
//     public void best(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {
//         // Not needed for wrapper - this is for market making logic
//     }
    
//     @Override
//     public void mapOrderIdToReqId(String orderId, int reqId) {
//         orderIdToReqIdMap.put(orderId, reqId);
//         reqIdToOrderIdMap.put(reqId, orderId);
//         LOGGER.debug("Mapped orderId {} to reqId {}", orderId, reqId);
//     }
    
//     @Override
//     public void removeOrder(int reqId) {
//         LOGGER.debug("Removing order with reqId: {}", reqId);
//         activeOrders.remove(reqId);
        
//         String orderId = reqIdToOrderIdMap.remove(reqId);
//         if (orderId != null) {
//             orderIdToReqIdMap.remove(orderId);
//         }
//     }
    
//     @Override
//     public String getApplicationId() {
//         return applicationId;
//     }
    
//     // ========== Redis Publishing Methods ==========
    
//     /**
//      * Publish status message to Redis
//      */
//     private void publishStatus(String status, String message, String requestId) {
//         Map<String, Object> statusMsg = new HashMap<>();
//         statusMsg.put("timestamp", System.currentTimeMillis());
//         statusMsg.put("status", status);
//         statusMsg.put("message", message);
//         if (requestId != null) {
//             statusMsg.put("requestId", requestId);
//         }
        
//         publishToRedis(STATUS_CHANNEL, statusMsg);
//     }
    
//     /**
//      * Publish order update to Redis
//      */
//     private void publishOrderUpdate(OrderInfo orderInfo, String eventType, String requestId) {
//         Map<String, Object> msg = new HashMap<>();
//         msg.put("timestamp", System.currentTimeMillis());
//         msg.put("eventType", eventType);
//         msg.put("order", orderInfo.toMap());
//         if (requestId != null) {
//             msg.put("requestId", requestId);
//         }
        
//         publishToRedis(ORDER_UPDATE_CHANNEL, msg);
//     }
    
//     /**
//      * Publish message to Redis channel
//      */
//     private void publishToRedis(String channel, Map<String, Object> message) {
//         try (Jedis jedis = jedisPool.getResource()) {
//             String json = OBJECT_MAPPER.writeValueAsString(message);
//             jedis.publish(channel, json);
//         } catch (Exception e) {
//             LOGGER.error("Error publishing to Redis channel {}", channel, e);
//         }
//     }
    
//     /**
//      * Cleanup resources
//      */
//     private void cleanup() {
//         LOGGER.info("Cleaning up resources...");
        
//         isRunning.set(false);
        
//         // Stop health check
//         healthCheckExecutor.shutdown();
        
//         // Interrupt control listener
//         if (controlListenerThread != null) {
//             controlListenerThread.interrupt();
//         }
        
//         // Wait for threads to finish
//         try {
//             healthCheckExecutor.awaitTermination(5, TimeUnit.SECONDS);
//         } catch (InterruptedException e) {
//             Thread.currentThread().interrupt();
//         }
        
//         // Close Redis pool
//         if (jedisPool != null && !jedisPool.isClosed()) {
//             publishStatus("SHUTDOWN", "ION Wrapper shutting down", null);
//             jedisPool.close();
//         }
//     }
    
//     /**
//      * Main method to start the wrapper
//      */
//     public static void main(String[] args) {
//         try {
//             LOGGER.info("Starting ION Wrapper v2.0...");
            
//             // Create wrapper instance
//             OrderManagementWrapper wrapper = new OrderManagementWrapper();
            
//             // Create MKV QoS configuration
//             MkvQoS qos = new MkvQoS();
//             qos.setArgs(args);
//             qos.setPublishListeners(new MkvPublishListener[] { wrapper });
//             qos.setPlatformListeners(new MkvPlatformListener[] { wrapper });
//             // Note: setFunctionCallListeners might not be available in this version
//             // The MkvFunctionCallListener is registered differently
            
//             // Start MKV (ION Platform connection)
//             Mkv mkv = Mkv.start(qos);
            
//             LOGGER.info("ION Wrapper started successfully");
            
//             // Add shutdown hook
//             Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                 LOGGER.info("Shutdown hook triggered");
//                 wrapper.cleanup();
//             }));
            
//         } catch (Exception e) {
//             LOGGER.error("Failed to start wrapper", e);
//             System.exit(1);
//         }
//     }
// }