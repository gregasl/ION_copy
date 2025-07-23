import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Redis Message Bridge
 */
public class RedisMessageBridge {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisMessageBridge.class);
    
    // Redis Connection
    private final String redisHost;
    private final int redisPort;
    private final JedisPool jedisPool;
    
    // Redis Channels
    public static final String MARKET_DATA_CHANNEL = "ION:ION_TEST:market_data";
    public static final String ORDER_COMMAND_CHANNEL = "ION:ION_TEST:ORDER_COMMANDS";
    public static final String ORDER_RESPONSE_CHANNEL = "ION:ION_TEST:ORDER_RESPONSES";
    public static final String HEARTBEAT_CHANNEL = "ION:ION_TEST:heartbeat";
    
    // Thread pool
    private final ExecutorService executorService;
    private volatile boolean running = false;
    
    /**
     * Constructor
     */
    public RedisMessageBridge(String host, int port) {
        this.redisHost = host;
        this.redisPort = port;
        this.executorService = Executors.newCachedThreadPool();

        // Initialize Redis connection pool
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(2);
        poolConfig.setTestOnBorrow(true);
        
        this.jedisPool = new JedisPool(poolConfig, host, port);
    }
    
    /**
     * Start the bridge service
     */
    public void start() {
        running = true;
        
        // Start the order command listener
        startOrderCommandListener();
        LOGGER.info("Redis bridge ready - {}:{} on {}", redisHost, redisPort, ORDER_COMMAND_CHANNEL);
    }
    
    /**
     * Stop the bridge service
     */
    public void stop() {
        running = false;
        if (jedisPool != null) {
            jedisPool.close();
        }
        executorService.shutdown();
    }
    
    /**
     * Publish market data to Redis - silent operation
     */
    public void publishMarketData(String instrument, double bidPrice, double askPrice, 
                                 double bidSize, double askSize, long timestamp) {
        try (Jedis jedis = jedisPool.getResource()) {
            JSONObject marketData = new JSONObject();
            marketData.put("type", "market_data");
            marketData.put("instrument", instrument);
            marketData.put("bid_price", bidPrice);
            marketData.put("ask_price", askPrice);
            marketData.put("bid_size", bidSize);
            marketData.put("ask_size", askSize);
            marketData.put("timestamp", timestamp);
            
            jedis.publish(MARKET_DATA_CHANNEL, marketData.toString());
            // No logging for market data

        } catch (Exception e) {
            // Silent failure for market data
        }
    }
    
    /**
     * Publish order response to Redis
     */
    public void publishOrderResponse(String orderId, String status, String message, 
                                   String venue, long timestamp) {
        try (Jedis jedis = jedisPool.getResource()) {
            JSONObject response = new JSONObject();
            response.put("type", "order_response");
            response.put("order_id", orderId);
            response.put("status", status);
            response.put("message", message);
            response.put("venue", venue);
            response.put("timestamp", timestamp);
            
            jedis.publish(ORDER_RESPONSE_CHANNEL, response.toString());
            
            // Only log important status updates
            // if ("SUBMITTED".equals(status) || "FILLED".equals(status) || 
            //     "CANCELLED".equals(status) || "FAILED".equals(status)) {
            //     LOGGER.info("Order response published: {} -> {}", orderId, status);
            // }

        } catch (Exception e) {
            LOGGER.error("Failed to publish order response: ", e);
        }
    }
    
    /**
     * Publish heartbeat signal - silent operation
     */
    public void publishHeartbeat() {
        try (Jedis jedis = jedisPool.getResource()) {
            JSONObject heartbeat = new JSONObject();
            heartbeat.put("type", "heartbeat");
            heartbeat.put("timestamp", System.currentTimeMillis());
            heartbeat.put("status", "alive");
            
            jedis.publish(HEARTBEAT_CHANNEL, heartbeat.toString());
            // No logging for heartbeat

        } catch (Exception e) {
            // Only log if error occurs
            LOGGER.error("Failed to publish heartbeat: ", e);
        }
    }
    
    /**
     * Start the order command listener
     */
    private void startOrderCommandListener() {
        executorService.submit(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.subscribe(new OrderCommandSubscriber(), ORDER_COMMAND_CHANNEL);
            } catch (Exception e) {
                LOGGER.error("Failed to start order command listener: ", e);
            }
        });
    }
    
    /**
     * Order command subscriber
     */
    private class OrderCommandSubscriber extends JedisPubSub {
        @Override
        public void onMessage(String channel, String message) {
            try {
                // Clean and validate the JSON message
                String cleanedMessage = message.trim();
                
                // Remove surrounding quotes if present
                if (cleanedMessage.startsWith("'") && cleanedMessage.endsWith("'")) {
                    cleanedMessage = cleanedMessage.substring(1, cleanedMessage.length() - 1);
                }
                if (cleanedMessage.startsWith("\"") && cleanedMessage.endsWith("\"")) {
                    cleanedMessage = cleanedMessage.substring(1, cleanedMessage.length() - 1);
                }
                
                // Validate JSON format
                if (!cleanedMessage.startsWith("{") || !cleanedMessage.endsWith("}")) {
                    LOGGER.error("Invalid JSON format received");
                    return;
                }

                JSONObject command = new JSONObject(cleanedMessage);
                String type = command.getString("type");
                
                if ("create_order".equals(type)) {
                    handleCreateOrderCommand(command);
                } else if ("cancel_order".equals(type)) {
                    handleCancelOrderCommand(command);
                } else {
                    LOGGER.warn("Unknown command type: {}", type);
                }
                
            } catch (Exception e) {
                LOGGER.error("Failed to process order command: ", e);
            }
        }
        
        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            // 静默操作 - 不输出日志
        }
        
        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            // 静默操作 - 不输出日志
        }
    }
    
    /**
     * Handle create order command
     */
    private void handleCreateOrderCommand(JSONObject command) {
        try {
            // 必需参数
            String instrument = command.getString("instrument");
            String side = command.getString("side");
            double quantity = command.getDouble("quantity");
            double price = command.getDouble("price");  // 价格是必需的
            String venue = command.getString("venue");   // 场地是必需的
            
            LOGGER.info("Order command received: {} {} {} @ {} on {}", 
                side, quantity, instrument, price, venue);
            
            // 验证价格
            if (price <= 0) {
                throw new IllegalArgumentException("Price must be positive");
            }
            
            // 验证场地
            if (venue == null || venue.trim().isEmpty()) {
                throw new IllegalArgumentException("Venue must be specified");
            }
            
            // 创建订单
            boolean success = createRealOrder(instrument, side, quantity, price, venue);
            
            if (success) {
                // 临时订单ID
                String tempOrderId = "REDIS_" + System.currentTimeMillis();
                publishOrderResponse(tempOrderId, 
                    "SUBMITTED", "Order submitted successfully", 
                    venue, 
                    System.currentTimeMillis());
            } else {
                publishOrderResponse("ERROR_" + System.currentTimeMillis(), 
                    "FAILED", "Order creation failed", 
                    venue,
                    System.currentTimeMillis());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to process create order command: ", e);
            publishOrderResponse("ERROR_" + System.currentTimeMillis(),
                "FAILED", "Order creation failed: " + e.getMessage(), "UNKNOWN",
                System.currentTimeMillis());
        }
    }
    
    /**
     * Create real order 直接用价格
     */
    private boolean createRealOrder(String instrument, String side, double quantity, double price, String venue) {
        try {
            // 直接调用 MultiVenueOrderCreator指定的价格
            boolean success = MultiVenueOrderCreator.createOrderFromRedis(
                instrument, side, quantity, price, venue);
            
            if (!success) {
                LOGGER.error("Order submission to venue failed");
            }
            
            return success;
            
        } catch (Exception e) {
            LOGGER.error("Error creating order: ", e);
            return false;
        }
    }
    
    /**
     * Handle cancel order command
     */
    private void handleCancelOrderCommand(JSONObject command) {
        try {
            String orderId = command.getString("order_id");
            String venue = command.getString("venue");  // 场地是必需的
            
            LOGGER.info("Cancel order request: {} on {}", orderId, venue);
            
            // 验证场地
            if (venue == null || venue.trim().isEmpty()) {
                throw new IllegalArgumentException("Venue must be specified");
            }
            
            // Call MultiVenueOrderCreator to handle cancellation
            boolean success = MultiVenueOrderCreator.cancelOrder(orderId, venue);
            
            if (success) {
                publishOrderResponse(orderId, "CANCEL_PENDING", 
                    "Cancel request sent", 
                    venue, 
                    System.currentTimeMillis());
            } else {
                publishOrderResponse(orderId, "CANCEL_FAILED", 
                    "Cancel request failed", 
                    venue, 
                    System.currentTimeMillis());
            }
            
            LOGGER.info("----");
                
        } catch (Exception e) {
            LOGGER.error("Failed to process cancel order command: ", e);
            publishOrderResponse("ERROR_" + System.currentTimeMillis(), 
                "FAILED", "Order cancellation failed: " + e.getMessage(), "UNKNOWN",
                System.currentTimeMillis());
        }
    }
    
    /**
     * Test Redis connection - silent when successful
     */
    public boolean testConnection() {
        try (Jedis jedis = jedisPool.getResource()) {
            String response = jedis.ping();
            boolean isConnected = "PONG".equals(response);
            if (!isConnected) {
                LOGGER.warn("Redis connection test failed: {}", response);
            }
            return isConnected;
        } catch (Exception e) {
            LOGGER.error("Failed to test Redis connection: ", e);
            return false;
        }
    }
}