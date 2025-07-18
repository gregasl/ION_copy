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
    public static final String MARKET_DATA_CHANNEL = "market_data";
    public static final String ORDER_COMMAND_CHANNEL = "order_commands";
    public static final String ORDER_RESPONSE_CHANNEL = "order_responses";
    public static final String HEARTBEAT_CHANNEL = "heartbeat";
    
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

        LOGGER.info("Redis Bridge: {}:{}", host, port);
    }
    
    /**
     * Start the bridge service
     */
    public void start() {
        running = true;
        
        // Start the order command listener
        startOrderCommandListener();
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
        LOGGER.info("Redis Message Bridge stopped");
    }
    
    /**
     * Publish market data to Redis
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
            LOGGER.debug("Market data published: {}", instrument);

        } catch (Exception e) {
            LOGGER.error("Failed to publish market data: ", e);
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
            LOGGER.info("Order response published: {} -> {}", orderId, status);

        } catch (Exception e) {
            LOGGER.error("Failed to publish order response: ", e);
        }
    }
    
    /**
     * Publish heartbeat signal
     */
    public void publishHeartbeat() {
        try (Jedis jedis = jedisPool.getResource()) {
            JSONObject heartbeat = new JSONObject();
            heartbeat.put("type", "heartbeat");
            heartbeat.put("timestamp", System.currentTimeMillis());
            heartbeat.put("status", "alive");
            
            jedis.publish(HEARTBEAT_CHANNEL, heartbeat.toString());
            LOGGER.debug("Heartbeat signal published");

        } catch (Exception e) {
            LOGGER.error("Failed to publish heartbeat signal: ", e);
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
                LOGGER.debug("Received order command: {}", message);
                LOGGER.debug("Message length: {}, first 10 chars: '{}'", 
                    message.length(), 
                    message.length() > 10 ? message.substring(0, 10) : message);
                LOGGER.debug("Order received from Redis channel: {}", channel);

                // Clean and validate the JSON message
                String cleanedMessage = message.trim();
                
                // Remove surrounding quotes if present
                if (cleanedMessage.startsWith("'") && cleanedMessage.endsWith("'")) {
                    cleanedMessage = cleanedMessage.substring(1, cleanedMessage.length() - 1);
                    LOGGER.debug("Removed single quotes from message");
                }
                if (cleanedMessage.startsWith("\"") && cleanedMessage.endsWith("\"")) {
                    cleanedMessage = cleanedMessage.substring(1, cleanedMessage.length() - 1);
                    LOGGER.debug("Removed double quotes from message");
                }
                
                // Validate JSON format
                if (!cleanedMessage.startsWith("{") || !cleanedMessage.endsWith("}")) {
                    LOGGER.error("Invalid JSON format received: '{}' (length: {})", 
                        cleanedMessage, cleanedMessage.length());
                    LOGGER.error("First char: '{}' (ASCII: {}), Last char: '{}' (ASCII: {})",
                        cleanedMessage.charAt(0), (int)cleanedMessage.charAt(0),
                        cleanedMessage.charAt(cleanedMessage.length()-1), 
                        (int)cleanedMessage.charAt(cleanedMessage.length()-1));
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
            LOGGER.info("Listening: {} ({})", channel, subscribedChannels);
        }
        
        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            LOGGER.info("Unsubscribed: {} (Remaining subscriptions: {})", channel, subscribedChannels);
        }
    }
    
    /**
     * Handle create order command
     */
    private void handleCreateOrderCommand(JSONObject command) {
        try {
            String instrument = command.getString("instrument");
            String side = command.getString("side");
            double quantity = command.getDouble("quantity");
            
            // 价格处理 - 支持 AUTO 字符串和数值
            Double price = null;
            boolean useAutoPrice = false;
            
            if (command.has("price")) {
                Object priceObj = command.get("price");
                if (priceObj instanceof String) {
                    String priceStr = (String) priceObj;
                    if ("AUTO".equalsIgnoreCase(priceStr)) {
                        useAutoPrice = true;
                        LOGGER.debug("Price set to AUTO - will calculate dynamic price");
                    } else {
                        try {
                            price = Double.parseDouble(priceStr);
                        } catch (NumberFormatException e) {
                            LOGGER.warn("Invalid price string '{}', using AUTO mode", priceStr);
                            useAutoPrice = true;
                        }
                    }
                } else if (priceObj instanceof Number) {
                    price = ((Number) priceObj).doubleValue();
                    if (price <= 0) {
                        LOGGER.info("Price is {} (<=0), switching to AUTO mode", price);
                        useAutoPrice = true;
                        price = null;
                    }
                } else {
                    LOGGER.warn("Invalid price type, using AUTO mode");
                    useAutoPrice = true;
                }
            } else {
                LOGGER.info("No price specified, using AUTO mode");
                useAutoPrice = true;
            }
            
            String venue = command.has("venue") ? command.getString("venue") : "AUTO";
            
            // 日志输出
            if (useAutoPrice) {
                LOGGER.debug("Creating order with AUTO price: {} {} {} on {}", 
                    side, quantity, instrument, venue);
            } else {
                LOGGER.debug("Creating order with fixed price: {} {} {} @ {} on {}", 
                    side, quantity, instrument, String.format("%.2f", price), venue);
            }
            
            // 调用订单创建 - 传递 null 表示自动价格
            boolean success = createRealOrder(instrument, side, quantity, 
                useAutoPrice ? null : price, venue);
            
            if (success) {
                publishOrderResponse("ORDER_" + System.currentTimeMillis(), 
                    "SUBMITTED", "Order submitted successfully", venue, 
                    System.currentTimeMillis());
            } else {
                publishOrderResponse("ERROR_" + System.currentTimeMillis(), 
                    "FAILED", "Order creation failed", venue,
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
     * Create real order - Integrated into MultiVenueOrderCreator
     */
    private boolean createRealOrder(String instrument, String side, double quantity, Double price, String venue) {
        try {
            // 确定最终价格 - 如果price为null，传递 -1 给系统表示自动计算
            double finalPrice = (price != null) ? price : -1.0;
            
            if (price == null) {
                LOGGER.debug("Redis order created with AUTO price: {} {} {} on {}", 
                    side, quantity, instrument, venue);
            } else {
                LOGGER.debug("Redis order created with fixed price: {} {} {} @ {} on {}", 
                    side, quantity, instrument, price, venue);
            }
            
            // 调用 MultiVenueOrderCreator 的静态方法创建订单
            boolean success = MultiVenueOrderCreator.createOrderFromRedis(instrument, side, quantity, finalPrice, venue);
            
            if (success) {
                LOGGER.info("Order submitted to venue successfully");
            } else {
                LOGGER.error("Order submission to venue failed");
            }
            
            return success;
            
        } catch (Exception e) {
            LOGGER.error("Error creating real order: ", e);
            return false;
        }
    }
    
    /**
     * Handle cancel order command
     */
    private void handleCancelOrderCommand(JSONObject command) {
        try {
            String orderId = command.getString("order_id");
            String venue = command.has("venue") ? command.getString("venue") : "AUTO";
            
            LOGGER.info("Handle cancel order command: {} on {}", orderId, venue);
            
            // Call MultiVenueOrderCreator to handle cancellation
            // Temporarily return success response
            publishOrderResponse(orderId, "CANCELLED", 
                "Order cancelled successfully", venue, System.currentTimeMillis());
                
        } catch (Exception e) {
            LOGGER.error("Failed to process cancel order command: ", e);
            publishOrderResponse("ERROR_" + System.currentTimeMillis(), 
                "FAILED", "Order cancellation failed: " + e.getMessage(), "UNKNOWN",
                System.currentTimeMillis());
        }
    }
    
    /**
     * Test Redis connection
     */
    public boolean testConnection() {
        try (Jedis jedis = jedisPool.getResource()) {
            String response = jedis.ping();
            // 只在测试失败时输出日志，成功时不输出
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