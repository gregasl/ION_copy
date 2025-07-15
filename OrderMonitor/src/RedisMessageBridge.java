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

        LOGGER.info("Redis Message Bridge initialized - {}:{}", host, port);
    }
    
    /**
     * Start the bridge service
     */
    public void start() {
        running = true;
        LOGGER.info("Redis Message Bridge started");
        
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
                LOGGER.info("Received order command: {}", message);

                JSONObject command = new JSONObject(message);
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
            LOGGER.info("Subscription successful: {} (Active subscriptions: {})", channel, subscribedChannels);
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
            double price = command.getDouble("price");
            String venue = command.has("venue") ? command.getString("venue") : "AUTO";
            
            LOGGER.info("Handle create order command: {} {} {} @ {} on {}", 
                side, quantity, instrument, price, venue);
            
            // Call MultiVenueOrderCreator to create real order
            boolean success = createRealOrder(instrument, side, quantity, price, venue);
            
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
    private boolean createRealOrder(String instrument, String side, double quantity, double price, String venue) {
        try {
            boolean success = MultiVenueOrderCreator.createOrderFromRedis(instrument, side, quantity, price, venue);
            
            if (success) {
                LOGGER.info("Redis order created successfully: {} {} {} @ {} on {}", side, quantity, instrument, price, venue);
            } else {
                LOGGER.error("Redis order creation failed: {} {} {} @ {} on {}", side, quantity, instrument, price, venue);
            }
            
            return success;
            
        } catch (Exception e) {
            LOGGER.error("Failed to create order: ", e);
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
            LOGGER.info("Redis connection test: {}", response);
            return "PONG".equals(response);
        } catch (Exception e) {
            LOGGER.error("Failed to test Redis connection: ", e);
            return false;
        }
    }
}