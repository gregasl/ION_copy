package com.iontrading.redisbridge.redis;

import com.iontrading.redisbridge.admin.AdminCommandHandler;
import com.iontrading.redisbridge.config.ConfigurationManager;
import com.iontrading.redisbridge.ion.MarketDef;
import com.iontrading.redisbridge.order.OrderProcessor;
import com.iontrading.redisbridge.util.JsonUtils;
import com.iontrading.redisbridge.util.LoggingUtils;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisPool;

/**
 * Listens for messages from Redis.
 * This class is responsible for listening for messages from Redis.
 */
public class RedisListener {
    
    // Singleton instance
    private static RedisListener instance;
    
    // Reference to ConfigurationManager
    private final ConfigurationManager config;
    
    // Reference to RedisPublisher
    private final RedisPublisher publisher;
    
    // Reference to OrderProcessor
    private final OrderProcessor orderProcessor;
    
    // Reference to AdminCommandHandler
    private final AdminCommandHandler adminHandler;
    
    // Redis connection object
    private final Object redisConnection;
    
    // Jedis connection pool
    private JedisPool jedisPool;
    
    // Executor service for running subscribers
    private final ExecutorService executorService;
    
    // Running flag
    private volatile boolean running;
    
    /**
     * Gets the singleton instance of RedisListener.
     */
    public static synchronized RedisListener getInstance(Object redisConnection, OrderProcessor orderProcessor, AdminCommandHandler adminHandler) {
        if (instance == null) {
            instance = new RedisListener(redisConnection, orderProcessor, adminHandler);
        }
        return instance;
    }
    
    /**
     * Private constructor to enforce singleton pattern.
     */
    private RedisListener(Object redisConnection, OrderProcessor orderProcessor, AdminCommandHandler adminHandler) {
        this.redisConnection = redisConnection;
        this.config = ConfigurationManager.getInstance();
        this.publisher = RedisPublisher.getInstance(redisConnection);
        this.orderProcessor = orderProcessor;
        this.adminHandler = adminHandler;
        this.executorService = Executors.newFixedThreadPool(2);
        this.running = false;
        
        // 初始化 JedisPool
        try {
            this.jedisPool = new JedisPool(config.getRedisHost(), config.getRedisPort());
        } catch (Exception e) {
            System.err.println("Failed to initialize Jedis pool: " + e.getMessage());
        }
    }
    
    /**
     * Starts the Redis listener.
     */
    public void start() {
        if (running) {
            return;
        }
        
        running = true;
        
        // Start order channel subscriber
        executorService.submit(new OrderChannelSubscriber());
        
        // Start admin channel subscriber
        executorService.submit(new AdminChannelSubscriber());
    }
    
    /**
     * Stops the Redis listener.
     */
    public void stop() {
        if (!running) {
            return;
        }
        
        running = false;
        
        // Shutdown executor service
        executorService.shutdown();
        
        // Close Jedis pool
        if (jedisPool != null) {
            jedisPool.close();
        }
        
        System.out.println("Redis listener stopped");
    }
    
    /**
     * Checks if the Redis listener is running.
     */
    public boolean isRunning() {
        return running;
    }
    
    /**
     * Processes an order message.
     */
    private void processOrderMessage(String message) {
        try {
            Map<String, Object> orderMessage = JsonUtils.fromJson(message);
            String type = (String) orderMessage.get("type");
            if ("response".equals(type) || "error".equals(type)) {
                return;  // 静默忽略响应消息
            }
            String commandType = (String) orderMessage.get("CommandType");
            
            if (commandType == null) {
                System.err.println("Invalid order message: missing CommandType");
                return;
            }

            switch (commandType) {
                case "OrderCreate":
                    orderProcessor.processNewOrder(orderMessage);
                    break;
                case "OrderCancel":
                    orderProcessor.processCancelOrder(orderMessage);
                    break;
                default:
                    System.err.println("Unknown order message type: " + commandType);
                    break;
            }
        } catch (Exception e) {
            System.err.println("Error processing order message: " + e.getMessage());
            publisher.publishError(
                MarketDef.ERROR_INTERNAL_ERROR,
                "Error processing order message: " + e.getMessage(),
                LoggingUtils.createDetailsMap("message", message));
        }
    }
    
    /**
     * Processes an admin message.
     */
    private void processAdminMessage(String message) {
        try {
            Map<String, Object> adminMessage = JsonUtils.fromJson(message);
            String command = (String) adminMessage.get("command");
            
            if (command != null) {
                adminHandler.handleCommand(command, adminMessage);
            }
        } catch (Exception e) {
            System.err.println("Error processing admin message: " + e.getMessage());
        }
    }
    
    /**
     * Order channel subscriber.
     */
    private class OrderChannelSubscriber implements Runnable {
        @Override
        public void run() {
            
            if (jedisPool == null) {
                System.err.println("Jedis pool not initialized, running in simulation mode");
                runSimulation();
                return;
            }
            
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                String channel = config.getRedisOrdersChannel();
                
                jedis.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        System.out.println("Received order message: " + message);
                        processOrderMessage(message);
                    }
                }, channel);
            } catch (Exception e) {
                System.err.println("Error in order subscriber: " + e.getMessage());
                runSimulation();
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
        
        private void runSimulation() {
            while (running) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
    
    /**
     * Admin channel subscriber.
     */
    private class AdminChannelSubscriber implements Runnable {
        @Override
        public void run() {
            
            if (jedisPool == null) {
                System.err.println("Jedis pool not initialized, running in simulation mode");
                runSimulation();
                return;
            }
            
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                String channel = config.getRedisAdminChannel();
                
                jedis.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        System.out.println("Received admin message: " + message);
                        processAdminMessage(message);
                    }
                }, channel);
            } catch (Exception e) {
                System.err.println("Error in admin subscriber: " + e.getMessage());
                runSimulation();
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
        
        private void runSimulation() {
            while (running) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}