package com.iontrading.redisbridge.redis;

import com.iontrading.redisbridge.config.ConfigurationManager;
import com.iontrading.redisbridge.util.JsonUtils;
import com.iontrading.redisbridge.util.LoggingUtils;
import java.util.HashMap;
import java.util.Map;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisPublisher {
    
    private static RedisPublisher instance;
    private final ConfigurationManager config;
    private final Object redisConnection;
    private JedisPool jedisPool;
    
    public static synchronized RedisPublisher getInstance(Object redisConnection) {
        if (instance == null) {
            instance = new RedisPublisher(redisConnection);
        }
        return instance;
    }
    
    private RedisPublisher(Object redisConnection) {
        this.redisConnection = redisConnection;
        this.config = ConfigurationManager.getInstance();
        
        // 初始化 Redis 连接池
        try {
            String host = config.getRedisHost();
            int port = config.getRedisPort();
            this.jedisPool = new JedisPool(host, port);
            
            // 测试连接
            try (Jedis jedis = jedisPool.getResource()) {
                String pong = jedis.ping();
                System.out.println("RedisPublisher: Connected to Redis at " + host + ":" + port + " (ping: " + pong + ")");
            }
        } catch (Exception e) {
            System.err.println("RedisPublisher: Failed to connect to Redis: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public long publishOrderMessage(String message) {
        if (message == null) {
            return 0;
        }
        
        try {
            String channel = config.getRedisOrdersChannel();
            System.out.println("Publishing to orders channel: " + message);
            
            // 真正发布到 Redis
            if (jedisPool != null) {
                try (Jedis jedis = jedisPool.getResource()) {
                    long result = jedis.publish(channel, message);
                    System.out.println("REDIS: Published to " + channel + " - " + result + " subscribers received");
                    return result;
                }
            } else {
                System.out.println("REDIS: Connection not available - message not sent");
                return 0;
            }
        } catch (Exception e) {
            System.err.println("Error publishing order message: " + e.getMessage());
            e.printStackTrace();
            return 0;
        }
    }
    
    public long publishAdminMessage(String message) {
        if (message == null) {
            return 0;
        }
        
        try {
            String channel = config.getRedisAdminChannel();
            System.out.println("Publishing to admin channel: " + message);
            
            // 真正发布到 Redis
            if (jedisPool != null) {
                try (Jedis jedis = jedisPool.getResource()) {
                    long result = jedis.publish(channel, message);
                    System.out.println("REDIS: Published to " + channel + " - " + result + " subscribers received");
                    return result;
                }
            } else {
                System.out.println("REDIS: Connection not available - message not sent");
                return 0;
            }
        } catch (Exception e) {
            System.err.println("Error publishing admin message: " + e.getMessage());
            e.printStackTrace();
            return 0;
        }
    }
    
    public long publishHeartbeatMessage(String message) {
        if (message == null) {
            return 0;
        }
        
        try {
            String channel = config.getRedisHeartbeatChannel();
            
            // 真正发布到 Redis
            if (jedisPool != null) {
                try (Jedis jedis = jedisPool.getResource()) {
                    long result = jedis.publish(channel, message);
                    System.out.println("REDIS: Published to " + channel + " - " + result + " subscribers received");
                    return result;
                }
            } else {
                System.out.println("REDIS: Connection not available - message not sent");
                return 0;
            }
        } catch (Exception e) {
            System.err.println("Error publishing heartbeat message: " + e.getMessage());
            e.printStackTrace();
            return 0;
        }
    }
    
    public long publishOrderResponse(String clientOrderId, boolean success, String message, Map<String, Object> details) {
        Map<String, Object> response = new HashMap<>();
        response.put("type", "response");
        response.put("clientOrderId", clientOrderId);
        response.put("success", success);
        response.put("message", message);
        response.put("details", details);
        response.put("timestamp", LoggingUtils.getCurrentTimestamp());
        
        return publishOrderMessage(JsonUtils.toJson(response));
    }
    
    public long publishHeartbeat(String status) {
        Map<String, Object> heartbeat = new HashMap<>();
        heartbeat.put("type", "heartbeat");
        heartbeat.put("status", status);
        heartbeat.put("timestamp", LoggingUtils.getCurrentTimestamp());
        
        return publishHeartbeatMessage(JsonUtils.toJson(heartbeat));
    }
    
    public long publishError(int code, String message, Map<String, Object> details) {
        Map<String, Object> error = new HashMap<>();
        error.put("type", "error");
        error.put("code", code);
        error.put("message", message);
        error.put("details", details);
        error.put("timestamp", LoggingUtils.getCurrentTimestamp());
        
        return publishAdminMessage(JsonUtils.toJson(error));
    }
}