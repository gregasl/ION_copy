package com.iontrading.samples.simplePermChainSubscriber.config;

import redis.clients.jedis.Jedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisConfig {
    private static final Logger logger = LoggerFactory.getLogger(RedisConfig.class);
    private static final String REDIS_HOST = "cacheuat";
    private static final int REDIS_PORT = 6379;
    private static final int TIMEOUT = 5000;

    public Jedis getJedisClient() {
        try {
            String hostname = System.getenv("COMPUTERNAME");
            String connectionString = String.format("%s:%d", REDIS_HOST, REDIS_PORT);
            String clientName = String.format("%s:ION_MARKET_DATA", hostname);
            
            Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT, TIMEOUT);
            jedis.clientSetname(clientName);
            jedis.ping(); // Test connection

            logger.info("Successfully connected to Redis at {}:{}", REDIS_HOST, REDIS_PORT);
            return jedis;
        } catch (Exception e) {
            logger.error("Failed to connect to Redis: {}", e.getMessage());
            throw new RuntimeException("Redis connection failed", e);
        }
    }
}