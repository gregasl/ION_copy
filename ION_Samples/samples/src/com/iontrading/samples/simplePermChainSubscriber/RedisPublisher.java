package com.iontrading.samples.simplePermChainSubscriber;

import redis.clients.jedis.Jedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RedisPublisher {
    private static final Logger logger = LoggerFactory.getLogger(RedisPublisher.class);
    private static final int EXPIRY_SECONDS = 86400; // 24 hours
    private final Jedis jedis;
    private final String publishChannel;
    private final ExecutorService executor;

    public RedisPublisher(Jedis jedis, String channel) {
        this.jedis = jedis;
        this.publishChannel = channel;
        this.executor = Executors.newSingleThreadExecutor();
    }

    public void publishMessageAsync(String channel, String message) {
        try {
            executor.submit(() -> {
                try {
                    // Store message with 24-hour expiry
                    jedis.setex(channel, EXPIRY_SECONDS, message);
                    logger.info("Stored message for channel {} with 24-hour expiry", channel);

                    // Publish message
                    jedis.publish(channel, message);
                    logger.info("Published message to channel {}", channel);
                } catch (Exception e) {
                    logger.error("Failed to publish/store message: {}", e.getMessage());
                }
            });
        } catch (Exception e) {
            logger.error("Failed to submit publish task: {}", e.getMessage());
        }
    }

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        if (jedis != null) {
            jedis.close();
        }
    }
}