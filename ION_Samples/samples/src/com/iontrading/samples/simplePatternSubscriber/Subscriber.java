package com.iontrading.samples.simplePatternSubscriber;

import com.iontrading.mkv.*;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.util.Map;
import java.io.IOException;

public class Subscriber {
    // Static logger configuration
    private static final Logger LOGGER = Logger.getLogger(Subscriber.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // Static block for logging setup
    static {
        try {
            FileHandler fileHandler = new FileHandler("subscriber.log", true);
            fileHandler.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(fileHandler);
        } catch (IOException e) {
            System.err.println("Error setting up file logging: " + e.getMessage());
        }
    }
    
    // Configuration constants
    private static final String SOURCE = "VMO_REPO_US";
    private static final String PATTERN = "USD.CM_DEPTH." + SOURCE + ".";
    private static final String REDIS_CHANNEL = "ION_MARKET_DATA";
    private static final String REDIS_HOST = "cacheuat";
    private static final int REDIS_PORT = 6379;
    private static final int TIMEOUT = 5000;
    private static final int EXPIRY_SECONDS = 86400; // 24 hours

    // Concurrent data structures
    private Map<String, Object> marketData = new HashMap<>();

    // Fields for subscription
    private String[] fields = new String[]{
        "Bid0", "Bid1", "Bid2", "Bid3", "Bid4", "Bid5", "Bid6", "Bid7", "Bid8", "Bid9",
        "Ask0", "Ask1", "Ask2", "Ask3", "Ask4", "Ask5", "Ask6", "Ask7", "Ask8", "Ask9",
        "AskSize0", "BidSize0", "AskSize1", "BidSize1", "AskSize2", "BidSize2", "AskSize3", "BidSize3", 
        "AskSize4", "BidSize4", "AskSize5", "BidSize5", "AskSize6", "BidSize6", "AskSize7", "BidSize7", 
        "AskSize8", "BidSize8", "AskSize9", "BidSize9", "AskMrk0","AskMrk1", "AskMrk2","AskMrk3","AskMrk4",
        "AskMrk5","AskMrk6","AskMrk7","AskMrk8","AskMrk9","BidMrk0","BidMrk1","BidMrk2","BidMrk3","BidMrk4",
        "BidMrk5","BidMrk6","BidMrk7","BidMrk8","BidMrk9","TrdValueLast","TrdValueAvg","VolumeMarket"
    };
    
    // Thread pools and control flags
    private final ExecutorService publishExecutor;
    private final ExecutorService processingExecutor;
    private volatile boolean running = true;
    private boolean isRedisConnected = false;
    
    // Redis connection
    private Jedis jedis;

    // Initialize market data with zeros
    private void initializeMarketData() {
        marketData.clear();
        for (String field : fields) {
            marketData.put(field, "0");
        }
    }
    
    // Constructor
    public Subscriber(String[] args) {
        initializeMarketData();
        publishExecutor = createExecutorService("Redis-Publisher");
        processingExecutor = createExecutorService("MKV-Processor");

        initializeRedisConnection();
        initializeMkvConnection(args);

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    // Create executor service with named threads
    private ExecutorService createExecutorService(String threadNamePrefix) {
        return Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName(threadNamePrefix + "-" + t.getId());
                return t;
            }
        );
    }

    // Initialize Redis connection
    private void initializeRedisConnection() {
        if (!isRedisConnected) {
            try {
                jedis = new Jedis(REDIS_HOST, REDIS_PORT, TIMEOUT);
                LOGGER.info("Connected to Redis at " + REDIS_HOST + ":" + REDIS_PORT);
                isRedisConnected = true;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error connecting to Redis", e);
                throw new RuntimeException("Redis connection failed", e);
            }
        }
    }

    // Initialize MKV connection
    private void initializeMkvConnection(String[] args) {
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        qos.setPlatformListeners(new MkvPlatformListener[] { new PlatformListener() });

        try {
            Mkv.start(qos);
            LOGGER.info("MKV connection initialized.");
        } catch (MkvException e) {
            LOGGER.log(Level.SEVERE, "MKV connection failed", e);
            throw new RuntimeException("MKV initialization failed", e);
        }
    }

    // Publish method with JSON support
    public void publishToRedis(String key, String recordName, Map<String, Object> data) {
        try {
            // Create a comprehensive payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("timestamp", Instant.now().toEpochMilli());
            payload.put("source", SOURCE);
            payload.put("instrument", recordName);
            payload.putAll(data);

            // Convert payload to JSON string
            String jsonPayload = OBJECT_MAPPER.writeValueAsString(payload);

            // Publish to Redis
            jedis.setex(key, EXPIRY_SECONDS, jsonPayload);
            jedis.publish(key, jsonPayload);

            // LOGGER.info("Published data to Redis - Key: " + key);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error publishing to Redis", e);
        }
    }
    
    // Data Listener inner class
    private class DataListener implements MkvRecordListener {
        
        private final Map<String, Map<String, Object>> recordDataMap = new ConcurrentHashMap<>(); // Store full records per RECORDNAME
        
        // Convert fields array to a List for easier checking
        private final List<String> fieldsList = Arrays.asList(fields);

        public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
            updateRecord(mkvRecord, mkvSupply);
        }

        public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
            updateRecord(mkvRecord, mkvSupply);
        }

        private void updateRecord(MkvRecord mkvRecord, MkvSupply mkvSupply) {
            try {
                String recordName = mkvRecord.getName();
                int cursor = mkvSupply.firstIndex();

                // Retrieve or initialize record storage
                recordDataMap.putIfAbsent(recordName, new HashMap<>());
                Map<String, Object> recordData = recordDataMap.get(recordName);

                // Process updates
                while (cursor != -1) {
                    String fieldName = mkvRecord.getMkvType().getFieldName(cursor);
                    Object fieldValue = mkvSupply.getObject(cursor);
                    
                    // Only update if the field is in the subscribed fields list
                    if (fieldsList.contains(fieldName)) {
                        recordData.put(fieldName, fieldValue != null ? fieldValue : "null");
                    }

                    cursor = mkvSupply.nextIndex(cursor);
                }

                // Publish the updated record to Redis
                String key = REDIS_CHANNEL + ":" + recordName;
                publishToRedis(key, recordName, recordData);

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error processing market data update", e);
            }
        }
    }

    // Platform Listener inner class
    private class PlatformListener implements MkvPlatformListener {
        public void onComponent(MkvComponent component, boolean start) {}
        public void onConnect(String component, boolean start) {}

        public void onMain(MkvPlatformEvent event) {
            if (event.equals(MkvPlatformEvent.START)) {
                Mkv.getInstance().getPublishManager().addPublishListener(new PublishListener());
            }
        }
    }

    // Publish Listener inner class
    private class PublishListener implements MkvPublishListener {
        public void onPublish(MkvObject mkvObject, boolean start, boolean download) {
            if (start && !download && mkvObject.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                try {
                    MkvRecordListener listener = new DataListener();
                    ((MkvPattern) mkvObject).subscribe(fields, listener);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error subscribing to market data", e);
                }
            }
        }

        public void onPublishIdle(String component, boolean start) {
            MkvObject mkvObj = Mkv.getInstance().getPublishManager().getMkvObject(PATTERN);
            if (mkvObj != null && mkvObj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                try {
                    MkvRecordListener listener = new DataListener();
                    ((MkvPattern) mkvObj).subscribe(fields, listener);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error subscribing during idle event", e);
                }
            }
        }

        public void onSubscribe(MkvObject mkvObject) {}
    }

    // Shutdown method
    public void shutdown() {
        running = false;
        if (jedis != null) {
            jedis.close();
        }
        publishExecutor.shutdown();
        processingExecutor.shutdown();
        LOGGER.info("Subscriber shutdown complete.");
    }

    // Main method
    public static void main(String[] args) {
        Subscriber subscriber = new Subscriber(new String[] {}); // Passing an empty array
        subscriber.initializeRedisConnection(); 
    }
}