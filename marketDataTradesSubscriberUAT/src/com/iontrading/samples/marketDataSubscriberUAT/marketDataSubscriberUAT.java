package com.iontrading.samples.marketDataSubscriberUAT;

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
import java.util.logging.*;
import java.util.Map;
import java.io.IOException;

import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class marketDataSubscriberUAT {
    // Static logger configuration
    private static final Logger LOGGER = Logger.getLogger(marketDataSubscriberUAT.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // Static block for logging setup with async handler
    static {
        try {
            // Create an async logger with a separate thread
            AsyncHandler asyncHandler = new AsyncHandler(500000); // Queue size of 10000 logs
            
            // Create a file handler and attach to async handler
            FileHandler fileHandler = new FileHandler("subscriber.log", true);
            fileHandler.setFormatter(new SimpleFormatter());
            asyncHandler.setHandler(fileHandler);
            
            // Set async handler to the logger
            LOGGER.addHandler(asyncHandler);
            
            // Set the logger level
            LOGGER.setLevel(Level.INFO);
            
            LOGGER.info("Async logging initialized");
        } catch (IOException e) {
            System.err.println("Error setting up file logging: " + e.getMessage());
        }
    }
    
    // Custom AsyncHandler implementation for asynchronous logging
    static class AsyncHandler extends Handler {
        private final BlockingQueue<LogRecord> queue;
        private Handler handler;
        private final Thread worker;
        private volatile boolean isRunning = true;

        public AsyncHandler(int queueSize) {
            this.queue = new LinkedBlockingQueue<>(queueSize);
            
            worker = new Thread(() -> {
                while (isRunning) {
                    try {
                        LogRecord record = queue.poll(20, TimeUnit.MILLISECONDS);
                        if (record != null && handler != null) {
                            handler.publish(record);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        // Prevent the worker from dying due to unexpected exceptions
                        System.err.println("Error in async logger: " + e.getMessage());
                    }
                }
                // Flush remaining records
                flush();
            });
            
            worker.setDaemon(true);
            worker.setName("AsyncLogger-Worker");
            worker.start();
        }

        public void setHandler(Handler handler) {
            this.handler = handler;
            if (handler != null) {
                handler.setErrorManager(new ErrorManager());
            }
        }

        @Override
        public void publish(LogRecord record) {
            if (!isLoggable(record)) {
                return;
            }
            
            try {
                // Wait up to 100ms to add to queue
                boolean added = queue.offer(record, 100, TimeUnit.MILLISECONDS);
                if (!added) {
                    // Log to stderr when queue is full
                    System.err.println("Async logger queue full, dropping log record");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void flush() {
            if (handler != null) {
                handler.flush();
            }
        }

        @Override
        public void close() throws SecurityException {
            isRunning = false;
            try {
                worker.join(5000); // Wait up to 5 second for worker to finish
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            if (handler != null) {
                handler.close();
            }
        }
    }
    
    // Configuration constants
    private static final String SOURCE = "VMO_REPO_US";
    private static final String PATTERN = "USD.CM_DEPTH." + SOURCE + ".";
    private static final String PATTERNINSTRUMENT = "USD.CM_INSTRUMENT." + SOURCE + ".";
    private static final String PATTERNTRADE = "USD.CM_TRADE." + SOURCE + ".";
    private static final String REDIS_CHANNEL = "ION:ION_MARKET_DATA_UAT";
    private static final String REDIS_CHANNEL_INSTRUMENT = "ION:ION_INSTRUMENT_DATA_UAT";
    private static final String REDIS_CHANNEL_TRADE = "ION:ION_TRADE_DATA_UAT";
    private static final String REDIS_HOST = "cacheuat";
    private static final int REDIS_PORT = 6379;
    private static final int TIMEOUT = 5000;
    private static final int EXPIRY_SECONDS = 86400; // 24 hours

    private volatile long lastUpdateTimestamp = System.currentTimeMillis();
    private final ScheduledExecutorService shutdownScheduler = Executors.newScheduledThreadPool(1);
    
    // Add a latch to keep main thread alive
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    
    // Concurrent data structures
    private Map<String, Object> marketData = new HashMap<>();

    // Fields for subscription
    private String[] fields = new String[]{
        "Id", "Bid0", "Bid1", "Bid2", "Bid3", "Bid4", "Bid5", "Bid6", "Bid7", "Bid8", "Bid9",
        "Ask0", "Ask1", "Ask2", "Ask3", "Ask4", "Ask5", "Ask6", "Ask7", "Ask8", "Ask9",
        "AskSize0", "BidSize0", "AskSize1", "BidSize1", "AskSize2", "BidSize2", "AskSize3", "BidSize3", 
        "AskSize4", "BidSize4", "AskSize5", "BidSize5", "AskSize6", "BidSize6", "AskSize7", "BidSize7", 
        "AskSize8", "BidSize8", "AskSize9", "BidSize9", "AskMrk0","AskMrk1", "AskMrk2","AskMrk3","AskMrk4",
        "AskMrk5","AskMrk6","AskMrk7","AskMrk8","AskMrk9","BidMrk0","BidMrk1","BidMrk2","BidMrk3","BidMrk4",
        "BidMrk5","BidMrk6","BidMrk7","BidMrk8","BidMrk9","TrdValueLast","TrdValueAvg","VolumeMarket",
        "AskAttribute0", "AskAttribute1", "AskAttribute2", "AskAttribute3", "AskAttribute4", "AskAttribute5", 
        "AskAttribute6", "AskAttribute7", "AskAttribute8", "AskAttribute9", "BidAttribute0", "BidAttribute1",
        "BidAttribute2", "BidAttribute3", "BidAttribute4", "BidAttribute5", "BidAttribute6", "BidAttribute7", 
        "BidAttribute8", "BidAttribute9"
    };

    // Fields for subscription
    private String[] fieldsInstrument = new String[]{
        "Id", "TermCode", "DateStart", "DateStop" 
    };
    
    // Fields for subscription
    private String[] fieldsTrade = new String[]{
        "Id", "Code", "InstrumentId", "Desc", "CompNameOrigin", "DPriceStart", "CashStart", 
        "OrigSrc","Price", "Qty", "TimeCreation"
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
    public marketDataSubscriberUAT(String[] args) {
        initializeMarketData();
        publishExecutor = createExecutorService("Redis-Publisher");
        processingExecutor = createExecutorService("MKV-Processor");

        initializeRedisConnection();
        initializeMkvConnection(args);
        
        // Start the auto-shutdown monitoring
//        scheduleAutoShutdown();

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

//    private void scheduleAutoShutdown() {
//        LOGGER.info("Starting auto-shutdown monitor. Will shutdown at 5:00 PM local time.");
//        
//        shutdownScheduler.scheduleAtFixedRate(() -> {
//
//            // Check for time-based shutdown (5:00 PM)
//            LocalTime currentTime = LocalTime.now();
//            LocalTime shutdownTime = LocalTime.of(17, 0); // 5:00 PM
//            
//            if (currentTime.isAfter(shutdownTime)) {
//                LOGGER.warning("Current time " + currentTime + " is after scheduled shutdown time " + 
//                              shutdownTime + ". Shutting down...");
//                shutdown();
//            }
//        }, 0, 5, TimeUnit.MINUTES); // Check every minute
//    }
    
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
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String formattedDate = LocalDateTime.now().format(formatter);
            payload.put("timestamp", formattedDate);
            payload.put("source", SOURCE);
            payload.put("instrument", recordName);
            payload.putAll(data);

            // Convert payload to JSON string
            String jsonPayload = OBJECT_MAPPER.writeValueAsString(payload);

            // Publish to Redis
            jedis.setex(key, EXPIRY_SECONDS, jsonPayload);
            jedis.publish(key, jsonPayload);

            // LOGGER.info("Published data to Redis - Key: " + key);
        } catch (JedisConnectionException jce) {
            LOGGER.log(Level.SEVERE, "Redis connection lost, attempting to reconnect", jce);
            try {
                jedis.close();
                initializeRedisConnection();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to Redis", e);
            }
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
                lastUpdateTimestamp = System.currentTimeMillis(); // Update last data received timestamp
                String recordName = mkvRecord.getName();
                int cursor = mkvSupply.firstIndex();

                // Retrieve or initialize record storage
                recordDataMap.putIfAbsent(recordName, new HashMap<>());
                Map<String, Object> recordData = recordDataMap.get(recordName);

                // Process updates
                while (cursor != -1) {
                    String fieldName = mkvRecord.getMkvType().getFieldName(cursor);
                    Object fieldValue = mkvSupply.getObject(cursor);
                    
                    //updates for AONs
                    if (fieldName.startsWith("BidAttribute") || fieldName.startsWith("AskAttribute")) {
                        try {
                            int attributeIndex = -1;
                            String prefix = null;
                            
                            // Extract the index from the attribute field name
                            if (fieldName.startsWith("BidAttribute")) {
                                attributeIndex = Integer.parseInt(fieldName.substring("BidAttribute".length()));
                                prefix = "Bid";
                            } else { // AskAttribute
                                attributeIndex = Integer.parseInt(fieldName.substring("AskAttribute".length()));
                                prefix = "Ask";
                            }
                            
                            // Process the attribute - check if it ends with "AON"
                            if (fieldValue != null) {
                                String strValue = fieldValue.toString();
                                boolean isAON = strValue.endsWith("AON");
                                
                                // Store with a new field name format: Ask0IsAON, Bid2IsAON, etc.
                                String newFieldName = prefix + attributeIndex + "IsAON";
                                recordData.put(newFieldName, isAON);
                                
                                // Optionally, also log this transformation for debugging
                                LOGGER.fine("Transformed " + fieldName + "=" + strValue + 
                                        " to " + newFieldName + "=" + isAON);
                            }
                        } catch (NumberFormatException e) {
                            LOGGER.log(Level.SEVERE, "Error parsing attribute index from field: " + fieldName, e);
                        } catch (Exception e) {
                            LOGGER.log(Level.SEVERE, "Error processing attribute field: " + fieldName, e);
                        }
                    }
                    // Regular way updates
                    else if (fieldsList.contains(fieldName)) {
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

    private class InstrumentListener implements MkvRecordListener {
        private final Map<String, Map<String, Object>> recordDataMap = new ConcurrentHashMap<>();
        private final List<String> fieldsList = Arrays.asList(fieldsInstrument);

        public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
            updateRecord(mkvRecord, mkvSupply);
        }

        public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
            updateRecord(mkvRecord, mkvSupply);
        }

        private void updateRecord(MkvRecord mkvRecord, MkvSupply mkvSupply) {
            try {
                String recordName = mkvRecord.getName();
                recordName = recordName.replace(":", "_");
                int cursor = mkvSupply.firstIndex();

                recordDataMap.putIfAbsent(recordName, new HashMap<>());
                Map<String, Object> recordData = recordDataMap.get(recordName);

                while (cursor != -1) {
                    String fieldName = mkvRecord.getMkvType().getFieldName(cursor);
                    Object fieldValue = mkvSupply.getObject(cursor);
                    
                    if (fieldsList.contains(fieldName)) {
                        recordData.put(fieldName, fieldValue != null ? fieldValue : "null");
                    }
                    cursor = mkvSupply.nextIndex(cursor);
                }

                // Publish instrument data to Redis
                String key = REDIS_CHANNEL_INSTRUMENT + ":" + recordName;
                publishToRedis(key, recordName, recordData);

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error processing instrument data update", e);
            }
        }
    }
    
    private class TradeDataListener implements MkvRecordListener {
        private final Map<String, Map<String, Object>> recordDataMap = new ConcurrentHashMap<>();
        private final List<String> fieldsList = Arrays.asList(fieldsTrade);

        public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
            updateRecord(mkvRecord, mkvSupply);
        }

        public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
            updateRecord(mkvRecord, mkvSupply);
        }

        private void updateRecord(MkvRecord mkvRecord, MkvSupply mkvSupply) {
            try {
                String recordName = mkvRecord.getName();
                recordName = recordName.replace(":", "_");
                int cursor = mkvSupply.firstIndex();

                recordDataMap.putIfAbsent(recordName, new HashMap<>());
                Map<String, Object> recordData = recordDataMap.get(recordName);

                while (cursor != -1) {
                    String fieldName = mkvRecord.getMkvType().getFieldName(cursor);
                    Object fieldValue = mkvSupply.getObject(cursor);
                    
                    if (fieldsList.contains(fieldName)) {
                        recordData.put(fieldName, fieldValue != null ? fieldValue : "null");
                    }
                    cursor = mkvSupply.nextIndex(cursor);
                }

                // Publish trade data to Redis
                String key = REDIS_CHANNEL_TRADE + ":" + recordName;
                publishToRedis(key, recordName, recordData);

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error processing trade data update", e);
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
        private final MkvRecordListener dataListener = new DataListener();
        private final MkvRecordListener instrumentListener = new InstrumentListener();
        private final MkvRecordListener tradeListener = new TradeDataListener();
        private volatile boolean isDepthSubscribed = false;
        private volatile boolean isInstrumentSubscribed = false;
        private volatile boolean isTradeSubscribed = false;
        
        private synchronized void safeSubscribe(MkvPattern pattern) {
            // Check which pattern we're dealing with
            if (pattern.getName().startsWith("USD.CM_DEPTH")) {
                if (!isDepthSubscribed) {
                    try {
                        pattern.subscribe(fields, dataListener);
                        isDepthSubscribed = true;
                        LOGGER.info("Subscribed to market depth pattern: " + pattern.getName());
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error subscribing to market depth data", e);
                        isDepthSubscribed = false;
                    }
                }
            } else if (pattern.getName().startsWith("USD.CM_TRADE")) {
                if (!isTradeSubscribed) {
                    try {
                        pattern.subscribe(fieldsTrade, tradeListener);
                        isTradeSubscribed = true;
                        LOGGER.info("Subscribed to trade pattern: " + pattern.getName());
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error subscribing to trade data", e);
                        isTradeSubscribed = false;
                    }
                }
            } else if (pattern.getName().startsWith("USD.CM_INSTRUMENT")) {
                if (!isInstrumentSubscribed) {
                    try {
                        pattern.subscribe(fieldsInstrument, instrumentListener);
                        isInstrumentSubscribed = true;
                        LOGGER.info("Subscribed to instrument pattern: " + pattern.getName());
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error subscribing to instrument data", e);
                        isInstrumentSubscribed = false;
                    }
                }
            }
        }
        
        public void onPublish(MkvObject mkvObject, boolean start, boolean download) {
            if (start && !download && mkvObject.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                safeSubscribe((MkvPattern) mkvObject);
            }
        }

        public void onPublishIdle(String component, boolean start) {
            // Check both patterns during idle events
            MkvObject depthObj = Mkv.getInstance().getPublishManager().getMkvObject(PATTERN);
            if (depthObj != null && depthObj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                safeSubscribe((MkvPattern) depthObj);
            }
            
            MkvObject tradeObj = Mkv.getInstance().getPublishManager().getMkvObject(PATTERNTRADE);
            if (tradeObj != null && tradeObj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                safeSubscribe((MkvPattern) tradeObj);
            }
            
            MkvObject instrObj = Mkv.getInstance().getPublishManager().getMkvObject(PATTERNINSTRUMENT);
            if (instrObj != null && instrObj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                safeSubscribe((MkvPattern) instrObj);
            }
        }

        public void onSubscribe(MkvObject mkvObject) {}
    }

    // Shutdown method
    private void shutdown() {
        if (!running) return; // Prevent multiple shutdown calls
        running = false;

        LOGGER.info("Shutting down market data subscriber...");
        try {
            if (jedis != null) {
                jedis.close();
                LOGGER.info("Redis connection closed");
            }
            
            shutdownScheduler.shutdownNow();
            LOGGER.info("Shutdown scheduler terminated");
            
            publishExecutor.shutdownNow();
            processingExecutor.shutdownNow();
            LOGGER.info("Executor services terminated");
            
            Mkv.stop();
            LOGGER.info("MKV connection stopped");
            
            // Release the latch to allow main thread to exit
            terminationLatch.countDown();
            
            LOGGER.info("Shutdown complete.");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error during shutdown", e);
        }
    }
    
    // Method to wait for shutdown signal
    public void waitForTermination() {
        try {
            terminationLatch.await();
        } catch (InterruptedException e) {
            LOGGER.log(Level.WARNING, "Termination wait interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    // Main method
    public static void main(String[] args) {
        LOGGER.info("Starting Market Data Subscriber UAT application");
        LOGGER.info("Current time: " + LocalDateTime.now());
        
        try {
//            // Check if current time is already past 5:00 PM
//            LocalTime currentTime = LocalTime.now();
//            LocalTime shutdownTime = LocalTime.of(17, 0); // 5:00 PM
//            
//            if (currentTime.isAfter(shutdownTime)) {
//                LOGGER.warning("Current time " + currentTime + " is already after scheduled shutdown time " + 
//                              shutdownTime + ". Not starting the application.");
//                return;
//            }
            
            marketDataSubscriberUAT subscriber = new marketDataSubscriberUAT(args);
            
            // Log the scheduled shutdown time
            LOGGER.info("Application will automatically shut down at 5:00 PM local time if still running");
            
            // Keep the application running until shutdown is triggered
            subscriber.waitForTermination();
            
            LOGGER.info("Application exiting normally");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Fatal error in application", e);
            System.exit(1);
        }
    }
}