package com.iontrading.samples.marketDataSubscriberPROD;

import com.iontrading.mkv.*;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.util.concurrent.atomic.AtomicReference;

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
import java.time.Duration;

public class marketDataSubscriberPROD {
    // Static logger configuration
    private static final Logger LOGGER = Logger.getLogger(marketDataSubscriberPROD.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public final String hostname = System.getenv("COMPUTERNAME");

    // Static block for logging setup with async handler
    static {
        try {
            // Create an async logger with a separate thread
            AsyncHandler asyncHandler = new AsyncHandler(20000); // Queue size of 20000 logs

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
    private static final String PATTERNTRADE = "USD.CM_TRADE." + SOURCE + ".";
    private static final String PATTERNINSTRUMENT = "USD.CM_INSTRUMENT." + SOURCE + ".";
    private static final String PATTERNORDER = "USD.CM_ORDER." + SOURCE + ".";
    private static final String BTECREFERENCEPRICE = "USD.CM_BOND.BTEC_REPO_US.";
    private static final String REDIS_CHANNEL = "ION:ION_MARKET_DATA";
    private static final String REDIS_CHANNEL_INSTRUMENT = "ION:ION_INSTRUMENT_DATA";
    private static final String REDIS_CHANNEL_TRADE = "ION:ION_TRADE_DATA";
    private static final String REDIS_CHANNEL_ORDER = "ION:ION_ORDER_DATA";
    private static final String REDIS_CHANNEL_REF_PRICE = "ION:ION_REF_PRICE_DATA";
    private static final String REDIS_HOST = "cacheprod";
    private static final int REDIS_PORT = 6379;
    private static final int TIMEOUT = 5000;
    private static final int EXPIRY_SECONDS = 86400; // 24 hours

    private static final int PRICE_AON = 0x0010;
    private static final int PRICE_MINE = 0x0020;  // Price belongs to the bank

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
        "BidAttribute8", "BidAttribute9","Ask0Status", "Ask1Status", "Ask2Status", "Ask3Status", "Ask4Status", 
        "Ask5Status", "Ask6Status", "Ask7Status", "Ask8Status", "Ask9Status", "Bid0Status", "Bid1Status", 
        "Bid2Status", "Bid3Status", "Bid4Status", "Bid5Status", "Bid6Status", "Bid7Status", "Bid8Status", "Bid9Status"
    };

    // Fields for subscription
    private String[] fieldsInstrument = new String[]{
        "Id", "TermCode", "DateStart", "DateStop" 
    };
    
    // Fields for subscription
    private String[] fieldsTrade = new String[]{
            "Id", "Code", "CashStop", "Date", "DateCreation", "DateMaturity", "DateSettl", "InstrumentId", "Desc", "CompNameOrigin", "DPriceStart", "CashStart", 
            "MarketAlias", "OrderId", "OrderNo", "OrigSrc", "OrigTrader","Price", "Qty", "QtyDeleted", "QtyNominal", "QtyTick", "Rejectable", "TimeCreation",
            "StatusStr", "TermCode", "Time", "TimeCreation", "Trader", "Type", "VerbStr", "Yield"
    };

    // Fields for subscription
    private String[] fieldsOrder = new String[]{
            "Active", "ActiveStr", "Code", "CompNameOrigin", "CurrentAction", "CurrentActionStr", "Date", "Desc", "Id", "InstrumentId", "IntQtyGoal", "MarketStatus", 
            "OrderNo", "OrigCode", "OrigId", "OrigSrc", "Price", "QtyFill", "QtyGoal", "QtyHidden", "QtyHit", "QtyHitDeleted", "QtyHitExt", "QtyHitWorkUp", 
            "QtyLocked", "QtyLost", "QtyMin", "QtyOvertraded", "QtyShown", "QtyStatus", "QtyStatusStr", "QtyTick", "QtyTot", "QtyTotReq", "StatusStr", 
            "Time", "TimeCreation", "TimeInForce", "TimeUpd", "Trader", "TradingStatus", "TradingStatusStr", "Verb", "VerbStr", "Yield" 
    };
    
    private String[] fieldsRefPrice = new String[]{
        "Id", "Desc", "Code",  "ReferencePrice", "ReferencePriceUpdDate", "ReferencePriceUpdTime"
    };
    
    // Thread pools and control flags
    private final ExecutorService publishExecutor;
    private final ExecutorService processingExecutor;
    private volatile boolean running = true;
    private final AtomicReference<Boolean> isRedisConnected = new AtomicReference<>(false);
    
    // Redis connection
    private JedisPool jedisPool;

    private static final String HEARTBEAT_CHANNEL = "HEARTBEAT:ION:MARKET_DATA";
    private static final int HEARTBEAT_INTERVAL_SECONDS = 30;
    private ScheduledExecutorService heartbeatScheduler;

    // Initialize market data with zeros
    private void initializeMarketData() {
        marketData.clear();
        for (String field : fields) {
            marketData.put(field, "0");
        }
    }
    
    // Constructor
    public marketDataSubscriberPROD(String[] args) {
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
        if (!isRedisConnected.get()) {
            try {
                // Configure JedisPool
                JedisPoolConfig poolConfig = new JedisPoolConfig();
                poolConfig.setMaxTotal(16);              // Maximum active connections
                poolConfig.setMaxIdle(8);                // Maximum idle connections
                poolConfig.setMinIdle(2);                // Minimum idle connections 
                poolConfig.setTestOnBorrow(true);        // Test connections when borrowed
                poolConfig.setTestOnReturn(true);        // Test connections when returned
                poolConfig.setTestWhileIdle(true);       // Test idle connections
                poolConfig.setMaxWaitMillis(10000);      // Max wait time for connection
                
                jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, TIMEOUT);
                
                // Test connection
                try (Jedis testJedis = jedisPool.getResource()) {
                    testJedis.clientSetname(hostname + ":marketDataSubscriber");
                    isRedisConnected.set(true);
                    LOGGER.info("Connected to Redis at " + REDIS_HOST + ":" + REDIS_PORT + " using JedisPool");
                    
                    // Start heartbeat after successful connection
                    startHeartbeat();
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error connecting to Redis", e);
                throw new RuntimeException("Redis connection failed", e);
            }
        }
    }
    private void startHeartbeat() {
        if (heartbeatScheduler == null || heartbeatScheduler.isShutdown()) {
            heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("Redis-Heartbeat");
                return t;
            });
            
            heartbeatScheduler.scheduleAtFixedRate(() -> {
                if (isRedisConnected.get()) {
                    sendHeartbeat();
                }
            }, 0, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
            
            LOGGER.info("Redis heartbeat scheduled every " + HEARTBEAT_INTERVAL_SECONDS + " seconds");
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
        if (!isRedisConnected.get()) {
            initializeRedisConnection();
            return;
        }

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
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.setex(key, EXPIRY_SECONDS, jsonPayload);
                jedis.publish(key, jsonPayload);
            }

            // LOGGER.info("Published data to Redis - Key: " + key);
        } catch (JedisConnectionException jce) {
            LOGGER.log(Level.SEVERE, "Redis connection lost, attempting to reconnect", jce);
            try {
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
                    
                    //updates for AONs and ASL Orders
                    if ((fieldName.startsWith("Bid") || fieldName.startsWith("Ask")) && fieldName.endsWith("Status")) {
                        try {
                            int attributeIndex = -1;
                            String prefix = null;
                            attributeIndex = Integer.parseInt(fieldName.substring(3, fieldName.length()-6));
                            // Extract the index from the attribute field name
                            if (fieldName.startsWith("Bid")) {
                                prefix = "Bid";
                            } else { // AskAttribute
                                prefix = "Ask";
                            }
                            
                            // Process the attribute - check if it ends with "AON"
                            if (fieldValue != null) {
           
                            	int intValue = Integer.parseInt(fieldValue.toString());
                                boolean isAON = isBitSet(intValue, PRICE_AON);
                                boolean isASLOrder = isBitSet(intValue, PRICE_MINE);
                                
                                // Store with a new field name format: Ask0IsAON, Bid2IsAON, etc.
                                String newFieldName = prefix + attributeIndex + "IsAON";
                                recordData.put(newFieldName, isAON);
                                
                                // Store with a new field name format: Ask0IsASL, Bid2IsASL, etc.
                                String newASLOrderName = prefix + attributeIndex + "IsASL";
                                recordData.put(newASLOrderName, isASLOrder);

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

                // Publish trade data to Redis
                String key = REDIS_CHANNEL_INSTRUMENT + ":" + recordName;
                publishToRedis(key, recordName, recordData);

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error processing trade data update", e);
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

    private class OrderDataListener implements MkvRecordListener {
        private final Map<String, Map<String, Object>> recordDataMap = new ConcurrentHashMap<>();
        private final List<String> fieldsList = Arrays.asList(fieldsOrder);

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
                String key = REDIS_CHANNEL_ORDER+ ":" + recordName;
                publishToRedis(key, recordName, recordData);

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error processing trade data update", e);
            }
        }
    }
    
    private class ReferencePriceListener implements MkvRecordListener {
        private final Map<String, Map<String, Object>> recordDataMap = new ConcurrentHashMap<>();
        private final List<String> fieldsList = Arrays.asList(fieldsRefPrice);

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
                String key = REDIS_CHANNEL_REF_PRICE+ ":" + recordName;
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
        private final MkvRecordListener orderListener = new OrderDataListener();
        private final MkvRecordListener referencePriceListener = new ReferencePriceListener();
        private volatile boolean isDepthSubscribed = false;
        private volatile boolean isInstrumentSubscribed = false;
        private volatile boolean isTradeSubscribed = false;
        private volatile boolean isOrderSubscribed = false;
        private volatile boolean isReferencePriceSubscribed = false;
        
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
            } else if (pattern.getName().startsWith("USD.CM_ORDER")) {
                if (!isOrderSubscribed) {
                    try {
                        pattern.subscribe(fieldsOrder, orderListener);
                        isOrderSubscribed = true;
                        LOGGER.info("Subscribed to order pattern: " + pattern.getName());
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error subscribing to order data", e);
                        isOrderSubscribed = false;
                    }
                }
            } else if (pattern.getName().startsWith("USD.CM_BOND.BTEC_REPO_US.")) {
                if (!isReferencePriceSubscribed) {
                    try {
                        pattern.subscribe(fieldsRefPrice, referencePriceListener);
                        isReferencePriceSubscribed = true;
                        LOGGER.info("Subscribed to reference price pattern: " + pattern.getName());
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error subscribing to reference price data", e);
                        isReferencePriceSubscribed = false;
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

            MkvObject instrObj = Mkv.getInstance().getPublishManager().getMkvObject(PATTERNINSTRUMENT);
            if (instrObj != null && instrObj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                safeSubscribe((MkvPattern) instrObj);
            }
            
            MkvObject tradeObj = Mkv.getInstance().getPublishManager().getMkvObject(PATTERNTRADE);
            if (tradeObj != null && tradeObj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                safeSubscribe((MkvPattern) tradeObj);
            }

            MkvObject orderObj = Mkv.getInstance().getPublishManager().getMkvObject(PATTERNORDER);
            if (orderObj != null && orderObj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                safeSubscribe((MkvPattern) orderObj);
            }
            
            MkvObject refPriceObj = Mkv.getInstance().getPublishManager().getMkvObject(BTECREFERENCEPRICE);
            if (refPriceObj != null && refPriceObj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                safeSubscribe((MkvPattern) refPriceObj);
            }
        }

        public void onSubscribe(MkvObject mkvObject) {}
    }

    private boolean isBitSet(int bitmask, int bit) {
        return (bitmask & bit) != 0;
    }

// Shutdown method
    private void shutdown() {
        if (!running) return; // Prevent multiple shutdown calls
        running = false;

        LOGGER.info("Shutting down market data subscriber...");
        try {
            // Send final heartbeat with stopped status if still connected
            if (isRedisConnected.get()) {
                try (Jedis jedis = jedisPool.getResource()) {
                    Map<String, Object> heartbeatData = new HashMap<>();
                    heartbeatData.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    heartbeatData.put("source", hostname);
                    heartbeatData.put("application", "marketDataSubscriber");
                    heartbeatData.put("status", "STOPPING");
                    
                    String jsonPayload = OBJECT_MAPPER.writeValueAsString(heartbeatData);
                    jedis.publish(HEARTBEAT_CHANNEL, jsonPayload);
                    LOGGER.info("Final heartbeat sent with STOPPING status");
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Failed to send final heartbeat", e);
                }
            }
            
            // Stop heartbeat scheduler
            if (heartbeatScheduler != null) {
                heartbeatScheduler.shutdownNow();
                LOGGER.info("Heartbeat scheduler terminated");
            }
            
            // Close JedisPool
            if (jedisPool != null) {
                jedisPool.close();
                LOGGER.info("Redis connection pool closed");
            }
            
            shutdownScheduler.shutdownNow();
            LOGGER.info("Shutdown scheduler terminated");
            
            // Rest of the shutdown code remains the same...
            publishExecutor.shutdown();
            processingExecutor.shutdown();
            
            try {
                if (!publishExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    publishExecutor.shutdownNow();
                }
                if (!processingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    processingExecutor.shutdownNow();
                }
                LOGGER.info("Executor services terminated");
            } catch (InterruptedException e) {
                publishExecutor.shutdownNow();
                processingExecutor.shutdownNow();
                Thread.currentThread().interrupt();
                LOGGER.warning("Executor service shutdown interrupted");
            }
            
            Mkv.stop();
            LOGGER.info("MKV connection stopped");
            
            // Close logging handlers to ensure all logs are flushed
            for (Handler handler : LOGGER.getHandlers()) {
                handler.flush();
                handler.close();
            }
            
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
        LOGGER.info("Starting Market Data Subscriber PROD application");
        LOGGER.info("Current time: " + LocalDateTime.now());
        
        try {
            
            marketDataSubscriberPROD subscriber = new marketDataSubscriberPROD(args);
            
            // Keep the application running until shutdown is triggered
            subscriber.waitForTermination();
            
            LOGGER.info("Application exiting normally");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Fatal error in application", e);
            System.exit(1);
        }
    }
    private void sendHeartbeat() {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, Object> heartbeatData = new HashMap<>();
            heartbeatData.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            heartbeatData.put("source", hostname);
            heartbeatData.put("application", "marketDataSubscriber");
            heartbeatData.put("status", "RUNNING");
            heartbeatData.put("uptime_ms", System.currentTimeMillis() - lastUpdateTimestamp);
            
            String jsonPayload = OBJECT_MAPPER.writeValueAsString(heartbeatData);
            jedis.publish(HEARTBEAT_CHANNEL, jsonPayload);

            // LOGGER.fine("Heartbeat sent to Redis");
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to send heartbeat", e);
            isRedisConnected.set(false);
            
            // Try to reconnect
            try {
                if (jedisPool != null && !jedisPool.isClosed()) {
                    initializeRedisConnection();
                }
            } catch (Exception reconnectEx) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to Redis during heartbeat", reconnectEx);
            }
        }
    }

}