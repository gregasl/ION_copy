/*
 * Publisher - ION Market Data Publisher Implementation
 *
 * This class implements a market data publisher that:
 * 1. Connects to a Redis cache to receive market data updates
 * 2. Dynamically creates MKV schemas based on received data
 * 3. Publishes data through the ION MKV middleware
 * 

 * - Dynamic schema generation based on data content
 * - Efficient update processing through queued architecture
 * - Automatic schema versioning and compatibility checking
 * - Robust error handling and detailed logging
 * - Performance monitoring and statistics
 *
 * Data Flow:
 * Redis -> Message Queue -> Processor Thread -> MKV Publisher
 *
 * ION Trading U.K. Limited supplies this software code is for testing purposes
 * only. The scope of this software code is exclusively limited to the
 * demonstration of the structure of an application using the ION(tm) Common
 * Market and ION Trading U.K. Limited does not guarantee the correct behavior
 * of any deployed application using this software code.
 * This software code has not been thoroughly tested under all conditions.
 * ION, therefore, cannot guarantee or imply reliability, serviceability, or
 * function of this software.
 * Any use of this software outside of this defined scope is the sole
 * responsibility of the user.
 *
 * ION Trading ltd (2005)
 */

package com.iontrading.samples.simplePublisher;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvProperties;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.MkvLog;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.enums.MkvShutdownMode;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyBuilder;
import com.iontrading.mkv.qos.MkvQoS;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Publisher implements MkvPlatformListener {
    private MkvLog myLog;
    private int logLevel;
    private IONLogger logger;

    private static final String REDIS_KEY_SEPARATOR = ":";
    private static final String REDIS_CHANNEL_PATTERN = "ASL:*:*:*";
    private static final String DEFAULT_REDIS_HOST = "cacheprod";
    private static final int DEFAULT_REDIS_PORT = 6379;
    private static final String HEARTBEAT_CHANNEL = "HEARTBEAT:ION:PUBLISHER";
    private static final long HEARTBEAT_INTERVAL_SECONDS = 30;
    public final String hostname = System.getenv("COMPUTERNAME");

    // Keep only persistent storage and connections
    private final Map<String, SchemaInfo> schemasByPrefix = new ConcurrentHashMap<>();
    private final Map<String, MkvRecord> recordsByFullId = new ConcurrentHashMap<>();
    private final JedisPool jedisPool;
    private final Mkv mkv;
    private final BlockingQueue<RecordUpdate> updateQueue;
    private final ExecutorService processorPool;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Object recordLock = new Object();
    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

    // Add static class to hold parsed Redis key info
    private static class RedisKeyInfo {
        final String source;
        final String recordPrefix; 
        final String recordId;
        final String typeName;

        RedisKeyInfo(String source, String recordPrefix, String recordId, String typeName) {
            this.source = source;
            this.recordPrefix = recordPrefix;
            this.recordId = recordId;
            this.typeName = typeName;
        }
    }

    // Inner class for schema info
    private static class SchemaInfo {
        final String[] fieldNames;
        final MkvFieldType[] fieldTypes;
        final Map<String, Integer> fieldIndexMap;
        final String typeName;
        
        SchemaInfo(String[] fieldNames, MkvFieldType[] fieldTypes, 
                  Map<String, Integer> fieldIndexMap, String typeName) {
            this.fieldNames = fieldNames;
            this.fieldTypes = fieldTypes;
            this.fieldIndexMap = fieldIndexMap;
            this.typeName = typeName;
        }
    }

    // Class to track which record an update is for
    private static class RecordUpdate {
        final String channel;
        final String recordId;
        final Map<String, Object> updates;
        
        RecordUpdate(String channel, String recordId, Map<String, Object> updates) {
            this.channel = channel;
            this.recordId = recordId;
            this.updates = updates;
        }
    }

    private static class ParsedToken {
        final String value;
        final boolean wasQuoted;
        
        ParsedToken(String value, boolean wasQuoted) {
            this.value = value;
            this.wasQuoted = wasQuoted;
        }
    }

    public Publisher(String[] args) {
        // Create the initial configuration used to start the engine
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        
        try {
            // Start the engine and get back the instance of Mkv
            this.mkv = Mkv.start(qos);
            MkvProperties properties = mkv.getProperties();
            logLevel = properties.getIntProperty("DEBUG");
            // Initialize the log after starting Mkv
            this.myLog = mkv.getLogManager().createLogFile("PUBLISHER");
            this.logger = new IONLogger(myLog, logLevel, "Publisher");

        } catch (MkvException e) {
            if (myLog != null) {
                logger.info("Failed to initialize publisher: " + e.getMessage());
            }
            throw new RuntimeException("Failed to initialize publisher", e);
        }

        // Initialize fields in constructor
        this.updateQueue = new LinkedBlockingQueue<>(1000);
        this.processorPool = Executors.newSingleThreadExecutor();
        
        // Initialize Redis connection in constructor
        String redisHost = DEFAULT_REDIS_HOST;
        int redisPort = DEFAULT_REDIS_PORT;
        
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--redis-host") && i + 1 < args.length) {
                redisHost = args[i + 1];
            } else if (args[i].equals("--redis-port") && i + 1 < args.length) {
                try {
                    redisPort = Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException e) {
                    logger.warn("Invalid Redis port number, using default: " + DEFAULT_REDIS_PORT);
                }
            }
        }

        logger.info("Connecting to Redis at " + redisHost + ":" + redisPort);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        this.jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
        
        // Test Redis connection
        try (Jedis jedis = jedisPool.getResource()) {
            String pong = jedis.ping();
            logger.info("Redis connection test successful: " + pong);
        } catch (Exception e) {
            logger.error("Failed to connect to Redis: " + e.getMessage());
            throw new RuntimeException("Failed to connect to Redis", e);
        }
        
        try {
            logger.info("=== Starting ION Publisher ===");
            logger.info("Initializing with args: " + Arrays.toString(args));
            logger.info("Created MkvQoS configuration");
            logger.info("Created processing queue and thread pool");
            logger.info("MKV engine started successfully");

            // Start processor thread first
            logger.info("Starting processor thread...");
            startProcessorThread();
            Thread.sleep(1000); // Give processor thread time to initialize

            logger.info("Starting heartbeat...");
            startHeartbeat();

            logger.info("Loading existing data from Redis...");
            loadExistingData();

            // Then start Redis subscriber
            logger.info("Starting Redis subscriber...");
            startRedisSubscriber();

            logger.info("=== Publisher initialization complete ===");
            logger.info("Publisher components status:");
            logger.info("- Redis Connection: ACTIVE");
            logger.info("- MKV Engine: RUNNING");
            logger.info("- Update Processor: PROCESSING");
            logger.info("- Redis Subscriber: LISTENING");
            logger.info("Schema/Record Statistics:");
            logger.info("- Schemas loaded: " + schemasByPrefix.size());
            logger.info("- Records active: " + recordsByFullId.size());
            logger.info("- Queue size: " + updateQueue.size() + "/" + updateQueue.remainingCapacity());
            logger.info("");
            logger.info("Waiting for Redis messages on pattern: " + REDIS_CHANNEL_PATTERN);

        } catch (InterruptedException e) {
            if (myLog != null) {
                logger.info("Failed to initialize publisher: " + e.getMessage());
            }
            throw new RuntimeException("Failed to initialize publisher", e);
        }
    }

    public static void main(String[] args) {
        Publisher publisher = new Publisher(args);
        if (publisher.logger == null) {
                publisher.logger = new IONLogger(publisher.getMkvLog(), publisher.getLogLevel(), "Publisher");
        }
        publisher.logger.info("Market Data Publisher started successfully");
    }
    
    /**
     * Implements the MkvPlatformListener.onMain method to handle platform events.
     * This is where we'll handle the shutdown request from the daemon.
     */
    @Override
    public void onMain(MkvPlatformEvent event) {
        if (event.intValue() == MkvPlatformEvent.SHUTDOWN_REQUEST_code) {
            logger.info("Received shutdown request from MKV platform");

            try {
                // Do the shutdown work synchronously in this method
                boolean isReady = shutdown();
            if (isReady) {
                // Signal that we're completely done
                Mkv.getInstance().shutdown(MkvShutdownMode.SYNC, 
                    "SimplePublisher shutdown complete");
                logger.info("Signaled SYNC shutdown to platform");
            } else {
                // We need more time, request async and let platform retry
                Mkv.getInstance().shutdown(MkvShutdownMode.ASYNC, 
                    "SimplePublisher still processing...");
                logger.info("Requested ASYNC shutdown - platform will retry");
            }
            } catch (MkvException e) {
                logger.info("Error during shutdown signaling: " + e.getMessage());
            }
        }
    }

    /**
     * Implements the MkvPlatformListener.onComponent method.
     * This is called when component state changes.
     */
    @Override
    public void onComponent(MkvComponent comp, boolean start) {
        logger.info("Component " + comp.getName() + " " + (start ? "started" : "stopped"));
    }

    /**
     * Implements the MkvPlatformListener.onConnect method.
     * This is called when the connection state changes.
     */
    @Override
    public void onConnect(String comp, boolean start) {
        logger.info("Connection to " + comp + " " + (start ? "established" : "lost"));
    }

    /**
     * Parse Redis key to extract components - simplified to match simple publisher
     */
    private RedisKeyInfo parseRedisKey(String channel) {
        String[] parts = channel.split(REDIS_KEY_SEPARATOR);
        if (parts.length < 4) {
            logger.info("Invalid channel format (expected ASL:market:instrument:type): " + channel);
            return null;
        }
        
        String source = parts[0];
        String recordPrefix = "ALL." + parts[1] + "." + source + ".";
        String recordId = parts[2];
        String typeName = parts[3];

        logger.debug("Parsed Redis key - Source: " + source + 
               ", Record Prefix: " + recordPrefix + 
               ", Record ID: " + recordId + 
               ", Type Name: " + typeName);

        return new RedisKeyInfo(source, recordPrefix, recordId, typeName);
    }

    /**
     * Parse an update message in CSV format
     */
    private Map<String, Object> parseUpdateMessage(String channel, String message) {
        Map<String, Object> updates = new HashMap<>();
        List<ParsedToken> tokens = parseMessage(message);  // Changed from parseCSV to parseMessage
        
        if (tokens.isEmpty() || tokens.size() % 2 != 0) {
            logger.warn("Message must have complete name-value pairs. Got " + tokens.size() + " tokens");
            return updates;
        }

        // Process as name-value pairs
        for (int i = 0; i < tokens.size(); i += 2) {
            ParsedToken nameToken = tokens.get(i);
            ParsedToken valueToken = tokens.get(i + 1);
            String fieldName = nameToken.value.trim();

            if (!fieldName.isEmpty()) {
                // Use the value and quote info directly
                updates.put(fieldName, valueToken);
            }
        }

        // Add Id field if not present
        RedisKeyInfo keyInfo = parseRedisKey(channel);
        if (keyInfo != null && !updates.containsKey("Id")) {
            updates.put("Id", new ParsedToken(keyInfo.recordId, false));
        }

        return updates;
    }

    private List<ParsedToken> parseMessage(String message) {
        List<ParsedToken> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean wasQuoted = false;
        
        // Remove outer braces and trim
        message = message.trim();
        if (message.startsWith("{") && message.endsWith("}")) {
            message = message.substring(1, message.length() - 1).trim();
        }
        
        for (int i = 0; i < message.length(); i++) {
            char c = message.charAt(i);
            
            if (c == '"') {
                inQuotes = !inQuotes;
                wasQuoted = true;
                continue;
            }
            
            if (!inQuotes && (c == ',' || c == ':')) {
                // End of field or value
                String value = current.toString().trim();
                if (!value.isEmpty()) {
                    tokens.add(new ParsedToken(value, wasQuoted));
                }
                current = new StringBuilder();
                wasQuoted = false;
                continue;
            }
            
            current.append(c);
        }
        
        // Add final token if any
        String finalValue = current.toString().trim();
        if (!finalValue.isEmpty()) {
            tokens.add(new ParsedToken(finalValue, wasQuoted));
        }

        logger.debug("Parsed tokens: " + tokens.stream()
            .map(t -> String.format("%s(quoted=%s)", t.value, t.wasQuoted))
            .collect(Collectors.joining(", ")));
            
        return tokens;
    }

    // Add createSchemaInfo method
    private SchemaInfo createSchemaInfo(Map<String, Object> fieldValues) {
        List<String> fieldNamesList = new ArrayList<>();
        Map<String, Integer> fieldIndexMap = new HashMap<>();
        List<MkvFieldType> fieldTypesList = new ArrayList<>();
        
        // Always add Id field first
        fieldNamesList.add("Id");
        fieldIndexMap.put("Id", 0);
        fieldTypesList.add(MkvFieldType.STR);
        
        int index = 1;
        for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
            String fieldName = entry.getKey();
            if (!fieldName.equals("Id")) {
                fieldNamesList.add(fieldName);
                fieldIndexMap.put(fieldName, index);
                fieldTypesList.add(inferFieldType(entry.getValue()));
                index++;
            }
        }
        
        String[] fieldNames = fieldNamesList.toArray(new String[0]);
        MkvFieldType[] fieldTypes = fieldTypesList.toArray(new MkvFieldType[0]);
        
        return new SchemaInfo(fieldNames, fieldTypes, fieldIndexMap, "DynamicType");
    }

    /**
     * Initialize the schema from a Redis message - simplified approach
     */
    private void initializeSchemaFromMessage(String channel, Map<String, Object> fieldValues) {
        RedisKeyInfo keyInfo = parseRedisKey(channel);
        if (keyInfo == null) return;

        logger.info("Initializing schema for channel: " + channel);
        logger.info("Field values: " + fieldValues);

        String fullRecordId = keyInfo.recordPrefix + keyInfo.recordId;
        SchemaInfo schema = schemasByPrefix.get(keyInfo.recordPrefix);

        synchronized (recordLock) {
            if (schema == null || !isSchemaCompatible(schema, fieldValues)) {
                logger.info("Creating new schema for " + keyInfo.recordPrefix);
                schema = createAndPublishSchema(keyInfo, fieldValues);
                if (schema == null) return;
            }

            MkvRecord record = recordsByFullId.get(fullRecordId);
            if (record == null) {
                try {
                    logger.info("Publishing new record " + fullRecordId);
                    record = publishNewRecord(fullRecordId, schema, fieldValues);
                    if (record.isValid()) {
                        recordsByFullId.put(fullRecordId, record);
                        logger.info("Successfully published record " + fullRecordId);
                    } else {
                        logger.warn("Failed to publish valid record " + fullRecordId);
                    }
                } catch (Exception e) {
                    logger.error("Error publishing record " + fullRecordId + ": " + e.getMessage());
                }
            } else {
                logger.info("Updating existing record " + fullRecordId);
                applyUpdates(record, fieldValues, schema);
            }
        }
    }

    private boolean isSchemaCompatible(SchemaInfo schema, Map<String, Object> newValues) {
        // Check if schema has all fields from new values
        Set<String> schemaFields = new HashSet<>(Arrays.asList(schema.fieldNames));
        
        for (String fieldName : newValues.keySet()) {
            if (!schemaFields.contains(fieldName)) {
                logger.warn("Schema missing field: " + fieldName);
                return false;
            }
        }
        
        // Check type compatibility for existing fields
        for (Map.Entry<String, Object> entry : newValues.entrySet()) {
            String fieldName = entry.getKey();
            Integer index = schema.fieldIndexMap.get(fieldName);
            
            if (index != null) {  // Field exists in schema
                MkvFieldType existingType = schema.fieldTypes[index];
                MkvFieldType newType = inferFieldType(entry.getValue());
                
                if (existingType != newType && 
                    !(existingType == MkvFieldType.REAL && newType == MkvFieldType.INT)) {
                    logger.warn(String.format("Type mismatch for field %s: existing=%s, new=%s", 
                        fieldName, existingType, newType));
                    return false;
                }
            }
        }
        
        return true;
    }

    private SchemaInfo createAndPublishSchema(RedisKeyInfo keyInfo, Map<String, Object> updates) {
        // Add schema caching optimization
        String cacheKey = keyInfo.recordPrefix;
        SchemaInfo existingSchema = schemasByPrefix.get(cacheKey);
        
        if (existingSchema != null && isSchemaCompatible(existingSchema, updates)) {
            schemaHits.incrementAndGet();
            return existingSchema;
        }
        
        schemaMisses.incrementAndGet();
        
        // Create schema with unique type name per source/market
        // Sanitize type name to be a valid MKV identifier:
        // - Must start with letter or underscore
        // - Can only contain letters, numbers and underscores
        // - No spaces or special characters
        String sanitizedSource = keyInfo.source.replaceAll("[^A-Za-z0-9_]", "_");
        String sanitizedType = keyInfo.typeName.replaceAll("[^A-Za-z0-9_]", "_");
        String sanitizedHash = String.format("%08X", Math.abs(keyInfo.recordPrefix.hashCode()));
        String typeName = String.format("%s_%s_%s",
            sanitizedSource, sanitizedType, sanitizedHash);
            
        if (!typeName.matches("^[A-Za-z_].*")) {
            typeName = "T_" + typeName;
        }

        logger.info("Generated sanitized type name: " + typeName);

        SchemaInfo schema = createSchemaInfo(updates);
        
        try {
            logger.info(String.format(
                "Creating new schema - Type: %s, Prefix: %s\nFields: %s\nTypes: %s",
                typeName, keyInfo.recordPrefix,
                String.join(",", schema.fieldNames),
                Arrays.toString(schema.fieldTypes)
            ));

            // Add detailed logging for type creation
            logger.info("Checking for existing type: " + typeName);
            MkvType existingType = mkv.getPublishManager().getMkvType(typeName);
            
            if (existingType == null || !existingType.isValid()) {
                try {
                    logger.info("Creating new MkvType with fields:");
                    for (int i = 0; i < schema.fieldNames.length; i++) {
                        logger.info(String.format("  Field[%d]: name='%s', type=%s",
                            i, schema.fieldNames[i], schema.fieldTypes[i]));
                    }
                    
                    MkvType type = new MkvType(typeName, schema.fieldNames, schema.fieldTypes);
                    logger.info("Publishing type: " + typeName);
                    type.publish();
                    
                    if (!type.isValid()) {
                        throw new MkvException("Type was created but is not valid: " + typeName);
                    }

                    logger.info("Successfully published new type: " + typeName);
                } catch (MkvException e) {
                    logger.error(String.format(
                        "Failed to create/publish type %s: %s\nStack: %s",
                        typeName, e.getMessage(),
                        Arrays.toString(e.getStackTrace())
                    ));
                    return null;
                }
            } else {
                logger.info("Using existing valid type: " + typeName);
            }

            SchemaInfo newSchema = new SchemaInfo(
                schema.fieldNames, 
                schema.fieldTypes,
                schema.fieldIndexMap, 
                typeName
            );
            
            schemasByPrefix.put(keyInfo.recordPrefix, newSchema);
            logger.info("Stored schema for prefix: " + keyInfo.recordPrefix);

            return newSchema;
            
        } catch (Exception e) {
            logger.error(String.format(
                "Failed to create schema for prefix %s: %s\nStack: %s",
                keyInfo.recordPrefix, e.getMessage(),
                Arrays.toString(e.getStackTrace())
            ));
            return null;
        }
    }

    /**
     * Start the processor thread - simplified to a single thread model
     */
    private void startProcessorThread() {
        logger.info("=== Initializing Update Processor ===");
        logger.info("Max queue capacity: " + updateQueue.remainingCapacity());

        processorPool.submit(() -> {
            Thread.currentThread().setName("MKV-Update-Processor");
            logger.info("=== Processor Thread Started ===");
            logger.info("Thread name: " + Thread.currentThread().getName());
            logger.info("Priority: " + Thread.currentThread().getPriority());
            logger.info("Waiting for updates on queue...");

            while (running.get()) {
                try {
                    RecordUpdate update = updateQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (update != null) {
                        logger.info("=== Processing Update ===");
                        logger.info("Channel: " + update.channel);
                        logger.info("Record ID: " + update.recordId);
                        logger.info("Fields to update: " + update.updates.keySet());

                        try {
                            initializeSchemaFromMessage(update.channel, update.updates);
                            logger.info("=== Update Processing Complete ===");
                        } catch (Exception e) {
                            logger.error("Error processing update for channel " + update.channel + 
                                     "\nError: " + e.getMessage() + 
                                     "\nStack: " + Arrays.toString(e.getStackTrace()));
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Processor thread interrupted");
                    break;
                } catch (Exception e) {
                    logger.error("Unexpected error in processor thread: " + e.getMessage());
                }
            }
            logger.info("=== Processor Thread Shutting Down ===");
        });

        logger.info("Processor thread submitted to execution pool");
        logger.info("=== Update Processor Ready ===");
    }

    private void loadExistingData() {
        logger.info("=== Loading Existing Data From Redis ===");
        try (Jedis jedis = jedisPool.getResource()) {
            // Find all matching keys for our pattern
            // We need to convert the pattern used for subscription to a key pattern
            // ASL:*:*:* channel pattern needs to become ASL:*:*:* key pattern
            Set<String> keys = jedis.keys("ASL:*:*:*");
            logger.info("Found " + keys.size() + " existing keys in Redis");

            // Process each key
            for (String key : keys) {
                String value = jedis.get(key);
                if (value != null) {
                    logger.info("Processing existing key: " + key + " with value: " + value);

                    // Use the same processing logic as for messages
                    RedisKeyInfo keyInfo = parseRedisKey(key);
                    if (keyInfo != null) {
                        Map<String, Object> updates = parseUpdateMessage(key, value);
                        if (!updates.isEmpty()) {
                            // Process this directly (blocking) so it's guaranteed to finish before we subscribe
                            initializeSchemaFromMessage(key, updates);
                            logger.info("Processed existing key: " + key);
                        }
                    }
                }
            }
            logger.info("=== Completed Loading Existing Data ===");
            logger.info("Loaded " + recordsByFullId.size() + " records with " 
                    + schemasByPrefix.size() + " schemas");
        } catch (Exception e) {
            logger.error("Error loading existing data: " + e.getMessage());
        }
    }

    // Add queue size monitoring
    private static final int MAX_QUEUE_SIZE = 10000;
    private static final int QUEUE_WARN_THRESHOLD = 1000;
    private final AtomicInteger queueHighWaterMark = new AtomicInteger(0);
    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicLong schemaHits = new AtomicLong(0);
    private final AtomicLong schemaMisses = new AtomicLong(0);
    
    private void startRedisSubscriber() {
        try {
            logger.info("=== Starting Redis Subscriber ===");
            logger.info("Creating Redis subscriber connection");
            Jedis subscriberJedis = jedisPool.getResource();
            
            // Add connection status check
            String response = subscriberJedis.ping();
            logger.info("Subscriber connection test: " + response);
            logger.info("Subscribing to channel pattern: " + REDIS_CHANNEL_PATTERN);

            subscriberJedis.psubscribe(new JedisPubSub() {
                public void onPSubscribe(String pattern, int subscribedChannels) {
                    logger.info("=== Redis Pattern Subscription Active ===");
                    logger.info("Subscribed to pattern: " + pattern);
                    logger.info("Total subscribed channels: " + subscribedChannels);
                }
                
                public void onPMessage(String pattern, String channel, String message) {
                    // Add backpressure handling
                    int queueSize = updateQueue.size();
                    queueHighWaterMark.updateAndGet(curr -> Math.max(curr, queueSize));
                    
                    if (queueSize >= MAX_QUEUE_SIZE) {
                        logger.error("Queue full! Dropping message for channel: " + channel);
                        return;
                    }
                    
                    if (queueSize >= QUEUE_WARN_THRESHOLD) {
                        logger.warn("Queue size warning: " + queueSize + " messages pending");
                    }

                    // Track message stats
                    messageCount.incrementAndGet();
                    schemaHits.incrementAndGet(); // Increment by 1 for each message
                    logger.info("=== Redis Message Received ===");
                    logger.info("Pattern: " + pattern);
                    logger.info("Channel: " + channel);
                    logger.info("Message: " + message);

                    // Process message since it matched our pattern
                    RedisKeyInfo keyInfo = parseRedisKey(channel);
                    if (keyInfo != null) {
                        Map<String, Object> updates = parseUpdateMessage(channel, message);
                        if (!updates.isEmpty()) {
                            // Pass full channel info to processor
                            updateQueue.offer(new RecordUpdate(channel, keyInfo.recordId, updates));
                            logger.info("Queued update for processing - Channel: " + channel + ", Record: " + keyInfo.recordId);
                        }
                    }
                }
            }, REDIS_CHANNEL_PATTERN); // Use pattern subscription
            
            // Add monitoring thread
            monitor.scheduleAtFixedRate(() -> {
                logger.info(String.format(
                    "Stats - Messages: %d, Queue: %d/%d (max: %d), Schema Cache: %d hits/%d misses",
                    messageCount.get(),
                    updateQueue.size(), updateQueue.remainingCapacity(),
                    queueHighWaterMark.get(),
                    schemaHits.get(), schemaMisses.get()
                ));
            }, 60, 60, TimeUnit.SECONDS);

            logger.info("Redis subscriber started successfully");
        } catch (Exception e) {
            logger.error("Failed to start Redis subscriber: " + e.getMessage());
            throw new RuntimeException("Failed to start Redis subscriber", e);
        }
    }

    private void publishType(SchemaInfo schema, String typeName) throws MkvException {
        // Create and publish the type with fields and types
        MkvType type = new MkvType(typeName, schema.fieldNames, schema.fieldTypes);
        type.publish();
        
        if (!type.isValid()) {
            throw new MkvException("Failed to publish type " + typeName);
        }
        logger.info("Published new type " + typeName + " with fields: " + String.join(",", schema.fieldNames));
    }

    private MkvRecord publishNewRecord(String fullRecordId, SchemaInfo schema, Map<String, Object> values) throws MkvException {
        try {
            logger.info("Publishing record: " + fullRecordId + " with type: " + schema.typeName);
            
            MkvRecord record = new MkvRecord(fullRecordId, schema.typeName);
            record.publish();
            
            if (!record.isValid()) {
                throw new MkvException("Failed to create record: " + fullRecordId);
            }

            // Supply values in a single batch
            MkvSupplyBuilder builder = new MkvSupplyBuilder(record);
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                Integer fieldIndex = schema.fieldIndexMap.get(entry.getKey());
                if (fieldIndex != null) {
                    Object convertedValue = convertValueToType(entry.getValue(), schema.fieldTypes[fieldIndex]);
                    logger.info(String.format("Setting field %s[%d] = '%s' (%s)", 
                        entry.getKey(), fieldIndex, convertedValue, schema.fieldTypes[fieldIndex]));
                        
                    // Ensure primitive types are handled correctly
                    if (convertedValue != null) {
                        if (schema.fieldTypes[fieldIndex] == MkvFieldType.INT) {
                            builder.setField(fieldIndex, ((Number)convertedValue).intValue());
                        } else if (schema.fieldTypes[fieldIndex] == MkvFieldType.REAL) {
                            builder.setField(fieldIndex, ((Number)convertedValue).doubleValue());
                        } else {
                            builder.setField(fieldIndex, convertedValue.toString());
                        }
                    }
                }
            }

            record.supply(builder.getSupply());
            
            if (!record.isValid()) {
                throw new MkvException("Record invalid after supply: " + fullRecordId);
            }

            logger.info("Successfully published record: " + fullRecordId);
            return record;
            
        } catch (Exception e) {
            logger.error("Failed to publish record " + fullRecordId + ": " + e.getMessage());
            throw new MkvException("Record publication failed", e);
        }
    }

    private MkvFieldType inferFieldType(Object value) {
        if (value == null) return MkvFieldType.STR;

        if (!(value instanceof ParsedToken)) {
            logger.warn("Value not wrapped in ParsedToken: " + value);
            return MkvFieldType.STR;
        }

        ParsedToken token = (ParsedToken)value;
        if (token.wasQuoted) {
            return MkvFieldType.STR;
        }

        // Try numeric conversion for unquoted values
        try {
            if (token.value.contains(".")) {
                Double.parseDouble(token.value);
                return MkvFieldType.REAL;
            } else {
                Integer.parseInt(token.value);
                return MkvFieldType.INT;
            }
        } catch (NumberFormatException e) {
            return MkvFieldType.STR;
        }
    }

    private Object convertValueToType(Object value, MkvFieldType type) {
        if (value == null) return null;

        if (!(value instanceof ParsedToken)) {
            logger.warn("Value not wrapped in ParsedToken, treating as string: " + value);
            return value.toString();
        }

        ParsedToken token = (ParsedToken)value;
        if (token.wasQuoted) {
            return token.value;
        }

        try {
            if (type == MkvFieldType.STR) {
                return token.value;
            } else if (type == MkvFieldType.INT) {
                return Integer.parseInt(token.value.trim());
            } else if (type == MkvFieldType.REAL) {
                return Double.parseDouble(token.value.trim());
            }
            return token.value;
        } catch (Exception e) {
            logger.error("Conversion failed for " + token.value + " to " + type);
            return token.value;
        }
    }

    private void applyUpdates(MkvRecord record, Map<String, Object> updates, SchemaInfo schema) {
        if (record == null || updates == null) return;

        try {
            MkvSupplyBuilder builder = new MkvSupplyBuilder(record);
            boolean hasUpdates = false;

            for (Map.Entry<String, Object> entry : updates.entrySet()) {
                Integer fieldIndex = schema.fieldIndexMap.get(entry.getKey());
                if (fieldIndex != null) {
                    Object value = convertValueToType(entry.getValue(), schema.fieldTypes[fieldIndex]);
                    if (value != null) {
                        builder.setField(fieldIndex, value);
                        hasUpdates = true;
                    }
                }
            }

            if (hasUpdates) {
                record.supply(builder.getSupply());
            }
        } catch (Exception e) {
            logger.error("Failed to apply updates to record: " + record.getName());
        }
    }

    private void startHeartbeat() {
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (!running.get()) return;
            
            try (Jedis jedis = jedisPool.getResource()) {
            
                Instant instant = Instant.ofEpochMilli(System.currentTimeMillis());
                LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

                LocalDate date = dateTime.toLocalDate(); // For date only
                LocalTime time = dateTime.toLocalTime(); // For time only
                String heartbeatMessage = String.format("HEARTBEAT,%s,%s,%s,%s,%s,%s", 
                    "ION-Publisher-" + mkv.getApplicationVersion(),
                    "Hostname: " + hostname,
                    "Date: " + date,
                    "Time: " + time,
                    "Messages: " + messageCount.get(),
                    "Queue High Water Mark: " + queueHighWaterMark.get()
                );

                jedis.publish(HEARTBEAT_CHANNEL, heartbeatMessage);
                logger.info("Sent heartbeat: " + heartbeatMessage);
            } catch (Exception e) {
                logger.error("Failed to send heartbeat: " + e.getMessage());
            }
        }, 0, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);

        logger.info("Heartbeat started on channel: " + HEARTBEAT_CHANNEL);
    }

    public boolean shutdown() {
        logger.info("Shutting down publisher");
        running.set(false);
        
        // Shutdown heartbeat executor
        heartbeatExecutor.shutdown();
        try {
            heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            heartbeatExecutor.shutdownNow();
        }
        
        // Gracefully shutdown processor pool
        processorPool.shutdown();
        try {
            if (!processorPool.awaitTermination(60, TimeUnit.SECONDS)) {
                processorPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            processorPool.shutdownNow();
        }
        
        // Close Redis pool
        jedisPool.close();
        
        // Stop MKV using the instance method  
        mkv.stop();
        
        // Add monitoring shutdown
        monitor.shutdown();
        try {
            monitor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            monitor.shutdownNow();
        }
        
        // Log final stats
        logger.info(String.format(
            "Final Stats - Messages Processed: %d, Max Queue Size: %d, Schema Cache Hits/Misses: %d/%d",
            messageCount.get(), queueHighWaterMark.get(),
            schemaHits.get(), schemaMisses.get()
        ));

        logger.info("Publisher shutdown complete");
        return true;
    }
    
    /**
     * Gets the MkvLog instance for use by other components
     * @return The MkvLog instance
     */
    public MkvLog getMkvLog() {
        return myLog;
    }

    /**
     * Gets the current log level
     * @return The log level
     */
    public int getLogLevel() {
        return logLevel;
    }

}
