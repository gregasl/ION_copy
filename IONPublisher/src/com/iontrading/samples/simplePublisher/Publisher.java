/*
 * Publisher - ION Market Data Publisher Implementation
 *
 * This class implements a market data publisher that:
 * 1. Connects to a Redis cache to receive market data updates
 * 2. Dynamically creates MKV schemas based on received data
 * 3. Publishes data through the ION MKV middleware
 * 
 * Key Features:
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
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.enums.MkvFieldType;
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
import java.sql.Date;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.logging.ConsoleHandler;
import java.util.logging.SimpleFormatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.stream.Collectors;

public class Publisher {
    private static final Logger LOGGER = Logger.getLogger("SimplePublisher");
    private static final Level PRODUCTION_LOG_LEVEL = Level.WARNING; // Change to WARNING in production
    private static final int LOG_BUFFER_SIZE = 8192; // Increased buffer size
    private static final ThreadLocal<StringBuilder> logBuffer = ThreadLocal.withInitial(() -> new StringBuilder(LOG_BUFFER_SIZE));
    private static final boolean IS_PRODUCTION = Boolean.getBoolean("production.mode");
    
    static {
        // Configure performance-optimized logging
        ConsoleHandler handler = new ConsoleHandler() {
            private final AtomicLong lastFlush = new AtomicLong();
            private static final long FLUSH_INTERVAL_MS = 5000; // Increased flush interval for production
            
            @Override
            public void publish(LogRecord record) {
                if (!isLoggable(record)) return;
                
                // In production, only log WARNING and above
                if (IS_PRODUCTION && record.getLevel().intValue() < Level.WARNING.intValue()) {
                    return;
                }
                
                super.publish(record);
                
                // Periodic flush with backoff
                long now = System.currentTimeMillis();
                long last = lastFlush.get();
                if (now - last > FLUSH_INTERVAL_MS && lastFlush.compareAndSet(last, now)) {
                    flush();
                }
            }
        };

        SimpleFormatter formatter = new SimpleFormatter() {
            private static final String format = "[%1$tT] [%2$s] %3$s: %5$s%n"; // Removed method name
            private final AtomicLong lastTimestamp = new AtomicLong();
            private volatile String cachedTimeString = "";
            private final Map<Level, String> levelCache = new ConcurrentHashMap<>();

            @Override
            public synchronized String format(LogRecord lr) {
                if (!isLoggable(lr)) return "";
                
                StringBuilder sb = logBuffer.get();
                sb.setLength(0);
                
                // Cache timestamp string for 1 second
                long now = lr.getMillis() / 1000;
                if (now != lastTimestamp.get()) {
                    cachedTimeString = String.format("[%tT]", lr.getMillis());
                    lastTimestamp.set(now);
                }
                
                String levelString = levelCache.computeIfAbsent(lr.getLevel(), 
                    level -> String.format("[%s]", level.getName()));
                
                sb.append(cachedTimeString)
                  .append(' ')
                  .append(levelString);
                
                // Only add class name for warnings and errors
                if (lr.getLevel().intValue() >= Level.WARNING.intValue()) {
                    String className = lr.getSourceClassName();
                    sb.append(' ')
                      .append(className.substring(className.lastIndexOf('.') + 1))
                      .append(':');
                }
                
                sb.append(' ')
                  .append(lr.getMessage());
                
                return sb.toString();
            }

            private boolean isLoggable(LogRecord lr) {
                return !IS_PRODUCTION || lr.getLevel().intValue() >= Level.WARNING.intValue();
            }
        };
        
        handler.setFormatter(formatter);
        handler.setLevel(IS_PRODUCTION ? PRODUCTION_LOG_LEVEL : Level.INFO);
        
        // Remove existing handlers and set optimized one
        Logger root = Logger.getLogger("");
        for (Handler h : root.getHandlers()) {
            root.removeHandler(h);
        }
        
        LOGGER.setLevel(IS_PRODUCTION ? PRODUCTION_LOG_LEVEL : Level.INFO);
        LOGGER.addHandler(handler);
        LOGGER.setUseParentHandlers(false);
    }

    // Replace all direct logging calls with these optimized methods
    private static void logDebug(String msg) {
        if (!IS_PRODUCTION && LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(msg);
        }
    }

    private static void logInfo(String msg) {
        if (!IS_PRODUCTION && LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(msg);
        }
    }

    private static void logWarning(String msg) {
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning(msg);
        }
    }

    private static void logError(String msg, Throwable t) {
        if (LOGGER.isLoggable(Level.SEVERE)) {
            StringBuilder sb = logBuffer.get();
            sb.setLength(0);
            sb.append(msg);
            if (t != null) {
                sb.append(": ").append(t.getMessage());
            }
            LOGGER.severe(sb.toString());
        }
    }

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
        LOGGER.info("=== Starting ION Publisher ===");
        LOGGER.info("Initializing with args: " + Arrays.toString(args));

        // Create the initial configuration used to start the engine
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        LOGGER.info("Created MkvQoS configuration");
        
        // Initialize fields in constructor
        this.updateQueue = new LinkedBlockingQueue<>(1000);
        this.processorPool = Executors.newSingleThreadExecutor();
        LOGGER.info("Created processing queue and thread pool");
        
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
                    LOGGER.warning("Invalid Redis port number, using default: " + DEFAULT_REDIS_PORT);
                }
            }
        }
        
        LOGGER.info("Connecting to Redis at " + redisHost + ":" + redisPort);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        this.jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
        
        // Test Redis connection
        try (Jedis jedis = jedisPool.getResource()) {
            String pong = jedis.ping();
            LOGGER.info("Redis connection test successful: " + pong);
        } catch (Exception e) {
            LOGGER.severe("Failed to connect to Redis: " + e.getMessage());
            throw new RuntimeException("Failed to connect to Redis", e);
        }
        
        try {
            LOGGER.info("Starting MKV engine...");
            // Start the engine and get back the instance of Mkv
            this.mkv = Mkv.start(qos);
            LOGGER.info("MKV engine started successfully");
            
            // Start processor thread first
            LOGGER.info("Starting processor thread...");
            startProcessorThread();
            Thread.sleep(1000); // Give processor thread time to initialize
            
            LOGGER.info("Starting heartbeat...");
            startHeartbeat();
            
            LOGGER.info("Loading existing data from Redis...");
            loadExistingData();

            // Then start Redis subscriber
            LOGGER.info("Starting Redis subscriber...");
            startRedisSubscriber();
            
            LOGGER.info("=== Publisher initialization complete ===");
            LOGGER.info("Publisher components status:");
            LOGGER.info("- Redis Connection: ACTIVE");
            LOGGER.info("- MKV Engine: RUNNING");
            LOGGER.info("- Update Processor: PROCESSING");
            LOGGER.info("- Redis Subscriber: LISTENING");
            LOGGER.info("Schema/Record Statistics:");
            LOGGER.info("- Schemas loaded: " + schemasByPrefix.size());
            LOGGER.info("- Records active: " + recordsByFullId.size());
            LOGGER.info("- Queue size: " + updateQueue.size() + "/" + updateQueue.remainingCapacity());
            LOGGER.info("");
            LOGGER.info("Waiting for Redis messages on pattern: " + REDIS_CHANNEL_PATTERN);
        } catch (MkvException | InterruptedException e) {
            LOGGER.severe("Failed to initialize publisher: " + e.getMessage());
            throw new RuntimeException("Failed to initialize publisher", e);
        }
    }

    public static void main(String[] args) {
        Publisher publisher = new Publisher(args);
        
        // Add shutdown hook to ensure clean shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown hook triggered");
            publisher.shutdown();
        }));
    }
    
    /**
     * Parse Redis key to extract components - simplified to match simple publisher
     */
    private RedisKeyInfo parseRedisKey(String channel) {
        String[] parts = channel.split(REDIS_KEY_SEPARATOR);
        if (parts.length < 4) {
            LOGGER.warning("Invalid channel format (expected ASL:market:instrument:type): " + channel);
            return null;
        }
        
        String source = parts[0];
        String recordPrefix = "ALL." + parts[1] + "." + source + ".";
        String recordId = parts[2];
        String typeName = parts[3];
        
        LOGGER.info("Parsed Redis key - Source: " + source + 
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
            logWarning("Message must have complete name-value pairs. Got " + tokens.size() + " tokens");
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

        LOGGER.info("Parsed tokens: " + tokens.stream()
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

        LOGGER.info("Initializing schema for channel: " + channel);
        LOGGER.info("Field values: " + fieldValues);

        String fullRecordId = keyInfo.recordPrefix + keyInfo.recordId;
        SchemaInfo schema = schemasByPrefix.get(keyInfo.recordPrefix);

        synchronized (recordLock) {
            if (schema == null || !isSchemaCompatible(schema, fieldValues)) {
                LOGGER.info("Creating new schema for " + keyInfo.recordPrefix);
                schema = createAndPublishSchema(keyInfo, fieldValues);
                if (schema == null) return;
            }

            MkvRecord record = recordsByFullId.get(fullRecordId);
            if (record == null) {
                try {
                    LOGGER.info("Publishing new record " + fullRecordId);
                    record = publishNewRecord(fullRecordId, schema, fieldValues);
                    if (record.isValid()) {
                        recordsByFullId.put(fullRecordId, record);
                        LOGGER.info("Successfully published record " + fullRecordId);
                    } else {
                        LOGGER.warning("Failed to publish valid record " + fullRecordId);
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error publishing record " + fullRecordId, e);
                }
            } else {
                LOGGER.info("Updating existing record " + fullRecordId);
                applyUpdates(record, fieldValues, schema);
            }
        }
    }

    private boolean isSchemaCompatible(SchemaInfo schema, Map<String, Object> newValues) {
        // Check if schema has all fields from new values
        Set<String> schemaFields = new HashSet<>(Arrays.asList(schema.fieldNames));
        
        for (String fieldName : newValues.keySet()) {
            if (!schemaFields.contains(fieldName)) {
                LOGGER.info("Schema missing field: " + fieldName);
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
                    LOGGER.info(String.format("Type mismatch for field %s: existing=%s, new=%s", 
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

        LOGGER.info("Generated sanitized type name: " + typeName);
        
        SchemaInfo schema = createSchemaInfo(updates);
        
        try {
            LOGGER.info(String.format(
                "Creating new schema - Type: %s, Prefix: %s\nFields: %s\nTypes: %s",
                typeName, keyInfo.recordPrefix,
                String.join(",", schema.fieldNames),
                Arrays.toString(schema.fieldTypes)
            ));

            // Add detailed logging for type creation
            LOGGER.info("Checking for existing type: " + typeName);
            MkvType existingType = mkv.getPublishManager().getMkvType(typeName);
            
            if (existingType == null || !existingType.isValid()) {
                try {
                    LOGGER.info("Creating new MkvType with fields:");
                    for (int i = 0; i < schema.fieldNames.length; i++) {
                        LOGGER.info(String.format("  Field[%d]: name='%s', type=%s", 
                            i, schema.fieldNames[i], schema.fieldTypes[i]));
                    }
                    
                    MkvType type = new MkvType(typeName, schema.fieldNames, schema.fieldTypes);
                    LOGGER.info("Publishing type: " + typeName);
                    type.publish();
                    
                    if (!type.isValid()) {
                        throw new MkvException("Type was created but is not valid: " + typeName);
                    }
                    
                    LOGGER.info("Successfully published new type: " + typeName);
                } catch (MkvException e) {
                    LOGGER.log(Level.SEVERE, String.format(
                        "Failed to create/publish type %s: %s\nStack: %s", 
                        typeName, e.getMessage(),
                        Arrays.toString(e.getStackTrace())
                    ));
                    return null;
                }
            } else {
                LOGGER.info("Using existing valid type: " + typeName);
            }

            SchemaInfo newSchema = new SchemaInfo(
                schema.fieldNames, 
                schema.fieldTypes,
                schema.fieldIndexMap, 
                typeName
            );
            
            schemasByPrefix.put(keyInfo.recordPrefix, newSchema);
            LOGGER.info("Stored schema for prefix: " + keyInfo.recordPrefix);
            
            return newSchema;
            
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, String.format(
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
        LOGGER.info("=== Initializing Update Processor ===");
        LOGGER.info("Max queue capacity: " + updateQueue.remainingCapacity());
        
        processorPool.submit(() -> {
            Thread.currentThread().setName("MKV-Update-Processor");
            LOGGER.info("=== Processor Thread Started ===");
            LOGGER.info("Thread name: " + Thread.currentThread().getName());
            LOGGER.info("Thread ID: " + Thread.currentThread().getId());
            LOGGER.info("Priority: " + Thread.currentThread().getPriority());
            LOGGER.info("Waiting for updates on queue...");
            
            while (running.get()) {
                try {
                    RecordUpdate update = updateQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (update != null) {
                        LOGGER.info("=== Processing Update ===");
                        LOGGER.info("Channel: " + update.channel);
                        LOGGER.info("Record ID: " + update.recordId); 
                        LOGGER.info("Fields to update: " + update.updates.keySet());
                        
                        try {
                            initializeSchemaFromMessage(update.channel, update.updates);
                            LOGGER.info("=== Update Processing Complete ===");
                        } catch (Exception e) {
                            LOGGER.log(Level.SEVERE, "Error processing update for channel " + update.channel + 
                                     "\nError: " + e.getMessage() + 
                                     "\nStack: " + Arrays.toString(e.getStackTrace()));
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.info("Processor thread interrupted");
                    break;
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Unexpected error in processor thread", e);
                }
            }
            LOGGER.info("=== Processor Thread Shutting Down ===");
        });
        
        LOGGER.info("Processor thread submitted to execution pool");
        LOGGER.info("=== Update Processor Ready ===");
    }

    private void loadExistingData() {
        LOGGER.info("=== Loading Existing Data From Redis ===");
        try (Jedis jedis = jedisPool.getResource()) {
            // Find all matching keys for our pattern
            // We need to convert the pattern used for subscription to a key pattern
            // ASL:*:*:* channel pattern needs to become ASL:*:*:* key pattern
            Set<String> keys = jedis.keys("ASL:*:*:*");
            LOGGER.info("Found " + keys.size() + " existing keys in Redis");
            
            // Process each key
            for (String key : keys) {
                String value = jedis.get(key);
                if (value != null) {
                    LOGGER.info("Processing existing key: " + key + " with value: " + value);
                    
                    // Use the same processing logic as for messages
                    RedisKeyInfo keyInfo = parseRedisKey(key);
                    if (keyInfo != null) {
                        Map<String, Object> updates = parseUpdateMessage(key, value);
                        if (!updates.isEmpty()) {
                            // Process this directly (blocking) so it's guaranteed to finish before we subscribe
                            initializeSchemaFromMessage(key, updates);
                            LOGGER.info("Processed existing key: " + key);
                        }
                    }
                }
            }
            LOGGER.info("=== Completed Loading Existing Data ===");
            LOGGER.info("Loaded " + recordsByFullId.size() + " records with " 
                    + schemasByPrefix.size() + " schemas");
        } catch (Exception e) {
            LOGGER.severe("Error loading existing data: " + e.getMessage());
            LOGGER.log(Level.SEVERE, "Stack trace: ", e);
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
            LOGGER.info("=== Starting Redis Subscriber ==="); 
            LOGGER.info("Creating Redis subscriber connection");
            Jedis subscriberJedis = jedisPool.getResource();
            
            // Add connection status check
            String response = subscriberJedis.ping();
            LOGGER.info("Subscriber connection test: " + response);
            LOGGER.info("Subscribing to channel pattern: " + REDIS_CHANNEL_PATTERN);

            subscriberJedis.psubscribe(new JedisPubSub() {
                @Override
                public void onPSubscribe(String pattern, int subscribedChannels) {
                    LOGGER.info("=== Redis Pattern Subscription Active ===");
                    LOGGER.info("Subscribed to pattern: " + pattern);
                    LOGGER.info("Total subscribed channels: " + subscribedChannels);
                }
                
                @Override 
                public void onPMessage(String pattern, String channel, String message) {
                    // Add backpressure handling
                    int queueSize = updateQueue.size();
                    queueHighWaterMark.updateAndGet(curr -> Math.max(curr, queueSize));
                    
                    if (queueSize >= MAX_QUEUE_SIZE) {
                        LOGGER.warning("Queue full! Dropping message for channel: " + channel);
                        return;
                    }
                    
                    if (queueSize >= QUEUE_WARN_THRESHOLD) {
                        LOGGER.warning("Queue size warning: " + queueSize + " messages pending");
                    }

                    // Track message stats
                    messageCount.incrementAndGet();
                    
                    LOGGER.info("=== Redis Message Received ===");
                    LOGGER.info("Pattern: " + pattern);
                    LOGGER.info("Channel: " + channel);
                    LOGGER.info("Message: " + message);

                    // Process message since it matched our pattern
                    RedisKeyInfo keyInfo = parseRedisKey(channel);
                    if (keyInfo != null) {
                        Map<String, Object> updates = parseUpdateMessage(channel, message);
                        if (!updates.isEmpty()) {
                            // Pass full channel info to processor
                            updateQueue.offer(new RecordUpdate(channel, keyInfo.recordId, updates));
                            LOGGER.info("Queued update for processing - Channel: " + channel + ", Record: " + keyInfo.recordId);
                        }
                    }
                }
            }, REDIS_CHANNEL_PATTERN); // Use pattern subscription
            
            // Add monitoring thread
            monitor.scheduleAtFixedRate(() -> {
                LOGGER.info(String.format(
                    "Stats - Messages: %d, Queue: %d/%d (max: %d), Schema Cache: %d hits/%d misses",
                    messageCount.get(),
                    updateQueue.size(), updateQueue.remainingCapacity(),
                    queueHighWaterMark.get(),
                    schemaHits.get(), schemaMisses.get()
                ));
            }, 60, 60, TimeUnit.SECONDS);

            LOGGER.info("Redis subscriber started successfully");
        } catch (Exception e) {
            LOGGER.severe("Failed to start Redis subscriber: " + e.getMessage());
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
        LOGGER.info("Published new type " + typeName + " with fields: " + String.join(",", schema.fieldNames));
    }

    private MkvRecord publishNewRecord(String fullRecordId, SchemaInfo schema, Map<String, Object> values) throws MkvException {
        try {
            LOGGER.info("Publishing record: " + fullRecordId + " with type: " + schema.typeName);
            
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
                    LOGGER.info(String.format("Setting field %s[%d] = '%s' (%s)", 
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

            LOGGER.info("Successfully published record: " + fullRecordId);
            return record;
            
        } catch (Exception e) {
            LOGGER.severe("Failed to publish record " + fullRecordId + ": " + e.getMessage());
            throw new MkvException("Record publication failed", e);
        }
    }

    private MkvFieldType inferFieldType(Object value) {
        if (value == null) return MkvFieldType.STR;

        if (!(value instanceof ParsedToken)) {
            LOGGER.warning("Value not wrapped in ParsedToken: " + value);
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
            LOGGER.warning("Value not wrapped in ParsedToken, treating as string: " + value);
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
            LOGGER.warning("Conversion failed for " + token.value + " to " + type);
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
            LOGGER.severe("Failed to apply updates to record: " + record.getName());
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
                String heartbeatMessage = String.format("HEARTBEAT,%s,%d", 
                    "ION-Publisher-" + mkv.getApplicationVersion(),
                    "Hostname: " + hostname,
                    "Date: " + date,
                    "Time: " + time,
                    "Messages: " + messageCount.get(),
                    "Queue High Water Mark: " + queueHighWaterMark.get()
                );

                jedis.publish(HEARTBEAT_CHANNEL, heartbeatMessage);
                logDebug("Sent heartbeat: " + heartbeatMessage);
            } catch (Exception e) {
                logWarning("Failed to send heartbeat: " + e.getMessage());
            }
        }, 0, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
        
        LOGGER.info("Heartbeat started on channel: " + HEARTBEAT_CHANNEL);
    }

    public void shutdown() {
        LOGGER.info("Shutting down publisher");
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
        
        // Stop MKV
        mkv.stop();
        
        // Add monitoring shutdown
        monitor.shutdown();
        try {
            monitor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            monitor.shutdownNow();
        }
        
        // Log final stats
        LOGGER.info(String.format(
            "Final Stats - Messages Processed: %d, Max Queue Size: %d, Schema Cache Hits/Misses: %d/%d",
            messageCount.get(), queueHighWaterMark.get(),
            schemaHits.get(), schemaMisses.get()
        ));
        
        LOGGER.info("Publisher shutdown complete");
    }
}
