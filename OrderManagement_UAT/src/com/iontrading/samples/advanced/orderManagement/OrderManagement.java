/*
 * OrderManagement
 *
 * OrderManagement object starts the component, register the relevant listeners
 * then subscribes to the depths of the configured instruments and to the orders
 * chain.
 * The set up of the subscription is done in onPublishIdle in order to not perform
 * expensive calculations in the onPublish method.
 */

package com.iontrading.samples.advanced.orderManagement;

import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvLog;
import com.iontrading.mkv.MkvPattern;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvChainAction;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.events.MkvChainListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.enums.MkvShutdownMode;
import com.iontrading.mkv.events.MkvPlatformListener;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * OrderManagement is the main component of the system.
 */
public class OrderManagement implements MkvPublishListener, MkvRecordListener,
    MkvChainListener, IOrderManager, MkvPlatformListener {
    
    private MkvLog myLog;
    private int logLevel;
    private IONLogger logger;

  private volatile boolean isShuttingDown = false;

  // Instance of DepthListener to load and access instrument data
  private static DepthListener depthListener;

  //Map to track which instrument pairs we've already traded
private final Map<String, Long> lastTradeTimeByInstrument = 
    Collections.synchronizedMap(new LinkedHashMap<String, Long>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > 10000;
        }
    });
  //Minimum time between trades for the same instrument (milliseconds)
  private static final long MIN_TRADE_INTERVAL_MS = 200; // 200 milliseconds
  //Flag to enable or disable continuous trading
  private boolean continuousTradingEnabled = true;
  //Timer for periodic market rechecks
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private final Map<String, String> venueToTraderMap = new HashMap<>();

  // Pattern identifiers from MarketDef
  private static final String ORDER_PATTERN = MarketDef.ORDER_PATTERN;
  private static final String[] ORDER_FIELDS = MarketDef.ORDER_FIELDS;
  private static final String DEPTH_PATTERN = MarketDef.DEPTH_PATTERN;
  private static final String[] DEPTH_FIELDS = MarketDef.DEPTH_FIELDS;
  private static final String LOGIN_PATTERN = MarketDef.LOGIN_PATTERN;
  private static final String[] LOGIN_FIELDS = MarketDef.LOGIN_FIELDS;
  private static final String INSTRUMENT_PATTERN = MarketDef.INSTRUMENT_PATTERN;
  private static final String[] INSTRUMENT_FIELDS = MarketDef.INSTRUMENT_FIELDS;
  
private final Set<String> subscribedPatterns = Collections.synchronizedSet(new HashSet<>());

  // Valid venue list using MarketDef constants
  Set<String> validVenues = new HashSet<>(Arrays.asList(
      MarketDef.DEALERWEB_REPO,
      MarketDef.BTEC_REPO_US, 
      MarketDef.FENICS_USREPO
  ));

  private boolean isPatternSubscribed = false;
  private final Object subscriptionLock = new Object();
 
  private final ScheduledExecutorService heartbeatScheduler = Executors.newScheduledThreadPool(1);

  private final ScheduledExecutorService orderExpirationScheduler = Executors.newScheduledThreadPool(1);
  
  private static final ThreadLocal<StringBuilder> messageBuilder = 
    ThreadLocal.withInitial(() -> new StringBuilder(512));

  // Redis connection constants
  private static final String HEARTBEAT_CHANNEL = "HEARTBEAT:ION:ORDERMANAGEMENTUAT";
  private static final String ADMIN_CHANNEL = "ADMIN:ION:ORDERMANAGEMENTUAT";
  private static final String REDIS_HOST = "cacheuat";
  private static final int REDIS_PORT = 6379;
  
  public final String hostname = System.getenv("COMPUTERNAME");
    
  //Flag to track if the system is in stopped state
  private volatile boolean isSystemStopped = false;
  
  /**
   * Time to live in milliseconds before automatic cancellation
   * Public to allow access from OrderManagement
   */
  public static final long ORDER_TTL_MS = 60000; // 1.0 minute until 7:15 am
  private static final long SHORT_ORDER_TTL_MS = 15000; // 15 seconds thereafter
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Redis connection pool
  private redis.clients.jedis.JedisPool jedisPool;
  private boolean isRedisConnected = false;
  
  private final Map<String, Integer> orderIdToReqIdMap = new HashMap<>();

  private final Object gcDataLock = new Object();
  private final AtomicReference<Double> latestCashGC = new AtomicReference<>(0.0);
  private final AtomicReference<Double> latestRegGC = new AtomicReference<>(0.0);
  private final AtomicReference<GCBest> sharedGCBestCash = new AtomicReference<>();
  private final AtomicReference<GCBest> sharedGCBestREG = new AtomicReference<>();

  private final ConcurrentHashMap<String, Integer> activeOrders = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Boolean> venueActiveStatus = new ConcurrentHashMap<>();

  private final Map<String, Best> latestBestByInstrument = new ConcurrentHashMap<String, Best>() {
    private static final int MAX_SIZE = 5000;
    @Override
    public Best put(String key, Best value) {
        if (size() >= MAX_SIZE) {
            // Remove oldest entries (simple FIFO)
            Iterator<String> iterator = keySet().iterator();
            while (iterator.hasNext() && size() >= MAX_SIZE * 0.9) {
                iterator.next();
                iterator.remove();
            }
        }
        return super.put(key, value);
    }
};

  private volatile boolean shutdownRequested = false;
  private volatile int pendingOperations = 0;
  private final Object shutdownLock = new Object();
  private static final long SHUTDOWN_TIMEOUT_MS = 30000; // 30 seconds

  // Initialize Redis connection pool
  private void initializeRedisConnection() {
      if (!isRedisConnected) {
          try {
              // Create a very simple Jedis connection pool without any advanced configuration
              // This will work with the oldest versions of Jedis
              jedisPool = new redis.clients.jedis.JedisPool(REDIS_HOST, REDIS_PORT);
              
              // Test the connection
              Jedis jedis = null;
              try {
                  jedis = jedisPool.getResource();
                  String pingResponse = jedis.ping();
                  if (!"PONG".equals(pingResponse)) {
                      throw new RuntimeException("Redis ping test failed, expected 'PONG', got: " + pingResponse);
                  }
                  logger.info("Connected to Redis pool at " + REDIS_HOST + ":" + REDIS_PORT);
                  isRedisConnected = true;
              } finally {
                  // Return the resource to the pool
                  if (jedis != null) {
                      jedisPool.returnResource(jedis);
                  }
              }
          } catch (Exception e) {
              logger.error("Error connecting to Redis pool " + e);
              throw new RuntimeException("Redis connection pool initialization failed", e);
          }
      }
  }
  
  // Publish method with JSON support
  public void publishToRedis(String key, Map<String, Object> data) {
      Jedis jedis = null;
      try {
          // Get a Jedis resource from the pool
          jedis = jedisPool.getResource();
          
          // Create a comprehensive payload
          Map<String, Object> payload = new HashMap<>();
          DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
          String formattedDate = LocalDateTime.now().format(formatter);
          payload.put("timestamp", formattedDate);
          payload.putAll(data);

          // Convert payload to JSON string
          String jsonPayload = OBJECT_MAPPER.writeValueAsString(payload);

          // Publish to Redis
          jedis.publish(key, jsonPayload);
          
      } catch (JedisConnectionException jce) {
          logger.error("Redis connection lost, attempting to reconnect " + jce);
          if (jedis != null) {
              jedisPool.returnBrokenResource(jedis);
              jedis = null;
          }
          try {
              initializeRedisConnection();
          } catch (Exception e) {
              logger.error("Failed to reconnect to Redis " + e);
          }
      } catch (Exception e) {
          logger.error("Error publishing to Redis " + e);
      } finally {
          if (jedis != null) {
              jedisPool.returnResource(jedis);
          }
      }
  }
  
  /**
   * Main method to start the application.
   * Initializes the MKV API and sets up subscriptions.
   */
  public static void main(String[] args) {
    // Create the order management instance
    OrderManagement om = new OrderManagement();
    
    try {
    	// Get thread information
    	Thread[] threads = new Thread[Thread.activeCount()];
    	Thread.enumerate(threads);
    	System.out.println("### DIRECT: Current threads:");
    	for (Thread t : threads) {
    	    if (t != null) {
    	        System.out.println("  - " + t.getName() + " (daemon: " + t.isDaemon() + ", alive: " + t.isAlive() + ", state: " + t.getState() + ")");
    	    }
    	}
      
      // Set up the Quality of Service for the MKV API
      MkvQoS qos = new MkvQoS();
      qos.setArgs(args);
      qos.setPublishListeners(new MkvPublishListener[] { om });
      qos.setPlatformListeners(new MkvPlatformListener[] { om });

      
      // Start the MKV API if it hasn't been started already
      Mkv existingMkv = Mkv.getInstance();
      om.logLevel = existingMkv.getProperties().getIntProperty("DEBUG");
      // Initialize the log after starting Mkv
      om.myLog = existingMkv.getLogManager().createLogFile("ORDER_MANAGEMENT");
      om.logger.info("Starting MKV API");
      if (existingMkv == null) {
          Mkv.start(qos);
      } else {
          om.logger.info("MKV API already started");
          existingMkv.getPublishManager().addPublishListener(om);
          existingMkv.getPlatform().addPlatformListener(om);
      }

      // Initialize DepthListener after MKV is started
      depthListener = new DepthListener(om);
      om.logger.info("DepthListener initialized");

      // DepthListener now handles instrument data loading internally
      // and reports status through its health monitoring methods
      om.logger.info("Instrument data will be loaded by DepthListener automatically");

      // Set up depth subscriptions - this will trigger the instrument data loading
      om.logger.info("Setting up depths subscriptions");
      om.subscribeToDepths();
    } catch (MkvException e) {
      om.logger.error("Failed to start MKV API: " + e.getMessage());
      om.logger.error("Error details: " + e);
    }
  }
    
  /**
   * Gets the native instrument ID for a source.
   * This delegates to the DepthListener.
   * 
   * @param instrumentId The instrument identifier
   * @param sourceId The source identifier
   * @return The native instrument ID or null if not found
   */
  public String getNativeInstrumentId(String instrumentId, String sourceId, Boolean isAON) {
    if (depthListener != null) {
      return depthListener.getInstrumentFieldBySourceString(instrumentId, sourceId, isAON);
    }
    logger.error("DepthListener not initialized - cannot get native instrument ID");
    return null;
  }
  
  /**
   * Map to cache market orders by their request ID.
   * This allows tracking orders throughout their lifecycle.
   */
  private final Map<Integer, MarketOrder> orders = new HashMap<>();

  /**
   * Unique identifier for this instance of the application.
   * Used to identify orders placed by this component.
   */
  private final String applicationId;

  /**
   * Creates a new instance of OrderManagement.
   * Generates a unique application ID based on the current time.
   */
  public OrderManagement() {

    applicationId = "Java_Order_Manager"; 
    logger.info("Application " + applicationId);
    
    initializeRedisConnection();
    
    // Add shutdown hook for Redis pool
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        if (jedisPool != null) {
            logger.info("Shutting down Redis connection pool");
            jedisPool.destroy(); // Use destroy() for older Jedis versions
        }
    }));
    
    // Initialize venue to trader mapping
    initializeTraderMap();

    // Initialize Redis control channel listener
    initializeRedisControlListener();
    
    // Initialize heartbeats
    initializeHeartbeat();

    // Initialize the market recheck scheduler
    initializeMarketRechecks();
   
    // Initialize the order expiration checker
    initializeOrderExpirationChecker();
}

  /**
   * Returns the unique application ID for this instance.
   * This is used to identify orders placed by this component.
   */
  public String getApplicationId() {
    return applicationId;
  }
  
  /**
   * Gets the DepthListener instance.
   * 
   * @return The DepthListener or null if not initialized
   */
  public static DepthListener getDepthListener() {
      return depthListener;
  }
  
  
  // Initialize the mapping of venues to trader IDs
  private void initializeTraderMap() {
    //  venueToTraderMap.put("BTEC_REPO_US", "EGEI");
    //  venueToTraderMap.put("DEALERWEB_REPO", "aslegerhard01");
    //  venueToTraderMap.put("FENICS_USREPO", "frosevan");
    venueToTraderMap.put("BTEC_REPO_US", "TEST2");
    venueToTraderMap.put("DEALERWEB_REPO", "asldevtrd1");
    venueToTraderMap.put("FENICS_USREPO", "frosasl1");
      // Add more venue-trader mappings as needed

    logger.info("Venue to trader mapping initialized with " + venueToTraderMap.size() + " entries");
  }

  public static long getOrderTtlForCurrentTime() {
	    // Get the current time
	    LocalTime currentTime = LocalTime.now();
	    
	    // Define time periods for different TTL values
	    LocalTime morningEnd = LocalTime.of(7, 15);   // 7:15 AM

	    // Determine which TTL to use based on time of day
	    if (currentTime.isBefore(morningEnd)) {
	        return ORDER_TTL_MS;
	    } else { 
	        return SHORT_ORDER_TTL_MS;
	    }
	}
  
  /**
   * Initializes the order expiration checker to automatically cancel orders
   * that have been in the market for too long.
   */
  private void initializeOrderExpirationChecker() {
      // Schedule a task to check for expired orders every 10 seconds
      orderExpirationScheduler.scheduleAtFixedRate(
          this::checkForExpiredOrders, 
          30,  // Initial delay (30 seconds)
          10,  // Check every 10 seconds
          TimeUnit.SECONDS
      );
      
      // Add a shutdown hook to clean up the scheduler
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          logger.info("Shutting down order expiration scheduler");
          shutdownExecutorGracefully(orderExpirationScheduler, "OrderExpiration", 60);
      }));

      logger.info("Order expiration checker initialized (TTL: 60 secs before 7:15, 15 secs after)");
  }

	//Helper method to get trader ID for a venue
	private String getTraderForVenue(String venue) {
	   String traderId = venueToTraderMap.get(venue);
	   if (traderId == null) {
	       logger.warn("No trader configured for venue: " + venue);
	       return "DEFAULT_TRADER"; // Fallback trader ID
	   }
	   return traderId;
	}

	/**
	 * Initializes the Redis control channel listener to handle stop/resume commands.
	 * This allows remote control of the order management system.
	 */
	private void initializeRedisControlListener() {
	    if (!isRedisConnected) {
	        logger.error("Cannot initialize Redis control listener - Redis is not connected");
	        return;
	    }

	    logger.info("Initializing Redis control channel listener on channel: " + ADMIN_CHANNEL);

	    // Start a separate thread for Redis pub/sub listening
	    Thread redisListenerThread = new Thread(() -> {
	        Jedis subscriberJedis = null;
	        try {
	            // Get a dedicated connection from the pool for pub/sub
	            subscriberJedis = jedisPool.getResource();

	            logger.info("Redis control listener started on channel: " + ADMIN_CHANNEL);

	            // Create a subscriber to process messages
	            subscriberJedis.subscribe(new JedisPubSubListener(), ADMIN_CHANNEL);
	            
	        } catch (Exception e) {
	            logger.error("Error in Redis control listener: " + e.getMessage());
	            if (subscriberJedis != null) {
	                jedisPool.returnBrokenResource(subscriberJedis);
	                subscriberJedis = null;
	            }
	        } finally {
	            // With older Jedis API, resources must be returned to the pool manually
	            if (subscriberJedis != null) {
	                try {
	                    jedisPool.returnResource(subscriberJedis);
	                } catch (Exception e) {
	                    logger.error("Error returning Redis subscriber to pool: " + e.getMessage());
	                }
	            }
	        }
	    }, "Redis-Control-Listener");
	    
	    // Set as daemon thread so it doesn't prevent JVM shutdown
	    redisListenerThread.setDaemon(true);
	    redisListenerThread.start();

	    logger.info("Redis control listener thread started");
	}
	
    /**
     * Initializes the heartbeat mechanism to publish periodic heartbeats to Redis.
     * This helps monitor the health of the OrderManagement component.
     */	
	private class JedisPubSubListener extends JedisPubSub {
	    @Override
	    public void onMessage(String channel, String message) {
	        if (!ADMIN_CHANNEL.equals(channel)) {
	            return; // Ignore messages from other channels
	        }

	        logger.info("Received control message: " + message + " on channel: " + channel);

	        try {
	            // Parse message as JSON to extract command and parameters
	            Map<String, Object> controlMessage = parseControlMessage(message);
	            String command = (String) controlMessage.getOrDefault("command", "");
	            
	            if ("STOP".equalsIgnoreCase(command)) {
	                handleStopCommand(controlMessage);
	            } else if ("RESUME".equalsIgnoreCase(command)) {
	                handleResumeCommand(controlMessage);
	            } else {
	                logger.warn("Unknown control command received: " + command);
	            }
	        } catch (Exception e) {
	            logger.error("Error processing control message: " + e.getMessage());
	        }
	    }

	    @Override
	    public void onSubscribe(String channel, int subscribedChannels) {
	        logger.info("Subscribed to Redis channel: " + channel);
	    }

	    @Override
	    public void onUnsubscribe(String channel, int subscribedChannels) {
	        logger.info("Unsubscribed from Redis channel: " + channel);
	    }
	}
        

    /**
     * Parses a control message string into a Map.
     * 
     * @param message The message string to parse
     * @return A Map containing the parsed message data
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseControlMessage(String message) {
        try {
            // Parse message as JSON
            return OBJECT_MAPPER.readValue(message, Map.class);
        } catch (Exception e) {
            // If not valid JSON, create a simple command map
            logger.warn("Failed to parse control message as JSON, treating as plain command: " + message);

            Map<String, Object> result = new HashMap<>();
            result.put("command", message.trim());
            return result;
        }
    }

    /**
     * Handles the STOP command, cancelling existing orders and preventing new ones.
     * 
     * @param controlMessage The control message containing command parameters
     */
    private void handleStopCommand(Map<String, Object> controlMessage) {
        // Skip if already stopped
        if (isSystemStopped) {
            logger.warn("System already in STOPPED state, ignoring redundant STOP command");
            return;
        }

        logger.info("Executing STOP command - cancelling orders and preventing new submissions");

        // Set the system to stopped state
        isSystemStopped = true;

        // Disable continuous trading to prevent auto-trading
        continuousTradingEnabled = false;
        
        // Cancel all existing orders
        cancelAllOrders();
        
        // Publish status update
    //    publishSystemStatus();

        logger.info("STOP command executed - system is now STOPPED");
    }

    /**
     * Handles the RESUME command, allowing new orders to be submitted.
     * 
     * @param controlMessage The control message containing command parameters
     */
    private void handleResumeCommand(Map<String, Object> controlMessage) {
        // Skip if not stopped
        if (!isSystemStopped) {
            logger.warn("System already in RUNNING state, ignoring redundant RESUME command");
            return;
        }

        logger.info("Executing RESUME command - allowing new order submissions");

        // Set the system back to running state
        isSystemStopped = false;

        // Re-enable continuous trading if requested
        Boolean enableTrading = (Boolean) controlMessage.getOrDefault("enableTrading", Boolean.TRUE);
        if (enableTrading) {
            continuousTradingEnabled = true;
            logger.info("Continuous trading re-enabled as part of RESUME command");
        } else {
            logger.info("Continuous trading remains disabled after RESUME command");
        }
        
        // Publish status update
    //    publishSystemStatus();

        logger.info("RESUME command executed - system is now RUNNING");
    }

    /**
     * Called when a publication event occurs.
     * This implementation doesn't handle individual publication events.
     */
    public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
        // Not handling individual publication events
        logger.info("Publication event for: {} " + mkvObject.getName());
    }

    /**
     * Called when a publication download is complete.
     * This is where we set up the order chain subscription and
     * potentially send an initial FAS order if configured.
     * 
     * @param component The component name
     * @param start Whether this is the start of idle time
     */
    public void onPublishIdle(String component, boolean start) {
        logger.info("Publication idle for component: " + component + ", start: " + start);
    }

    /**
     * Not interested in this event because our component is a pure subscriber
     * and is not supposed to receive requests for subscriptions.
     */
    public void onSubscribe(MkvObject mkvObject) {
        // No action needed - we are not a publisher
        logger.info("Subscription event for: " + mkvObject.getName());
    }

    /**
     * Implements the MkvPlatformListener.onMain method to handle platform events.
     * This is where we'll handle the shutdown request from the daemon.
     */
    @Override
    public void onMain(MkvPlatformEvent event) {
        if (event.intValue() == MkvPlatformEvent.SHUTDOWN_REQUEST_code) {
            logger.warn("Received shutdown request from MKV platform");

            // Set the shutdown flag
            // shutdownRequested = true;
            
            try {
                // Do the shutdown work synchronously in this method
                boolean isReady = performGracefulShutdown();
            if (isReady) {
                // Signal that we're completely done
                Mkv.getInstance().shutdown(MkvShutdownMode.SYNC, 
                    "OrderManagement shutdown complete");
                logger.info("Signaled SYNC shutdown to platform");
            } else {
                // We need more time, request async and let platform retry
                Mkv.getInstance().shutdown(MkvShutdownMode.ASYNC, 
                    "OrderManagement still processing...");
                logger.info("Requested ASYNC shutdown - platform will retry");
            }
            } catch (MkvException e) {
                logger.error("Error during shutdown signaling: " + e.getMessage() + " " + e);
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
     * Sets up subscriptions to depth records using a pattern-based approach.
     * Subscribes to the consolidated market data from VMO_REPO_US.
     */
    public void subscribeToDepths() {
            try {
            logger.info("Setting up subscriptions to instrument patterns");

            // Also subscribe to the instrument pattern explicitly
            subscribeToInstrumentPattern();

            logger.info("Subscribing to consolidated depth data using pattern: " + DEPTH_PATTERN);

            // Subscribe to the depth pattern
            subscribeToPattern();
            
            // // Subscribe to order records
            // subscribeToRecord();
            
            // Subscribe to order records
            subscribeToRecord(ORDER_PATTERN, ORDER_FIELDS);

            // Subscribe to login records
            subscribeToRecord(LOGIN_PATTERN, LOGIN_FIELDS);

            // Set up a monitor to check subscription status periodically
            setupPatternSubscriptionMonitor();
            
        } catch (Exception e) {
            logger.error("Error setting up depth subscriptions: " + e.getMessage() + e);
            logger.error("Error details: " + e);
        }
    }

    private void subscribeToRecord(String pattern, String[] fields) {
        try {
            // Get the publish manager to access patterns
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
        
            // Look up the pattern object
            MkvObject obj = pm.getMkvObject(pattern);

            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                // Subscribe within a synchronized block
                synchronized (subscriptionLock) {
                    // Check again inside synchronization to avoid race conditions
                    if (!subscribedPatterns.contains(pattern)) {
                        logger.info("Found " + pattern + ", subscribing: " + pattern);

                        ((MkvPattern) obj).subscribe(fields, this);

                        // Mark that we've successfully subscribed
                        subscribedPatterns.add(pattern);
                        logger.info("Successfully subscribed to " + pattern);
                    }
                }
            } else {
                logger.warn("Pattern not found: " + pattern);

                MkvPublishListener patternListener = new MkvPublishListener() {
                    @Override
                    public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
                        // Only proceed if we're not already subscribed
                        if (subscribedPatterns.contains(pattern)) {
                            return;
                        }
                        
                        // Check if this is our pattern being published
                        if (pub_unpub && mkvObject.getName().equals(pattern) &&
                            mkvObject.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                            trySubscribeAndRemoveListener(mkvObject, pm, this, pattern, fields);
                        }
                    }
                
                    @Override
                    public void onPublishIdle(String component, boolean start) {
                        // Only proceed if we're not already subscribed
                        if (subscribedPatterns.contains(pattern)) {
                            return;
                        }
                        
                        // Try looking for the pattern again at idle time
                        MkvObject obj = pm.getMkvObject(pattern);

                        if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                            trySubscribeAndRemoveListener(obj, pm, this, pattern, fields);
                        }
                    }
                
                    @Override
                    public void onSubscribe(MkvObject mkvObject) {
                        // Not needed
                    }
                };
                
                // Add the shared listener
                pm.addPublishListener(patternListener);
            }
        } catch (Exception e) {
            logger.error("Error subscribing to pattern: " + e.getMessage() + e);
        }
    }
	
    // Helper method to handle subscription and listener removal safely
    private void trySubscribeAndRemoveListener(MkvObject mkvObject, MkvPublishManager pm, MkvPublishListener listener, String pattern, String[] fields) {
        synchronized (subscriptionLock) {
            // Check again inside synchronization to avoid race conditions
            if (subscribedPatterns.contains(pattern)) {
                return;
            }
            
            try {
                logger.info("Pattern found, subscribing: " + pattern);

                ((MkvPattern) mkvObject).subscribe(fields, this);
                subscribedPatterns.add(pattern);

                logger.info("Successfully subscribed to " + pattern);

                // Remove the listener now that we've subscribed - safely outside the callback
                // but still inside synchronization
                pm.removePublishListener(listener);
            } catch (Exception e) {
                logger.error("Error subscribing to pattern: " + e.getMessage() + " " + e);
            }
        }
    }

	 /**
	* Subscribes to the instrument pattern to load instrument mapping data.
	* Uses an adaptive field subscription approach that handles missing fields.
	*/
	private void subscribeToInstrumentPattern() {
	   try {

	       logger.info("Subscribing to instrument data using pattern: {}" + INSTRUMENT_PATTERN);

	       // Get the publish manager to access patterns
	       MkvPublishManager pm = Mkv.getInstance().getPublishManager();
	      
	       // Look up the pattern object
	       MkvObject obj = pm.getMkvObject(INSTRUMENT_PATTERN);
	      
	       if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
	           logger.info("Found instrument pattern, subscribing: {}" + INSTRUMENT_PATTERN);
	           
	           // Initialize with the full field list from MarketDef
	           List<String> fieldsList = new ArrayList<>(Arrays.asList(INSTRUMENT_FIELDS));
	           boolean subscribed = false;
	           
	           // Keep trying with fewer fields until subscription succeeds
	           while (!subscribed && !fieldsList.isEmpty()) {
	               try {
	                   String[] fields = fieldsList.toArray(new String[0]);
	                   ((MkvPattern) obj).subscribe(fields, depthListener);
	                   subscribed = true;

	                   logger.info("Successfully subscribed to instrument pattern with {} fields" + fieldsList.size());
	               } catch (Exception e) {
	                   // If we have any fields left to remove
	                   if (fieldsList.size() > 3) {  // Keep at least 3 essential fields
	                       // Remove the last field in the list
	                       String lastField = fieldsList.remove(fieldsList.size() - 1);
	                       logger.info("Subscription failed with field: {}. Retrying with {} fields." + lastField + fieldsList.size());
	                   } else {
	                       // If we've removed too many fields, try with a minimal set
	                       logger.info("Failed with reduced field set, trying minimal fields");

	                       // Define minimal essential fields
	                       String[] minimalFields = new String[] {
	                           "Id", "IsAON"
	                       };
	                       
	                       try {
	                           ((MkvPattern) obj).subscribe(minimalFields, depthListener);
	                           subscribed = true;
	                           logger.info("Successfully subscribed with minimal fields");
	                       } catch (Exception minEx) {
	                           logger.info("Failed even with minimal fields: {}" + minEx.getMessage() + " " + minEx);
	                           throw minEx;  // Propagate the exception if even minimal fields fail
	                       }
	                       break;
	                   }
	               }
	           }
	       } else {
	           logger.info("Instrument pattern not found: {}" + INSTRUMENT_PATTERN);
	       }
	   } catch (Exception e) {
	       logger.info("Error subscribing to instrument pattern: {}" + e.getMessage() + " " + e);
	       logger.info("Error details " + e);
	   }
	}
	
	private void subscribeToPattern() {
	   try {
	       // Get the publish manager to access patterns
	       MkvPublishManager pm = Mkv.getInstance().getPublishManager();
	      
	       // Look up the pattern object
	       MkvObject obj = pm.getMkvObject(DEPTH_PATTERN);
	      
	       if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
	           // Subscribe within a synchronized block
	           synchronized (subscriptionLock) {
	               // Check again inside synchronization to avoid race conditions
	               if (!isPatternSubscribed) {
	                   logger.info("Found consolidated depth pattern, subscribing: {}" + DEPTH_PATTERN);

	                   ((MkvPattern) obj).subscribe(DEPTH_FIELDS, depthListener);
	
	                   // Mark that we've successfully subscribed
	                   isPatternSubscribed = true;

	                   logger.info("Successfully subscribed to consolidated depth pattern");
	               }
	           }
	       } else {
	           logger.info("Consolidated depth pattern not found: {}" + DEPTH_PATTERN);

	           // Create a single shared listener instance instead of an anonymous one
	           final MkvPublishListener patternListener = new MkvPublishListener() {
	               @Override
	               public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
	                   // Only proceed if we're not already subscribed
	                   if (isPatternSubscribed) {
	                       return;
	                   }
	                   
	                   // Check if this is our pattern being published
	                   if (pub_unpub && mkvObject.getName().equals(DEPTH_PATTERN) &&
	                       mkvObject.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
	                       trySubscribeAndRemoveDepthListener(mkvObject, pm, this);
	                   }
	               }
	              
	               @Override
	               public void onPublishIdle(String component, boolean start) {
	                   // Only proceed if we're not already subscribed
	                   if (isPatternSubscribed) {
	                       return;
	                   }
	                   
	                   // Try looking for the pattern again at idle time
	                   MkvObject obj = pm.getMkvObject(DEPTH_PATTERN);
	                   
	                   if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
	                       trySubscribeAndRemoveDepthListener(obj, pm, this);
	                   }
	               }
	              
	               @Override
	               public void onSubscribe(MkvObject mkvObject) {
	                   // Not needed
	               }
	           };
	           
	           // Add the shared listener
	           pm.addPublishListener(patternListener);
	       }
	   } catch (Exception e) {
	       logger.error("Error subscribing to pattern: {}" + e.getMessage() + " " + e);
	   }
	}
	
	//Helper method to handle subscription and listener removal safely
	private void trySubscribeAndRemoveDepthListener(MkvObject mkvObject, MkvPublishManager pm, MkvPublishListener listener) {
	   synchronized (subscriptionLock) {
	       // Check again inside synchronization to avoid race conditions
	       if (isPatternSubscribed) {
	           return;
	       }
	       
	       try {
	           logger.info("Pattern found, subscribing: {}" + DEPTH_PATTERN);

	           ((MkvPattern) mkvObject).subscribe(DEPTH_FIELDS, depthListener);
	           isPatternSubscribed = true;

	           logger.info("Successfully subscribed to pattern");

	           // Remove the listener now that we've subscribed - safely outside the callback
	           // but still inside synchronization
	           pm.removePublishListener(listener);
	       } catch (Exception e) {
	           logger.error("Error subscribing to pattern: {}" + e.getMessage() + " " + e);
	       }
	   }
	 }

  /**
   * Caches a MarketOrder record using the request ID as key.
   * This allows us to track the order throughout its lifecycle.
   * 
   * @param order The MarketOrder to cache
   */
  private void addOrder(MarketOrder order) {
    logger.info("Adding order to cache: reqId=" + order.getMyReqId());
    orders.put(Integer.valueOf(order.getMyReqId()), order);
  }

  /**
   * Removes a MarketOrder object from the cache by request ID.
   * Called when an order is considered "dead".
   * 
   * @param reqId The request ID of the order to remove
   */
  public void removeOrder(int reqId) {
    logger.info("Removing order from cache: reqId=" + reqId);
    orders.remove(Integer.valueOf(reqId));
  }

  /**
   * Retrieves a MarketOrder object from the cache by request ID.
   * 
   * @param reqId The request ID of the order to retrieve
   * @return The MarketOrder object or null if not found
   */
  public MarketOrder getOrder(int reqId) {
    MarketOrder order = orders.get(Integer.valueOf(reqId));
    logger.info("Retrieving order from cache: reqId={}, found={}" + reqId + ", " + (order != null));
    return order;
  }

  /**
   * Maps an order ID to a request ID for tracking purposes.
   * 
   * @param orderId The order ID to map
   * @param reqId The request ID to associate with the order ID
   */
  public void mapOrderIdToReqId(String orderId, int reqId) {
      if (orderId != null && !orderId.isEmpty()) {
          orderIdToReqIdMap.put(orderId, reqId);
      }
  }

    // Then your lookup becomes much more efficient
    public MarketOrder getOrderByOrderId(String orderId) {
        if (orderId == null || orderId.isEmpty()) {
            logger.error("Cannot find order with null or empty orderId");
            return null;
        }
        
        Integer reqId = orderIdToReqIdMap.get(orderId);
        if (reqId != null) {
            return getOrder(reqId);
        }
        
        // Fall back to full search if not found in map
        // This handles edge cases where the index wasn't updated
        for (MarketOrder order : orders.values()) {
            if (orderId.equals(order.getOrderId())) {
                // Update the map for future lookups
                orderIdToReqIdMap.put(orderId, order.getMyReqId());
                return order;
            }
        }
        
        return null;
    }

    /**
     * The component doesn't need to listen to partial updates for records.
     */
    public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
        boolean isSnap) {
        // Not interested in partial updates
        logger.info("Partial update for record: {}" + mkvRecord.getName());
    }

    /**
     * Processes full updates for CM_ORDER records.
     * If the update is for an order placed by this component
     * (based on UserData and FreeText), forwards the update to the
     * appropriate MarketOrder object.
     */
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
        boolean isSnap) {
        try {
            String recordName = mkvRecord.getName();
                if (recordName.contains("CM_ORDER")) {
                    processOrderUpdate(mkvRecord, mkvSupply, isSnap);
                } else if (recordName.contains("CM_LOGIN")) {
                    processLoginUpdate(mkvRecord, mkvSupply, isSnap);
                } else {
                    logger.warn("Received unexpected record type: {}" + recordName);
                }

        } catch (Exception e) {
            logger.error("Error processing order update: {}" + e.getMessage() + " " + e);
        }
    }

    private void processOrderUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnap) {
        try {
            // Get the UserData (contains our request ID) and FreeText (contains our application ID)
            String CompNameOrigin = mkvRecord.getValue("CompNameOrigin").getString();
            String orderId = mkvRecord.getValue("OrigId").getString();
            String activeStr = mkvRecord.getValue("ActiveStr").getString();

            // If this order wasn't placed by us, or is dead, ignore it
            if (!"OrderManagement".equals(CompNameOrigin) || (orderId == null || orderId.isEmpty()) || "No".equalsIgnoreCase(activeStr)) {
                logger.warn("Ignoring order update for non-matching order: {}" + mkvRecord.getName());
                return;
            } else {
                // This is an order we placed - forward the update to the MarketOrder
                try {
                    String origSrc = mkvRecord.getValue("OrigSrc").getString();
                    int timeStamp = mkvRecord.getValue("TimeStamp").getInt();

                    // Create a composite key using orderId and origSrc
                    String compositeKey = orderId + ":" + origSrc;

                    // Process the order based on its active status
                    if ("Yes".equalsIgnoreCase(activeStr)) {
                        // Order is active, add or update it in the map
                        activeOrders.put(compositeKey, timeStamp);
                        logger.info("Order is active: orderId="+orderId+" source="+origSrc+", created="+timeStamp);
                    } else {
                        // Order is no longer active, remove from the map
                        activeOrders.remove(compositeKey);
                        logger.info("Order removed from active list: orderId="+orderId+" source="+origSrc);
                    }
                } catch (Exception e) {
                    logger.error("Error accessing OrigID field: {}" + e.getMessage()+ e);
                }
            }
        } catch (Exception e) {
            logger.error("Error accessing order fields: {}" + e.getMessage()+ e);
        }
    }

    private void processLoginUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnap) {
        try {
            String trader = mkvRecord.getValue("Id").getString();
            
            if (!venueToTraderMap.containsValue(trader)) {
                logger.warn("Ignoring login update from another trader: {}"+  trader);
                return; // Not our trader
            }

            // for loop looping Src0 to Src7 checking if the Src is not null, the TstatusStr is not null and the TStatusStr is not "Unknown" and if its equal to "On"
            // need to check valid venues against tStatusStr, can only send orders if TStatusStr is "On" for a venue, so have to store as a public concurrent hashmap
            for (int i = 0; i < 8; i++) {
                String src = mkvRecord.getValue("Src" + i).getString();
                String tStatus = mkvRecord.getValue("TStatusStr" + i).getString();

                if (src != null && !src.isEmpty() && tStatus != null && !tStatus.isEmpty()) {
                    //check if valid venue
                    if (!validVenues.contains(src)) {
                        logger.warn("Not valid venue: {}" + src);
                        continue;
                    } else {
                        if (tStatus.equals("On")){
                            venueActiveStatus.put(src, true);
                            logger.info("Venue " + src + " is now active for trader " + trader);
                        } else {
                            venueActiveStatus.put(src, false);
                            logger.info("Venue " + src + " is now inactive for trader " + trader);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing login update: {}" + e.getMessage()+ e);
        }
    }

  /**
   * Notification that an order is no longer able to trade.
   * Removes the order from the cache.
   * 
   * Implementation of IOrderManager.orderDead()
   */
  public void orderDead(MarketOrder order) {
    incrementPendingOperations();
    try {
        logger.info("Order is dead: orderId=" + order.getOrderId() + ", reqId=" + order.getMyReqId());
        // Remove the order from the cache
        removeOrder(order.getMyReqId());
    } finally {
        decrementPendingOperations();
    }
}
  
  /**
   * Adds an order for the given instrument, quantity, and other parameters.
   * If the order creation succeeds, stores the order in the internal cache.
   * 
   * Implementation of IOrderManager.addOrder()
   */
  public MarketOrder addOrder(String MarketSource, String TraderId, String instrId, String verb, double qty,
      double price, String type, String tif) {
    
    // Check if the system is in stopped state
    if (isSystemStopped  || isShutdownRequested()) {
        logger.error("Order rejected - system is in STOPPED state: source=" + MarketSource + ", trader=" + TraderId + ", instrId=" + instrId);
        return null;
    }

    if (venueActiveStatus.getOrDefault(MarketSource, null) == null) {
        logger.error("Order rejected - venue is not active: source=" + MarketSource + ", trader=" + TraderId + ", instrId=" + instrId);
        return null;
    }

    // Increment pending operations before creating the order
    incrementPendingOperations();
    try {
        logger.info("Attempting to create order: source=" + MarketSource + ", trader=" + TraderId + ", instrId=" + instrId + ", verb=" + verb + ", qty=" + qty + ", price=" + price + ", type=" + type + ", tif=" + tif);

        // Create the order using the static factory method
        MarketOrder order = MarketOrder.orderCreate(MarketSource, TraderId, instrId, verb, qty, price,
            type, tif, this);
        
        if (order != null) {
        // If creation succeeded, add the order to the cache
        addOrder(order);
        
        // Check if there was an error code returned that indicates rejection
        if (order.getErrCode() != 0) {
            logger.error("Order rejected by market: reqId=" + order.getMyReqId() + ", instrId=" + instrId + ", errCode=" + order.getErrCode() + ", errStr=" + order.getErrStr() + ", source=" + MarketSource + ", trader=" + TraderId);

            // Remove the rejected order from the cache since it won't be processed
            removeOrder(order.getMyReqId());
            return null; // Return null for rejected orders to indicate failure
        }

        logger.info("Order created successfully: reqId=" + order.getMyReqId() + ", instrId=" + instrId);

        } else {
            // Log a more detailed error when order creation fails completely
            logger.error("Order creation failed: instrId=" + instrId + ", source=" + MarketSource + ", trader=" + TraderId + ", verb=" + verb + ", qty=" + qty + ", price=" + price + ", type=" + type + ", tif=" + tif);
            }
        return order;
    } finally {
            // Decrement pending operations after processing the order
            decrementPendingOperations();
    }
  }

  /**
   * Handles notification of a best price update.
   * Implementation of IOrderManager.best()
   */
  @Override
    public void best(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {
            logger.info("best() called: instrument=" + best.getId() + ", ask= (" + best.getAsk() + "), asksrc= (" + best.getAskSrc() + "), bid= (" + best.getBid() + "), bidsrc= (" + best.getBidSrc() + "), cash_gc=(" + cash_gc + "), reg_gc=(" + reg_gc + ")");

        // Store the latest best for this instrument
        synchronized(gcDataLock) {
            // latestCashGC = cash_gc;
            // latestRegGC = reg_gc;
            // if (gcBestCash != null) sharedGCBestCash = gcBestCash;
            // if (gcBestREG != null) sharedGCBestREG = gcBestREG;
            latestCashGC.set(cash_gc);
            latestRegGC.set(reg_gc);
            if (gcBestCash != null) sharedGCBestCash.set(gcBestCash);
            if (gcBestREG != null) sharedGCBestREG.set(gcBestREG);
            latestBestByInstrument.put(best.getInstrumentId(), best);
        }

        logger.info("Calling processMarketOpportunity for " + best.getId());

        processMarketOpportunity(best, cash_gc, reg_gc, gcBestCash, gcBestREG);
    }

	//Add this method to initialize the scheduled market rechecks
	private void initializeMarketRechecks() {
	   // Schedule a task to recheck the market every 1 second
	   scheduler.scheduleAtFixedRate(this::recheckMarket, 1, 2, TimeUnit.SECONDS);
	   
	   // Add a shutdown hook to clean up the scheduler
	   Runtime.getRuntime().addShutdownHook(new Thread(() -> {
	       logger.info("Shutting down market recheck scheduler");
	       scheduler.shutdown();
	       try {
	           if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
	               scheduler.shutdownNow();
	           }
	       } catch (InterruptedException e) {
	           scheduler.shutdownNow();
	           Thread.currentThread().interrupt();
	       }
	   }));
	   logger.info("Market recheck scheduler initialized");
       scheduler.scheduleAtFixedRate(this::cleanupOldEntries,
           30,     // Initial delay (30 minutes)
            60,    // Period (60 minutes)
            TimeUnit.MINUTES);
	}
	
	//Add this method to periodically recheck the market
	private void recheckMarket() {
	   if (!continuousTradingEnabled || isSystemStopped) {
	       return;
	   }
  	   
	   // Process each instrument's latest best price
        for (Best best : latestBestByInstrument.values()) {
            processMarketOpportunity(best, latestCashGC.get(), latestRegGC.get(),
                sharedGCBestCash.get(), sharedGCBestREG.get());
         } 
	}

    private void cleanupOldEntries() {
        try {
            long cutoffTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2);
            int removedTrades = 0;
            
            // Use iterator to avoid ConcurrentModificationException
            Iterator<Map.Entry<String, Long>> tradeIterator = 
                lastTradeTimeByInstrument.entrySet().iterator();
            while (tradeIterator.hasNext()) {
                Map.Entry<String, Long> entry = tradeIterator.next();
                if (entry.getValue() < cutoffTime) {
                    tradeIterator.remove();
                    removedTrades++;
                    
                    // Batch removal - check every 100 entries
                    if (removedTrades % 100 == 0) {
                        Thread.yield(); // Allow other threads to work
                    }
                }
            }
            
            // More efficient Best map cleanup
            if (latestBestByInstrument.size() > 3000) {
                List<String> keysToRemove = new ArrayList<>();
                int count = 0;
                for (String key : latestBestByInstrument.keySet()) {
                    if (count++ > 1000) break; // Remove oldest 1000 entries
                    keysToRemove.add(key);
                }
                keysToRemove.forEach(latestBestByInstrument::remove);
            }
            
        } catch (Exception e) {
            logger.error("Error during cleanup " + e);
        }
    }
	/**
	 * Determines if we should use a dynamic spread threshold based on the current time.
	 * 
	 * @return true if the current time is in the threshold adjustment period, false otherwise
	 */
	private boolean isDynamicThresholdPeriod() {
	    // Get the current time
	    LocalTime currentTime = LocalTime.now();
	    
	    // Define the start time for dynamic thresholds (e.g., 11:00am)
	    LocalTime dynamicStartTime = LocalTime.of(11, 0); 
	    
	    // Only use dynamic threshold calculation during the active trading period
	    return currentTime.isAfter(dynamicStartTime);
	}
	
	/**
	 * Calculates the spread threshold based on the current time.
	 * The threshold starts at 2.0 and gradually decreases to 0.10 by 2:50pm local time.
	 * 
	 * @return The calculated threshold value
	 */
	private double calculateSpreadThreshold() {
	    // If we're not in the dynamic threshold period, return the default value
	    if (!isDynamicThresholdPeriod()) {
	        return 2.0;
	    }
	    
	    // Get the current time
	    LocalTime currentTime = LocalTime.now();
	    
	    // Define the start time for dynamic calculation
	    LocalTime dynamicStartTime = LocalTime.of(7, 00);
	    
	    // Define the end time (2:50pm)
	    LocalTime endTime = LocalTime.of(14, 50);
	    
	    // If it's after the end time, use the minimum threshold
	    if (currentTime.isAfter(endTime)) {
	        return 0.10;
	    }
	    
	    // Calculate the total minutes between dynamic start and end times
	    int totalMinutes = (endTime.getHour() - dynamicStartTime.getHour()) * 60 + 
	                        (endTime.getMinute() - dynamicStartTime.getMinute());
	    
	    // Calculate the elapsed minutes since dynamic start time
	    int elapsedMinutes = (currentTime.getHour() - dynamicStartTime.getHour()) * 60 + 
	                         (currentTime.getMinute() - dynamicStartTime.getMinute());
	    
	    // Calculate the proportion of time elapsed (from 0 to 1)
	    double timeElapsedProportion = (double) elapsedMinutes / totalMinutes;
	    
	    // Calculate the threshold - linear interpolation from 2.0 to 0.10
	    double threshold = 2.0 - (timeElapsedProportion * (2.0 - 0.10));
	    
	    // Round to 2 decimal places for cleaner values
	    threshold = Math.round(threshold * 100) / 100.0;
	    
	    return threshold;
	}
	
	// Method to evaluate and execute on market opportunities, crosses and GC crosses
	private void processMarketOpportunity(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {

	    // Skip if the system is in stopped state
	    if (isSystemStopped) {
            logger.info("Skipping market opportunity processing - system is in STOPPED state");
	        return;
	    }
		
		// Skip if the best object is null
	    if (best == null) {
	        logger.warn("processMarketOpportunity called with null best object");
	        return;
	    }

	    String askSrc = best.getAskSrc();
  	    String bidSrc = best.getBidSrc();
        // ApplicationLogging.logAsync(LOGGER, Level.FINE, 
        // "Checking if askSrc is valid venue: " + askSrc);
  	    
        // Only proceed with order checks if we might actually place an order
        if (!validVenues.contains(askSrc)) 
        {
            return;
        }
	    
	    double bid = best.getBid();
  	    double ask = best.getAsk();

        logger.info("Price check: ask=" + ask + ", bid=" + bid + ", spread=" + (ask - bid));

        String id = best.getId();
        boolean isStrip = (id.startsWith("91283") || id.startsWith("912800") || id.startsWith("912815") || id.startsWith("912820") || id.startsWith("912821") || id.startsWith("912803"));
        
        if (isStrip) {
        	return;
        }
        
  	    boolean isCash = id.contains("C_Fixed");
  	    boolean isREG = id.contains("REG_Fixed");
  	    
  	    //do not process term trades
  	    if (!isCash && !isREG) {
  	    	return;
  	    }
  	      	    
        GCLevelResult gcLevelResult = null;

        if (isCash) {
        	if (gcBestCash == null) {
        		gcLevelResult = null;
        	} else {
        		gcLevelResult = gcBestCash.getGCLevel();
        	}
        } else if (isREG) {
        	if (gcBestREG == null) {
        		gcLevelResult = null;
        	} else {
        		gcLevelResult = gcBestREG.getGCLevel();
        	}
        }

  	    //Make sure that the ASK is AT LEAST equal to the BID, this is the arb
	  	if ((ask-0.01 < bid) || (bidSrc == null) || (!validVenues.contains(bidSrc))) {
	  		
	  		double orderAskSizeTotal = best.getAskSize();
	  		if (orderAskSizeTotal <= 0) {
	  			return;
	  		}
	  		
	  		if (gcLevelResult == null) {
	  			logger.warn("No GC market available for comparison - Type: " + (id.contains("C_Fixed") ? "CASH" : "REG") + ", Instrument: " + id);
	  			return;
        	} else if (gcLevelResult != null) {
	  			
                // Add null safety for the gcLevelResult methods
                Double gcBidPrice = null;
                Double gcAskPrice = null;
                
                try {
                    gcBidPrice = gcLevelResult.getBidPrice();
                    gcAskPrice = gcLevelResult.getAskPrice();
                } catch (Exception e) {
                    logger.error("Error accessing GC level prices for " + id + ": " + e.getMessage());
                    return;
                }
                if (gcBidPrice == null) {
                    logger.warn("Skipping trade: Missing GC level bid price - Instrument: " + id + ", Type: " + (id.contains("C_Fixed") ? "CASH" : "REG") + ", Current Ask: " + ask);
                    return;
                }
                
                if (ask < gcBidPrice) {
                     logger.warn("Skipping trade: ask " + ask + " is less than GC level bid " + gcBidPrice + " - Type: " + (id.contains("C_Fixed") ? "CASH" : "REG") + ", Instrument: " + id);
                    return;
                }
                
                if (gcAskPrice != null && ask < gcAskPrice) {
                     logger.warn("Skipping trade: ask " + ask + " is less than GC level ask " + gcAskPrice + " - Type: " + (id.contains("C_Fixed") ? "CASH" : "REG") + ", Instrument: " + id);
                    return;
                }
                
                // If we get here, process the opportunity
                processGCOpportunity(best, gcLevelResult);
                return;
	  		}
	  	}
  	
        if (!validVenues.contains(bidSrc)) {
                return;
        }
        
	    boolean skipSelfMatch = (best.isMinePrice(best.getAskStatus()) || best.isMinePrice(best.getBidStatus()));
	    
	      // Self-match detection
	    if  (skipSelfMatch) {
			logger.warn("Failed self-match prevention check");
			return;
	    }

        double orderBidSize = best.getBidSizeMin();
        double orderAskSize = best.getAskSizeMin();
        
        double orderBidSizeTotal = best.getBidSize(); 
        double orderAskSizeTotal = best.getAskSize();
        
        double sizeDiff = Math.round((orderAskSizeTotal - orderBidSize)*100)/100;
        double minsizeDiff = Math.round((orderAskSize - orderBidSize)*100)/100;
        // Do not process if shown size difference is too large
        if ((sizeDiff < 0) || (minsizeDiff > 45)) {
        	return;
        }
        
        int multiplier = (int)Math.ceil(orderBidSize / orderAskSize);
        orderAskSize = orderAskSize * multiplier;
        
  	    boolean sameSource = askSrc.equals(bidSrc);
  	    
  	    double cashRegAdjustment = 0.00;
	  	// adjust spread for cash versus reg
  	    if (isCash) {
  		  	if (cash_gc != 0) {
  		        double threshold = isDynamicThresholdPeriod() ? calculateSpreadThreshold() : 2.0;
  		  		if ((cash_gc - ask) > threshold) {
  		            if (isDynamicThresholdPeriod()) {
  		                // Only log during the dynamic threshold period to reduce overhead
  		                logger.warn("Skipping trade: spread " + (cash_gc - ask) + " exceeds time-based threshold " + threshold);
  		            }
  		            return;
  		  		}
  		  	}
  	    	cashRegAdjustment = 0.00;
  	    } else if (isREG) {
  	    	cashRegAdjustment = 0.0;
  	    }

        // calculate if trade is close enough to breakeven or not
  	    if (isCash && (cash_gc != 0) && (minsizeDiff > 0)) {
            if ((((ask-bid) * orderBidSize) - ((cash_gc - ask) * minsizeDiff)) / minsizeDiff > -0.02) {
                // Trade is not close enough to breakeven
                logger.warn("ID: " + id + " - Skipping trade as not close enough to breakeven: Details: [" + 
                    (isCash ? "CASH" : "REG") + "], ask=" + ask + ", bid=" + bid + 
                    ", orderBidSize=" + orderBidSize + ", cash_gc=" + cash_gc + 
                    ", ask=" + ask + ", minsizeDiff=" + minsizeDiff + ", orderAskSize=" + orderAskSize);
                return;
            }
        } else if (isREG && (reg_gc != 0) && (minsizeDiff > 0)) {
            if ((((ask-bid) * orderBidSize) - ((reg_gc - ask) * minsizeDiff)) / minsizeDiff > 0.005) {
                // Trade is not close enough to breakeven
                logger.warn("ID: " + id + " - Skipping trade as not close enough to breakeven: Details: [" + 
                    (isCash ? "CASH" : "REG") + "], ask=" + ask + ", bid=" + bid + 
                    ", orderBidSize=" + orderBidSize + ", reg_gc=" + reg_gc + 
                    ", ask=" + ask + ", minsizeDiff=" + minsizeDiff + ", orderAskSize=" + orderAskSize);

                return;
            }
        } 

        String reason = "";
  	    double spread = ask - bid;
  	    String securityType = isCash ? "CASH" : (isREG ? "REG" : "UNKNOWN");
        // Build detailed reason message
            if ((minsizeDiff == 0) && sameSource && (ask-(0.03 + cashRegAdjustment) < bid)) {
                reason = "Equal size, same source, spread < 0.03 + adjustment";
            } else if (minsizeDiff > 25 && (ask-(0.04 + cashRegAdjustment) < bid)) {
                reason = "Size difference > 25, spread < 0.04 + adjustment";
            } else if (minsizeDiff == 25 && orderAskSizeTotal == 50 && orderBidSizeTotal == 25 
                    && (ask-(0.02 + cashRegAdjustment) < bid)) {
                reason = "Size exactly 25 (50/25 pattern), spread < 0.02 + adjustment";
            } else if (minsizeDiff > 15 && minsizeDiff < 25 && (ask-(0.03 + cashRegAdjustment) < bid)) {
                reason = "Size difference 15-25, spread < 0.03 + adjustment";
            } else if (minsizeDiff >= 9 && minsizeDiff <= 15 && (ask-(0.02 + cashRegAdjustment) < bid)) {
                reason = "Size difference 9-15, spread < 0.02 + adjustment";
            } else if (minsizeDiff < 9 && minsizeDiff > 5 && (ask-0.02 < bid)) {
                reason = "Size difference 5-9, spread < 0.02";
            } else if (minsizeDiff <= 5 && (ask-0.01 < bid)) {
                reason = "Size difference <= 5, spread < 0.01";
            } else if (minsizeDiff == 0 && (ask-0.02 < bid)) {
                reason = "Equal size, spread < 0.02";
            }


	  	if (((minsizeDiff == 0) && sameSource && (ask-(0.03 + cashRegAdjustment) < bid)) || 
	  		    ((minsizeDiff > 25) && (ask-(0.04 + cashRegAdjustment) < bid)) || 
	  		    ((minsizeDiff == 25) && (orderAskSizeTotal == 50) && (orderBidSizeTotal == 25) && 
	  		        (ask-(0.02 + cashRegAdjustment) < bid)) ||
	  		    ((minsizeDiff > 15 && minsizeDiff < 25) && (ask-(0.03 + cashRegAdjustment) < bid)) ||
	  		    ((minsizeDiff >= 9 && minsizeDiff <= 15) && (ask-(0.02 + cashRegAdjustment) < bid)) || 
	  		    ((minsizeDiff < 9) && (minsizeDiff > 5) && ((ask-0.02) < bid)) ||
	  		    ((minsizeDiff <= 5) && ((ask-0.01) < bid)) || 
	  		    ((minsizeDiff == 0) && ((ask-0.02) < bid))
	  		) {
            logger.warn("ID: " + id + " - Skipping trade: Details: [" + reason + "] " + securityType + 
                " ask=" + ask + " (" + askSrc + "), bid=" + bid + " (" + bidSrc + "), " +
                "sizeDiff=" + sizeDiff + ", minSizeDiff=" + minsizeDiff + ", spread=" + spread + ", " +
                "askSize=" + orderAskSize + "/" + orderAskSizeTotal + ", bidSize=" + orderBidSize + "/" + orderBidSizeTotal + ", " +
                "multiplier=" + multiplier + ", adjustment=" + cashRegAdjustment);
                return;
	  		}
        
  	    String idFull = best.getInstrumentId();
 	    
  	    boolean AskIsAON = best.getAskIsAON();
 	    boolean BidIsAON = best.getBidIsAON();

            StringBuilder sb = messageBuilder.get();
            sb.setLength(0);
            sb.append("Processing trading opportunity for instrument id: ").append(idFull)
            .append(" id: ").append(id)
            .append(": askSize=").append(orderAskSizeTotal).append(" (").append(orderAskSize).append(")")
            .append(", bidSize=").append(orderBidSizeTotal).append(" (").append(orderBidSize).append(")")
            .append(": ask=").append(ask).append(" (").append(askSrc).append(")")
            .append(", bid=").append(bid).append(" (").append(bidSrc).append(")");
            logger.debug(sb.toString());

        // Create a unique key for this instrument pair to track trades
        String tradingKey = id + "|" + askSrc + "|" + bidSrc;
        
        // Check if we've traded this instrument recently
        long currentTime = System.currentTimeMillis();
        Long lastTradeTime = lastTradeTimeByInstrument.get(tradingKey);
        
        if (lastTradeTime != null && (currentTime - lastTradeTime) < MIN_TRADE_INTERVAL_MS) {
            // Too soon to trade this instrument again
            logger.debug("Skipping trade for " + tradingKey + " last trade was too recent: " + (currentTime - lastTradeTime) + "ms since last trade");
            return;
        }

        logger.debug("Found trading opportunity for " + id + ": IsAON=" + AskIsAON + " ask=" + ask + " (" + askSrc + ")");
        logger.debug("IsAON=" + BidIsAON + " bid=" + bid + " (" + bidSrc + ")");

      // Get the native instrument IDs
      String askVenueInstrument = depthListener.getInstrumentFieldBySourceString(id, askSrc, AskIsAON);
      String bidVenueInstrument = depthListener.getInstrumentFieldBySourceString(id, bidSrc, BidIsAON);
      
    // Add null checks
    if (askVenueInstrument == null || bidVenueInstrument == null) {
        logger.warn("Missing instrument mapping for " + id + ", ask source: " + askSrc + ", bid source: " + bidSrc);
        return; // Skip this opportunity
    }

      // Get the trader IDs for each venue
      String askTrader = getTraderForVenue(askSrc);
      String bidTrader = getTraderForVenue(bidSrc);
        
   // Add protection against zero or negative sizes
      if (orderAskSizeTotal <= 0 || orderBidSizeTotal <= 0) {
          logger.warn("Invalid sizes detected for "+ id +": ask="+ orderAskSizeTotal +", bid="+ orderBidSizeTotal);
          return;
      }

      // Calculate the order size
  	  String ordertypeAsk = null;
  	  String ordertypeBid = null;
  	  ordertypeBid = "FAS";
  	  ordertypeAsk = "FAS";

        logger.debug("Checking for self-match: " +
            "ASK: src=" + askSrc + ", trader=" + askTrader + ", venue=" + askVenueInstrument + 
            ", size=" + orderAskSize + ", price=" + ask + ", type=" + ordertypeAsk + ", IsAON=" + AskIsAON + 
            " | BID: src=" + bidSrc + ", trader=" + bidTrader + ", venue=" + bidVenueInstrument + 
            ", size=" + orderBidSize + ", price=" + bid + ", type=" + ordertypeBid + ", IsAON=" + BidIsAON);


        logger.debug("### BUY ORDER BEING SENT: src: " + askSrc + ", trader: " + askTrader + ", venue: " + askVenueInstrument + 
              ", direction: Buy, size: " + orderAskSize + ", prc: " + ask + ", Limit, ordertype: " + ordertypeAsk + ", IsAON: " + AskIsAON);
        logger.debug("### SELL ORDER BEING SENT: src: " + bidSrc + ", trader: " + bidTrader + ", venue: " + bidVenueInstrument + 
              ", direction: Sell, size: " + orderBidSize + ", prc: " + bid + ", Limit, ordertype: " + ordertypeBid + ", IsAON: " + BidIsAON);

  	   // Placing active orders
        addOrder(askSrc, askTrader, askVenueInstrument, "Buy", orderAskSize, 
                ask, "Limit", ordertypeAsk);
        addOrder(bidSrc, bidTrader, bidVenueInstrument, "Sell", orderBidSize, 
                bid, "Limit", ordertypeBid);

        // Record that we've traded this instrument
        lastTradeTimeByInstrument.put(tradingKey, currentTime);
        logger.debug("Recorded trade for " + tradingKey + " at " + new java.util.Date(currentTime));
    }
	
	/**
	 * Determines if we should use a dynamic spread threshold based on the current time.
	 * 
	 * @return true if the current time is in the threshold adjustment period, false otherwise
	 */
  	  private boolean isAfterEightThirtyFive() {
	    // Get the current time
	    LocalTime currentTime = LocalTime.now();
	    
	    // Define the start time for dynamic thresholds (e.g., 8:35am)
	    LocalTime dynamicStartTime = LocalTime.of(8, 35); 
	    
	    // Only use dynamic threshold calculation during the active trading period
	    return currentTime.isAfter(dynamicStartTime);
	}
	
    private void processGCOpportunity(Best best, GCLevelResult gclvlResult) {
        // electronic venue lines versus aggregated GC lines arbitrage
        String id = best.getId();
        
        // Log that we're starting GC opportunity processing
        logger.debug("Processing GC opportunity - checking for potential GC-level arbitrage for id: " + id);

        boolean skipSelfMatch = (best.isMinePrice(best.getAskStatus()));

        // Self-match detection
        if (skipSelfMatch) {
            logger.warn("Failed self-match prevention for Processing GC check");
            return;
        }
        
        double ask = best.getAsk();
        Double gcBidPrice = gclvlResult.getBidPrice();
        
        // Log that we found a GC bid price
        logger.debug("Found GC bid price for arbitrage: " + gcBidPrice);

        double threshold = isAfterEightThirtyFive() ? 0.05 : 0.03;
        // Skip if spread is insufficient for the time
        if ((ask - threshold) < gcBidPrice) {
            logger.debug("Skipping GC trade for id: " + id + " - insufficient spread - ask=" + ask + ", gcBid=" + gcBidPrice + ", threshold=" + threshold + ", spread=" + (ask - gcBidPrice));
            return;
        }

        String askSrc = best.getAskSrc();
        boolean AskIsAON = best.getAskIsAON();
        
        // Get the full instrument ID from the Best object
        String fullInstrumentId = best.getInstrumentId();
        
        // Get the native instrument ID for this venue
        String askVenueInstrument = depthListener.getInstrumentFieldBySourceString(id, askSrc, AskIsAON);
        
        // If the venue instrument is null, log an error and return
        if (askVenueInstrument == null) {
            logger.error("Missing instrument mapping for GC trade: " + id + ", source: " + askSrc);
            return;
        }

        double orderAskSize = best.getAskSize();
        String askTrader = getTraderForVenue(askSrc);

        String ordertypeAsk = "FAS";

        logger.debug("### GC BUY ORDER BEING SENT: src: " + askSrc + ", trader: " + askTrader + ", venue: " + askVenueInstrument + ", direction: Buy, size: " + orderAskSize + ", prc: " + ask + ", Limit, ordertype: " + ordertypeAsk + ", IsAON: " + AskIsAON);

        // Add more detailed debugging output
        logger.debug("Detailed order info - id: " + id + ", fullId: " + fullInstrumentId + ", venueId: " + askVenueInstrument + ", source: " + askSrc + ", trader: " + askTrader);

        // Send the order with the correct venue instrument ID
        addOrder(askSrc, askTrader, askVenueInstrument, "Buy", orderAskSize, 
                ask, "Limit", ordertypeAsk);
		
	    try {
	        boolean isGcCash = id.contains("C_Fixed");
	        String type = isGcCash ? "CASH" : "REG";
	        
	        StringBuilder sb = new StringBuilder();
	        sb.append("\n------- GC ARBITRAGE MARKET DETAILS -------\n");
	        
	        // Basic information
	        sb.append("Instrument: ").append(id).append(" (").append(best.getInstrumentId()).append(")\n");
	        sb.append("Type: ").append(type).append("\n");
	        
	        // Price information from Best
	        sb.append(String.format("Ask: %.6f (%s) IsAON=%s, Size=%.2f/%.2f\n", 
	                  best.getAsk(), best.getAskSrc(), best.getAskIsAON(), 
	                  best.getAskSizeMin(), best.getAskSize()));
	        sb.append(String.format("Bid: %.6f (%s) IsAON=%s, Size=%.2f/%.2f\n", 
	                  best.getBid(), best.getBidSrc(), best.getBidIsAON(), 
	                  best.getBidSizeMin(), best.getBidSize()));
	        sb.append(String.format("Spread: %.6f\n", best.getAsk() - best.getBid()));
	        
	        
	    } catch (Exception e) {
	        // Make sure logging errors don't disrupt trading
	        logger.error("Error logging market details: " + e.getMessage());
	    }
	}


	// Add this method to enable/disable continuous trading
	public void setContiguousTradingEnabled(boolean enabled) {
	    this.continuousTradingEnabled = enabled;
        logger.debug("Continuous trading " + (enabled ? "enabled" : "disabled"));

	}
  /**
   * Handles changes to the order chain.
   * For new records, sets up subscriptions to receive updates.
   */
  public void onSupply(MkvChain chain, String record, int pos,
      MkvChainAction action) {
    logger.debug("Order chain update: chain=" + chain.getName() + ", record=" + record + ", pos=" + pos + ", action=" + action);

    switch (action.intValue()) {
    case MkvChainAction.INSERT_code:
    case MkvChainAction.APPEND_code:
      // For new records, subscribe to receive updates
      logger.debug("New record in chain, subscribing: " + record);
      subscribeToRecord(record);
      break;
    case MkvChainAction.SET_code:
      // For a SET action (chain is being completely redefined),
      // subscribe to all records in the chain
      logger.debug("Chain SET action, subscribing to all records");
      for (Iterator iter = chain.iterator(); iter.hasNext();) {
        String recName = (String) iter.next();
        logger.debug("Subscribing to chain record: " + recName);
        subscribeToRecord(recName);
      }
      break;
    case MkvChainAction.DELETE_code:
        logger.debug("Ignoring DELETE action for record: " + record);
        break;
    }
  }

  /**
   * Sets up a subscription to an order record.
   * This allows receiving updates for the record.
   * 
   * @param record The name of the record to subscribe to
   */
  private void subscribeToRecord(String record) {
    try {
      logger.debug("Subscribing to record: " + record);

      // Get the record object
      MkvRecord rec = Mkv.getInstance().getPublishManager().getMkvRecord(record);
      
      // Subscribe to receive updates for the configured fields
      rec.subscribe(MarketDef.ORDER_FIELDS, this);
      logger.debug("Successfully subscribed to record: " + record);
    } catch (Exception e) {
      logger.error("Error subscribing to record: " + record + ", error: " + e.getMessage());
    }
  }

private void initializeHeartbeat() {
    heartbeatScheduler.scheduleAtFixedRate(() -> {
        try {
        	
            Map<String, Object> status = new HashMap<>();
            status.put("hostname", hostname);
            status.put("application", "OrderManagementUAT");
            status.put("state", isSystemStopped ? "STOPPED" : "RUNNING");
            status.put("continuousTrading", continuousTradingEnabled);
            status.put("activeOrders", orders.size());

            // Gather trading statistics
            int instrumentCount = depthListener != null ? 
                depthListener.getInstrumentCount() : 0;
            status.put("instruments", instrumentCount);
            status.put("cached orders", orders.size());
            status.put("latest best prices", latestBestByInstrument.size());
            
            publishToRedis(HEARTBEAT_CHANNEL, status);
            
        } catch (Exception e) {
            logger.error("Error in heartbeat logging: " + e.getMessage());
        }
    }, 30, 30, TimeUnit.SECONDS); // Run every 30 seconds

    // Add a shutdown hook to properly close the heartbeat scheduler
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        logger.debug("Shutting down heartbeat scheduler");
        heartbeatScheduler.shutdown();
        try {
            if (!heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                heartbeatScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            heartbeatScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }));
}
 
private void setupPatternSubscriptionMonitor() {
    scheduler.scheduleAtFixedRate(() -> {
        try {
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            
            // Check instrument pattern
            String instrumentPattern = INSTRUMENT_PATTERN;
            MkvObject instrObj = pm.getMkvObject(instrumentPattern);
            if (instrObj != null && instrObj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                MkvPattern pattern = (MkvPattern) instrObj;
                
                boolean isInstrSubscribed = false;
                    // Try with an absolute minimum set
                    try {
                        String[] bareMinimumFields = new String[] { "Id" };
                        logger.debug("Retrying with absolute minimum field: " + Arrays.toString(bareMinimumFields));
                        isInstrSubscribed = pattern.isSubscribed(depthListener, bareMinimumFields);
                    } catch (Exception e2) {
                        logger.error("Error checking with minimum field. Using status from DepthListener: " + e2.getMessage());

                        // Fall back to the cached state in the DepthListener
                        if (depthListener != null) {
                            Map<String, Object> healthStatus = depthListener.getHealthStatus();
                            Object subscriptionStatus = healthStatus.get("instrumentPatternSubscribed");
                            isInstrSubscribed = Boolean.TRUE.equals(subscriptionStatus);
                        }
                    }
                
                    if (depthListener != null) {
                        depthListener.setInstrumentPatternSubscribed(isInstrSubscribed);
                    }

                logger.debug("Instrument pattern subscription check: subscribed=" + isInstrSubscribed);

                // If not subscribed, resubscribe using our adaptive approach
                if (!isInstrSubscribed) {
                    logger.debug("Instrument pattern not subscribed, subscribing now");

                    try {
                        // Try using the full subscription method with failover logic
                        subscribeToInstrumentPattern();
                    } catch (Exception e) {
                        logger.error("Error calling subscribeToInstrumentPattern: " + e.getMessage());

                        // Last resort: try to subscribe with just the absolute minimum field
                        try {
                            String[] bareMinimumFields = new String[] { "Id" };
                            logger.debug("Trying last resort subscription with field: " + Arrays.toString(bareMinimumFields));
                            pattern.subscribe(bareMinimumFields, depthListener);
                            logger.debug("Successfully subscribed with bare minimum field");
                        } catch (Exception e2) {
                            logger.error("All subscription attempts failed: " + e2.getMessage());
                        }
                    }
                }
            } else {
                // The instrument pattern doesn't exist yet
                logger.debug("Instrument pattern not found: {}" + instrumentPattern);
            }

            // Check depth pattern
            MkvObject depthObj = pm.getMkvObject(DEPTH_PATTERN);
            if (depthObj != null && depthObj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                MkvPattern pattern = (MkvPattern) depthObj;
                
                // Use a try-catch block to safely check subscription status
                boolean currentlySubscribed = false;
                try {
                    currentlySubscribed = pattern.isSubscribed(depthListener, DEPTH_FIELDS);
                } catch (Exception e) {
                    logger.error("Error checking depth pattern subscription: " + e.getMessage());
                }

                logger.debug("Depth pattern subscription check: subscribed=" + currentlySubscribed + ", our state=" + isPatternSubscribed);

                // If we think we're subscribed but actually aren't, resubscribe
                if (isPatternSubscribed && !currentlySubscribed) {
                    logger.debug("Depth pattern subscription lost, resubscribing");
                    pattern.subscribe(DEPTH_FIELDS, depthListener);
                }

                // Update our tracking state
                isPatternSubscribed = currentlySubscribed;
            } else {
                if (isPatternSubscribed) {
                    logger.debug("Depth pattern not found but we thought we were subscribed");
                    isPatternSubscribed = false;
                }
                
                // Try to find and subscribe to the pattern again
                subscribeToPattern();
            }
            
            // Additional detailed logging
            if (depthListener != null) {
                Map<String, Object> healthStatus = depthListener.getHealthStatus();
                logger.debug("Market data health status: " + healthStatus);
            }
        } catch (Exception e) {
            logger.error("Error in pattern subscription monitor: " + e.getMessage());
        }
    }, 15, 30, TimeUnit.SECONDS);  // Check sooner initially (15 sec), then every 30 sec
  }

  /**
   * Checks for and cancels any orders that have been in the market for too long.
   */
  private void checkForExpiredOrders() {
    // Skip if we're already shutting down
    if (isShuttingDown) {
        return;
    }
    
    try {
        // Create a copy of the orders map to avoid concurrent modification
        Map<Integer, MarketOrder> ordersCopy = new HashMap<>(orders);
        int expiredCount = 0;
        logger.debug("Checking for expired orders - currently tracking " + ordersCopy.size() + " orders");

        for (MarketOrder order : ordersCopy.values()) {
            // Skip cancel requests
            if ("Cancel".equals(order.getVerb())) {
                continue;
            }
            
            // Check if the order is expired
            if (order.isExpired()) {
                // Get necessary info for the cancel request
                String orderId = order.getOrderId();
                String marketSource = order.getMarketSource();

                // Check if the order is in a valid state for cancellation
                if (orderId == null || orderId.isEmpty()) {
                    logger.error("Order ID is null or empty for reqId: " + order.getMyReqId());
                    continue;
                }
                
                // Only try to cancel if we have an order ID
                if (orderId != null && !orderId.isEmpty()) {
                    String traderId = getTraderForVenue(marketSource);

                    logger.info("Auto-cancelling expired order: reqId=" + order.getMyReqId() + ", orderId=" + orderId + ", age=" + (order.getAgeMillis() / 1000) + " seconds");

                    // Issue the cancel request
                    MarketOrder cancelOrder = MarketOrder.orderCancel(
                        marketSource,
                        traderId,
                        orderId,
                        this
                    );
                    
                    if (cancelOrder != null) {
                        // Track the cancel request
                        removeOrder(order.getMyReqId());
                        expiredCount++;
                   }
                }
            } 
        }
        
        // secondary cleanup of orders map
        if (activeOrders != null) {
            // Iterate through active orders and cancel those older than 60 seconds
            int cancelledActiveCount = 0;
            long currentTime = System.currentTimeMillis();
            long cutoffTime = currentTime - 60000; // 60 seconds ago

            for (Map.Entry<String, Integer> entry : activeOrders.entrySet()) {
                try {
                    String compositeKey = entry.getKey();
                    int timeStamp = entry.getValue();

                    // Parse the composite key
                    String[] parts = compositeKey.split(":");
                    if (parts.length < 2) {
                        continue; // Skip invalid entries
                    }
                    
                    String orderId = parts[0];
                    String marketSource = parts[1];
                    
                    // Skip entries with invalid timestamps
                    if (timeStamp <= 0) {
                        activeOrders.remove(compositeKey);
                        continue;
                    }

                    // Convert the timestamp to milliseconds since epoch
                    // First create a calendar for today
                    java.util.Calendar cal = java.util.Calendar.getInstance();
                    
                    // Extract time components from the timestamp
                    // Format: HHMMSSMMM (70004566 = 7:00:00.456)
                    int hours = timeStamp / 10000000;
                    int minutes = (timeStamp % 10000000) / 100000;
                    int seconds = (timeStamp % 100000) / 1000;
                    int milliseconds = timeStamp % 1000;
                    
                    // Set the time components for today
                    cal.set(java.util.Calendar.HOUR_OF_DAY, hours);
                    cal.set(java.util.Calendar.MINUTE, minutes);
                    cal.set(java.util.Calendar.SECOND, seconds);
                    cal.set(java.util.Calendar.MILLISECOND, milliseconds);
                    
                    // Get the time in milliseconds
                    long entryTime = cal.getTimeInMillis();

                    logger.debug("Processing order: " + orderId + " with timestamp: " + timeStamp + " (" + new java.util.Date(entryTime) + ")");

                    if (entryTime < cutoffTime) {
                        // Order is older than 60 seconds, cancel it
                        String traderId = getTraderForVenue(marketSource);
                        logger.info("Cancelling stale active order: orderId=" + orderId + ", source=" + marketSource + ", age=" + (currentTime - entryTime) / 1000 + " seconds");
                            
                        // Issue the cancel request
                        MarketOrder cancelOrder = MarketOrder.orderCancel(
                            marketSource,
                            traderId,
                            orderId,
                            this
                        );
                        
                        if (cancelOrder != null) {
                            // Remove from active orders map
                            activeOrders.remove(compositeKey);
                            cancelledActiveCount++;
      
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error processing active order entry: " + e.getMessage());
                }
            }

            if (cancelledActiveCount > 0) {
                logger.info("Cancelled " + cancelledActiveCount + " stale active orders");
            }
        }

        if (expiredCount > 0) {
            logger.info("Auto-cancelled " + expiredCount + " expired orders");
        }
    } catch (Exception e) {
        logger.error("Error checking for expired orders: " + e.getMessage() + ", " + e);
    }
}

/**
 * Initiates shutdown process for the application.
 */
public void initiateShutdown() {
    // Mark as shutting down
    isShuttingDown = true;
    shutdownRequested = true;
        
    // Log that we're starting shutdown
    logger.info("Initiating application shutdown");

    // Start the graceful shutdown process in a separate thread
    Thread shutdownThread = new Thread(this::performGracefulShutdown, "Manual-Shutdown");
    shutdownThread.setDaemon(true);
    shutdownThread.start();
}

/**
 * Performs a complete shutdown of the application.
 * @deprecated Use {@link #initiateShutdown()} instead.
 */
@Deprecated
public void shutdown() {
    logger.warn("shutdown() called - redirecting to initiateShutdown()");
    initiateShutdown();
}

/**
 * Checks if the application is currently shutting down.
 * 
 * @return true if the application is shutting down, false otherwise
 */
public boolean isShuttingDown() {
    return isShuttingDown;
}

private void shutdownExecutorGracefully(ExecutorService executor, String name, int timeoutSeconds) {
    if (executor == null || executor.isShutdown()) {
        return;
    }
    
    try {
        logger.info("Shutting down {} executor" + name);
        executor.shutdown();
        
        if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
            logger.warn(name + " executor did not terminate gracefully, forcing shutdown");
            List<Runnable> pendingTasks = executor.shutdownNow();
            logger.info("Cancelled " + pendingTasks.size() + " pending tasks for " + name);
        }
    } catch (InterruptedException e) {
        logger.warn("Interrupted while shutting down " + name + " executor");
        executor.shutdownNow();
        Thread.currentThread().interrupt();
    } catch (Exception e) {
        logger.error("Error shutting down " + name + " executor: " + e.getMessage());
        executor.shutdownNow();
    }
}

    /**
     * Performs a graceful shutdown of the OrderManagement component.
     * This includes canceling all orders and cleaning up resources.
     */
    private boolean performGracefulShutdown() {
        logger.info("Starting graceful shutdown sequence");

        // Mark as shutting down
        if (isShuttingDown) {
            logger.warn("Shutdown already in progress, ignoring duplicate request");
            return true;
        }

        logger.info("Starting graceful shutdown sequence");
        isShuttingDown = true;
        isSystemStopped = true;
        continuousTradingEnabled = false;

        try {
            // 1. Stop accepting new orders
            logger.info("Stopped accepting new orders");
            // 2. Cancel all existing orders
            cancelAllOrders();
            logger.info("All orders canceled");

            // 3. Wait for any pending operations to complete (with timeout)
            waitForPendingOperations();
            
            // 4. Cleanup resources
            cleanupResources();
            
        } catch (Exception e) {
            logger.error("Error during graceful shutdown: " + e.getMessage() + e);
            return false;
        }

        return true;
    }

/**
 * Waits for any pending operations to complete, with a timeout.
 */
private void waitForPendingOperations() {
    logger.info("Waiting for " + pendingOperations + " pending operations to complete");

    long startTime = System.currentTimeMillis();
    long endTime = startTime + SHUTDOWN_TIMEOUT_MS;
    
    while (pendingOperations > 0 && System.currentTimeMillis() < endTime) {
        synchronized (shutdownLock) {
            try {
                // Wait for notification with timeout
                long remainingTime = endTime - System.currentTimeMillis();
                if (remainingTime > 0) {
                    shutdownLock.wait(Math.min(1000, remainingTime));
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for pending operations: " + e.getMessage());
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        // Log progress periodically
        if (pendingOperations > 0 && System.currentTimeMillis() > startTime + 5000) {
            startTime = System.currentTimeMillis();
            logger.info("Still waiting for " + pendingOperations + " pending operations to complete");
        }
    }
    
    if (pendingOperations > 0) {
        logger.warn("Timeout waiting for " + pendingOperations + " pending operations to complete");
    } else {
        logger.info("All pending operations completed");
    }
}

/**
 * Cleans up resources before shutdown.
 */
private void cleanupResources() {
    logger.info("Cleaning up resources");

    // Shutdown Redis connection pool
    if (jedisPool != null  && !jedisPool.isClosed()) {
        try {
            logger.info("Closing Redis connection pool");
            jedisPool.destroy(); // Use destroy() for older Jedis versions
        } catch (Exception e) {
            logger.error("Error closing Redis connection pool: " + e.getMessage() + e);
        }
    }

    // Shutdown all executors
    shutdownExecutorGracefully(heartbeatScheduler, "Heartbeat", 2);
    shutdownExecutorGracefully(scheduler, "Market recheck scheduler", 10);
    shutdownExecutorGracefully(orderExpirationScheduler, "Order expiration scheduler", 5);

    logger.info("Resource cleanup completed");
}

/**
 * Increments the count of pending operations.
 * This should be called when starting an operation that must complete
 * before shutdown.
 */
public void incrementPendingOperations() {
    synchronized (shutdownLock) {
        pendingOperations++;
        logger.info("Incremented pending operations to " + pendingOperations);
    }
}

/**
 * Decrements the count of pending operations.
 * This should be called when an operation completes.
 */
public void decrementPendingOperations() {
    synchronized (shutdownLock) {
        if (pendingOperations > 0) {
            pendingOperations--;
        }
        logger.info("Decremented pending operations to " + pendingOperations);

        // If we're shutting down and there are no more pending operations,
        // notify any waiting threads
        if (shutdownRequested && pendingOperations == 0) {
            shutdownLock.notifyAll();
        }
    }
}

/**
 * Checks if shutdown has been requested.
 * This can be used to avoid starting new operations during shutdown.
 * 
 * @return true if shutdown has been requested, false otherwise
 */
public boolean isShutdownRequested() {
    return shutdownRequested;
}

/**
 * Cancels all outstanding orders currently tracked by this OrderManagement instance.
 */
private final AtomicBoolean ordersCancelled = new AtomicBoolean(false);

public void cancelAllOrders() {
    logger.info("Cancelling all outstanding orders");

    if (ordersCancelled.getAndSet(true)) {
        logger.info("Orders already cancelled, skipping");
        return;
    }

    // Create a copy of the orders map to avoid concurrent modification
    Map<Integer, MarketOrder> ordersCopy = new HashMap<>(orders);
    for (MarketOrder order : ordersCopy.values()) {
        // Skip cancel requests
        if ("Cancel".equals(order.getVerb())) {
            continue;
        }
        
        // Get necessary info for the cancel request
        String orderId = order.getOrderId();
        String marketSource = order.getMarketSource();
        
        // Check if the order is in a valid state for cancellation
        if (orderId == null || orderId.isEmpty()) {
            logger.warn("Order ID is null or empty for order: " + order);
            continue;
        }

        // Only try to cancel if we have an order ID
        if (orderId != null && !orderId.isEmpty()) {
            String traderId = getTraderForVenue(marketSource);

            logger.info("Cancelling order: reqId=" + order.getMyReqId() + ", orderId=" + orderId);

            // Issue the cancel request
            MarketOrder cancelOrder = MarketOrder.orderCancel(
                marketSource, 
                traderId, 
                orderId, 
                this
            );
            
            if (cancelOrder != null) {
                // Track the cancel request
                removeOrder(order.getMyReqId());
                
                // Log to machine-readable format
                // ApplicationLogging.logOrderUpdate(
                //     "AUTO_CANCEL", 
                //     order.getMyReqId(),
                //     order.getOrderId(),
                //     "Order cancelled by system shutdown"
                // );
            }
        }
    }

    logger.info("All outstanding orders cancelled");
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