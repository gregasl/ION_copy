/*
 * OrderManagement
 *
 * OrderManagement object starts the component, register the relevant listeners
 * then subscribes to the depths of the configured instruments and to the orders
 * chain.
 * The set up of the subscription is done in onPublishIdle in order to not perform
 * expensive calculations in the onPublish method.
 */

package com.iontrading.automatedMarketMaking;

import java.util.Set;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import com.iontrading.mkv.qos.MkvQoS;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.enums.MkvShutdownMode;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.exceptions.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * OrderManagement is the main component of the system.
 */
public class OrderManagement implements MkvPublishListener, MkvRecordListener,
    MkvChainListener, IOrderManager {
  private MkvLog myLog;
  private int logLevel;
  private IONLogger logger;

  private volatile boolean isShuttingDown = false;

  // Instance of DepthListener to load and access instrument data
  private static DepthListener depthListener;

  private BondEligibilityListener bondEligibilityListener;

  // Market maker instance for automated market making
  private MarketMaker marketMaker;
  
  // order repository instance for managing market orders
  private OrderRepository orderRepository;
  
  // Market maker configuration
  private MarketMakerConfig marketMakerConfig;

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
    
  // Valid venue list using MarketDef constants
  Set<String> validVenues = new HashSet<>(Arrays.asList(
      MarketDef.DEALERWEB_REPO,
      MarketDef.BTEC_REPO_US, 
      MarketDef.FENICS_USREPO
  ));

  private final Set<String> subscribedPatterns = Collections.synchronizedSet(new HashSet<>());
  private final Object subscriptionLock = new Object();
 
  private final ScheduledExecutorService heartbeatScheduler = Executors.newScheduledThreadPool(1);
  private final ScheduledExecutorService orderExpirationScheduler = Executors.newScheduledThreadPool(1);

  // Redis connection constants
  private final String HEARTBEAT_CHANNEL;
  private final String ADMIN_CHANNEL;
  private final String REDIS_HOST;
  private static final int REDIS_PORT = 6379;
  
  private Mkv mkv;

  public final String hostname = System.getenv("COMPUTERNAME");
    
  //Flag to track if the system is in stopped state
  private volatile boolean isSystemStopped = false;
  private final boolean isProduction = hostname != null && hostname.equalsIgnoreCase("aslionapp01");

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
  
  private final Map<String, Object> orderActiveStatusMap = new ConcurrentHashMap<>();
  private static final String STATUS_KEY_PREFIX = "status:";

  private final AtomicInteger readyToStop = new AtomicInteger(2);
  private boolean asyncShutdownInProgress = false;

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
                  logger.info("Connected to Redis pool at "+ REDIS_HOST + ":" + REDIS_PORT);
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

        OrderManagement om = new OrderManagement();
        om.initializeMkv(args);

        // Initialize centralized logging before anything else
        BondEligibilityListener bondEligibilityListener = new BondEligibilityListener();
        om.initializeOM(bondEligibilityListener);

        MkvQoS qos = om.mkv.getQoS();
        qos.setPublishListeners(new MkvPublishListener[] { om });
        qos.setPlatformListeners(new MkvPlatformListener[] { new OrderManagementPlatformListener(om) });

        try {

            // Initialize DepthListener after MKV is started
            depthListener = new DepthListener(om);
            om.logger.info("DepthListener initialized");

            // DepthListener now handles instrument data loading internally
            // and reports status through its health monitoring methods
            om.logger.info("Instrument data will be loaded by DepthListener automatically");

            // Initialize the market maker component
            om.logger.info("Initializing market maker component");
            om.initializeMarketMaker();

            // Set up depth subscriptions - this will trigger the instrument data loading
            om.logger.info("Setting up depths subscriptions");
            om.subscribeToDepths();

            om.logger.info("Completing market maker initialization");
            om.completeMarketMakerInitialization();

        } catch (Exception e) {
            om.logger.warn("Failed to start MKV API: {}" + e.getMessage() + e);
            om.logger.warn("Error details " + e);
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
        logger.warn("DepthListener not initialized - cannot get native instrument ID");
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

        if (isProduction) {
            HEARTBEAT_CHANNEL = "HEARTBEAT:ION:MARKETMAKER";
            ADMIN_CHANNEL = "ADMIN:ION:MARKETMAKER";
            REDIS_HOST = "cacheprod";
        } else {
            HEARTBEAT_CHANNEL = "HEARTBEAT:ION:MARKETMAKERUAT";
            ADMIN_CHANNEL = "ADMIN:ION:MARKETMAKERUAT";
            REDIS_HOST = "cacheuat";
        }

        applicationId = "Java_Order_Manager";
        
        this.bondEligibilityListener = null;
        this.orderRepository = null;
    }


    /**
     * Creates a new instance of OrderManagement.
     * Generates a unique application ID based on the current time.
     */
    public void initializeOM(BondEligibilityListener bondEligibilityListener) {

        this.bondEligibilityListener = bondEligibilityListener;
        this.orderRepository = OrderRepository.getInstance();

        logger.info("Application ID: " + applicationId);

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
    
    /**
     * Initializes MKV API with appropriate platform listeners for async shutdown.
     * This should be called during startup before Mkv.start().
     * 
     * @param args Command line arguments to pass to MKV
     */
    public void initializeMkv(String[] args) {
        try {
            
            MkvQoS qos = new MkvQoS();
            qos.setArgs(args);

            try {
                mkv = Mkv.start(qos);
                logLevel = mkv.getProperties().getIntProperty("DEBUG");
                // Initialize the log after starting Mkv
                myLog = mkv.getLogManager().createLogFile("MarketMaker");
                logger = new IONLogger(myLog, logLevel, "orderManagement");
                logger.info("MKV connection established successfully");
                mkv.getPublishManager().addPublishListener(this);
            } catch (MkvException e) {
                logger.error("MKV connection failed: " + e.getMessage());
                throw new RuntimeException("MKV initialization failed", e);
            }
            logger.info("MKV API already started, adding our listeners");

        } catch (Exception e) {
            logger.error("Failed to initialize MKV with async shutdown: " + e.getMessage() + " " + e);
        }
    }

/**
 * Initialize the mapping of venues to trader IDs
 */
private void initializeTraderMap() {
    if (isProduction) {
        venueToTraderMap.put("BTEC_REPO_US", "EGEI");
        venueToTraderMap.put("DEALERWEB_REPO", "aslegerhard01");
        venueToTraderMap.put("FENICS_USREPO", "frosevan");
    } else {
        venueToTraderMap.put("BTEC_REPO_US", "TEST2");
        venueToTraderMap.put("DEALERWEB_REPO", "asldevtrd1");
        venueToTraderMap.put("FENICS_USREPO", "frosasl1");
    }
      logger.info("Venue to trader mapping initialized with " + venueToTraderMap.size() + " entries");
  }

    /**
     * Initializes the market maker component with default configuration
     */
    private void initializeMarketMaker() {
        try {
            // Create default market maker configuration
            marketMakerConfig = new MarketMakerConfig.Builder()
                .setAutoEnabled(false)
                .setQuoteUpdateIntervalSeconds(30)
                .setMarketSource("FENICS_USREPO")
                .setTargetVenues("BTEC_REPO_US", "DEALERWEB_REPO")
                .build();
            logger.info("Default market maker configuration created: " + marketMakerConfig);
            // Create the market maker instance
            marketMaker = new MarketMaker(this, marketMakerConfig, bondEligibilityListener);
            logger.info("Market maker initialized with default configuration: " + marketMakerConfig);

            // Add a shutdown hook for the market maker
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (marketMaker != null) {
                    logger.info("Shutting down market maker");
                    marketMaker.shutdown();
                }
            }));
        } catch (Exception e) {
            logger.error("Error initializing market maker " + e);
        }
    }

    private void initializeOrderExpirationChecker() {
        logger.info("Initializing order expiration checker");
        
        // Schedule the order expiration task to run every 5 seconds
        orderExpirationScheduler.scheduleAtFixedRate(() -> {
            try {
                logger.debug("Checking for expired orders");
                monitorActiveOrders();
            } catch (Exception e) {
                logger.error("Error checking for expired orders: " + e.getMessage() + " " + e);
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    // Add this method to complete market maker initialization after MKV is ready
    private void completeMarketMakerInitialization() {
        if (marketMaker != null) {
            try {
                logger.info("Completing market maker initialization with MKV subscriptions");
                marketMaker.initializeMkvSubscriptions();
            } catch (Exception e) {
                logger.error("Error completing market maker initialization" + e);
            }
        }
    }

    /**
     * Starts the automated market making process
     * 
     * @return true if started successfully, false otherwise
     */
    public boolean startMarketMaking() {
        try {
            if (marketMaker != null) {
                marketMaker.startAutomatedMarketMaking();
                logger.info("Market making started");
                return true;
            }
            return false;
        } catch (Exception e) {
            logger.error("Error starting market making: " + e.getMessage() + " " + e);
            return false;
        }
    }

    /**
     * Stops the automated market making process
     * 
     * @return true if stopped successfully, false otherwise
     */
    public boolean stopMarketMaking() {
        try {
            if (marketMaker != null) {
                marketMaker.stopAutomatedMarketMaking();
                logger.info("Market making stopped");
                return true;
            }
            return false;
        } catch (Exception e) {
            logger.error("Error stopping market making " + e);
            return false;
        }
    }


    /**
     * Gets the market maker instance
     * 
     * @return The market maker instance
     */
    public MarketMaker getMarketMaker() {
        return marketMaker;
    }

    /**
     * Gets the current market maker configuration
     * 
     * @return The market maker configuration
     */
    public MarketMakerConfig getMarketMakerConfig() {
        return marketMakerConfig;
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

	//Helper method to get trader ID for a venue
	public String getTraderForVenue(String venue) {
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
	            logger.error("Error in Redis control listener: " + e.getMessage() + " " + e);
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
	                    logger.warn("Error returning Redis subscriber to pool: " + e.getMessage());
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
                } else if ("MM_START".equalsIgnoreCase(command)) {
                    handleMarketMakerStartCommand(controlMessage);
                } else if ("MM_STOP".equalsIgnoreCase(command)) {
                    handleMarketMakerStopCommand(controlMessage);
                } else if ("MM_CONFIG".equalsIgnoreCase(command)) {
                    handleMarketMakerConfigCommand(controlMessage);
                } else {
                    logger.warn("Unknown control command received: " + command);
                }
	        } catch (Exception e) {
	            logger.error("Error processing control message: " + e.getMessage() + " " + e);
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
            logger.info("System already in STOPPED state, ignoring redundant STOP command");
            return;
        }

        logger.warn("Executing STOP command - cancelling orders and preventing new submissions");

        // Set the system to stopped state
        isSystemStopped = true;

        // Disable continuous trading to prevent auto-trading
        continuousTradingEnabled = false;
        
        // Cancel all existing orders
        cancelAllOrders();
        
        // Publish status update
    //    publishSystemStatus();

        logger.warn("STOP command executed - system is now STOPPED");
    }

    /**
     * Handles the RESUME command, allowing new orders to be submitted.
     * 
     * @param controlMessage The control message containing command parameters
     */
    private void handleResumeCommand(Map<String, Object> controlMessage) {
        // Skip if not stopped
        if (!isSystemStopped) {
            logger.info("System already in RUNNING state, ignoring redundant RESUME command");
            return;
        }

        logger.warn("Executing RESUME command - allowing new order submissions");

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

        logger.warn("RESUME command executed - system is now RUNNING");
    }

        /**
     * Handles the MM_START command to start market making.
     * 
     * @param controlMessage The control message containing command parameters
     */
    private void handleMarketMakerStartCommand(Map<String, Object> controlMessage) {
        logger.info("Executing MM_START command - starting market maker");

        boolean success = startMarketMaking();
        
        Map<String, Object> status = new HashMap<>();
        status.put("command", "MM_START");
        status.put("success", success);
        status.put("status", success ? "RUNNING" : "FAILED");
        status.put("timestamp", new Date().getTime());
        
        publishToRedis(ADMIN_CHANNEL + "_RESPONSE", status);

        logger.info("Market maker start command executed - result: " + (success ? "SUCCESS" : "FAILED"));
    }

    /**
     * Handles the MM_STOP command to stop market making.
     * 
     * @param controlMessage The control message containing command parameters
     */
    private void handleMarketMakerStopCommand(Map<String, Object> controlMessage) {
        logger.info("Executing MM_STOP command - stopping market maker");

        boolean success = stopMarketMaking();
        
        Map<String, Object> status = new HashMap<>();
        status.put("command", "MM_STOP");
        status.put("success", success);
        status.put("status", success ? "STOPPED" : "FAILED");
        status.put("timestamp", new Date().getTime());
        
        publishToRedis(ADMIN_CHANNEL + "_RESPONSE", status);

        logger.info("Market maker stop command executed - result: " + (success ? "SUCCESS" : "FAILED"));
    }

    /**
     * Handles the MM_CONFIG command to update market maker configuration.
     * 
     * @param controlMessage The control message containing configuration parameters
     */
    private void handleMarketMakerConfigCommand(Map<String, Object> controlMessage) {
        logger.info("Executing MM_CONFIG command - updating market maker configuration");
        
        try {
        // Extract configuration parameters with proper defaults
        Boolean autoEnabled = (Boolean) controlMessage.getOrDefault("autoEnabled", marketMakerConfig.isAutoEnabled());
        Number updateInterval = (Number) controlMessage.getOrDefault("updateInterval", 
            marketMakerConfig.getQuoteUpdateIntervalSeconds());
        
        // Extract other optional parameters
        String marketSource = (String) controlMessage.getOrDefault("marketSource", marketMakerConfig.getMarketSource());
        Boolean autoHedge = (Boolean) controlMessage.getOrDefault("autoHedge", marketMakerConfig.isAutoHedge());
        Double minSize = ((Number) controlMessage.getOrDefault("minSize", marketMakerConfig.getMinSize())).doubleValue();
        
        // Create new configuration using Builder pattern
        MarketMakerConfig newConfig = new MarketMakerConfig.Builder()
            .setAutoEnabled(autoEnabled)
            .setQuoteUpdateIntervalSeconds(updateInterval.intValue())
            .setMarketSource(marketSource)
            .setAutoHedge(autoHedge)
            .setMinSize(minSize)
            .setOrderType(marketMakerConfig.getOrderType())
            .setTimeInForce(marketMakerConfig.getTimeInForce())
            .setTargetVenues(marketMakerConfig.getTargetVenues())
            .setCashMarketHours(marketMakerConfig.getCashMarketOpenTime(), marketMakerConfig.getCashMarketCloseTime())
            .setRegMarketHours(marketMakerConfig.getRegMarketOpenTime(), marketMakerConfig.getRegMarketCloseTime())
            .build();
        
            // Update the configuration
            updateMarketMakerConfig(newConfig);
            
            Map<String, Object> status = new HashMap<>();
            status.put("command", "MM_CONFIG");
            status.put("success", true);
            status.put("config", marketMaker.getConfigSummary());
            status.put("timestamp", new Date().getTime());
            
            publishToRedis(ADMIN_CHANNEL + "_RESPONSE", status);
            
            logger.info("Market maker configuration updated: " + newConfig);
        } catch (Exception e) {
            logger.error("Error updating market maker configuration: " + e.getMessage() + " " + e);
            
            Map<String, Object> status = new HashMap<>();
            status.put("command", "MM_CONFIG");
            status.put("success", false);
            status.put("error", e.getMessage());
            status.put("timestamp", new Date().getTime());
            
            publishToRedis(ADMIN_CHANNEL + "_RESPONSE", status);
        }
    }

  /**
   * Called when a publication event occurs.
   * This implementation doesn't handle individual publication events.
   */
  public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
    // Not handling individual publication events
    logger.debug("Publication event for: " + mkvObject.getName());

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
      logger.debug("Publication idle for component: " + component + ", start: " + start);
  }

  /**
   * Not interested in this event because our component is a pure subscriber
   * and is not supposed to receive requests for subscriptions.
   */
  public void onSubscribe(MkvObject mkvObject) {
    // No action needed - we are not a publisher
    logger.debug("Subscription event for: " + mkvObject.getName());
      }

    /**
     * Platform listener implementation that handles the async shutdown process.
     * This allows the application to gracefully shut down when requested by the daemon.
     */
    private static class OrderManagementPlatformListener implements MkvPlatformListener {
        private final OrderManagement om;

        public OrderManagementPlatformListener(OrderManagement orderManagement) {
            this.om = orderManagement;
        }
    
        @Override
        public void onMain(MkvPlatformEvent event) {
            switch (event.intValue()) {
                case MkvPlatformEvent.START_code:
                    om.logger.info("MKV platform START event received");
                    break;
                    
                case MkvPlatformEvent.STOP_code:
                    om.logger.info("MKV platform STOP event received");
                    if (om.asyncShutdownInProgress) {
                        om.logger.info("Final STOP event received after async shutdown - completing shutdown process");
                    }
                    break;
                    
                case MkvPlatformEvent.SHUTDOWN_REQUEST_code:
                    om.logger.info("MKV platform SHUTDOWN_REQUEST event received - beginning async shutdown sequence");
                    
                    try {
                        // First phase of the shutdown process
                        om.asyncShutdownInProgress = true;
                        
                        // If we're not ready to stop yet, ask for an async shutdown
                        if (om.readyToStop.decrementAndGet() > 0) {
                            om.logger.info("Not ready to stop - requesting async shutdown (pending phases: " + om.readyToStop.get() + ")");

                            // Start the graceful shutdown in a separate thread
                            Thread shutdownThread = new Thread(() -> {
                                try {
                                    om.logger.info("Beginning first phase of graceful shutdown...");
                                    
                                    // First prepare the market maker (disable but don't cancel orders yet)
                                    if (om.marketMaker != null) {
                                        om.logger.info("Preparing market maker for shutdown");
                                        om.marketMaker.prepareForShutdown();
                                    }
                                    
                                    // Cancel market maker orders with a timeout
                                    om.logger.info("Cancelling market maker orders");
                                    boolean cancelled = om.cancelMarketMakerOrders(10000);
                                    om.logger.info("Market maker order cancellation complete: all cancelled=" + cancelled);

                                    // Then cancel any remaining orders
                                    om.logger.info("Cancelling remaining orders");
                                    om.cancelAllOrders();
                                    
                                    // Notify that we're ready for final shutdown
                                    om.logger.info("All orders cancelled, ready for final shutdown");
                                    om.readyToStop.decrementAndGet();
                                    
                                    // Request a synchronous shutdown now that we're ready
                                    om.logger.info("Requesting synchronous shutdown to complete process");
                                    Mkv.getInstance().shutdown(MkvShutdownMode.SYNC, "Component is stopping...");
                                } catch (Exception e) {
                                    om.logger.error("Error during async shutdown sequence: " + e.getMessage() + " " + e);

                                    // Force sync shutdown if we hit an error
                                    try {
                                        Mkv.getInstance().shutdown(MkvShutdownMode.SYNC, "Component is stopping due to error...");
                                    } catch (Exception ex) {
                                        om.logger.error("Failed to request sync shutdown: " + ex.getMessage() + " " + ex);
                                    }
                                }
                            }, "AsyncShutdown-Thread");
                            
                            // Make sure this doesn't prevent JVM exit if it hangs
                            shutdownThread.setDaemon(true);
                            shutdownThread.start();
                            
                            // Request async shutdown to delay the process
                            Mkv.getInstance().shutdown(MkvShutdownMode.ASYNC, "Not yet ready to stop, cancelling orders...");
                        } else {
                            // We're already ready to stop, so request a sync shutdown
                            om.logger.info("Ready to stop - requesting sync shutdown");
                            Mkv.getInstance().shutdown(MkvShutdownMode.SYNC, "Component is stopping...");
                        }
                    } catch (MkvException e) {
                        om.logger.error("Error handling shutdown request: " + e.getMessage() + " " + e);
                    }
                    break;
            }
        }

        @Override
        public void onComponent(MkvComponent comp, boolean start) {
            om.logger.info("MKV component event: component=" + comp.getName() + ", start=" + start);
        }

        @Override
        public void onConnect(String comp, boolean start) {
            om.logger.info("MKV connect event: component=" + comp + ", start=" + start);
        }
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
            
            // Subscribe to order records
            subscribeToRecord(ORDER_PATTERN, ORDER_FIELDS);

            // Subscribe to login records
            subscribeToRecord(LOGIN_PATTERN, LOGIN_FIELDS);

            // Set up a monitor to check subscription status periodically
            setupPatternSubscriptionMonitor();
            
        } catch (Exception e) {
            logger.error("Error setting up depth subscriptions: " + e.getMessage() + " " + e);
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
            logger.error("Error subscribing to pattern: " + e.getMessage() + " " + e);
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

        logger.info("Subscribing to instrument data using pattern: " + INSTRUMENT_PATTERN);

        // Get the publish manager to access patterns
        MkvPublishManager pm = Mkv.getInstance().getPublishManager();
       
        // Look up the pattern object
        MkvObject obj = pm.getMkvObject(INSTRUMENT_PATTERN);
       
        if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
            logger.info("Found instrument pattern, subscribing: " + INSTRUMENT_PATTERN);

            // Initialize with the full field list from MarketDef
            List<String> fieldsList = new ArrayList<>(Arrays.asList(INSTRUMENT_FIELDS));
            boolean subscribed = false;
            
            // Keep trying with fewer fields until subscription succeeds
            while (!subscribed && !fieldsList.isEmpty()) {
                try {
                    String[] fields = fieldsList.toArray(new String[0]);
                    ((MkvPattern) obj).subscribe(fields, depthListener);
                    subscribed = true;

                    logger.info("Successfully subscribed to instrument pattern with " + fieldsList.size() + " fields");
                } catch (Exception e) {
                    // If we have any fields left to remove
                    if (fieldsList.size() > 3) {  // Keep at least 3 essential fields
                        // Remove the last field in the list
                        String lastField = fieldsList.remove(fieldsList.size() - 1);
                        logger.warn("Subscription failed with field: " + lastField + ". Retrying with " + fieldsList.size() + " fields.");
                    } else {
                        // If we've removed too many fields, try with a minimal set
                        logger.warn("Failed with reduced field set, trying minimal fields");

                        // Define minimal essential fields
                        String[] minimalFields = new String[] {
                            "Id", "IsAON"
                        };
                        
                        try {
                            ((MkvPattern) obj).subscribe(minimalFields, depthListener);
                            subscribed = true;
                            logger.info("Successfully subscribed with minimal fields");
                        } catch (Exception minEx) {
                            logger.error("Failed even with minimal fields: " + minEx.getMessage() + " " + minEx);
                            throw minEx;  // Propagate the exception if even minimal fields fail
                        }
                        break;
                    }
                }
            }
        } else {
            logger.warn("Instrument pattern not found: " + INSTRUMENT_PATTERN);
        }
    } catch (Exception e) {
        logger.error("Error subscribing to instrument pattern: " + e.getMessage() + " " + e);
        logger.error("Error details: " + e.getMessage() + " " + e);
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
                if (!subscribedPatterns.contains(DEPTH_PATTERN)) {
                    logger.info("Found consolidated depth pattern, subscribing: " + DEPTH_PATTERN);

                    ((MkvPattern) obj).subscribe(DEPTH_FIELDS, depthListener);

                    // Mark that we've successfully subscribed
                    subscribedPatterns.add(DEPTH_PATTERN);

                    logger.info("Successfully subscribed to consolidated depth pattern");
                }
            }
        } else {
            logger.warn("Consolidated depth pattern not found: " + DEPTH_PATTERN);

            // Create a single shared listener instance instead of an anonymous one
            final MkvPublishListener patternListener = new MkvPublishListener() {
                @Override
                public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
                    // Only proceed if we're not already subscribed
                    if (subscribedPatterns.contains(DEPTH_PATTERN)) {
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
                    if (subscribedPatterns.contains(DEPTH_PATTERN)) {
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
        logger.error("Error subscribing to pattern: " + e.getMessage() + " " + e);
    }
}

    // Helper method to handle subscription and listener removal safely
    private void trySubscribeAndRemoveDepthListener(MkvObject mkvObject, MkvPublishManager pm, MkvPublishListener listener) {
        synchronized (subscriptionLock) {
            // Check again inside synchronization to avoid race conditions
            if (subscribedPatterns.contains(DEPTH_PATTERN)) {
                return;
            }
            
            try {
                logger.info("Pattern found, subscribing: " + DEPTH_PATTERN);

                ((MkvPattern) mkvObject).subscribe(DEPTH_FIELDS, depthListener);
                subscribedPatterns.add(DEPTH_PATTERN);

                logger.info("Successfully subscribed to consolidated depth pattern");

                // Remove the listener now that we've subscribed - safely outside the callback
                // but still inside synchronization
                pm.removePublishListener(listener);
            } catch (Exception e) {
                logger.error("Error subscribing to pattern: " + e.getMessage() + " " + e);
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
        logger.debug("Adding order to cache: reqId=" + order.getMyReqId());
        orderRepository.storeOrder(order);
    }

    /**
     * Removes a MarketOrder object from the cache by request ID.
     * Called when an order is considered "dead".
     * 
     * @param reqId The request ID of the order to remove
     */
    public void removeOrder(int reqId) {
        logger.debug("Removing order from cache: reqId=" + reqId);
        orderRepository.removeOrder(reqId);
    }

    /**
     * Retrieves a MarketOrder object from the cache by request ID.
     * 
     * @param reqId The request ID of the order to retrieve
     * @return The MarketOrder object or null if not found
     */
    public MarketOrder getOrder(int reqId) {
        MarketOrder order = orderRepository.getOrderByReqId(reqId);
        logger.debug("Retrieving order from cache: reqId=" + reqId + ", found=" + (order != null));
        return order;
    }

    @Override
    public void mapOrderIdToReqId(String orderId, int reqId) {
        if (orderId != null && !orderId.isEmpty()) {
            orderRepository.mapOrderIdToReqId(orderId, reqId);
        }
    }

    public MarketOrder getOrderByOrderId(String orderId) {
        if (orderId == null || orderId.isEmpty()) {
            logger.warn("Cannot find order with null or empty orderId");
            return null;
        }
        MarketOrder order = orderRepository.getOrderByOrderId(orderId);
        int reqId = (order != null) ? order.getMyReqId() : -1;
        if (reqId != -1) {
            return getOrder(reqId);
        }
        
        // Fall back to full search if not found in map
        // This handles edge cases where the index wasn't updated
        for (MarketOrder foundOrder : orderRepository.getAllOrders()) {
            if (orderId.equals(order.getOrderId())) {
                // Update the map for future lookups
                orderRepository.mapOrderIdToReqId(orderId, foundOrder.getMyReqId());
                return foundOrder;
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
    logger.debug("Partial update for record: " + mkvRecord.getName());

  }

    public int timeToInt(LocalTime t) {
        return t.getHour() * 10000 + t.getMinute() * 100 + t.getSecond();
    }

/**
 * Categorize and handle updates from CM_Order and CM_Login records.
 * This method processes full updates and determines the type of record
 */
@Override
public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnap) {
    try {
        String recordName = mkvRecord.getName();
            if (recordName.contains("CM_ORDER")) {
                processOrderUpdate(mkvRecord, mkvSupply, isSnap);
            } else if (recordName.contains("CM_LOGIN")) {
                processLoginUpdate(mkvRecord, mkvSupply, isSnap);
            } else {
                logger.warn("Received unexpected record type: " + recordName);
            }

    } catch (Exception e) {
        logger.error("Error processing order update: " + e.getMessage() + " " + e);
    }
}

/**
 * Process full updates for CM_ORDER records.
 * Detects order fills and triggers hedge trades when appropriate.
 * Also identifies and handles duplicate MarketMaker orders.
 */
private void processOrderUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnap) {
    try {
        // Check if this is our order by looking at CompNameOrigin (should match our app ID)
        String active = mkvRecord.getValue("ActiveStr").getString();
        String appName = mkvRecord.getValue("CompNameOrigin").getString();
        String src = mkvRecord.getValue("OrigSrc").getString();
        String orderId = mkvRecord.getValue("Id").getString();
        String Id = mkvRecord.getValue("InstrumentId").getString();
        String origId = mkvRecord.getValue("OrigId").getString();
        String VerbStr = mkvRecord.getValue("VerbStr").getString();
        String tradeStatus = mkvRecord.getValue("TradingStatusStr").getString();
        double qtyFill = mkvRecord.getValue("QtyFill").getReal();
        double price = mkvRecord.getValue("Price").getReal();
        double intQtyGoal = mkvRecord.getValue("IntQtyGoal").getReal();
        double qtyHit = mkvRecord.getValue("QtyHit").getReal();
        double qtyStatus = mkvRecord.getValue("QtyStatus").getReal();
        String qtyStatusStr = mkvRecord.getValue("QtyStatusStr").getString();
        double qtyTot = mkvRecord.getValue("QtyTot").getReal();
        int time = mkvRecord.getValue("Time").getInt();
        String activityKey = origId; // Using origId as the unique identifier
        String statusKey = STATUS_KEY_PREFIX + activityKey + ":" + Id;

        // Safely retrieve previous active status with type checking
        boolean currentlyActive = "Yes".equals(active);
        orderActiveStatusMap.put(statusKey, currentlyActive);

        boolean thisApp = appName.equals("automatedMarketMaking");
        logger.info("Processing order from app: " + appName);
        boolean isEligible = bondEligibilityListener.isIdEligible(Id);
        
        if (!thisApp) {
            // If this is not our app, we don't process it
            logger.debug("Ignoring order from another app: " + appName);
            return;
        }
        boolean wasOrderActive = orderRepository.getOrderStatus(orderId);
        logger.info("Order status for " + orderId + ": currentlyActive=" + currentlyActive + ", wasOrderActive=" + wasOrderActive);
        orderRepository.setOrderStatus(orderId, currentlyActive);
        String traderId = getTraderForVenue(src);

        if (currentlyActive && !isEligible) {
            MarketOrder cancelOrder = MarketOrder.orderCancel(
                        src, traderId, origId, this);
            orderRepository.updateOrderStatus(Id, VerbStr, !currentlyActive);
            logger.info("Order " + origId + " is not eligible for trading, cancelling order: " + cancelOrder.getOrderId());
        }
        orderRepository.setOrderStatus(orderId, currentlyActive);

        if (currentlyActive) {
            ActiveQuote existingQuote = orderRepository.getQuote(Id);
            if (VerbStr.equals("Buy")) {
                logger.info("Processing active buy order: " + Id + ", orderId=" + orderId + ", origId=" + origId);
                MarketOrder existingBidOrder = existingQuote.getBidOrder();
                if (existingBidOrder != null) {
                    logger.info("Found existing bid order: " + existingBidOrder.getOrderId());
                    if (!existingBidOrder.getOrderId().equals(origId)) {
                        logger.warn("Duplicate active buy orders detected for " + Id + ": existingOrderId=" + existingBidOrder.getOrderId() + ", newOrderId=" + origId);
                        MarketOrder cancelOrder = MarketOrder.orderCancel(
                            src, traderId, origId, this);       
                    }
                }
            } else if (VerbStr.equals("Sell")) {
                MarketOrder existingAskOrder = existingQuote.getAskOrder();
                logger.info("Processing active sell order: " + Id + ", orderId=" + orderId + ", origId=" + origId);
                if (existingAskOrder != null) {
                    logger.info("Found existing ask order: " + existingAskOrder.getOrderId());
                    if (!existingAskOrder.getOrderId().equals(origId)) {
                        logger.warn("Duplicate active sell orders detected for " + Id + ": existingOrderId=" + existingAskOrder.getOrderId() + ", newOrderId=" + origId);
                        MarketOrder cancelOrder = MarketOrder.orderCancel(
                            src, traderId, origId, this);
                    }
                }
            }
        }

        Map<String, Map<String, List<String>>> multipleOrders = checkForMultipleActiveOrders(Id);
        if (!multipleOrders.isEmpty()) {
            logger.warn("Multiple active orders detected after processing order update for " + Id + "/" + VerbStr);
            // Cancel oldest orders so only newest order is remaining per side
            cancelOldestOrders(multipleOrders);
        }
        
        ActiveQuote quote = orderRepository.getQuote(Id);

        // Update order status in ActiveQuote
        if (quote != null) {
            if ("Buy".equals(VerbStr)) {
                quote.setBidActive(currentlyActive);
            } else if ("Sell".equals(VerbStr)) {
                quote.setAskActive(currentlyActive);
            }
        }
        
        // // Update order object if we have it
        // MarketOrder order = orderRepository.getOrderByOrderId(orderId);
        // if (order != null) {
        //     order.setActive(currentlyActive);
        //     // If filled, update the quote
        //     if (qtyFill > 0) {
        //         quote.recordFill(orderId, qtyFill);
        //     }
        // }
        
        // If the order is now inactive, update OrderRepository
        if (!currentlyActive) {
            orderRepository.updateOrderStatus(Id, VerbStr, false);
        }

            // Only process first transition from active to inactive (this is needed as get duplicate Order messages) and requires a Complete or Partial fill
        if (currentlyActive == false && wasOrderActive == true && (qtyStatusStr.equals("Cf") || qtyStatusStr.equals("Pf"))) {
            logger.info("Order status change detected for origId=" + origId + ": ActiveStr changed to No and qtyStatusStr=" + qtyStatusStr);

            logger.info("Processing order for hedging: " + mkvRecord.getName() + ", active=" + active + ", appName=" + appName + ", src=" + src + ", Id=" + Id + ", orderId=" + orderId + ", origId=" + origId + ", VerbStr=" + VerbStr + ", tradeStatus=" + tradeStatus + ", qtyFill=" + qtyFill + ", price=" + price + ", intQtyGoal=" + intQtyGoal + ", qtyHit=" + qtyHit + ", qtyStatus=" + qtyStatus + ", qtyStatusStr=" + qtyStatusStr + ", qtyTot=" + qtyTot + ", time=" + time);

            if (!"FENICS_USREPO".equals(src) || active.equals("Yes")) {
                return; // Exit without hedging order if not hedging FENICS USREPO orders or if order is active
            }

            double orderPrice = 0.0;
            String market = null;
            boolean validVenue = false;
            boolean positiveArb = false;
            // Get the best price information for this instrument
            Best bestMarket = orderRepository.getLatestBest(Id);
            if (bestMarket == null) {
                logger.warn("No best market found for instrument: " + Id);
                return; // Cannot process without best market
            } else {
                if (VerbStr.equals("Buy")) {
                    orderPrice = bestMarket.getBid();
                    positiveArb = (price >= orderPrice);
                    market = bestMarket.getBidSrc();
                } else if (VerbStr.equals("Sell")) {
                    orderPrice = bestMarket.getAsk();
                    positiveArb = (price <= orderPrice);
                    market = bestMarket.getAskSrc();
                } else {
                    logger.warn("Unknown verb for order: " + VerbStr);
                    return; // Cannot process unknown verb
                }
            }
            
            validVenue = marketMaker.isTargetVenue(market);
            if (!validVenue) {
                logger.warn("Invalid market venue for hedging: " + market);
                return; // Cannot hedge on invalid venue
            }
            // Now we use the market variable to get the trader
            String trader = getTraderForVenue(market);
            if (trader == null || trader.isEmpty()) {
                logger.warn("No trader found for market: " + market);
                return; // Cannot hedge without a trader
            }
            String hedgeDirection = null;
            if (VerbStr.equals("Buy")) {
                hedgeDirection = "Sell"; // Hedge buy with sell
            } else if (VerbStr.equals("Sell")) {
                hedgeDirection = "Buy"; // Hedge sell with buy
            } else {
                logger.warn("Invalid verb for hedging: " + VerbStr);
                return; // Cannot hedge unknown verb
            }

            // Check if execution price is approximately near the best price
            if (!positiveArb) {
                logger.warn("Not positive arb for: orderPrice=\"" + orderPrice + "\", execPrice=\"" + price + "\"");
                return; // Reject hedge if price is too far off
            }

            String nativeId = getNativeInstrumentId(Id, market, false);
            if (nativeId == null || nativeId.isEmpty()) {
                logger.warn("No native ID found for instrument: " + Id + ", market: " + market);
                return; // Cannot hedge without a native ID
            }
            
            if (orderPrice > 0.0 && qtyHit > 0.0) {
                logger.info("Preparing hedging order before self match: market=" + market + ", trader=" + trader + ", Id=" + Id + ", nativeId=" + nativeId + ", verb=" + hedgeDirection + ", qtyFilled=" + qtyHit + ", price=" + orderPrice);
                if (hedgeDirection.equals("Buy")) {
                    if (bestMarket.isMinePrice(bestMarket.getAskStatus())) {
                        logger.info("Self match prevention on the ask side: nativeId=" + nativeId + ", qtyHit=" + qtyHit);
                        return;
                    }
                    logger.info("Hedging offer lifted with buy order: nativeId=" + nativeId + ", qtyHit=" + qtyHit);
                } else {
                    if (bestMarket.isMinePrice(bestMarket.getBidStatus())) {
                        logger.info("Self match prevention on the bid side: nativeId=" + nativeId + ", qtyHit=" + qtyHit);
                        return;
                    }
                }
                addOrder(market, trader, nativeId, hedgeDirection, qtyHit, orderPrice, "Limit", "FAK"); 
            } else {
                logger.warn("Hedging not applicable for Id=" + Id + ", market=" + src + ", Verb=" + VerbStr + ", currentlyActive=" + currentlyActive + ", priorActive=" + wasOrderActive + ", qtyHit=" + qtyHit + ", price=" + orderPrice + ", wasOrderActive=" + wasOrderActive);
            }
        } else {
            logger.info("Hedging not applicable for Id=" + Id + ", market=" + src + ", Verb=" + VerbStr + ", currentlyActive=" + currentlyActive + ", priorActive=" + wasOrderActive);
        }
    } catch (Exception e) {
        logger.error("Error processing order update: " + e.getMessage() + " " + e);
    }
}

private void processLoginUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnap) {
    try {
        //        public static String[] LOGIN_FIELDS = { "Id", "Src0", "Src1", "Src2", "Src3", "Src4", "Src5", "Src6", "Src7", "TStatusStr", "TStatusStr0", "TStatusStr1", "TStatusStr2", "TStatusStr3", "TStatusStr4", "TStatusStr5", "TStatusStr6", "TStatusStr7"};
        String trader = mkvRecord.getValue("Id").getString();

        if (!trader.contains("evan_gerhard")) {
            logger.debug("Ignoring login update from another trader: " + trader);
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
                    logger.info("Not valid venue: " + src);
                    continue;
                } else {
                    if (tStatus.equals("On")){
                        orderRepository.addVenueActive(src, true);
                        logger.info("Venue " + src + " is now active for trader " + trader);
                    } else {
                        orderRepository.addVenueActive(src, false);
                        logger.info("Venue " + src + " is now inactive for trader " + trader);
                    }
                }
            }
        }
    } catch (Exception e) {
        logger.error("Error processing login update: " + e.getMessage() + " " + e);
    }
}

@Override
public void orderDead(MarketOrder order) {
    logger.info("Order is now dead: reqId=" + order.getMyReqId() + ", orderId=" + order.getOrderId());

    // Remove the order from the cache
    removeOrder(order.getMyReqId());
  }
  
  /**
   * Adds an order for the given instrument, quantity, and other parameters.
   * If the order creation succeeds, stores the order in the internal cache.
   * 
   * Implementation of IOrderManager.addOrder()
   */
  public MarketOrder addOrder(String marketSource, String traderId, String instrId, String verb, double qty,
      double price, String type, String tif) {

	logger.info("Attempting to create order: source=" + marketSource + ", trader=" + traderId + ", instrId=" + instrId + ", verb=" + verb + ", qty=" + qty + ", price=" + price + ", type=" + type + ", tif=" + tif);

	// Create the order using the static factory method
    MarketOrder order = MarketOrder.orderCreate(marketSource, traderId, instrId, verb, qty, price,
        type, tif, this);
    
    if (order != null) {
      // If creation succeeded, add the order to the cache
      addOrder(order);
      
      // Check if there was an error code returned that indicates rejection
      if (order.getErrCode() != 0) {
            logger.warn("Order rejected by market: reqId=" + order.getMyReqId() + ", instrId=" + instrId + ", errCode=" + order.getErrCode() + ", errStr=" + order.getErrStr() + ", source=" + marketSource + ", trader=" + traderId);
            // Remove the rejected order from the cache since it won't be processed
            removeOrder(order.getMyReqId());
            return null; // Return null for rejected orders to indicate failure
      } else {
            logger.info("Order created successfully: reqId=" + order.getMyReqId() + ", instrId=" + instrId + ", source=" + marketSource + ", trader=" + traderId);
            // Get or create a quote for this instrument
            ActiveQuote quote = orderRepository.getQuote(instrId);
            
            // Update the quote with order information
            if (quote != null) {
                if ("Buy".equals(verb)) {
                    quote.setBidVenue(marketSource, traderId, instrId);
                    quote.setBidOrder(order, "DEFAULT", price);
                    quote.setBidActive(true);
                } else {
                    quote.setAskVenue(marketSource, traderId, instrId);
                    quote.setAskOrder(order, "DEFAULT", price);
                    quote.setAskActive(true);
                }
            }
            logger.info("Order created successfully: " + order.getMyReqId());
        }
    }
    return order;
}

  /**
   * Handles notification of a best price update.
   * Implementation of IOrderManager.best()
   */
  @Override
    public void best(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {
        try {
            // Validate the best object
            if (best == null || best.getId() == null || best.getInstrumentId() == null) {
                logger.warn("Received invalid best object: " + best);
                return; // Cannot process invalid best
            }
            logger.debug("OrderManagement.best() called: instrument=" + best.getId() + ", instrumentId=" + best.getInstrumentId() + ", ask=(" + best.getAsk() + "), askSrc=(" + best.getAskSrc() + "), bid=(" + best.getBid() + "), bidSrc=(" + best.getBidSrc() + "), cash_gc=(" + cash_gc + "), reg_gc=(" + reg_gc + ")");
            orderRepository.storeBestPrice(best.getId(), best);

            // Call marketMaker.best() if it's available and initialized
            if (marketMaker != null) {
                try {

                    logger.info("Forwarding best price to market maker: instrument=" + best.getId() + ", instrumentId=" + best.getInstrumentId() + ", ask=(" + best.getAsk() + "), bid=(" + best.getBid() + ")");
                    // Pass the best information to the market maker
                    // marketMaker.best(best, currentCashGC, currentRegGC, gcBestCashCopy, gcBestREGCopy);
                    marketMaker.best(best, 
                                    GCBestManager.getInstance().getCashGCRate(), 
                                    GCBestManager.getInstance().getRegGCRate(),
                                    GCBestManager.getInstance().getCashGCBest(),
                                    GCBestManager.getInstance().getRegGCBest());

                    // Map<String, Map<String, List<String>>> multipleOrders = checkForMultipleActiveOrders(best.getId());
                    // if (!multipleOrders.isEmpty()) {
                    //     LOGGER.warn("Multiple active orders detected after processing order update for {}/{}", 
                    //         best.getId());
                    //     // Cancel oldest orders so only newest order is remaining per side
                    //     cancelOldestOrders(multipleOrders);
                    // }

                } catch (Exception e) {
                    logger.error("Error forwarding best price to market maker: " + e.getMessage() + " " + e);
                }
            }

        } catch (Exception e) {
                logger.error("Error validating best object: " + e.getMessage() + " " + e);
                return; // Cannot process if validation fails
        }
    }
    
    public Map<String, Best> getLatestBestByInstrument() {
        // Create view from repository data
        Map<String, Best> result = new HashMap<>();
        for (String id : orderRepository.getTrackedInstruments()) {
            Best best = orderRepository.getLatestBest(id);
            if (best != null) {
                result.put(id, best);
            }
        }
        return Collections.unmodifiableMap(result);
    }

  /**
   * Handles changes to the order chain.
   * For new records, sets up subscriptions to receive updates.
   */
  public void onSupply(MkvChain chain, String record, int pos,
      MkvChainAction action) {

  }

private void monitorActiveOrders() {
    try {
        // Full system check periodically
        Map<String, Map<String, List<String>>> multipleOrders = checkForMultipleActiveOrders(null);
        
        if (!multipleOrders.isEmpty()) {
            logger.warn("Active order monitoring detected issues with " + multipleOrders.size() + " instruments");

            // Here you could implement automatic resolution logic
            // For example, keep the newest order and cancel older ones
            cancelOldestOrders(multipleOrders);
        } else {
            logger.debug("Active order monitoring: No multiple active orders detected");
        }
    } catch (Exception e) {
        logger.error("Error in active order monitoring: " + e.getMessage() + " " + e);
    }
}

/**
 * Resolves multiple active orders by cancelling all but the newest order
 * for each instrument/side combination.
 * 
 * @param multipleOrders Map of instruments to sides to order lists
 */
private void cancelOldestOrders(Map<String, Map<String, List<String>>> multipleOrders) {
    try {
        for (Map.Entry<String, Map<String, List<String>>> entry : multipleOrders.entrySet()) {
            String instrumentId = entry.getKey();
            Map<String, List<String>> sides = entry.getValue();
            
            for (Map.Entry<String, List<String>> sideEntry : sides.entrySet()) {
                String side = sideEntry.getKey();
                List<String> orderIds = sideEntry.getValue();
                
                if (orderIds.size() <= 1) {
                    continue;
                }

                logger.info("Resolving multiple active orders for " + instrumentId + "/" + side);

                // Sort orders by creation time (newest first)
                List<MarketOrder> orders = new ArrayList<>();
                for (String orderId : orderIds) {
                    MarketOrder order = orderRepository.getOrderByOrderId(orderId);
                    if (order != null) {
                        orders.add(order);
                    }
                }
                
                orders.sort((a, b) -> Long.compare(b.getCreationTimestamp(), a.getCreationTimestamp()));
                
                // Keep the newest order, cancel all others
                if (!orders.isEmpty()) {
                    MarketOrder newestOrder = orders.get(0);
                    logger.info("Keeping newest order: " + newestOrder.getOrderId());
                    
                    // Cancel all other orders
                    for (int i = 1; i < orders.size(); i++) {
                        MarketOrder orderToCancel = orders.get(i);
                        logger.info("Cancelling duplicate order: " + orderToCancel.getOrderId() + " (age: " + 
                            (System.currentTimeMillis() - orderToCancel.getCreationTimestamp()) + " ms)");

                        String marketSource = orderToCancel.getMarketSource();
                        String traderId = getTraderForVenue(marketSource);
                        String orderId = orderToCancel.getOrderId();
                        
                        if (marketSource != null && traderId != null && orderId != null) {
                            MarketOrder.orderCancel(marketSource, traderId, orderId, this);
                        }
                    }
                }
            }
        }
    } catch (Exception e) {
        logger.error("Error resolving multiple active orders: " + e.getMessage() + " " + e);
    }
}

/**
 * Checks for multiple active orders on the same side of the market
 * and logs any found.
 * 
 * @param instrumentId Optional instrument ID to check, or null to check all tracked instruments
 * @return Map of instruments with multiple active orders
 */
public Map<String, Map<String, List<String>>> checkForMultipleActiveOrders(String instrumentId) {
    try {
        Map<String, Map<String, List<String>>> multipleOrders;
        
        if (instrumentId != null) {
            // Check specific instrument
            multipleOrders = new HashMap<>();
            List<String> buyOrders = orderRepository.getActiveOrdersForSide(instrumentId, "Buy");
            List<String> sellOrders = orderRepository.getActiveOrdersForSide(instrumentId, "Sell");
            
            if (buyOrders.size() > 1 || sellOrders.size() > 1) {
                Map<String, List<String>> sidesToOrders = new HashMap<>();
                
                if (buyOrders.size() > 1) {
                    sidesToOrders.put("Buy", buyOrders);
                }
                
                if (sellOrders.size() > 1) {
                    sidesToOrders.put("Sell", sellOrders);
                }
                
                if (!sidesToOrders.isEmpty()) {
                    multipleOrders.put(instrumentId, sidesToOrders);
                }
            }
        } else {
            // Check all tracked instruments
            multipleOrders = orderRepository.detectMultipleActiveOrders();
        }
        
        // Log any issues found
        if (!multipleOrders.isEmpty()) {
            logger.warn("Detected multiple active orders on the same side for " + multipleOrders.size() + " instruments");
                
            for (Map.Entry<String, Map<String, List<String>>> entry : multipleOrders.entrySet()) {
                String instrId = entry.getKey();
                Map<String, List<String>> sides = entry.getValue();
                
                for (Map.Entry<String, List<String>> sideEntry : sides.entrySet()) {
                    String side = sideEntry.getKey();
                    List<String> orderIds = sideEntry.getValue();
                    
                    logger.warn("Instrument " + instrId + " has " + orderIds.size() + " active " + side + " orders: " + String.join(", ", orderIds));
                        
                    // Add order details for each order
                    for (String orderId : orderIds) {
                        MarketOrder order = orderRepository.getOrderByOrderId(orderId);
                        if (order != null) {
                            logger.warn("  - OrderId: " + orderId + ", ReqId: " + order.getMyReqId() + ", MarketSource: " + order.getMarketSource() + ", Age: " + (System.currentTimeMillis() - order.getCreationTimestamp()) + " ms");
                        }
                    }
                }
            }
        }
        
        return multipleOrders;
    } catch (Exception e) {
        logger.error("Error checking for multiple active orders: " + e.getMessage() + " " + e);
        return Collections.emptyMap();
    }
}

private void initializeHeartbeat() {
    heartbeatScheduler.scheduleAtFixedRate(() -> {
        try {
        	
            Map<String, Object> status = new HashMap<>();
            status.put("hostname", hostname);
            status.put("application", "MarketMaker");
            status.put("state", isSystemStopped ? "STOPPED" : "RUNNING");
            status.put("continuousTrading", continuousTradingEnabled);
            status.put("activeOrders", orderRepository.getStatistics().get("totalOrders"));

            // Add market maker status
            if (marketMaker != null) {
                status.put("marketMaker", marketMaker.isRunning() ? "RUNNING" : "STOPPED");
                status.put("marketMakerConfig", marketMaker.getConfigSummary());
            }

            // Gather trading statistics
            int instrumentCount = depthListener != null ? 
                depthListener.getInstrumentCount() : 0;
            status.put("instruments", instrumentCount);
            status.put("cached orders", orderRepository.getStatistics().get("totalOrders"));
            status.put("latest best prices", orderRepository.getLatestBestCount());
            
            publishToRedis(HEARTBEAT_CHANNEL, status);
            
        } catch (Exception e) {
            logger.warn("Error in heartbeat logging: " + e.getMessage() + " " + e);
        }
    }, 30, 30, TimeUnit.SECONDS); // Run every 30 seconds

    // Add a shutdown hook to properly close the heartbeat scheduler
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        logger.info("Shutting down heartbeat scheduler");
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
                    logger.warn("Error checking with minimum field. Using status from DepthListener: " + e2.getMessage());
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

                logger.info("Instrument pattern subscription check: subscribed=" + isInstrSubscribed);

                // If not subscribed, resubscribe using our adaptive approach
                if (!isInstrSubscribed) {
                    logger.warn("Instrument pattern not subscribed, subscribing now");

                    try {
                        // Try using the full subscription method with failover logic
                        subscribeToInstrumentPattern();
                    } catch (Exception e) {
                        logger.error("Error calling subscribeToInstrumentPattern: " + e.getMessage() + " " + e);

                        // // Last resort: try to subscribe with just the absolute minimum field
                        // try {
                        //     String[] bareMinimumFields = new String[] { "Id" };
                        //     LOGGER.warn("Trying last resort subscription with field: {}", Arrays.toString(bareMinimumFields));
                        //     pattern.subscribe(bareMinimumFields, depthListener);
                        //     LOGGER.info("Successfully subscribed with bare minimum field");
                        // } catch (Exception e2) {
                        //     LOGGER.error("All subscription attempts failed: {}", e2.getMessage(), e2);
                        // }
                    }
                }
            } else {
                // The instrument pattern doesn't exist yet
                logger.debug("Instrument pattern not found: " + instrumentPattern);
            }

            // Check depth pattern
            MkvObject depthObj = pm.getMkvObject(DEPTH_PATTERN);
            MkvPattern pattern = (MkvPattern) depthObj;
            if (depthObj != null && depthObj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                // Use a try-catch block to safely check subscription status
                boolean currentlySubscribed = false;
                try {
                    currentlySubscribed = pattern.isSubscribed(depthListener, DEPTH_FIELDS);
                } catch (Exception e) {
                    logger.warn("Error checking depth pattern subscription: " + e.getMessage());
                }

                logger.debug("Depth pattern subscription check: subscribed=" + currentlySubscribed + ", our state=" + subscribedPatterns.contains(pattern));

                // If we think we're subscribed but actually aren't, resubscribe
                if (subscribedPatterns.contains(pattern) && !currentlySubscribed) {
                    logger.warn("Depth pattern subscription lost, resubscribing");
                    pattern.subscribe(DEPTH_FIELDS, depthListener);
                }

                // Update our tracking state
                if (currentlySubscribed) {
                    subscribedPatterns.add(DEPTH_PATTERN);
                } else {
                    subscribedPatterns.remove(DEPTH_PATTERN);
                }
            } else {
                if (subscribedPatterns.contains(pattern)) {
                    logger.warn("Depth pattern not found but we thought we were subscribed");
                    subscribedPatterns.remove(pattern);
                }
                // Try to find and subscribe to the pattern again
                subscribeToPattern();
            }
            


            // Additional detailed logging
            if (depthListener != null) {
                Map<String, Object> healthStatus = depthListener.getHealthStatus();
                logger.info("Market data health status: " + healthStatus);
            }
        } catch (Exception e) {
            logger.error("Error in pattern subscription monitor: " + e.getMessage() + " " + e);
        }
    }, 15, 30, TimeUnit.SECONDS);  // Check sooner initially (15 sec), then every 30 sec
  }


    /**
     * Initiates shutdown process for the application.
     */
    public void initiateShutdown() {
        // Mark as shutting down
        isShuttingDown = true;
        
        // Trigger the full shutdown process
        shutdown();
    }

    /**
     * Checks if the application is currently shutting down.
     * 
     * @return true if the application is shutting down, false otherwise
     */
    public boolean isShuttingDown() {
        return isShuttingDown;
    }

    /**
     * Helper method to shutdown an executor service
     */
    private void shutdownExecutor(ExecutorService executor, String name) {
        if (executor == null) {
            return;
        }
        
        try {
            logger.info("Shutting down " + name + " executor");
            executor.shutdown();
            
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn(name + " executor did not terminate gracefully, forcing shutdown");
                List<Runnable> pendingTasks = executor.shutdownNow();
                logger.warn("Cancelled " + pendingTasks.size() + " pending tasks for " + name);
                
                if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                    logger.error(name + " executor did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted while shutting down " + name + " executor");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Error shutting down " + name + " executor: " + e.getMessage() + " " + e);
        }
    }

    /**
     * Updates the existing shutdown method to work with async shutdown
     */
    public void shutdown() {
        // Mark as shutting down
        isShuttingDown = true;

        logger.info("Starting application shutdown sequence");

        // If async shutdown is in progress, this is being called as part
        // of that process, so handle differently
        if (asyncShutdownInProgress) {
            logger.info("Async shutdown in progress, performing resource cleanup only");

            // Clean up executors
            shutdownExecutor(heartbeatScheduler, "Heartbeat scheduler");
            shutdownExecutor(scheduler, "Market recheck scheduler");
            shutdownExecutor(orderExpirationScheduler, "Order expiration scheduler");
            
            // Shutdown Redis connection pool
            if (jedisPool != null) {
                try {
                    logger.info("Closing Redis connection pool");
                    jedisPool.destroy(); // Use destroy() for older Jedis versions
                } catch (Exception e) {
                    logger.warn("Error closing Redis connection pool: " + e.getMessage());
                }
            }

            logger.info("Resource cleanup for async shutdown complete");
            return;
        }
        
        // Regular synchronous shutdown process:
        // First notify the market maker to prepare for shutdown 
        if (marketMaker != null) {
            try {
                logger.info("Preparing market maker for shutdown");
                marketMaker.prepareForShutdown();
            } catch (Exception e) {
                logger.warn("Error preparing market maker for shutdown: " + e.getMessage());
            }
        }
        
        // Cancel market maker orders
        try {
            logger.info("Cancelling all market maker orders");
            boolean allCancelled = cancelMarketMakerOrders(10000);  // 10 second timeout
            if (!allCancelled) {
                logger.warn("Not all market maker orders were cancelled before timeout");
            }
        } catch (Exception e) {
            logger.error("Error cancelling market maker orders: " + e.getMessage() + " " + e);
        }
        
        // Cancel all remaining orders
        try {
            logger.info("Cancelling all remaining orders");
            cancelAllOrders();
        } catch (Exception e) {
            logger.warn("Error cancelling orders during shutdown: " + e.getMessage());
        }
        
        // Complete market maker shutdown
        if (marketMaker != null) {
            try {
                logger.info("Completing market maker shutdown");
                marketMaker.completeShutdown();
            } catch (Exception e) {
                logger.warn("Error completing market maker shutdown: " + e.getMessage());
            }
        }

        // Shutdown Redis connection pool
        if (jedisPool != null) {
            try {
                logger.info("Closing Redis connection pool");
                jedisPool.destroy(); // Use destroy() for older Jedis versions
            } catch (Exception e) {
                logger.warn("Error closing Redis connection pool: " + e.getMessage());
            }
        }

        // Shutdown all executors
        shutdownExecutor(heartbeatScheduler, "Heartbeat scheduler");
        shutdownExecutor(scheduler, "Market recheck scheduler");
        shutdownExecutor(orderExpirationScheduler, "Order expiration scheduler");
        
        // Log successful shutdown
        logger.info("OrderManagement shutdown complete");
    }

    /**
     * Cancels all orders placed by the market maker component.
     * Used during shutdown or when disabling the market maker.
     * 
     * @param timeoutMs Maximum time to wait for cancellations (in milliseconds)
     * @return true if all orders were successfully cancelled, false otherwise
     */
    public boolean cancelMarketMakerOrders(long timeoutMs) {
        logger.info("Cancelling all market maker orders");
        
        if (marketMaker == null) {
            logger.info("Market maker is null, no orders to cancel");
            return true;
        }
        
        try {
            // Get all active quotes from the market maker
            Map<String, ActiveQuote> activeQuotes = marketMaker.getActiveQuotes();
            
            if (activeQuotes.isEmpty()) {
                logger.info("No active quotes found, nothing to cancel");
                return true;
            }
            
            int totalOrderCount = 0;
            Set<String> pendingCancellations = Collections.newSetFromMap(new ConcurrentHashMap<>());
            
            // Count active orders and add them to pending cancellations
            for (Map.Entry<String, ActiveQuote> entry : activeQuotes.entrySet()) {
                ActiveQuote quote = entry.getValue();
                String instrumentId = entry.getKey();
                
                // Process bid order
                MarketOrder bidOrder = quote.getBidOrder();
                if (bidOrder != null && !bidOrder.isDead() && bidOrder.getOrderId() != null) {
                    pendingCancellations.add(bidOrder.getOrderId());
                    totalOrderCount++;
                    
                    // Issue cancel request
                    String traderId = getTraderForVenue(bidOrder.getMarketSource());
                    logger.info("Cancelling bid order for " + instrumentId + ": orderId=" + bidOrder.getOrderId());

                    MarketOrder cancelOrder = MarketOrder.orderCancel(
                        bidOrder.getMarketSource(),
                        traderId,
                        bidOrder.getOrderId(),
                        this
                    );
                    
                    if (cancelOrder == null) {
                        logger.warn("Failed to create cancel request for bid order: " + bidOrder.getOrderId());
                    }
                }
                
                // Process ask order
                MarketOrder askOrder = quote.getAskOrder();
                if (askOrder != null && !askOrder.isDead() && askOrder.getOrderId() != null) {
                    pendingCancellations.add(askOrder.getOrderId());
                    totalOrderCount++;
                    
                    // Issue cancel request
                    String traderId = getTraderForVenue(askOrder.getMarketSource());
                    logger.info("Cancelling ask order for " + instrumentId + ": orderId=" + askOrder.getOrderId());

                    MarketOrder cancelOrder = MarketOrder.orderCancel(
                        askOrder.getMarketSource(),
                        traderId,
                        askOrder.getOrderId(),
                        this
                    );
                    
                    if (cancelOrder == null) {
                        logger.warn("Failed to create cancel request for ask order: " + askOrder.getOrderId());
                    }
                }
            }

            logger.info("Sent " + totalOrderCount + " cancel requests, waiting for confirmation...");

            if (totalOrderCount == 0) {
                return true; // Nothing to cancel
            }
            
            // Wait for cancellations to complete
            long startTime = System.currentTimeMillis();
            long endTime = startTime + timeoutMs;
            
            while (!pendingCancellations.isEmpty() && System.currentTimeMillis() < endTime) {
                // Check if any orders are still active
                for (Iterator<String> it = pendingCancellations.iterator(); it.hasNext();) {
                    String orderId = it.next();
                    MarketOrder order = getOrderByOrderId(orderId);
                    
                    if (order == null || order.isDead()) {
                        // Order is no longer active or no longer in our cache
                        it.remove();
                        logger.info("Confirmed cancellation for order: " + orderId);
                    }
                }
                
                // Log progress periodically
                if ((System.currentTimeMillis() - startTime) % 1000 < 50) { // Approximately every second
                    logger.info("Waiting for " + pendingCancellations.size() + " order cancellations to complete...");
                }
                
                // Brief pause to avoid CPU spinning
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while waiting for cancellations");
                    break;
                }
            }
            
            boolean allCancelled = pendingCancellations.isEmpty();
            logger.info("Market maker order cancellation completed: " + (totalOrderCount - pendingCancellations.size()) + " of " + totalOrderCount + " orders confirmed cancelled" + (allCancelled ? "" : " (TIMEOUT OCCURRED)"));

            return allCancelled;
        } catch (Exception e) {
            logger.error("Error cancelling market maker orders: " + e.getMessage() + " " + e);
            return false;
        }
    }

    /*
    * Cancels all outstanding orders currently tracked by this OrderManagement instance.
    */
    public void cancelAllOrders() {
        logger.info("Cancelling all outstanding orders");

        // Create a copy of the orders map to avoid concurrent modification
        Collection<MarketOrder> orderCollection = orderRepository.getAllOrders();
        Map<Integer, MarketOrder> ordersCopy = new HashMap<>();
        for (MarketOrder order : orderCollection) {
            ordersCopy.put(order.getMyReqId(), order);
        }
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

                // Check if the order is in a valid state for cancellation
                if (orderId == null || orderId.isEmpty()) {
                    logger.warn("Order ID is null or empty for order: " + order);
                    continue;
                }

                // Only try to cancel if we have an order ID
                if (orderId != null && !orderId.isEmpty()) {
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
   
                    }
                }
            }
            logger.info("All outstanding orders cancelled");
        }
    }
    /**
     * Updates the market maker configuration
     * 
     * @param newConfig The new configuration to apply
     */
    public void updateMarketMakerConfig(MarketMakerConfig newConfig) {
        try {
            // Store the new configuration
            this.marketMakerConfig = newConfig;
            
            // Update the market maker with the new configuration
            if (marketMaker != null) {
                marketMaker.updateConfig(newConfig);
                logger.info("Market maker configuration updated successfully");
            } else {
                logger.warn("Market maker is null, configuration stored but not applied");
            }
        } catch (Exception e) {
            logger.error("Error updating market maker configuration: " + e.getMessage() + " " + e);
            throw e; // Re-throw to be handled by the caller
        }
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