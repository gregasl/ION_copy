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
import java.util.Date;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iontrading.lowlatency.net.bytebuddy.dynamic.TypeResolutionStrategy.Active;
import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvObject;
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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * OrderManagement is the main component of the system.
 */
public class OrderManagement implements MkvPublishListener, MkvRecordListener,
    MkvChainListener, IOrderManager {
    
  // Add logger for debugging
private static final Logger LOGGER = LoggerFactory.getLogger(OrderManagement.class);

    /**
     * Ultra-minimal production logging - only critical events
     */
    public static void configureProductionLogging() {
        // Set global default to ERROR (minimal logging)
        ApplicationLogging.setGlobalLogLevel("ERROR");
        
        // Only critical business events at WARN level
        ApplicationLogging.setLogLevels("WARN",
            "com.iontrading.automatedMarketMaking.OrderManagement"
        );
        
        // Keep order tracking at WARN for business monitoring
        ApplicationLogging.setLogLevels("WARN",
            "com.iontrading.automatedMarketMaking.MarketOrder"
        );
        
        // All infrastructure components at ERROR only
        ApplicationLogging.setLogLevels("ERROR",
            "com.iontrading.automatedMarketMaking.DepthListener",
            "com.iontrading.automatedMarketMaking.AsyncLoggingManager",
            "com.iontrading.automatedMarketMaking.ApplicationLogging",
            "com.iontrading.automatedMarketMaking.MarketDef",
            "com.iontrading.automatedMarketMaking.Best",
            "com.iontrading.automatedMarketMaking.Instrument",
            "com.iontrading.automatedMarketMaking.GCBest",
            "com.iontrading.automatedMarketMaking.GCLevelResult"
        );

        System.out.println("PRODUCTION logging configured - minimal output mode");
    }

    /**
     * Development logging with reasonable verbosity
     */
    public static void configureDevelopmentLogging() {
        // Set global default to WARN
        ApplicationLogging.setGlobalLogLevel("WARN");

        ApplicationLogging.setLogLevels("DEBUG",
            "com.iontrading.automatedMarketMaking.MarketMaker",
            "com.iontrading.automatedMarketMaking.OrderManagement"
        );

        // Core business logic at INFO
        ApplicationLogging.setLogLevels("INFO",
            "com.iontrading.automatedMarketMaking.MarketOrder"
        );

        // Infrastructure at ERROR
        ApplicationLogging.setLogLevels("ERROR",
            "com.iontrading.automatedMarketMaking.BondEligibilityListener",
            "com.iontrading.automatedMarketMaking.Instrument",
            "com.iontrading.automatedMarketMaking.EligibilityChangeListener",
            "com.iontrading.automatedMarketMaking.AsyncLoggingManager",
            "com.iontrading.automatedMarketMaking.ApplicationLogging",
            "com.iontrading.automatedMarketMaking.MarketDef",
            "com.iontrading.automatedMarketMaking.Best",
            "com.iontrading.automatedMarketMaking.BondConsolidatedData",
            "com.iontrading.automatedMarketMaking.DepthListener",
            "com.iontrading.automatedMarketMaking.GCBest",
            "com.iontrading.automatedMarketMaking.GCLevelResult",
            "com.iontrading.automatedMarketMaking.IOrderManager",
            "com.iontrading.automatedMarketMaking.MarketMakerConfig"
        );
    }

    /**
     * Debug mode for specific troubleshooting
     */
    public static void configureDebugLogging(String focusComponent) {
        configureDevelopmentLogging();
        
        if (focusComponent != null && !focusComponent.isEmpty()) {
            ApplicationLogging.setLogLevel(
                "com.iontrading.automatedMarketMaking." + focusComponent,
                "DEBUG"
            );
            System.out.println("DEBUG logging enabled for " + focusComponent);
        }
    }

    public static void configureLogging(boolean isProduction, String focusComponent) {
        try {
            if (isProduction) {
                configureProductionLogging();
                LOGGER.info("Configured for PRODUCTION mode");
            } else {
                configureDevelopmentLogging();
                LOGGER.info("Configured for DEVELOPMENT mode");

                // If a focus component is specified, enable detailed debugging for it
                if (focusComponent != null && !focusComponent.isEmpty()) {
                    configureDebugLogging(focusComponent);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error configuring logging: {}", e.getMessage(), e);
        }
    }

  private volatile boolean isShuttingDown = false;

  // Instance of DepthListener to load and access instrument data
  private static DepthListener depthListener;

  // Market maker instance for automated market making
  private MarketMaker marketMaker;
  
  // Market maker configuration
  private MarketMakerConfig marketMakerConfig;

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
  private static final String INSTRUMENT_PATTERN = MarketDef.INSTRUMENT_PATTERN;
  private static final String[] INSTRUMENT_FIELDS = MarketDef.INSTRUMENT_FIELDS;
    
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
  private static final String HEARTBEAT_CHANNEL = "HEARTBEAT:ION:MARKETMAKER";
  private static final String ADMIN_CHANNEL = "ADMIN:ION:MARKETMAKER";
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

  private static final double MAX_PRICE_DEVIATION = 0.05; // 5 bps max price deviation for hedging orders

  // Add this field to the OrderManagement class
  private final Map<String, Boolean> orderActiveStatusMap = new ConcurrentHashMap<>();

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
                  LOGGER.info("Connected to Redis pool at {}:{}", REDIS_HOST, REDIS_PORT);
                  isRedisConnected = true;
              } finally {
                  // Return the resource to the pool
                  if (jedis != null) {
                      jedisPool.returnResource(jedis);
                  }
              }
          } catch (Exception e) {
              LOGGER.error("Error connecting to Redis pool", e);
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
          LOGGER.error("Redis connection lost, attempting to reconnect", jce);
          if (jedis != null) {
              jedisPool.returnBrokenResource(jedis);
              jedis = null;
          }
          try {
              initializeRedisConnection();
          } catch (Exception e) {
              LOGGER.error("Failed to reconnect to Redis", e);
          }
      } catch (Exception e) {
          LOGGER.error("Error publishing to Redis", e);
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
      // Initialize centralized logging before anything else
      try {
          System.out.println("### STARTUP: Beginning application initialization");
    	  if (!ApplicationLogging.initialize("MarketMaker")) {
    		  System.err.println("Failed to initialize logging, exiting");
    		  System.exit(1);
    	  }

    	  LOGGER.info("Starting OrderManagement");
      } catch (Exception e) {
    	  LOGGER.error("Failed to start: {}", e.getMessage(), e);
      }

    System.out.println("### DIRECT: Creating OrderManagement instance");
    // Create the order management instance
    OrderManagement om = new OrderManagement();
    System.out.println("### DIRECT: OrderManagement instance created");    
    
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
    	
    	AsyncLoggingManager manager = AsyncLoggingManager.getInstance();
    	System.out.println("### DIRECT: AsyncLoggingManager instance: " + manager);
    	System.out.println("### DIRECT: Current queue size: " + manager.getCurrentQueueSize());
    	System.out.println("### DIRECT: Messages queued: " + manager.getMessagesQueued());
    	System.out.println("### DIRECT: Messages processed: " + manager.getMessagesProcessed());
      
      // Set up the Quality of Service for the MKV API
      MkvQoS qos = new MkvQoS();
      qos.setArgs(args);
      qos.setPublishListeners(new MkvPublishListener[] { om });

      LOGGER.info("Starting MKV API");
      // Start the MKV API if it hasn't been started already
      try {
          Mkv existingMkv = Mkv.getInstance();
          if (existingMkv == null) {
              Mkv.start(qos);
              LOGGER.info("Mkv started successfully.");
          } else {
              LOGGER.info("MKV API already started");
              existingMkv.getPublishManager().addPublishListener(om);
        }
        } catch (Exception e) {
            LOGGER.warn("Failed to start Mkv", e);
        }
      // Initialize DepthListener after MKV is started
      depthListener = new DepthListener(om);
      LOGGER.info("DepthListener initialized");

      // DepthListener now handles instrument data loading internally
      // and reports status through its health monitoring methods
      LOGGER.info("Instrument data will be loaded by DepthListener automatically");

      // Initialize the market maker component
      LOGGER.info("Initializing market maker component");
      om.initializeMarketMaker();

      // Set up depth subscriptions - this will trigger the instrument data loading
      LOGGER.info("Setting up depths subscriptions");
      om.subscribeToDepths();

      LOGGER.info("Completing market maker initialization");
      om.completeMarketMakerInitialization();

    } catch (Exception e) {
      LOGGER.warn("Failed to start MKV API: {}", e.getMessage(), e);
      LOGGER.warn("Error details", e);
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
  public static String getNativeInstrumentId(String instrumentId, String sourceId, Boolean isAON) {
    if (depthListener != null) {
      return depthListener.getInstrumentFieldBySourceString(instrumentId, sourceId, isAON);
    }
    LOGGER.warn("DepthListener not initialized - cannot get native instrument ID");
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
    configureLogging(false, null);

    applicationId = "Java_Order_Manager"; 
    LOGGER.info("Application ID: {}", applicationId);
    
    initializeRedisConnection();
    
    // Add shutdown hook for Redis pool
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        if (jedisPool != null) {
            LOGGER.info("Shutting down Redis connection pool");
            jedisPool.destroy(); // Use destroy() for older Jedis versions
        }
    }));
    
    // Initialize venue to trader mapping
    initializeTraderMap();

    // Initialize Redis control channel listener
    initializeRedisControlListener();
    
    // Initialize heartbeats
    initializeHeartbeat();

    initializeSignalHandlers();

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

      LOGGER.info("Venue to trader mapping initialized with {} entries", venueToTraderMap.size());
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
            LOGGER.info("Default market maker configuration created: {}", marketMakerConfig);
            // Create the market maker instance
            marketMaker = new MarketMaker(this, marketMakerConfig);
            LOGGER.info("Market maker initialized with default configuration: {}", marketMakerConfig);

            // Add a shutdown hook for the market maker
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (marketMaker != null) {
                    LOGGER.info("Shutting down market maker");
                    marketMaker.shutdown();
                }
            }));
        } catch (Exception e) {
            LOGGER.error("Error initializing market maker", e);
        }
    }

    /**
     * Initialize signal handlers to ensure proper shutdown sequence
     */
    private void initializeSignalHandlers() {
        LOGGER.info("Initializing signal handlers for graceful shutdown");
        
        // Create a special handler for SIGINT (Ctrl+C)
        final Thread ctrlCHandler = new Thread(() -> {
            LOGGER.info("SIGINT (Ctrl+C) received, starting controlled shutdown");
            
            try {
                // Create a countdown latch to block the shutdown process
                final CountDownLatch shutdownLatch = new CountDownLatch(1);
                
                // Start the cancellation in a separate thread
                Thread cancelThread = new Thread(() -> {
                    try {
                        LOGGER.info("Cancelling all orders before shutdown");
                        
                        // First cancel market maker orders
                        if (marketMaker != null) {
                            try {
                                LOGGER.info("Cancelling market maker orders");
                                marketMaker.cancelAllOrders(10000);
                            } catch (Exception e) {
                                LOGGER.error("Error cancelling market maker orders: {}", e.getMessage());
                            }
                        }
                        
                        // Then cancel any remaining orders
                        try {
                            cancelAllOrders();
                        } catch (Exception e) {
                            LOGGER.error("Error cancelling orders: {}", e.getMessage());
                        }
                        
                        LOGGER.info("All cancellations complete, releasing shutdown latch");
                    } finally {
                        // Always release the latch to avoid deadlock
                        shutdownLatch.countDown();
                    }
                });
                
                // Make this a daemon thread so it doesn't prevent JVM exit if it hangs
                cancelThread.setDaemon(true);
                cancelThread.setName("Ctrl+C-CancellationThread");
                cancelThread.start();
                
                // Wait for cancellations to complete with a timeout
                if (!shutdownLatch.await(15, TimeUnit.SECONDS)) {
                    LOGGER.warn("Timeout waiting for order cancellations, proceeding with shutdown anyway");
                }
                
                LOGGER.info("Controlled shutdown sequence completed");
            } catch (Exception e) {
                LOGGER.error("Error during controlled shutdown: {}", e.getMessage());
            }
        });
        
        // Set the name for easier identification
        ctrlCHandler.setName("Ctrl+C-ShutdownHandler");
        
        // Register our handler to run before the JVM shutdown hooks
        Runtime.getRuntime().addShutdownHook(ctrlCHandler);
        
        LOGGER.info("Signal handlers initialized");
    }

    // Add this method to complete market maker initialization after MKV is ready
    private void completeMarketMakerInitialization() {
        if (marketMaker != null) {
            try {
                LOGGER.info("Completing market maker initialization with MKV subscriptions");
                marketMaker.initializeMkvSubscriptions();
            } catch (Exception e) {
                LOGGER.error("Error completing market maker initialization", e);
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
                LOGGER.info("Market making started");
                return true;
            }
            return false;
        } catch (Exception e) {
            LOGGER.error("Error starting market making", e);
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
                LOGGER.info("Market making stopped");
                return true;
            }
            return false;
        } catch (Exception e) {
            LOGGER.error("Error stopping market making", e);
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
	       LOGGER.warn("No trader configured for venue: {}", venue);
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
	        LOGGER.error("Cannot initialize Redis control listener - Redis is not connected");
	        return;
	    }
	    
	    LOGGER.info("Initializing Redis control channel listener on channel: {}", ADMIN_CHANNEL);
	    
	    // Start a separate thread for Redis pub/sub listening
	    Thread redisListenerThread = new Thread(() -> {
	        Jedis subscriberJedis = null;
	        try {
	            // Get a dedicated connection from the pool for pub/sub
	            subscriberJedis = jedisPool.getResource();

	            LOGGER.info("Redis control listener started on channel: {}", ADMIN_CHANNEL);
	            
	            // Create a subscriber to process messages
	            subscriberJedis.subscribe(new JedisPubSubListener(), ADMIN_CHANNEL);
	            
	        } catch (Exception e) {
	            LOGGER.error("Error in Redis control listener: {}", e.getMessage(), e);
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
	                    LOGGER.warn("Error returning Redis subscriber to pool: {}", e.getMessage());
	                }
	            }
	        }
	    }, "Redis-Control-Listener");
	    
	    // Set as daemon thread so it doesn't prevent JVM shutdown
	    redisListenerThread.setDaemon(true);
	    redisListenerThread.start();
	    
	    LOGGER.info("Redis control listener thread started");
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

	        LOGGER.info("Received control message: {} on channel: {}", message, channel);

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
                    LOGGER.warn("Unknown control command received: {}", command);
                }
	        } catch (Exception e) {
	            LOGGER.error("Error processing control message: {}", e.getMessage(), e);
	        }
	    }

	    @Override
	    public void onSubscribe(String channel, int subscribedChannels) {
	        LOGGER.info("Subscribed to Redis channel: {}", channel);
	    }

	    @Override
	    public void onUnsubscribe(String channel, int subscribedChannels) {
	        LOGGER.info("Unsubscribed from Redis channel: {}", channel);
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
            LOGGER.warn("Failed to parse control message as JSON, treating as plain command: {}", message);
            
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
            LOGGER.info("System already in STOPPED state, ignoring redundant STOP command");
            return;
        }

        LOGGER.warn("Executing STOP command - cancelling orders and preventing new submissions");

        // Set the system to stopped state
        isSystemStopped = true;

        // Disable continuous trading to prevent auto-trading
        continuousTradingEnabled = false;
        
        // Cancel all existing orders
        cancelAllOrders();
        
        // Publish status update
    //    publishSystemStatus();

        LOGGER.warn("STOP command executed - system is now STOPPED");
    }   

    /**
     * Handles the RESUME command, allowing new orders to be submitted.
     * 
     * @param controlMessage The control message containing command parameters
     */
    private void handleResumeCommand(Map<String, Object> controlMessage) {
        // Skip if not stopped
        if (!isSystemStopped) {
            LOGGER.info("System already in RUNNING state, ignoring redundant RESUME command");
            return;
        }

        LOGGER.warn("Executing RESUME command - allowing new order submissions");

        // Set the system back to running state
        isSystemStopped = false;

        // Re-enable continuous trading if requested
        Boolean enableTrading = (Boolean) controlMessage.getOrDefault("enableTrading", Boolean.TRUE);
        if (enableTrading) {
            continuousTradingEnabled = true;
            LOGGER.info("Continuous trading re-enabled as part of RESUME command");
        } else {
            LOGGER.info("Continuous trading remains disabled after RESUME command");
        }
        
        // Publish status update
    //    publishSystemStatus();

        LOGGER.warn("RESUME command executed - system is now RUNNING");
    }

        /**
     * Handles the MM_START command to start market making.
     * 
     * @param controlMessage The control message containing command parameters
     */
    private void handleMarketMakerStartCommand(Map<String, Object> controlMessage) {
        LOGGER.info("Executing MM_START command - starting market maker");
        
        boolean success = startMarketMaking();
        
        Map<String, Object> status = new HashMap<>();
        status.put("command", "MM_START");
        status.put("success", success);
        status.put("status", success ? "RUNNING" : "FAILED");
        status.put("timestamp", new Date().getTime());
        
        publishToRedis(ADMIN_CHANNEL + "_RESPONSE", status);
        
        LOGGER.info("Market maker start command executed - result: {}", success ? "SUCCESS" : "FAILED");
    }

    /**
     * Handles the MM_STOP command to stop market making.
     * 
     * @param controlMessage The control message containing command parameters
     */
    private void handleMarketMakerStopCommand(Map<String, Object> controlMessage) {
        LOGGER.info("Executing MM_STOP command - stopping market maker");
        
        boolean success = stopMarketMaking();
        
        Map<String, Object> status = new HashMap<>();
        status.put("command", "MM_STOP");
        status.put("success", success);
        status.put("status", success ? "STOPPED" : "FAILED");
        status.put("timestamp", new Date().getTime());
        
        publishToRedis(ADMIN_CHANNEL + "_RESPONSE", status);
        
        LOGGER.info("Market maker stop command executed - result: {}", success ? "SUCCESS" : "FAILED");
    }

    /**
     * Handles the MM_CONFIG command to update market maker configuration.
     * 
     * @param controlMessage The control message containing configuration parameters
     */
    private void handleMarketMakerConfigCommand(Map<String, Object> controlMessage) {
        LOGGER.info("Executing MM_CONFIG command - updating market maker configuration");
        
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
            
            LOGGER.info("Market maker configuration updated: {}", newConfig);
        } catch (Exception e) {
            LOGGER.error("Error updating market maker configuration: {}", e.getMessage(), e);
            
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
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Publication event for: {}", mkvObject.getName());
    }
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
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Publication idle for component: {}, start: {}", component, start);
    }
  }

  /**
   * Not interested in this event because our component is a pure subscriber
   * and is not supposed to receive requests for subscriptions.
   */
  public void onSubscribe(MkvObject mkvObject) {
    // No action needed - we are not a publisher
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Subscription event for: {}", mkvObject.getName());
    }
  }

  /**
   * Sets up subscriptions to depth records using a pattern-based approach.
   * Subscribes to the consolidated market data from VMO_REPO_US.
   */
  public void subscribeToDepths() {
        try {
            LOGGER.info("Setting up subscriptions to instrument patterns");

            // Also subscribe to the instrument pattern explicitly
            subscribeToInstrumentPattern();

            LOGGER.info("Subscribing to consolidated depth data using pattern: {}", DEPTH_PATTERN);

            // Subscribe to the depth pattern
            subscribeToPattern();
            
            // Subscribe to order records
            subscribeToRecord();

            // Set up a monitor to check subscription status periodically
            setupPatternSubscriptionMonitor();
            
        } catch (Exception e) {
            LOGGER.error("Error setting up depth subscriptions: {}", e.getMessage(), e);
            LOGGER.error("Error details", e);
        }
    }

private void subscribeToRecord() {
    try {
        // Get the publish manager to access patterns
        MkvPublishManager pm = Mkv.getInstance().getPublishManager();
       
        // Look up the pattern object
        MkvObject obj = pm.getMkvObject(ORDER_PATTERN);
       
        if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
            // Subscribe within a synchronized block
            synchronized (subscriptionLock) {
                // Check again inside synchronization to avoid race conditions
                if (!isPatternSubscribed) {
                    LOGGER.info("Found order pattern, subscribing: {}", ORDER_PATTERN);

                    ((MkvPattern) obj).subscribe(ORDER_FIELDS, this);

                    // Mark that we've successfully subscribed
                    isPatternSubscribed = true;

                    LOGGER.info("Successfully subscribed to order pattern");
                }
            }
        } else {
            LOGGER.warn("Order pattern not found: {}", ORDER_PATTERN);

            // Create a single shared listener instance instead of an anonymous one
            final MkvPublishListener patternListener = new MkvPublishListener() {
                @Override
                public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
                    // Only proceed if we're not already subscribed
                    if (isPatternSubscribed) {
                        return;
                    }
                    
                    // Check if this is our pattern being published
                    if (pub_unpub && mkvObject.getName().equals(ORDER_PATTERN) &&
                        mkvObject.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                        trySubscribeAndRemoveOrderListener(mkvObject, pm, this);
                    }
                }
               
                @Override
                public void onPublishIdle(String component, boolean start) {
                    // Only proceed if we're not already subscribed
                    if (isPatternSubscribed) {
                        return;
                    }
                    
                    // Try looking for the pattern again at idle time
                    MkvObject obj = pm.getMkvObject(ORDER_PATTERN);

                    if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                        trySubscribeAndRemoveOrderListener(obj, pm, this);
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
        LOGGER.error("Error subscribing to pattern: {}", e.getMessage(), e);
    }
}

// Helper method to handle subscription and listener removal safely
private void trySubscribeAndRemoveOrderListener(MkvObject mkvObject, MkvPublishManager pm, MkvPublishListener listener) {
    synchronized (subscriptionLock) {
        // Check again inside synchronization to avoid race conditions
        if (isPatternSubscribed) {
            return;
        }
        
        try {
            LOGGER.info("Pattern found, subscribing: {}", ORDER_PATTERN);

            ((MkvPattern) mkvObject).subscribe(ORDER_FIELDS, this);
            isPatternSubscribed = true;

            LOGGER.info("Successfully subscribed to pattern");

            // Remove the listener now that we've subscribed - safely outside the callback
            // but still inside synchronization
            pm.removePublishListener(listener);
        } catch (Exception e) {
            LOGGER.error("Error subscribing to pattern: {}", e.getMessage(), e);
        }
    }
  }

  /**
 * Subscribes to the instrument pattern to load instrument mapping data.
 * Uses an adaptive field subscription approach that handles missing fields.
 */
private void subscribeToInstrumentPattern() {
    try {

        LOGGER.info("Subscribing to instrument data using pattern: {}", INSTRUMENT_PATTERN);

        // Get the publish manager to access patterns
        MkvPublishManager pm = Mkv.getInstance().getPublishManager();
       
        // Look up the pattern object
        MkvObject obj = pm.getMkvObject(INSTRUMENT_PATTERN);
       
        if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
            LOGGER.info("Found instrument pattern, subscribing: {}", INSTRUMENT_PATTERN);
            
            // Initialize with the full field list from MarketDef
            List<String> fieldsList = new ArrayList<>(Arrays.asList(INSTRUMENT_FIELDS));
            boolean subscribed = false;
            
            // Keep trying with fewer fields until subscription succeeds
            while (!subscribed && !fieldsList.isEmpty()) {
                try {
                    String[] fields = fieldsList.toArray(new String[0]);
                    ((MkvPattern) obj).subscribe(fields, depthListener);
                    subscribed = true;

                    LOGGER.info("Successfully subscribed to instrument pattern with {} fields", fieldsList.size());
                } catch (Exception e) {
                    // If we have any fields left to remove
                    if (fieldsList.size() > 3) {  // Keep at least 3 essential fields
                        // Remove the last field in the list
                        String lastField = fieldsList.remove(fieldsList.size() - 1);
                        LOGGER.warn("Subscription failed with field: {}. Retrying with {} fields.", lastField, fieldsList.size());
                    } else {
                        // If we've removed too many fields, try with a minimal set
                        LOGGER.warn("Failed with reduced field set, trying minimal fields");

                        // Define minimal essential fields
                        String[] minimalFields = new String[] {
                            "Id", "IsAON"
                        };
                        
                        try {
                            ((MkvPattern) obj).subscribe(minimalFields, depthListener);
                            subscribed = true;
                            LOGGER.info("Successfully subscribed with minimal fields");
                        } catch (Exception minEx) {
                            LOGGER.error("Failed even with minimal fields: {}", minEx.getMessage(), minEx);
                            throw minEx;  // Propagate the exception if even minimal fields fail
                        }
                        break;
                    }
                }
            }
        } else {
            LOGGER.warn("Instrument pattern not found: {}", INSTRUMENT_PATTERN);
        }
    } catch (Exception e) {
        LOGGER.error("Error subscribing to instrument pattern: {}", e.getMessage(), e);
        LOGGER.error("Error details", e);
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
                    LOGGER.info("Found consolidated depth pattern, subscribing: {}", DEPTH_PATTERN);

                    ((MkvPattern) obj).subscribe(DEPTH_FIELDS, depthListener);

                    // Mark that we've successfully subscribed
                    isPatternSubscribed = true;

                    LOGGER.info("Successfully subscribed to consolidated depth pattern");
                }
            }
        } else {
            LOGGER.warn("Consolidated depth pattern not found: {}", DEPTH_PATTERN);

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
        LOGGER.error("Error subscribing to pattern: {}", e.getMessage(), e);
    }
}

// Helper method to handle subscription and listener removal safely
private void trySubscribeAndRemoveDepthListener(MkvObject mkvObject, MkvPublishManager pm, MkvPublishListener listener) {
    synchronized (subscriptionLock) {
        // Check again inside synchronization to avoid race conditions
        if (isPatternSubscribed) {
            return;
        }
        
        try {
            LOGGER.info("Pattern found, subscribing: {}", DEPTH_PATTERN);

            ((MkvPattern) mkvObject).subscribe(DEPTH_FIELDS, depthListener);
            isPatternSubscribed = true;

            LOGGER.info("Successfully subscribed to pattern");

            // Remove the listener now that we've subscribed - safely outside the callback
            // but still inside synchronization
            pm.removePublishListener(listener);
        } catch (Exception e) {
            LOGGER.error("Error subscribing to pattern: {}", e.getMessage(), e);
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
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Adding order to cache: reqId={}", order.getMyReqId());
    }
    orders.put(Integer.valueOf(order.getMyReqId()), order);
  }

  /**
   * Removes a MarketOrder object from the cache by request ID.
   * Called when an order is considered "dead".
   * 
   * @param reqId The request ID of the order to remove
   */
  public void removeOrder(int reqId) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Removing order from cache: reqId={}", reqId);
    }
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
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Retrieving order from cache: reqId={}, found={}", reqId, (order != null));
    }
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
            LOGGER.warn("Cannot find order with null or empty orderId");
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
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Partial update for record: {}", mkvRecord.getName());
    }
  }

    public int timeToInt(LocalTime t) {
        return t.getHour() * 10000 + t.getMinute() * 100 + t.getSecond();
    }

    /**
     * Process full updates for CM_ORDER records.
     * Detects order fills and triggers hedge trades when appropriate.
     */
    @Override
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnap) {
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
            Boolean previouslyActive = orderActiveStatusMap.get(activityKey);
            boolean currentlyActive = "Yes".equals(active);

            // Store current status for future reference
            orderActiveStatusMap.put(activityKey, currentlyActive);

            // Update ActiveQuote status if this is a market maker order
            if ("automatedMarketMaking".equals(appName) && orderId != null) {
                if (marketMaker != null) {
                    // Find the ActiveQuote that contains this order and update its status
                    if (previouslyActive != null && previouslyActive != currentlyActive) {
                        updateActiveQuoteStatus(Id, VerbStr, currentlyActive);
                        
                        LOGGER.info("Order status changed for {}/{}: active={} -> {}", 
                            Id, VerbStr, previouslyActive, currentlyActive);
                    }
                }
            }

            // Process only if transitioning from active to inactive
            if ((previouslyActive == null || previouslyActive) && !currentlyActive) {
                LOGGER.info("Order status change detected for origId={}: ActiveStr changed from Yes to No", origId);
            } else {
                // Not a transition from active to inactive, skip processing
                if (previouslyActive == null) {
                    LOGGER.debug("First time seeing order origId={}, active status={}", origId, active);
                } else if (!previouslyActive && !currentlyActive) {
                    LOGGER.debug("Order origId={} remains inactive, skipping", origId);
                } else if (previouslyActive && currentlyActive) {
                    LOGGER.debug("Order origId={} remains active, skipping", origId);
                } else if (!previouslyActive && currentlyActive) {
                    LOGGER.debug("Order origId={} transitioned from inactive to active, skipping", origId);
                }
                return;
            }

            LOGGER.info("Processing full update for record: {}, active={}, appName={}, src={}, Id={}, orderId={}, origId={}, VerbStr={}, tradeStatus={}, qtyFill={}, price={}, intQtyGoal={}, qtyHit={}, qtyStatus={}, qtyStatusStr={}, qtyTot={}, time={}", 
                mkvRecord.getName(), active, appName, src, Id, orderId, origId, VerbStr, tradeStatus, qtyFill, price, intQtyGoal, qtyHit, qtyStatus, qtyStatusStr, qtyTot, time);

            if (!"automatedMarketMaking".equals(appName) || !"FENICS_USREPO".equals(src) || active.equals("Yes")) {
                return; // Not a market maker order nor a FENICS_USREPO order, or already active
            }

            LOGGER.debug("Processing order update: origId={}, orderId={}, status={}, fill={}", 
                origId, orderId, tradeStatus, qtyFill);


            double orderPrice = 0.0;
            String market = null;
            boolean validVenue = false;
            // Get the best price information for this instrument
            Best bestMarket = latestBestByInstrument.get(Id);
            if (bestMarket == null) {
                LOGGER.warn("No best market found for instrument: {}", Id);
                return; // Cannot process without best market
            } else {
                if (VerbStr.equals("Buy")) {
                    orderPrice = bestMarket.getBid();
                    market = bestMarket.getBidSrc();
                } else if (VerbStr.equals("Sell")) {
                    orderPrice = bestMarket.getAsk();
                    market = bestMarket.getAskSrc();
                } else {
                    LOGGER.warn("Unknown verb for order: {}", VerbStr);
                    return; // Cannot process unknown verb
                }
            }
            
            validVenue = marketMaker.isTargetVenue(market);
            if (!validVenue) {
                LOGGER.warn("Invalid market venue for hedging: {}", market);
                return; // Cannot hedge on invalid venue
            }
            // Now we use the market variable to get the trader
            String trader = getTraderForVenue(market);
            String hedgeDirection = null;
            if (VerbStr.equals("Buy")) {
                hedgeDirection = "Sell"; // Hedge buy with sell
            } else if (VerbStr.equals("Sell")) {
                hedgeDirection = "Buy"; // Hedge sell with buy
            } else {
                LOGGER.warn("Invalid verb for hedging: {}", VerbStr);
                return; // Cannot hedge unknown verb
            }

            // Check if execution price is approximately near the best price
            if (Math.abs(orderPrice - price) > MAX_PRICE_DEVIATION) {
                LOGGER.warn("Order price deviation too high: orderPrice={}, execPrice={}, maxDeviation={}",
                    orderPrice, price, MAX_PRICE_DEVIATION);
                return; // Reject hedge if price is too far off
            }

            String nativeId = getNativeInstrumentId(Id, market, false);

            if (!(trader == null) && orderPrice > 0.0 && qtyHit > 0.0) {
                LOGGER.info("Preparing hedging order before self match: market={}, trader={}, Id={}, nativeId={}, verb={}, qtyFilled={}, price={}",
                    market, trader, Id, nativeId, hedgeDirection, qtyHit, orderPrice);
                if (hedgeDirection.equals("Buy")) {
                    if (bestMarket.isMinePrice(bestMarket.getAskStatus())) {
                        LOGGER.info("Self match prevention on the ask side: nativeId={}, qtyHit={}", nativeId, qtyHit);
                        return;
                    }
                    LOGGER.info("Hedging offer lifted with buy order: nativeId={}, qtyHit={}", nativeId, qtyHit);                    
                } else {
                    if (bestMarket.isMinePrice(bestMarket.getBidStatus())) {
                        LOGGER.info("Self match prevention on the bid side: nativeId={}, qtyHit={}", nativeId, qtyHit);
                        return;
                    }
                    LOGGER.info("Hedging bid hit with sell order: nativeId={}, qtyHit={}", nativeId, qtyHit);
                }
                
                addOrder(market, trader, nativeId, hedgeDirection, qtyHit, orderPrice, "Limit", "FAS");
            } else {
                LOGGER.warn("No trader configured for market: {}", market);
                return; // Cannot process without trader+
            }

            // // Handle order completion/cancellation
            // if (!"Active".equals(tradeStatus)) {
            //     orderDead(order);
            // }
        } catch (Exception e) {
            LOGGER.error("Error processing order update: {}", e.getMessage(), e);
        }
    }

    /**
     * Updates the ActiveQuote status for a specific order
     * 
     * @param orderId The order ID
     * @param side The order side (Buy or Sell)
     * @param isActive Whether the order is active
     */
    private void updateActiveQuoteStatus(String Id, String side, boolean isActive) {
        if (marketMaker == null) {
            return;
        }
        
        try {
            // Ask the market maker to update the appropriate ActiveQuote
            marketMaker.updateOrderStatus(Id, side, isActive);
        } catch (Exception e) {
            LOGGER.error("Error updating ActiveQuote status for order {}/{}: {}", 
                Id, side, e.getMessage(), e);
        }
    }

  /**
   * Notification that an order is no longer able to trade.
   * Removes the order from the cache.
   * 
   * Implementation of IOrderManager.orderDead()
   */
  public void orderDead(MarketOrder order) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Order is dead: orderId={}, reqId={}", order.getOrderId(), order.getMyReqId());
    }
    // Remove the order from the cache
    removeOrder(order.getMyReqId());
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
    if (isSystemStopped) {
        LOGGER.error("Order rejected - system is in STOPPED state: source={}, trader={}, instrId={}",
            MarketSource, TraderId, instrId);

        // Log the rejection
        ApplicationLogging.logOrderEvent(
            "REJECTED_STOPPED", 
            MarketSource, 
            TraderId, 
            instrId, 
            verb, 
            qty, 
            price, 
            type, 
            tif, 
            -1,  // No request ID assigned
            null // No order ID assigned
        );
        
        return null;
    }

	LOGGER.info("Attempting to create order: source={}, trader={}, instrId={}, verb={}, qty={}, price={}, type={}, tif={}",
	             MarketSource, TraderId, instrId, verb, qty, price, type, tif);

    // Log to machine-readable format BEFORE sending the order
    ApplicationLogging.logOrderEvent(
        "SEND", 
        MarketSource, 
        TraderId, 
        instrId, 
        verb, 
        qty, 
        price, 
        type, 
        tif, 
        -1,  // Request ID not assigned yet
        null // Order ID not assigned yet
    );
	
	// Create the order using the static factory method
    MarketOrder order = MarketOrder.orderCreate(MarketSource, TraderId, instrId, verb, qty, price,
        type, tif, this);
    
    if (order != null) {
      // If creation succeeded, add the order to the cache
      addOrder(order);
      
      // Update machine-readable log with the assigned request ID
      ApplicationLogging.logOrderEvent(
          "SENT", 
          MarketSource, 
          TraderId, 
          instrId, 
          verb, 
          qty, 
          price, 
          type, 
          tif, 
          order.getMyReqId(),
          order.getOrderId()
      );
      
      // Check if there was an error code returned that indicates rejection
      if (order.getErrCode() != 0) {
        LOGGER.warn("Order rejected by market: reqId={}, instrId={}, errCode={}, errStr={}, source={}, trader={}",
            order.getMyReqId(), instrId, order.getErrCode(), order.getErrStr(), MarketSource, TraderId);
        // Log rejection to machine-readable format
        ApplicationLogging.logOrderUpdate(
            "REJECTED",
            order.getMyReqId(),
            order.getOrderId(),
            "Error: " + order.getErrCode() + " - " + order.getErrStr()
        );
        
        // Remove the rejected order from the cache since it won't be processed
        removeOrder(order.getMyReqId());
        return null; // Return null for rejected orders to indicate failure
      }

      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Order created successfully: reqId={}, instrId={}", order.getMyReqId(), instrId);
      }
    } else {
        // Log a more detailed error when order creation fails completely
        LOGGER.error("Order creation failed: instrId={}, source={}, trader={}, verb={}, qty={}, price={}, type={}, tif={}",
            instrId, MarketSource, TraderId, verb, qty, price, type, tif);

        // Log failure to machine-readable format
        ApplicationLogging.logOrderEvent(
            "FAILED", 
            MarketSource, 
            TraderId, 
            instrId, 
            verb, 
            qty, 
            price, 
            type, 
            tif, 
            -1,  // No request ID assigned
            null // No order ID assigned
        );
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
                LOGGER.warn("Received invalid best object: {}", best);
                return; // Cannot process invalid best
            }
            LOGGER.debug("OrderManagement.best() called: instrument={}, instrumentId={}, ask=({}), askSrc=({}), bid=({}), bidSrc=({}), cash_gc=({}), reg_gc=({})", 
                best.getId(), 
                best.getInstrumentId(), 
                best.getAsk(), best.getAskSrc(), 
                best.getBid(), best.getBidSrc(),
                cash_gc, reg_gc);

            latestBestByInstrument.put(best.getId(), best);

            // Call marketMaker.best() if it's available and initialized
            if (marketMaker != null) {
                try {

                    LOGGER.info("Forwarding best price to market maker: instrument={}, instrumentId={}, ask=({}), bid=({})",
                        best.getId(), best.getInstrumentId(), best.getAsk(), best.getBid());
                    // Pass the best information to the market maker
                    // marketMaker.best(best, currentCashGC, currentRegGC, gcBestCashCopy, gcBestREGCopy);
                    marketMaker.best(best, 
                                    GCBestManager.getInstance().getCashGCRate(), 
                                    GCBestManager.getInstance().getRegGCRate(),
                                    GCBestManager.getInstance().getCashGCBest(),
                                    GCBestManager.getInstance().getRegGCBest());
                } catch (Exception e) {
                    LOGGER.error("Error forwarding best price to market maker: {}", e.getMessage(), e);
                }
            }

        } catch (Exception e) {
                LOGGER.error("Error validating best object: {}", e.getMessage(), e);
                return; // Cannot process if validation fails
        }
    }
    
    public Map<String, Best> getLatestBestByInstrument() {
        return latestBestByInstrument;
    }
  /**
   * Handles changes to the order chain.
   * For new records, sets up subscriptions to receive updates.
   */
  public void onSupply(MkvChain chain, String record, int pos,
      MkvChainAction action) {

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

            // Add market maker status
            if (marketMaker != null) {
                status.put("marketMaker", marketMaker.isRunning() ? "RUNNING" : "STOPPED");
                status.put("marketMakerConfig", marketMaker.getConfigSummary());
            }

            // Gather trading statistics
            int instrumentCount = depthListener != null ? 
                depthListener.getInstrumentCount() : 0;
            status.put("instruments", instrumentCount);
            status.put("cached orders", orders.size());
            status.put("latest best prices", latestBestByInstrument.size());
            
            publishToRedis(HEARTBEAT_CHANNEL, status);
            
        } catch (Exception e) {
            LOGGER.warn("Error in heartbeat logging: {}", e.getMessage(), e);
        }
    }, 30, 30, TimeUnit.SECONDS); // Run every 30 seconds

    // Add a shutdown hook to properly close the heartbeat scheduler
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        LOGGER.info("Shutting down heartbeat scheduler");
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
                        LOGGER.debug("Retrying with absolute minimum field: {}", Arrays.toString(bareMinimumFields));
                        isInstrSubscribed = pattern.isSubscribed(depthListener, bareMinimumFields);
                    } catch (Exception e2) {
                        LOGGER.warn("Error checking with minimum field. Using status from DepthListener: {}", e2.getMessage());
                        
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

                LOGGER.info("Instrument pattern subscription check: subscribed={}", isInstrSubscribed);

                // If not subscribed, resubscribe using our adaptive approach
                if (!isInstrSubscribed) {
                    LOGGER.warn("Instrument pattern not subscribed, subscribing now");

                    try {
                        // Try using the full subscription method with failover logic
                        subscribeToInstrumentPattern();
                    } catch (Exception e) {
                        LOGGER.error("Error calling subscribeToInstrumentPattern: {}", e.getMessage(), e);

                        // Last resort: try to subscribe with just the absolute minimum field
                        try {
                            String[] bareMinimumFields = new String[] { "Id" };
                            LOGGER.warn("Trying last resort subscription with field: {}", Arrays.toString(bareMinimumFields));
                            pattern.subscribe(bareMinimumFields, depthListener);
                            LOGGER.info("Successfully subscribed with bare minimum field");
                        } catch (Exception e2) {
                            LOGGER.error("All subscription attempts failed: {}", e2.getMessage(), e2);
                        }
                    }
                }
            } else {
                // The instrument pattern doesn't exist yet
                LOGGER.debug("Instrument pattern not found: {}", instrumentPattern);
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
                    LOGGER.warn("Error checking depth pattern subscription: {}", e.getMessage());
                }

                LOGGER.debug("Depth pattern subscription check: subscribed={}, our state={}",
                    currentlySubscribed, isPatternSubscribed);

                // If we think we're subscribed but actually aren't, resubscribe
                if (isPatternSubscribed && !currentlySubscribed) {
                    LOGGER.warn("Depth pattern subscription lost, resubscribing");
                    pattern.subscribe(DEPTH_FIELDS, depthListener);
                }

                // Update our tracking state
                isPatternSubscribed = currentlySubscribed;
            } else {
                if (isPatternSubscribed) {
                    LOGGER.warn("Depth pattern not found but we thought we were subscribed");
                    isPatternSubscribed = false;
                }
                
                // Try to find and subscribe to the pattern again
                subscribeToPattern();
            }
            
            // Additional detailed logging
            if (depthListener != null) {
                Map<String, Object> healthStatus = depthListener.getHealthStatus();
                LOGGER.info("Market data health status: {}", healthStatus);
            }
        } catch (Exception e) {
            LOGGER.error("Error in pattern subscription monitor: {}", e.getMessage(), e);
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
        LOGGER.info("Checking for expired orders - currently tracking {} orders", ordersCopy.size());

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
                    LOGGER.warn("Order ID is null or empty for order: {}", order);
                    continue;
                }

                // Only try to cancel if we have an order ID
                if (orderId != null && !orderId.isEmpty()) {
                    String traderId = getTraderForVenue(marketSource);

                    LOGGER.info("Auto-cancelling expired order: reqId={}, orderId={}, age={} seconds",
                        order.getMyReqId(), orderId, (order.getAgeMillis() / 1000));

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
                        
                        // Log to machine-readable format
                        ApplicationLogging.logOrderUpdate(
                            "AUTO_CANCEL", 
                            order.getMyReqId(),
                            order.getOrderId(),
                            "Order expired after " + (order.getAgeMillis() / 1000) + " seconds"
                        );
                    }
                }
            }
        }
        
        if (expiredCount > 0) {
            LOGGER.info("Auto-cancelled {} expired orders", expiredCount);
        }
    } catch (Exception e) {
        LOGGER.error("Error checking for expired orders: {}", e.getMessage(), e);
    }
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
        LOGGER.info("Shutting down {} executor", name);
        executor.shutdown();
        
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.warn("{} executor did not terminate gracefully, forcing shutdown", name);
            List<Runnable> pendingTasks = executor.shutdownNow();
            LOGGER.warn("Cancelled {} pending tasks for {}", pendingTasks.size(), name);
            
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                LOGGER.error("{} executor did not terminate after forced shutdown", name);
            }
        }
    } catch (InterruptedException e) {
        LOGGER.warn("Interrupted while shutting down {} executor", name);
        executor.shutdownNow();
        Thread.currentThread().interrupt();
    } catch (Exception e) {
        LOGGER.error("Error shutting down {} executor: {}", name, e.getMessage(), e);
    }
}

    /**
     * Performs a complete shutdown of the application with explicit MKV lifecycle control.
     * Uses Thread.join() to ensure order cancellations complete before MKV shutdown.
     */
    public void shutdown() {
        // Mark as shutting down
        isShuttingDown = true;
        
        LOGGER.info("Starting application shutdown sequence");
        
        // First notify the market maker to prepare for shutdown 
        // (disable but don't cancel orders yet)
        if (marketMaker != null) {
            try {
                LOGGER.info("Preparing market maker for shutdown");
                marketMaker.prepareForShutdown();
            } catch (Exception e) {
                LOGGER.warn("Error preparing market maker for shutdown: {}", e.getMessage());
            }
        }
        
        // Cancel market maker orders directly through OrderManagement
        try {
            LOGGER.info("Cancelling all market maker orders");
            boolean allCancelled = cancelMarketMakerOrders(10000);  // 10 second timeout
            if (!allCancelled) {
                LOGGER.warn("Not all market maker orders were cancelled before timeout");
            }
        } catch (Exception e) {
            LOGGER.error("Error cancelling market maker orders: {}", e.getMessage(), e);
        }
        
        // Now cancel all remaining orders (non-market maker orders)
        try {
            LOGGER.info("Cancelling all remaining orders");
            cancelAllOrders();
        } catch (Exception e) {
            LOGGER.warn("Error cancelling orders during shutdown: {}", e.getMessage());
        }
        
        // Complete market maker shutdown (cleanup without MKV operations)
        if (marketMaker != null) {
            try {
                LOGGER.info("Completing market maker shutdown");
                marketMaker.completeShutdown();
            } catch (Exception e) {
                LOGGER.warn("Error completing market maker shutdown: {}", e.getMessage());
            }
        }

        // Shutdown Redis connection pool
        if (jedisPool != null) {
            try {
                LOGGER.info("Closing Redis connection pool");
                jedisPool.destroy(); // Use destroy() for older Jedis versions
            } catch (Exception e) {
                LOGGER.warn("Error closing Redis connection pool: {}", e.getMessage());
            }
        }

        // Shutdown all executors
        shutdownExecutor(heartbeatScheduler, "Heartbeat scheduler");
        shutdownExecutor(scheduler, "Market recheck scheduler");
        shutdownExecutor(orderExpirationScheduler, "Order expiration scheduler");
        
        // Log successful shutdown
        LOGGER.info("OrderManagement shutdown complete");
    }

    /**
     * Cancels all orders placed by the market maker component.
     * Used during shutdown or when disabling the market maker.
     * 
     * @param timeoutMs Maximum time to wait for cancellations (in milliseconds)
     * @return true if all orders were successfully cancelled, false otherwise
     */
    public boolean cancelMarketMakerOrders(long timeoutMs) {
        LOGGER.info("Cancelling all market maker orders");
        
        if (marketMaker == null) {
            LOGGER.info("Market maker is null, no orders to cancel");
            return true;
        }
        
        try {
            // Get all active quotes from the market maker
            Map<String, ActiveQuote> activeQuotes = marketMaker.getActiveQuotes();
            
            if (activeQuotes.isEmpty()) {
                LOGGER.info("No active quotes found, nothing to cancel");
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
                    LOGGER.info("Cancelling bid order for {}: orderId={}", instrumentId, bidOrder.getOrderId());
                    
                    MarketOrder cancelOrder = MarketOrder.orderCancel(
                        bidOrder.getMarketSource(),
                        traderId,
                        bidOrder.getOrderId(),
                        this
                    );
                    
                    if (cancelOrder == null) {
                        LOGGER.warn("Failed to create cancel request for bid order: {}", bidOrder.getOrderId());
                    }
                }
                
                // Process ask order
                MarketOrder askOrder = quote.getAskOrder();
                if (askOrder != null && !askOrder.isDead() && askOrder.getOrderId() != null) {
                    pendingCancellations.add(askOrder.getOrderId());
                    totalOrderCount++;
                    
                    // Issue cancel request
                    String traderId = getTraderForVenue(askOrder.getMarketSource());
                    LOGGER.info("Cancelling ask order for {}: orderId={}", instrumentId, askOrder.getOrderId());
                    
                    MarketOrder cancelOrder = MarketOrder.orderCancel(
                        askOrder.getMarketSource(),
                        traderId,
                        askOrder.getOrderId(),
                        this
                    );
                    
                    if (cancelOrder == null) {
                        LOGGER.warn("Failed to create cancel request for ask order: {}", askOrder.getOrderId());
                    }
                }
            }
            
            LOGGER.info("Sent {} cancel requests, waiting for confirmation...", totalOrderCount);
            
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
                        LOGGER.info("Confirmed cancellation for order: {}", orderId);
                    }
                }
                
                // Log progress periodically
                if ((System.currentTimeMillis() - startTime) % 1000 < 50) { // Approximately every second
                    LOGGER.info("Waiting for {} order cancellations to complete...", pendingCancellations.size());
                }
                
                // Brief pause to avoid CPU spinning
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Interrupted while waiting for cancellations");
                    break;
                }
            }
            
            boolean allCancelled = pendingCancellations.isEmpty();
            LOGGER.info("Market maker order cancellation completed: {} of {} orders confirmed cancelled{}",
                totalOrderCount - pendingCancellations.size(), 
                totalOrderCount,
                allCancelled ? "" : " (TIMEOUT OCCURRED)");
            
            return allCancelled;
        } catch (Exception e) {
            LOGGER.error("Error cancelling market maker orders: {}", e.getMessage(), e);
            return false;
        }
    }

/**
 * Cancels all outstanding orders currently tracked by this OrderManagement instance.
 */
public void cancelAllOrders() {
    LOGGER.info("Cancelling all outstanding orders");

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
            LOGGER.warn("Order ID is null or empty for order: {}", order);
            continue;
        }

        // Only try to cancel if we have an order ID
        if (orderId != null && !orderId.isEmpty()) {
            String traderId = getTraderForVenue(marketSource);

            LOGGER.info("Cancelling order: reqId={}, orderId={}", order.getMyReqId(), orderId);
            
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
                ApplicationLogging.logOrderUpdate(
                    "AUTO_CANCEL", 
                    order.getMyReqId(),
                    order.getOrderId(),
                    "Order cancelled by system shutdown"
                );
            }
        }
    }
    LOGGER.info("All outstanding orders cancelled");
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
                LOGGER.info("Market maker configuration updated successfully");
            } else {
                LOGGER.warn("Market maker is null, configuration stored but not applied");
            }
        } catch (Exception e) {
            LOGGER.error("Error updating market maker configuration: {}", e.getMessage(), e);
            throw e; // Re-throw to be handled by the caller
        }
    }
}