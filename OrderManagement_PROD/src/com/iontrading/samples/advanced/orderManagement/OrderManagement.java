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
import java.util.Date;
import java.util.HashSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.databind.ObjectMapper;
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
  private static final Logger LOGGER = Logger.getLogger(OrderManagement.class.getName());

  private volatile boolean isShuttingDown = false;

  // Instance of InstrumentSubscriber to load and access instrument data
  private static InstrumentSubscriber instrumentSubscriber;

  //Map to track which instrument pairs we've already traded
  private final Map<String, Long> lastTradeTimeByInstrument = new ConcurrentHashMap<>();
  //Minimum time between trades for the same instrument (milliseconds)
  private static final long MIN_TRADE_INTERVAL_MS = 200; // 200 milliseconds
  //Flag to enable or disable continuous trading
  private boolean continuousTradingEnabled = true;
  //Timer for periodic market rechecks
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  
  private final Map<String, String> venueToTraderMap = new HashMap<>();
  Set<String> validVenues = new HashSet<>(Arrays.asList("DEALERWEB_REPO", "BTEC_REPO_US", "FENICS_USREPO"));
  //Set<String> validVenues = new HashSet<>(Arrays.asList("DEALERWEB_REPO", "BTEC_REPO_US"));
  
  private DepthListener depthListener;
  private boolean isPatternSubscribed = false;
  private final Object subscriptionLock = new Object();

  private VenueTraderListener venueTraderListener = null;
  private final Object venueTraderLock = new Object();
  
  private final ScheduledExecutorService heartbeatScheduler = Executors.newScheduledThreadPool(1);

  private final ScheduledExecutorService shutdownScheduler = Executors.newScheduledThreadPool(1);
  
  private final ScheduledExecutorService orderExpirationScheduler = Executors.newScheduledThreadPool(1);
  
  // Timeout for instrument data loading (in seconds)
  private static final int INSTRUMENT_DATA_TIMEOUT = 30;
  private static final String CM_SOURCE = "VMO_REPO_US";
  private static final String PATTERN = "USD.CM_DEPTH." + CM_SOURCE + ".";

  private static final String HEARTBEAT_CHANNEL = "HEARTBEAT:ION:ORDERMANAGEMENT";
  private static final String ADMIN_CHANNEL = "ADMIN:ION:ORDERMANAGEMENT";
  private static final String REDIS_HOST = "cacheprod";
  private static final int REDIS_PORT = 6379;
  private static final int TIMEOUT = 5000;
  private static final int EXPIRY_SECONDS = 86400; // 24 hours
  public final String hostname = System.getenv("COMPUTERNAME");
    
  //Flag to track if the system is in stopped state
  private volatile boolean isSystemStopped = false;
  
  /**
   * Time to live in milliseconds before automatic cancellation
   * Public to allow access from OrderManagement
   */
  public static final long ORDER_TTL_MS = 60000; // 1.0 minute until 7:15 am
  public static final long SHORT_ORDER_TTL_MS = 15000; // 15 seconds thereafter
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Redis connection
  private Jedis jedis;
  private boolean isRedisConnected = false;
  
  private final Object gcDataLock = new Object();
  private volatile double latestCashGC = 0.0;
  private volatile double latestRegGC = 0.0;
  private volatile GCBest sharedGCBestCash = null;
  private volatile GCBest sharedGCBestREG = null;
  private final Map<String, Best> latestBestByInstrument = new ConcurrentHashMap<>();

  // Initialize Redis connection
  private void initializeRedisConnection() {
      if (!isRedisConnected) {
          try {
              jedis = new Jedis(REDIS_HOST, REDIS_PORT, TIMEOUT);
              LOGGER.info("Connected to Redis at " + REDIS_HOST + ":" + REDIS_PORT);
              isRedisConnected = true;
          } catch (Exception e) {
              LOGGER.log(Level.SEVERE, "Error connecting to Redis", e);
              isRedisConnected = false;
          }
      }
  }
  
  // Publish method with JSON support
  public void publishToRedis(String key, Map<String, Object> data) {
      try {
          // Create a comprehensive payload
          Map<String, Object> payload = new HashMap<>();
          DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
          String formattedDate = LocalDateTime.now().format(formatter);
          payload.put("timestamp", formattedDate);
//          payload.put("message", data);
          payload.putAll(data);

          // Convert payload to JSON string
          String jsonPayload = OBJECT_MAPPER.writeValueAsString(payload);

          // Publish to Redis
//          jedis.setex(key, EXPIRY_SECONDS, jsonPayload);
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
  
  /**
   * Main method to start the application.
   * Initializes the MKV API and sets up subscriptions.
   */
  public static void main(String[] args) {
      // Initialize centralized logging before anything else
      try {
          System.out.println("### STARTUP: Beginning application initialization");
          String dateStr = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    	  if (!ApplicationLogging.initialize("orderManagement_" + dateStr)) {
    		  System.err.println("Failed to initialize logging, exiting");
    		  System.exit(1);
    	  }

//    	    Examples setting global logging levels, choices are:
//    	  	FINEST
//    	  	FINER
//    	  	FINE
//    	  	CONFIG
//    	  	INFO
//    	  	WARNING
//    	  	SEVERE
//          // Set specific logger to DEBUG
//          ApplicationLogging.setLogLevel("com.iontrading.samples.advanced.orderManagement.InstrumentSubscriber", Level.FINE);
//          
//          // Set multiple loggers to a specific level
//          ApplicationLogging.setLogLevels(Level.INFO, 
//              "com.iontrading.samples.advanced.orderManagement.DepthListener",
//              "com.iontrading.samples.advanced.orderManagement.MarketOrder"
//          );
//          
    	    boolean isProduction = true;
    	  
    	    if (isProduction) {
    	        configureProductionLogging();
    	        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Starting in PRODUCTION mode");
    	    } else {
    	        configureDevelopmentLogging();
    	        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Starting in DEVELOPMENT mode");
    	    }
    	  
    	  ApplicationLogging.logAsync(LOGGER, Level.INFO, "Starting OrderManagement");
      } catch (Exception e) {
    	  ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Failed to start: " + e.getMessage(), e);
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
    	
      // First, initialize the InstrumentSubscriber and wait for data to load
      instrumentSubscriber = new InstrumentSubscriber(args);
      
      // Set up a force data loaded call after timeout
      Thread timeoutThread = new Thread(() -> {
          try {
              Thread.sleep(INSTRUMENT_DATA_TIMEOUT * 1000);
              if (!instrumentSubscriber.isDataLoaded()) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, "Initializing InstrumentSubscriber");
                  instrumentSubscriber.forceDataLoaded();
              }
          } catch (InterruptedException e) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, "Timeout thread interrupted");  
          }
      });
      timeoutThread.setDaemon(true);
      timeoutThread.start();
      
      // Register a callback for when data is loaded
      instrumentSubscriber.registerDataLoadedCallback(() -> {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Instrument data loaded successfully");  
          // No need to make these calls here since they'll be triggered by onPublishIdle events
      });
      
      // Set up the Quality of Service for the MKV API
      MkvQoS qos = new MkvQoS();
      qos.setArgs(args);
      qos.setPublishListeners(new MkvPublishListener[] { om });

      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Starting MKV API");
      // Start the MKV API if it hasn't been started already
      Mkv existingMkv = Mkv.getInstance();
      if (existingMkv == null) {
          Mkv.start(qos);
      } else {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "MKV API already started");
          existingMkv.getPublishManager().addPublishListener(om);
      }

      // Wait for instrument data to be loaded, but with a timeout
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Waiting for instrument data to be loaded...");
      boolean dataLoaded = false;
      try {
          for (int i = 0; i < INSTRUMENT_DATA_TIMEOUT; i++) {
              if (instrumentSubscriber.isDataLoaded()) {
                  dataLoaded = true;
                  ApplicationLogging.logAsync(LOGGER, Level.INFO, "Instrument data loaded successfully");
                  break;
              }
              Thread.sleep(1000); // Check once per second
          }
          
          if (!dataLoaded) {
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, "Timeout waiting for instrument data");  
          }
      } catch (InterruptedException e) {
        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Interrupted while waiting for instrument data: " + e.getMessage());  
          Thread.currentThread().interrupt();
      }
     
      om.initializeVenueTraderListener();
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "VenueTraderListener initialization requested");
      
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Setting up depths subscriptions");
      om.subscribeToDepths();
    } catch (MkvException e) {
      ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Failed to start MKV API: " + e.getMessage(), e);
      e.printStackTrace();
    }
  }
    
  /**
   * Gets the native instrument ID for a source.
   * This delegates to the InstrumentSubscriber.
   * 
   * @param instrumentId The instrument identifier
   * @param sourceId The source identifier
   * @return The native instrument ID or null if not found
   */
  public static String getNativeInstrumentId(String instrumentId, String sourceId, Boolean isAON) {
    if (instrumentSubscriber != null) {
      return instrumentSubscriber.getInstrumentFieldBySourceString(instrumentId, sourceId, isAON);
    }
    ApplicationLogging.logAsync(LOGGER, Level.WARNING, "InstrumentSubscriber not initialized - cannot get native instrument ID");
    return null;
  }
  
  /**
   * Map to cache market orders by their request ID.
   * This allows tracking orders throughout their lifecycle.
   */
    private final Map<Integer, MarketOrder> orders = new ConcurrentHashMap<>();

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
    // Generate a hex representation of the current time (last 3 digits)
    // This provides a reasonably unique ID for this instance
    applicationId = "Java_Order_Manager"; 
    ApplicationLogging.logAsync(LOGGER, Level.INFO, "Application ID: " + applicationId);
    
    initializeRedisConnection();
    
    // Initialize venue to trader mapping
    initializeTraderMap();

    // Initialize Redis control channel listener
    initializeRedisControlListener();
    
    // Initialize heartbeats
    initializeHeartbeat();

    // Initialize the market recheck scheduler
    initializeMarketRechecks();
   
//    scheduleAutoShutdown();
    
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
   * Gets the InstrumentSubscriber instance.
   * 
   * @return The InstrumentSubscriber or null if not initialized
   */
  public static InstrumentSubscriber getInstrumentSubscriber() {
      return instrumentSubscriber;
  }
  
  private void initializeVenueTraderListener() {
	    synchronized (venueTraderLock) {
	        if (venueTraderListener == null) {
	            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Creating VenueTraderListener instance");
	            try {
	                venueTraderListener = new VenueTraderListener();
	                ApplicationLogging.logAsync(LOGGER, Level.INFO, "VenueTraderListener created successfully");

	            } catch (Exception e) {
	                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
	                    "Failed to create VenueTraderListener: " + e.getMessage(), e);
	            }
	        }
	    }
	}
  
  // Initialize the mapping of venues to trader IDs
  private void initializeTraderMap() {
     venueToTraderMap.put("BTEC_REPO_US", "EGEI");
     venueToTraderMap.put("DEALERWEB_REPO", "aslegerhard01");
     venueToTraderMap.put("FENICS_USREPO", "frosevan");
//     venueToTraderMap.put("BTEC_REPO_US", "TEST2");
//     venueToTraderMap.put("DEALERWEB_REPO", "asldevtrd1");
//     venueToTraderMap.put("FENICS_USREPO", "frosasl1");
      // Add more venue-trader mappings as needed
      
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Venue to trader mapping initialized with " + 
                                  venueToTraderMap.size() + " entries");
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
          ApplicationLogging.logAsync(LOGGER, Level.INFO, "Shutting down order expiration scheduler");
          shutdownExecutor(orderExpirationScheduler, "Order expiration scheduler");
      }));
      
      ApplicationLogging.logAsync(LOGGER, Level.INFO, 
          "Order expiration checker initialized (TTL: 60 secs before 7:15, 15 secs after)");
  }
  
	//Helper method to get trader ID for a venue
	private String getTraderForVenue(String venue) {
	   String traderId = venueToTraderMap.get(venue);
	   if (traderId == null) {
	       ApplicationLogging.logAsync(LOGGER, Level.WARNING, "No trader configured for venue: " + venue);
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
	        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
	            "Cannot initialize Redis control listener - Redis is not connected");
	        return;
	    }
	    
	    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
	        "Initializing Redis control channel listener on channel: " + ADMIN_CHANNEL);
	    
	    // Start a separate thread for Redis pub/sub listening
	    Thread redisListenerThread = new Thread(() -> {
	        Jedis subscriberJedis = null;
	        try {
	            // Create a separate Jedis connection for the subscriber
	            // This is needed because a Redis connection in pub/sub mode cannot be used for other commands
	            subscriberJedis = new Jedis(REDIS_HOST, REDIS_PORT, TIMEOUT);
	            
	            ApplicationLogging.logAsync(LOGGER, Level.INFO, 
	                "Redis control listener started on channel: " + ADMIN_CHANNEL);
	            
	            // Publish a startup message
//	            publishToRedis(ADMIN_CHANNEL + ":STATUS", "ORDER_MANAGEMENT_STARTED");
	            
	            // Create a subscriber to process messages
	            subscriberJedis.subscribe(new JedisPubSubListener(), ADMIN_CHANNEL);
	            
	        } catch (Exception e) {
	            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
	                "Error in Redis control listener: " + e.getMessage(), e);
	        } finally {
	            if (subscriberJedis != null) {
	                try {
	                    subscriberJedis.close();
	                } catch (Exception e) {
	                    ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
	                        "Error closing Redis subscriber: " + e.getMessage());
	                }
	            }
	        }
	    }, "Redis-Control-Listener");
	    
	    // Set as daemon thread so it doesn't prevent JVM shutdown
	    redisListenerThread.setDaemon(true);
	    redisListenerThread.start();
	    
	    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
	        "Redis control listener thread started");
	}
	
	private class JedisPubSubListener extends JedisPubSub {
	    @Override
	    public void onMessage(String channel, String message) {
	        if (!ADMIN_CHANNEL.equals(channel)) {
	            return; // Ignore messages from other channels
	        }
	        
	        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
	            "Received control message: " + message + " on channel: " + channel);
	        
	        try {
	            // Parse message as JSON to extract command and parameters
	            Map<String, Object> controlMessage = parseControlMessage(message);
	            String command = (String) controlMessage.getOrDefault("command", "");
	            
	            if ("STOP".equalsIgnoreCase(command)) {
	                handleStopCommand(controlMessage);
	            } else if ("RESUME".equalsIgnoreCase(command)) {
	                handleResumeCommand(controlMessage);
	            } else {
	                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
	                    "Unknown control command received: " + command);
	            }
	        } catch (Exception e) {
	            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
	                "Error processing control message: " + e.getMessage(), e);
	        }
	    }
	    
	    @Override
	    public void onSubscribe(String channel, int subscribedChannels) {
	        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
	            "Subscribed to Redis channel: " + channel);
	    }
	    
	    @Override
	    public void onUnsubscribe(String channel, int subscribedChannels) {
	        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
	            "Unsubscribed from Redis channel: " + channel);
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
        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
            "Failed to parse control message as JSON, treating as plain command: " + message);
        
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
        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
            "System already in STOPPED state, ignoring redundant STOP command");
        return;
    }
    
    ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
        "Executing STOP command - cancelling orders and preventing new submissions");
    
    // Set the system to stopped state
    isSystemStopped = true;
    
    // Disable continuous trading to prevent auto-trading
    continuousTradingEnabled = false;
    
    // Cancel all existing orders
    cancelAllOrders();
    
    // Publish status update
//    publishSystemStatus();
    
    ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
        "STOP command executed - system is now STOPPED");
}

/**
 * Handles the RESUME command, allowing new orders to be submitted.
 * 
 * @param controlMessage The control message containing command parameters
 */
private void handleResumeCommand(Map<String, Object> controlMessage) {
    // Skip if not stopped
    if (!isSystemStopped) {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
            "System already in RUNNING state, ignoring redundant RESUME command");
        return;
    }
    
    ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
        "Executing RESUME command - allowing new order submissions");
    
    // Set the system back to running state
    isSystemStopped = false;
    
    // Re-enable continuous trading if requested
    Boolean enableTrading = (Boolean) controlMessage.getOrDefault("enableTrading", Boolean.TRUE);
    if (enableTrading) {
        continuousTradingEnabled = true;
        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
            "Continuous trading re-enabled as part of RESUME command");
    } else {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
            "Continuous trading remains disabled after RESUME command");
    }
    
    // Publish status update
//    publishSystemStatus();
    
    ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
        "RESUME command executed - system is now RUNNING");
}

	
  /**
   * Called when a publication event occurs.
   * This implementation doesn't handle individual publication events.
   */
  public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
    // Not handling individual publication events
    if (LOGGER.isLoggable(Level.FINEST)) {
      ApplicationLogging.logAsync(LOGGER, Level.FINEST, "Publication event for: " + mkvObject.getName());
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
    if (LOGGER.isLoggable(Level.FINEST)) {
      ApplicationLogging.logAsync(LOGGER, Level.FINEST, "Publication idle for component: " + component + ", start: " + start);
    }
  }

  /**
   * Not interested in this event because our component is a pure subscriber
   * and is not supposed to receive requests for subscriptions.
   */
  public void onSubscribe(MkvObject mkvObject) {
    // No action needed - we are not a publisher
    if (LOGGER.isLoggable(Level.FINEST)) {
      ApplicationLogging.logAsync(LOGGER, Level.FINEST, "Subscription event for: " + mkvObject.getName());
    }
  }

  /**
   * Sets up subscriptions to depth records using a pattern-based approach.
   * Subscribes to the consolidated market data from VMO_REPO_US.
   */
  public void subscribeToDepths() {
	    try {
	        // Create a single depth listener instance that will be reused
	        depthListener = new DepthListener(this);
	        
	        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
	            "Subscribing to consolidated depth data using pattern: " + PATTERN);
	            
	        // Subscribe to the pattern
	        subscribeToPattern();
	        
	        // Set up a monitor to check subscription status periodically
	        setupPatternSubscriptionMonitor();
	        
	    } catch (Exception e) {
	        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
	            "Error setting up depth subscriptions: " + e.getMessage(), e);
	        e.printStackTrace();
	    }
	}

private void subscribeToPattern() {
    try {
        // Get the publish manager to access patterns
        MkvPublishManager pm = Mkv.getInstance().getPublishManager();
       
        // Look up the pattern object
        MkvObject obj = pm.getMkvObject(PATTERN);
       
        if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
            // Subscribe within a synchronized block
            synchronized (subscriptionLock) {
                // Check again inside synchronization to avoid race conditions
                if (!isPatternSubscribed) {
                    ApplicationLogging.logAsync(LOGGER, Level.INFO,
                        "Found consolidated depth pattern, subscribing: " + PATTERN);
                       
                    ((MkvPattern) obj).subscribe(MarketDef.DEPTH_FIELDS, depthListener);
                   
                    // Mark that we've successfully subscribed
                    isPatternSubscribed = true;
                   
                    ApplicationLogging.logAsync(LOGGER, Level.INFO,
                        "Successfully subscribed to consolidated depth pattern");
                }
            }
        } else {
            ApplicationLogging.logAsync(LOGGER, Level.WARNING,
                "Consolidated depth pattern not found: " + PATTERN);
               
            // Create a single shared listener instance instead of an anonymous one
            final MkvPublishListener patternListener = new MkvPublishListener() {
                @Override
                public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
                    // Only proceed if we're not already subscribed
                    if (isPatternSubscribed) {
                        return;
                    }
                    
                    // Check if this is our pattern being published
                    if (pub_unpub && mkvObject.getName().equals(PATTERN) &&
                        mkvObject.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                        trySubscribeAndRemoveListener(mkvObject, pm, this);
                    }
                }
               
                @Override
                public void onPublishIdle(String component, boolean start) {
                    // Only proceed if we're not already subscribed
                    if (isPatternSubscribed) {
                        return;
                    }
                    
                    // Try looking for the pattern again at idle time
                    MkvObject obj = pm.getMkvObject(PATTERN);
                    
                    if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                        trySubscribeAndRemoveListener(obj, pm, this);
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
        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
            "Error subscribing to pattern: " + e.getMessage(), e);
    }
}

// Helper method to handle subscription and listener removal safely
private void trySubscribeAndRemoveListener(MkvObject mkvObject, MkvPublishManager pm, MkvPublishListener listener) {
    synchronized (subscriptionLock) {
        // Check again inside synchronization to avoid race conditions
        if (isPatternSubscribed) {
            return;
        }
        
        try {
            ApplicationLogging.logAsync(LOGGER, Level.INFO,
                "Pattern found, subscribing: " + PATTERN);
                
            ((MkvPattern) mkvObject).subscribe(MarketDef.DEPTH_FIELDS, depthListener);
            isPatternSubscribed = true;
            
            ApplicationLogging.logAsync(LOGGER, Level.INFO,
                "Successfully subscribed to pattern");
                
            // Remove the listener now that we've subscribed - safely outside the callback
            // but still inside synchronization
            pm.removePublishListener(listener);
        } catch (Exception e) {
            ApplicationLogging.logAsync(LOGGER, Level.SEVERE,
                "Error subscribing to pattern: " + e.getMessage(), e);
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
    if (LOGGER.isLoggable(Level.FINE)) {
      ApplicationLogging.logAsync(LOGGER, Level.FINE, "Adding order to cache: reqId=" + order.getMyReqId());
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
    if (LOGGER.isLoggable(Level.FINE)) {
      ApplicationLogging.logAsync(LOGGER, Level.FINE, "Removing order from cache: reqId=" + reqId);
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
    if (LOGGER.isLoggable(Level.FINE)) {
      ApplicationLogging.logAsync(LOGGER, Level.FINE, "Retrieving order from cache: reqId=" + reqId + 
               ", found=" + (order != null));
    }
    return order;
  }

  /**
   * The component doesn't need to listen to partial updates for records.
   */
  public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
      boolean isSnap) {
    // Not interested in partial updates
    if (LOGGER.isLoggable(Level.FINEST)) {
      ApplicationLogging.logAsync(LOGGER, Level.FINEST, "Partial update for record: " + mkvRecord.getName());
    }
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
      // Get the UserData (contains our request ID) and FreeText (contains our application ID)
      String userData = mkvRecord.getValue("UserData").getString();
      String freeText = mkvRecord.getValue("FreeText").getString();
      
      // If this order wasn't placed by us, ignore it
      if ("".equals(userData) || !freeText.equals(getApplicationId())) {
        if (LOGGER.isLoggable(Level.FINEST)) {
          ApplicationLogging.logAsync(LOGGER, Level.FINEST, "Ignoring order update for non-matching order: " + 
                     mkvRecord.getName() + ", userData=" + userData + 
                     ", freeText=" + freeText);
        }
        return;
      } else {
        // This is an order we placed - forward the update to the MarketOrder
        try {
          // Parse the request ID from the UserData
          int reqId = Integer.parseInt(userData);
          if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, "Processing order update: reqId=" + reqId);
          }        
          // Get the corresponding MarketOrder from the cache
          MarketOrder order = getOrder(reqId);
          
          if (order != null) {
            // Forward the update to the order
            if (LOGGER.isLoggable(Level.FINE)) {
              ApplicationLogging.logAsync(LOGGER, Level.FINE, "Forwarding update to order: reqId=" + reqId);
            }
            order.onFullUpdate(mkvRecord, mkvSupply, isSnap);
            String orderStatus = mkvRecord.getValue("Status").getString();
            if ("Canceled".equals(orderStatus) || "Filled".equals(orderStatus) || 
                "Rejected".equals(orderStatus)) {
                removeOrder(order.getMyReqId());
            }
          } else {
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, "Order not found in cache: reqId=" + reqId);
          }
        } catch (Exception e) {
          ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error processing order update: " + e.getMessage(), e);
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error accessing order fields: " + e.getMessage(), e);
      e.printStackTrace();
    }
  }

  /**
   * Notification that an order is no longer able to trade.
   * Removes the order from the cache.
   * 
   * Implementation of IOrderManager.orderDead()
   */
  public void orderDead(MarketOrder order) {
    if (LOGGER.isLoggable(Level.INFO)) {
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Order is dead: orderId=" + order.getOrderId() + 
               ", reqId=" + order.getMyReqId());
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
        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
            "Order rejected - system is in STOPPED state: source=" + MarketSource + 
            ", trader=" + TraderId + ", instrId=" + instrId);
        
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
	  
	ApplicationLogging.logAsync(LOGGER, Level.INFO, "Attempting to create order: source=" + MarketSource + 
	             ", trader=" + TraderId + ", instrId=" + instrId + ", verb=" + verb + 
	             ", qty=" + qty + ", price=" + price + ", type=" + type + ", tif=" + tif);
	  
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
        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Order rejected by market: reqId=" + order.getMyReqId() + 
                 ", instrId=" + instrId + ", errCode=" + order.getErrCode() + 
                 ", errStr=" + order.getErrStr() + 
                 ", source=" + MarketSource + ", trader=" + TraderId);
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
      
      if (LOGGER.isLoggable(Level.INFO)) {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Order created successfully: reqId=" + order.getMyReqId() + 
                 ", instrId=" + instrId);
      }
    } else {
        // Log a more detailed error when order creation fails completely
        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Order creation failed: instrId=" + instrId + 
                 ", source=" + MarketSource + ", trader=" + TraderId + 
                 ", verb=" + verb + ", qty=" + qty + ", price=" + price + 
                 ", type=" + type + ", tif=" + tif);
        
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
	      // Store the latest best for this instrument
          synchronized(gcDataLock) {
                latestCashGC = cash_gc;
                latestRegGC = reg_gc;
                if (gcBestCash != null) sharedGCBestCash = gcBestCash;
                if (gcBestREG != null) sharedGCBestREG = gcBestREG;
	            latestBestByInstrument.put(best.getInstrumentId(), best);
          }	      
	      // Process this update for potential trading
	      processMarketOpportunity(best, latestCashGC, latestRegGC, sharedGCBestCash, sharedGCBestREG);
	  }

	//Add this method to initialize the scheduled market rechecks
	private void initializeMarketRechecks() {
	   // Schedule a task to recheck the market every 1 second
	   scheduler.scheduleAtFixedRate(this::recheckMarket, 1, 2, TimeUnit.SECONDS);
	   
	   // Add a shutdown hook to clean up the scheduler
	   Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Shutting down market recheck scheduler");
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
	   ApplicationLogging.logAsync(LOGGER, Level.INFO, "Market recheck scheduler initialized");
	}
	
	//Add this method to periodically recheck the market
	private void recheckMarket() {
	   if (!continuousTradingEnabled || isSystemStopped) {
	       return;
	   }
  	   
	   // Process each instrument's latest best price
       synchronized(gcDataLock) {
        for (Best best : latestBestByInstrument.values()) {
            processMarketOpportunity(best, latestCashGC, latestRegGC, 
                sharedGCBestCash, sharedGCBestREG);
        }
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
	    double threshold = 2.0 - (timeElapsedProportion * (2.0 - 0.05));
	    
	    // Round to 2 decimal places for cleaner values
	    threshold = Math.round(threshold * 100) / 100.0;
	    
	    return threshold;
	}
	
	// Method to evaluate and execute on market opportunities, crosses and GC crosses
	private void processMarketOpportunity(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {
        
	    // Skip if the system is in stopped state
	    if (isSystemStopped) {
	        if (LOGGER.isLoggable(Level.FINE)) {
	            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
	                "Skipping market opportunity processing - system is in STOPPED state");
	        }
	        return;
	    }
		
		// Skip if the best object is null
	    if (best == null) {
	        return;
	    }
  	    
  	    String askSrc = best.getAskSrc();
  	    String bidSrc = best.getBidSrc();
  	    
        // Only proceed with order checks if we might actually place an order
        if (!validVenues.contains(askSrc)) 
        {
            return;
        }
	    
	    double bid = best.getBid();
  	    double ask = best.getAsk();

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
	  			ApplicationLogging.logAsync(LOGGER, Level.FINE, "No GC market available for comparison - Type: " + 
                    (id.contains("C_Fixed") ? "CASH" : "REG") + ", Instrument: " + id);
	  			return;
        	} else if (gcLevelResult != null) {
	  			
        		Double gcBidPrice = gcLevelResult.getBidPrice();
	  		    Double gcAskPrice = gcLevelResult.getAskPrice();
	  		    
                if (gcBidPrice == null) {
                    ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                        "Skipping trade: Missing GC level bid price - Instrument: " + id + 
                        ", Type: " + (id.contains("C_Fixed") ? "CASH" : "REG") + 
                        ", Current Ask: " + ask);
                    return;
                }
                
                if (ask < gcBidPrice) {
                     ApplicationLogging.logAsync(LOGGER, Level.FINE, String.format(
                         "Skipping trade: ask %.6f is less than GC level bid %.6f - Type: %s, Instrument: %s",
                         ask, gcBidPrice, (id.contains("C_Fixed") ? "CASH" : "REG"), id));
                    return;
                }
                
                if (gcAskPrice != null && ask < gcAskPrice) {
                     ApplicationLogging.logAsync(LOGGER, Level.FINE, String.format(
                         "Skipping trade: ask %.6f is less than GC level ask %.6f - Type: %s, Instrument: %s",
                         ask, gcAskPrice, (id.contains("C_Fixed") ? "CASH" : "REG"), id));
                    return;
                }
                
                // If we get here, process the opportunity
                processGCOpportunity(best, gcLevelResult);
                return;
	  		}
	  		return;
	  	}
	  	
        if (!validVenues.contains(bidSrc)) {
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
  		            if (LOGGER.isLoggable(Level.FINE) && isDynamicThresholdPeriod()) {
  		                // Only log during the dynamic threshold period to reduce overhead
  		                ApplicationLogging.logAsync(LOGGER, Level.FINE, 
  		                    "Skipping trade: spread " + (cash_gc - ask) + 
  		                    " exceeds time-based threshold " + threshold);
  		            }
  		            return;
  		  		}
  		  	}
  	    	cashRegAdjustment = 0.00;
  	    } else if (isREG) {
  	    	// for REG, we're not applying the avoid specials crosses logic
//  		  	if (reg_gc != 0) {
//  		  		if ((reg_gc - ask) > 2.0) {
//  		  			return;
//  		  		}
//  		  	}
  	    	cashRegAdjustment = 0.02;
  	    }
  	  
  	   
	  boolean afterEightThirtyFive = isAfterEightThirtyFive();
      double spreadEightThirtyFiveAdjustment = (int)Math.ceil(best.getAskSizeMin() / orderBidSize) * Math.abs(cash_gc - ask);
      
  	  if (LOGGER.isLoggable(Level.INFO)) {
  	    String reason = "";
  	    double spread = ask - bid;
  	    String securityType = isCash ? "CASH" : (isREG ? "REG" : "UNKNOWN");
  	    
  	    // Build detailed reason message
  	    if ((sizeDiff == 0) && sameSource && (ask-(0.03 + cashRegAdjustment) < bid)) {
  	        reason = "Equal size, same source, spread < 0.03 + adjustment";
  	    } else if (sizeDiff > 25 && (ask-(0.04 + cashRegAdjustment) < bid) && isREG) {
  	        reason = "Size difference > 25, spread < 0.04 + adjustment + isREG";
  	    } else if (sizeDiff > 25 && (ask-(0.04) < bid) && !afterEightThirtyFive && isCash) {
  	        reason = "Size difference > 25, spread < 0.04 + adjustment + before 8:30 + isCash";
  	    } else if (sizeDiff > 25 && (ask-spreadEightThirtyFiveAdjustment < bid) && afterEightThirtyFive && isCash) {
	        reason = "Size difference > 25, spread < spreadEightThirtyFiveAdjustment + after 8:35 + isCash ";
  	    } else if (sizeDiff == 25 && orderAskSizeTotal == 50 && orderBidSizeTotal == 25 
  	               && (ask-(0.02 + cashRegAdjustment) < bid)) {
  	        reason = "Size exactly 25 (50/25 pattern), spread < 0.02 + adjustment";
  	    } else if (sizeDiff > 15 && sizeDiff < 25 && (ask-(0.03 + cashRegAdjustment) < bid) && isREG) {
  	        reason = "Size difference 15-25, spread < 0.03 + adjustment + isREG";
  	    } else if (sizeDiff > 15 && sizeDiff < 25 && (ask-(0.03 + cashRegAdjustment) < bid)  && !afterEightThirtyFive && isCash) {
  	        reason = "Size difference 15-25, spread < 0.03 + adjustment + before 8:30 + isCash";
  	    } else if (sizeDiff > 15 && sizeDiff < 25 && (ask-spreadEightThirtyFiveAdjustment < bid)  && afterEightThirtyFive && isCash) {
  	        reason = "Size difference 15-25, spread < 0.03 + adjustment + before 8:30 + isCash";
  	    } else if (sizeDiff >= 9 && sizeDiff <= 15 && (ask-(0.02 + cashRegAdjustment) < bid)) {
  	        reason = "Size difference 9-15, spread < 0.02 + adjustment";
  	    } else if (sizeDiff < 9 && sizeDiff > 5 && (ask-0.02 < bid)) {
  	        reason = "Size difference 5-9, spread < 0.02";
  	    } else if (sizeDiff <= 5 && (ask-0.01 < bid)) {
  	        reason = "Size difference <= 5, spread < 0.01";
  	    } else if (sizeDiff == 0 && (ask-0.02 < bid)) {
  	        reason = "Equal size, spread < 0.02";
  	    }
  	    
  	  ApplicationLogging.logAsync(LOGGER, Level.INFO,
  		    String.format("ID: %s - Skipping trade: %s. Details: [%s] ask=%.6f (%s), bid=%.6f (%s), " +
  		        "sizeDiff=%.2f, minSizeDiff=%.2f, spread=%.6f, " +
  		        "askSize=%.2f/%.2f, bidSize=%.2f/%.2f, multiplier=%d, adjustment=%.6f",
  		        id, reason, securityType,
  		        ask, askSrc, bid, bidSrc,
  		        sizeDiff, minsizeDiff, spread,
  		        orderAskSize, orderAskSizeTotal,
  		        orderBidSize, orderBidSizeTotal,
  		        multiplier, cashRegAdjustment));
  	  }
	  	if (((sizeDiff == 0) && sameSource && (ask-(0.03 + cashRegAdjustment) < bid)) || 
	  		    ((sizeDiff > 25) && (ask-(0.04 + cashRegAdjustment) < bid) && isREG) ||
	  		    ((sizeDiff > 25) && (ask- 0.04  < bid) && !afterEightThirtyFive && isCash) ||
	  		    ((sizeDiff > 25) && (ask- spreadEightThirtyFiveAdjustment < bid) && afterEightThirtyFive && isCash) ||
	  		    ((sizeDiff == 25) && (orderAskSizeTotal == 50) && (orderBidSizeTotal == 25) && 
	  		        (ask-(0.02 + cashRegAdjustment) < bid)) ||
	  		    ((sizeDiff > 15 && sizeDiff < 25) && (ask-(0.03 + cashRegAdjustment) < bid) && isREG) ||
	  		    ((sizeDiff > 15 && sizeDiff < 25) && (ask-(0.03) < bid) && !afterEightThirtyFive && isCash) ||
	  		    ((sizeDiff > 15 && sizeDiff < 25) && (ask-spreadEightThirtyFiveAdjustment < bid) && afterEightThirtyFive && isCash) ||
	  		    ((sizeDiff >= 9 && sizeDiff <= 15) && (ask-(0.02 + cashRegAdjustment) < bid)) || 
	  		    ((sizeDiff < 9) && (sizeDiff > 5) && ((ask-0.02) < bid)) ||
	  		    ((sizeDiff <= 5) && ((ask-0.01) < bid)) || 
	  		    ((sizeDiff == 0) && ((ask-0.02) < bid))
	  		) {
	  		    return;
	  		}
        
  	    String idFull = best.getInstrumentId();
 	    
  	    boolean AskIsAON = best.getAskIsAON();
 	    boolean BidIsAON = best.getBidIsAON();

      if (LOGGER.isLoggable(Level.INFO)) {
          ApplicationLogging.logAsync(LOGGER, Level.INFO, "Processing trading opportunity for " + 
        		  "instrument id: " + idFull + " id: "  + id + 
        		  ": askSize=" + orderAskSizeTotal + " (" + orderAskSize + ")" +
                   ", bidSize=" + orderBidSizeTotal + " (" + orderBidSize + ")" +
        		   ": ask=" + ask + " (" + askSrc + ")" +
                   ", bid=" + bid + " (" + bidSrc + ")");
        }
    	
        // Create a unique key for this instrument pair to track trades
        String tradingKey = id + "|" + askSrc + "|" + bidSrc;
        
        // Check if we've traded this instrument recently
        long currentTime = System.currentTimeMillis();
        Long lastTradeTime = lastTradeTimeByInstrument.get(tradingKey);
        
        if (lastTradeTime != null && (currentTime - lastTradeTime) < MIN_TRADE_INTERVAL_MS) {
            // Too soon to trade this instrument again
          if (LOGGER.isLoggable(Level.FINE)) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, "Skipping trade for " + tradingKey + 
            		" last trade was too recent: " + (currentTime - lastTradeTime) + 
                   "ms since last trade");
          }
            return;
        }
        
        // Log that we found an opportunity
      if (LOGGER.isLoggable(Level.INFO)) {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Found trading opportunity for " + id + 
                 ": IsAON=" + AskIsAON + " ask=" + ask + " (" + askSrc + ")" +
                 ", IsAON=" + BidIsAON + " bid=" + bid + " (" + bidSrc + ")");
      }

      // Get the native instrument IDs
      String askVenueInstrument = instrumentSubscriber.getInstrumentFieldBySourceString(id, askSrc, AskIsAON);
      String bidVenueInstrument = instrumentSubscriber.getInstrumentFieldBySourceString(id, bidSrc, BidIsAON);
      
   // Add null checks
      if (askVenueInstrument == null || bidVenueInstrument == null) {
          if (LOGGER.isLoggable(Level.WARNING)) {
              ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                  "Missing instrument mapping for " + id + 
                  ", ask source: " + askSrc + 
                  ", bid source: " + bidSrc);
          }
          return; // Skip this opportunity
      }
      
      // Get the trader IDs for each venue
      String askTrader = getTraderForVenue(askSrc);
      String bidTrader = getTraderForVenue(bidSrc);
        
   // Add protection against zero or negative sizes
      if (orderAskSizeTotal <= 0 || orderBidSizeTotal <= 0) {
          ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
              "Invalid sizes detected for " + id + 
              ": ask=" + orderAskSizeTotal + 
              ", bid=" + orderBidSizeTotal);
          return;
      }

      // Calculate the order size
  	  String ordertypeAsk = null;
  	  String ordertypeBid = null;
  	  ordertypeBid = "FAS";
  	  ordertypeAsk = "FAS";
  	  //need to further examine the DEALERWEB ordertypes
//	  if ((BidIsAON && bidSrc.equals("DEALERWEB_REPO")) || bidSrc.equals("DEALERWEB_REPO")) {
//		  ordertypeBid = "FAS";
//	  } else if (BidIsAON) {
//		  ordertypeBid = "FOK";
//	  } else {
//		  ordertypeBid = "FAK";
//	  };
//	  
//  	  //need to further examine the DEALERWEB ordertypes
//	  if ((AskIsAON && askSrc.equals("DEALERWEB_REPO")) || askSrc.equals("DEALERWEB_REPO")) {
//		  ordertypeAsk = "FAS";
//	  } else if (AskIsAON) {
//		  ordertypeAsk = "FOK";
//	  } else {
//		  ordertypeAsk = "FAK";
//    	  }
 
      String ask0Traders = venueTraderListener.getTraderInfo(askSrc, askVenueInstrument, "Ask0Traders");
      String bid0Traders = venueTraderListener.getTraderInfo(bidSrc, bidVenueInstrument, "Bid0Traders");
   
      boolean askHasTraders = (ask0Traders != null && !ask0Traders.isEmpty());
      boolean bidHasTraders = (bid0Traders != null && !bid0Traders.isEmpty());
      boolean skipSelfMatch = false;
      
      if (askHasTraders || bidHasTraders) {
    	  skipSelfMatch = true;
      }
      
//      boolean skipSelfMatch = shouldSkipDueToSelfMatch(askSrc, askVenueInstrument, bidSrc, bidVenueInstrument, ask, bid);

      LocalTime askTraderUpdate = venueTraderListener.getTraderInfoTimestamp(askSrc, askVenueInstrument, "Ask0Traders");
      LocalTime bidTraderUpdate = venueTraderListener.getTraderInfoTimestamp(bidSrc, bidVenueInstrument, "Bid0Traders");
      
      // Before checking trader information (pre self-match check)
      if (LOGGER.isLoggable(Level.FINE)) {
    	    ApplicationLogging.logAsync(LOGGER, Level.FINE, "Checking for self-match: " +
    	        "ASK: src=" + askSrc + ", trader=" + askTrader + ", venue=" + askVenueInstrument + 
    	        ", size=" + orderAskSize + ", price=" + ask + ", type=" + ordertypeAsk + ", IsAON=" + AskIsAON + 
    	        " ask0Trader=" + ask0Traders + " | BID: src=" + bidSrc + ", trader=" + bidTrader + ", venue=" + 
    	        bidVenueInstrument + ", size=" + orderBidSize + ", price=" + bid + ", type=" + ordertypeBid + 
    	        ", IsAON=" + BidIsAON +" bid0Trader=" + bid0Traders);
    	}

    	// Trader information
    	if (LOGGER.isLoggable(Level.FINE)) {
    	    ApplicationLogging.logAsync(LOGGER, Level.FINE,
    	        "Trader information: ask instrument=" + askVenueInstrument + ", askTraders=" + ask0Traders +
    	        ", bid instrument=" + bidVenueInstrument + ", bidTraders=" + bid0Traders);
    	}
      
//       Self-match detection
      if  (skipSelfMatch) {
	      ApplicationLogging.logAsync(LOGGER, Level.INFO, 
    	            "Failed self-match prevention check");
    	  return;
      }
      
	  if (LOGGER.isLoggable(Level.INFO)) {
         ApplicationLogging.logAsync(LOGGER, Level.WARNING, "### BUY ORDER BEING SENT: src: " + askSrc + ", trader:  " + askTrader + ", venue:  " + askVenueInstrument + ", direction:  Buy, size:  " + orderAskSize + 
            ", prc:  " + ask + ", Limit, ordertype: " + ordertypeAsk + ", IsAON: " + AskIsAON  + ", ask0Traders: " + ask0Traders + ", askHasTraders: " + askHasTraders + ", askTraderUpdateTime: " + askTraderUpdate);
         ApplicationLogging.logAsync(LOGGER, Level.WARNING, "### SELL ORDER BEING SENT: src: " + bidSrc + ", trader:  " + bidTrader + ", venue:  " + bidVenueInstrument + ", direction:  Sell, size:  " + orderBidSize + 
            ", prc:  " + bid + ", Limit, ordertype: " + ordertypeBid + ", IsAON: " + BidIsAON  + ", bid0Traders: " + bid0Traders  + ", bidHasTraders: " + bidHasTraders  + ", bidTraderUpdateTime: " + bidTraderUpdate);
	  }

  	   // Placing active orders
        addOrder(askSrc, askTrader, askVenueInstrument, "Buy", orderAskSize, 
                ask, "Limit", ordertypeAsk);
        addOrder(bidSrc, bidTrader, bidVenueInstrument, "Sell", orderBidSize, 
                bid, "Limit", ordertypeBid);
        
        // Record that we've traded this instrument
        lastTradeTimeByInstrument.put(tradingKey, currentTime);
      if (LOGGER.isLoggable(Level.INFO)) {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Recorded trade for " + tradingKey + 
                 " at " + new java.util.Date(currentTime));
      }
    }
    /**
     * Checks if we should skip the trade due to self-match prevention.
     * 
     * @param askSrc The source of the ask side
     * @param askVenueInstrument The venue instrument for the ask side
     * @param bidSrc The source of the bid side
     * @param bidVenueInstrument The venue instrument for the bid side
     * @param omAsk0Price The ask price from OM
     * @param omBid0Price The bid price from OM
     * @return true if we should skip the trade, false otherwise
     */
//	private boolean shouldSkipDueToSelfMatch(String askSrc, String askVenueInstrument, String bidSrc, String bidVenueInstrument, double omAsk0Price, double omBid0Price) {
//	    // Get trader information for both sides
//	    String ask0Traders = venueTraderListener.getTraderInfo(askSrc, askVenueInstrument, "Ask0Traders");
//	    String bid0Traders = venueTraderListener.getTraderInfo(bidSrc, bidVenueInstrument, "Bid0Traders");
//	    
//        // Get Ask0 and Bid0 prices from VenueTraderListener
//        Double vtlAsk0Price = null;
//        Double vtlBid0Price = null;
//        
//        try {
//            vtlAsk0Price = venueTraderListener.getTraderInfoAsDouble(askSrc, askVenueInstrument, "Ask0");
//            vtlBid0Price = venueTraderListener.getTraderInfoAsDouble(bidSrc, bidVenueInstrument, "Bid0");
//        } catch (Exception e) {
//            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
//                "Error retrieving price data from VenueTraderListener: " + e.getMessage());
//            return true; // Conservative approach - skip trade on error
//        }
//        
//        boolean pricesAreFresh = false;
//        String freshnessReason = "";
//	    
//        if (vtlAsk0Price == null || vtlBid0Price == null) {
//            freshnessReason = "Missing price data from VenueTraderListener (Ask0=" + vtlAsk0Price + ", Bid0=" + vtlBid0Price + ")";
//        } else if (!vtlAsk0Price.equals(omAsk0Price)) {
//            freshnessReason = "Ask0 price mismatch: VTL=" + vtlAsk0Price + " vs OM=" + omAsk0Price;
//        } else if (!vtlBid0Price.equals(omBid0Price)) {
//            freshnessReason = "Bid0 price mismatch: VTL=" + vtlBid0Price + " vs OM=" + omBid0Price;
//        } else {
//            pricesAreFresh = true;
//            freshnessReason = "Prices match between VTL and OM";
//        }
//
//        if (!pricesAreFresh) {
//            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
//                "Self-match prevention check failed: " + freshnessReason);
//            return true;
//        }
//
//        // Get data quality information
//	    Map<String, Object> askDataQuality = venueTraderListener.getDataQuality(askSrc, askVenueInstrument, "Ask0Traders");
//	    Map<String, Object> bidDataQuality = venueTraderListener.getDataQuality(bidSrc, bidVenueInstrument, "Bid0Traders");
//	    
//	    // Log data quality for debugging
//	    if (LOGGER.isLoggable(Level.FINE)) {
//	        ApplicationLogging.logAsync(LOGGER, Level.FINE, 
//	            "Self-match check data quality - ASK: " + askDataQuality + ", BID: " + bidDataQuality);
//	    }
//	    
//	    // Check if we have reliable trader data
//	    boolean askDataReliable = isTraderDataReliable(askDataQuality);
//	    boolean bidDataReliable = isTraderDataReliable(bidDataQuality);
//	    
//	    if (!askDataReliable && !bidDataReliable) {
//	        // If we don't have reliable trader data, apply conservative approach
//	        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
//	            "Unreliable trader data detected for both venues - applying  self-match prevention. " +
//	            "ASK reliable: " + askDataReliable + ", BID reliable: " + bidDataReliable + 
//	            ", askSrc: " + askSrc + ", bidSrc: " + bidSrc);
//	         return true;
//	     } else {
//	            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
//	                "Proceeding as at least one venue has reliable trader data: " + 
//	                askSrc + " vs " + bidSrc);
//	     	}
//	    
//	    // We have reliable data - proceed with normal self-match detection
//	    boolean askHasTraders = (ask0Traders != null && !ask0Traders.isEmpty());
//	    boolean bidHasTraders = (bid0Traders != null && !bid0Traders.isEmpty());
//	    
//	    if (askHasTraders || bidHasTraders) {
//	        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
//	            "Self-match detected - ASK traders: '" + ask0Traders + 
//	            "', BID traders: '" + bid0Traders + "'");
//	        return true;
//	    }
//	    
//	    // Enhanced logging for successful trades
//	    ApplicationLogging.logAsync(LOGGER, Level.FINE, 
//	        "Self-match check passed - no traders detected. ASK: '" + ask0Traders + 
//	        "', BID: '" + bid0Traders + "' for " + askSrc + "/" + bidSrc);
//	    
//	    return false; // No self-match detected
//	}
//	
//	private boolean isTraderDataReliable(Map<String, Object> dataQuality) {
//	    try {
//	        // Check if venue is subscribed
//	        Boolean venueSubscribed = (Boolean) dataQuality.get("venueSubscribed");
//	        if (venueSubscribed == null || !venueSubscribed) {
//	            return false;
//	        }
//	        
//	        // Check if we have recent instrument activity
//	        Long instrumentActivityAge = (Long) dataQuality.get("instrumentActivityAge");
//	        if (instrumentActivityAge == null || instrumentActivityAge < 0) {
//	            return false; // No recent activity data
//	        }
//	        
//	        // If instrument activity is too old, data is not reliable
//	        if (instrumentActivityAge > 60000) { // 1 minute
//	            return false;
//	        }
//	        
//	        // Check trader data freshness if it exists
//	        Boolean hasTraderData = (Boolean) dataQuality.get("hasTraderData");
//	        if (hasTraderData != null && hasTraderData) {
//	            Boolean isExpired = (Boolean) dataQuality.get("isExpired");
//	            if (isExpired != null && isExpired) {
//	                return false; // Trader data is expired
//	            }
//	        }
//	        
//	        // If we have recent instrument activity, consider the data reliable
//	        // even if we don't have specific trader field data (which likely means no traders)
//	        return true;
//	        
//	    } catch (Exception e) {
//	        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
//	            "Error evaluating trader data reliability: " + e.getMessage());
//	        return false;
//	    }
//	}

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
		//electronic venue lines versus aggregated GC lines arbitrage
		String id = best.getId();
		// Log that we're starting GC opportunity processing
        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
            "Processing GC opportunity - checking for potential GC-level arbitrage for id: " + id);

  	    double ask = best.getAsk();
	    Double gcBidPrice = gclvlResult.getBidPrice();
	    
        // Log that we found a GC bid price
        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
            String.format("Found GC bid price for arbitrage: %.6f", gcBidPrice));

	    double threshold = isAfterEightThirtyFive() ? 0.05 : 0.03;
	    // Skip if spread is insufficient for the time
	    if ((ask - threshold) < gcBidPrice) {
            ApplicationLogging.logAsync(LOGGER, Level.FINE, String.format(
                "Skipping GC trade for id:  " + id + " insufficient spread - ask=%.6f, gcBid=%.6f, threshold=%.2f, spread=%.6f",
                ask, gcBidPrice, threshold, ask - gcBidPrice));
	        return;
	    }
  	    

  	    String askSrc = best.getAskSrc();
  	    boolean AskIsAON = best.getAskIsAON();
	    String askVenueInstrument = instrumentSubscriber.getInstrumentFieldBySourceString(id, askSrc, AskIsAON);
        if (askVenueInstrument == null) {
           ApplicationLogging.logAsync(LOGGER, Level.WARNING, "No instrument mapping found for " + id);
            return;
        }
        
        String ask0Traders = venueTraderListener.getTraderInfo(askSrc, askVenueInstrument, "Ask0Traders");
     
        boolean askHasTraders = (ask0Traders != null && !ask0Traders.isEmpty());
        boolean skipSelfMatch = false;
        
        if (askHasTraders) {
      	  skipSelfMatch = true;
        }
//	    boolean skipSelfMatch = shouldSkipGCDueToSelfMatch(askSrc, askVenueInstrument, ask);

	      // Self-match detection
//	    if  (skipSelfMatch) {
//			ApplicationLogging.logAsync(LOGGER, Level.INFO, 
//	    		"Failed self-match prevention check");
//			// System.out.println("*******Self match prevention in effect here***** ask0Traders: " + ask0Traders + " for id: " + id);
//	    	return;
//	      }
	    
	    double orderAskSize = best.getAskSize();
	    String askTrader = getTraderForVenue(askSrc);

	    String ordertypeAsk = null;

	    ordertypeAsk = "FAS";
//		if ((AskIsAON && askSrc.equals("DEALERWEB_REPO")) || askSrc.equals("DEALERWEB_REPO")) {
//			ordertypeAsk = "FAS";
//		} else if (AskIsAON) {
//			ordertypeAsk = "FOK";
//		} else {
//			ordertypeAsk = "FAK";
//		}
		
		if (LOGGER.isLoggable(Level.INFO)) {
             ApplicationLogging.logAsync(LOGGER, Level.WARNING, "### GC BUY ORDER BEING SENT: src: " + askSrc + ", trader:  " + askTrader + ", venue:  " + askVenueInstrument + ", direction:  Buy, size:  " + orderAskSize + 
                ", prc:  " + ask + ", Limit, ordertype: " + ordertypeAsk + ", IsAON: " + AskIsAON);
       }
      
       System.out.println("### GC BUY ORDER BEING SENT: src: " + askSrc + ", trader:  " + askTrader + ", venue:  " + askVenueInstrument + ", direction:  Buy, size:  " + orderAskSize + 
                ", prc:  " + ask + ", Limit, ordertype: " + ordertypeAsk + ", IsAON: " + AskIsAON);
		
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
	        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
	            "Error logging market details: " + e.getMessage());
	    }
	}
	
/**
     * Simplified self-match prevention for GC opportunities (ask side only)
     */
//    private boolean shouldSkipGCDueToSelfMatch(String askSrc, String askVenueInstrument, double omAsk0Price) {
//        // Get trader information for ask side only
//        String ask0Traders = venueTraderListener.getTraderInfo(askSrc, askVenueInstrument, "Ask0Traders");
//        
//        Double vtlAsk0Price = null;
//        try {
//            vtlAsk0Price = venueTraderListener.getTraderInfoAsDouble(askSrc, askVenueInstrument, "Ask0");
//        } catch (Exception e) {
//            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
//                "Error retrieving Ask0 price from VenueTraderListener for GC opportunity: " + e.getMessage());
//            return true; // Conservative approach
//        }
//        
//        // Check price freshness by comparing VenueTraderListener price with OrderManagement price
//        boolean priceIsFresh = false;
//        String freshnessReason = "";
//        
//        if (vtlAsk0Price == null) {
//            freshnessReason = "Missing Ask0 price data from VenueTraderListener";
//        } else if (!vtlAsk0Price.equals(omAsk0Price)) {
//            freshnessReason = "Ask0 price mismatch: VTL=" + vtlAsk0Price + " vs OM=" + omAsk0Price;
//        } else {
//            priceIsFresh = true;
//            freshnessReason = "Ask0 price matches between VTL and OM";
//        }
//        
//        if (!priceIsFresh) {
//            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
//                "GC opportunity: Ask0 price data is not fresh - " + freshnessReason + 
//                " for venue " + askSrc + ". Cannot rely on trader data for self-match prevention.");
//            return true; // Skip trade due to stale price data
//        }
//        
//        // Get data quality information
//        Map<String, Object> askDataQuality = venueTraderListener.getDataQuality(askSrc, askVenueInstrument, "Ask0Traders");
//        
//        // Log data quality for debugging
//        if (LOGGER.isLoggable(Level.FINE)) {
//            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
//                "GC self-match check data quality - ASK: " + askDataQuality);
//        }
//        
//        // Check if we have reliable trader data
//        boolean askDataReliable = isTraderDataReliable(askDataQuality);
//        
//        if (!askDataReliable) {
//            // If we don't have reliable trader data, apply conservative approach
//            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
//                "GC opportunity: Unreliable ask trader data detected - applying self-match prevention. " +
//                "ASK reliable: " + askDataReliable + ", askSrc: " + askSrc);
//            return true;
//        } else {
//                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
//                    "GC opportunity: Proceeding as ask venue has reliable trader data: " + askSrc);
//        }
//        
//        // We have reliable data and fresh prices - proceed with normal self-match detection
//        boolean askHasTraders = (ask0Traders != null && !ask0Traders.isEmpty());
//        
//        if (askHasTraders) {
//            ApplicationLogging.logAsync(LOGGER, Level.INFO, 
//                "GC opportunity: Self-match detected - ASK traders: '" + ask0Traders + "'");
//            return true;
//        }
//        
//        // Enhanced logging for successful trades
//        ApplicationLogging.logAsync(LOGGER, Level.FINE, 
//            "GC opportunity: Self-match check passed - no traders detected and price is fresh. " +
//            "ASK traders: '" + ask0Traders + "' for " + askSrc + 
//            " (VTL Ask0=" + vtlAsk0Price + " vs OM Ask0=" + omAsk0Price + ")");
//        
//        return false; // No self-match detected
//    }
//
//
//	// Add this method to enable/disable continuous trading
//	public void setContiguousTradingEnabled(boolean enabled) {
//	    this.continuousTradingEnabled = enabled;
//      if (LOGGER.isLoggable(Level.INFO)) {
//	      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Continuous trading " + (enabled ? "enabled" : "disabled"));
//      }
//	}
  /**
   * Handles changes to the order chain.
   * For new records, sets up subscriptions to receive updates.
   */
  public void onSupply(MkvChain chain, String record, int pos,
      MkvChainAction action) {
        if (LOGGER.isLoggable(Level.FINE)) {
          ApplicationLogging.logAsync(LOGGER, Level.FINE, "Order chain update: chain=" + chain.getName() + 
                   ", record=" + record + 
                   ", pos=" + pos + 
                   ", action=" + action);
        }
    switch (action.intValue()) {
    case MkvChainAction.INSERT_code:
    case MkvChainAction.APPEND_code:
      // For new records, subscribe to receive updates
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "New record in chain, subscribing: " + record);
      subscribeToRecord(record);
      break;
    case MkvChainAction.SET_code:
      // For a SET action (chain is being completely redefined),
      // subscribe to all records in the chain
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Chain SET action, subscribing to all records");
      for (Iterator iter = chain.iterator(); iter.hasNext();) {
        String recName = (String) iter.next();
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Subscribing to chain record: " + recName);
        subscribeToRecord(recName);
      }
      break;
    case MkvChainAction.DELETE_code:
    ApplicationLogging.logAsync(LOGGER, Level.FINE, "Ignoring DELETE action for record: " + record);  
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
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Subscribing to record: " + record);
            
      // Get the record object
      MkvRecord rec = Mkv.getInstance().getPublishManager().getMkvRecord(record);
      
      // Subscribe to receive updates for the configured fields
      rec.subscribe(MarketDef.ORDER_FIELDS, this);
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Successfully subscribed to record: " + record);
    } catch (Exception e) {
      ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error subscribing to record: " + record + 
                   ", error: " + e.getMessage(), e);
      e.printStackTrace();
    }
  }
  
private void initializeHeartbeat() {
    heartbeatScheduler.scheduleAtFixedRate(() -> {
        try {
        	
            Map<String, Object> status = new HashMap<>();
            status.put("hostname", hostname);
            status.put("application", "OrderManagement");
            status.put("state", isSystemStopped ? "STOPPED" : "RUNNING");
            status.put("continuousTrading", continuousTradingEnabled);
            status.put("activeOrders", orders.size());

            // Gather trading statistics
            int instrumentCount = instrumentSubscriber != null ? 
                instrumentSubscriber.getInstrumentCount() : 0;
            status.put("instruments", instrumentCount);
            status.put("cached orders", orders.size());
            status.put("latest best prices", latestBestByInstrument.size());
            
            publishToRedis(HEARTBEAT_CHANNEL, status);
            
        } catch (Exception e) {
            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                "Error in heartbeat logging: " + e.getMessage(), e);
        }
    }, 30, 30, TimeUnit.SECONDS); // Run every 30 seconds
    
    // Add a shutdown hook to properly close the heartbeat scheduler
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Shutting down heartbeat scheduler");
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
    // Schedule a task to check pattern subscription status every minute
    scheduler.scheduleAtFixedRate(() -> {
        try {
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            MkvObject obj = pm.getMkvObject(PATTERN);
            
            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                MkvPattern pattern = (MkvPattern) obj;
                
                // Check if our listener is still subscribed to the pattern
                boolean currentlySubscribed = pattern.isSubscribed(depthListener, MarketDef.DEPTH_FIELDS);
                
                ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                    "Pattern subscription check: subscribed=" + currentlySubscribed + 
                    ", our state=" + isPatternSubscribed);
                
                // If we think we're subscribed but actually aren't, resubscribe
                if (isPatternSubscribed && !currentlySubscribed) {
                    ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                        "Pattern subscription lost, resubscribing");
                        
                    pattern.subscribe(MarketDef.DEPTH_FIELDS, depthListener);
                    
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                        "Successfully resubscribed to pattern");
                }
                // Update our tracking state
                isPatternSubscribed = currentlySubscribed;
            } else {
                // Pattern doesn't exist anymore, need to reset our state
                if (isPatternSubscribed) {
                    ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                        "Pattern not found but we thought we were subscribed");
                    isPatternSubscribed = false;
                }
                
                // Try to find and subscribe to the pattern again
                subscribeToPattern();
            }
            // Additional detailed logging
            if (depthListener != null) {
              Map<String, Object> healthStatus = depthListener.getHealthStatus();
              ApplicationLogging.logAsync(LOGGER, Level.INFO,
                  "Market data health status: " + healthStatus);
          }
        } catch (Exception e) {
            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                "Error in pattern subscription monitor: " + e.getMessage(), e);
        }
    }, 30, 30, TimeUnit.SECONDS);
}

  private void logStatistics() {
    AsyncLoggingManager manager = AsyncLoggingManager.getInstance();
    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
          "Logging stats: queued=" + manager.getMessagesQueued() + 
          ", processed=" + manager.getMessagesProcessed() + 
          ", overflows=" + manager.getQueueOverflows() + 
          ", queueSize=" + manager.getCurrentQueueSize());
}

//Production default configuration
public static void configureProductionLogging() {
   // Set global default
   ApplicationLogging.setGlobalLogLevel(Level.WARNING);
   
    // Critical business logic at INFO level
    ApplicationLogging.setLogLevels(Level.INFO,
        "com.iontrading.samples.advanced.orderManagement.OrderManagement",
        "com.iontrading.samples.advanced.orderManagement.DepthListener",  // Added for GC data tracking
        "com.iontrading.samples.advanced.orderManagement.MarketOrder"  // to see cancel order 
    );

   // Core components at WARNING in production 
   ApplicationLogging.setLogLevels(Level.WARNING,
    "com.iontrading.samples.advanced.orderManagement.AsyncLoggingManager",
    "com.iontrading.samples.advanced.orderManagement.InstrumentSubscriber",
    "com.iontrading.samples.advanced.orderManagement.MarketDef",
    "com.iontrading.samples.advanced.orderManagement.Best"
   );
}

//Development configuration
public static void configureDevelopmentLogging() {
    // Set global default
    ApplicationLogging.setGlobalLogLevel(Level.INFO);
   
    // Market data and GC processing at FINE
    ApplicationLogging.setLogLevels(Level.FINE,
        "com.iontrading.samples.advanced.orderManagement.OrderManagement",
        "com.iontrading.samples.advanced.orderManagement.DepthListener"  // For detailed GC data tracking
    );
   
    // Order processing at FINE
    ApplicationLogging.setLogLevels(Level.FINE,
        "com.iontrading.samples.advanced.orderManagement.MarketOrder"
    );
   
    // Infrastructure at INFO
    ApplicationLogging.setLogLevels(Level.INFO,
        "com.iontrading.samples.advanced.orderManagement.MarketDef",
        "com.iontrading.samples.advanced.orderManagement.Best",
        "com.iontrading.samples.advanced.orderManagement.InstrumentSubscriber"
    );
   
    // Keep async logging at WARNING to avoid overwhelming the log
    ApplicationLogging.setLogLevels(Level.WARNING,
        "com.iontrading.samples.advanced.orderManagement.AsyncLoggingManager"
    );
}

//Debugging configuration for specific issues
public static void configureDebuggingLogging(String focusComponent) {
   // Start with development configuration
   configureDevelopmentLogging();
   
   // Set focused component to FINEST
   ApplicationLogging.setLogLevel(
       "com.iontrading.samples.advanced.orderManagement." + focusComponent, 
       Level.FINEST
   );
   
   ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
       "Detailed debugging enabled for " + focusComponent + 
       " - log volume will be high!");
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
        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                "Checking for expired orders - currently tracking " + ordersCopy.size() + " orders");
        
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
                    ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                        "Order ID is null or empty for order: " + order);
                    continue;
                }
                // Only try to cancel if we have an order ID
                if (orderId != null && !orderId.isEmpty()) {
                    String traderId = getTraderForVenue(marketSource);
                    
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                        "Auto-cancelling expired order: reqId=" + order.getMyReqId() + 
                        ", orderId=" + orderId + 
                        ", age=" + (order.getAgeMillis() / 1000) + " seconds");
                    
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
                            orderId,
                            "Order expired after " + (order.getAgeMillis() / 1000) + " seconds"
                        );
                    }
                }
            }
        }
        
        if (expiredCount > 0) {
            ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                "Auto-cancelled " + expiredCount + " expired orders");
        }
    } catch (Exception e) {
        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
            "Error checking for expired orders: " + e.getMessage(), e);
    }
}

//private void scheduleAutoShutdown() {
//    LOGGER.info("Starting auto-shutdown monitor. Will shutdown at 5:00 PM local time.");
//    
//    shutdownScheduler.scheduleAtFixedRate(() -> {
//        // Check for time-based shutdown (5:00 PM)
//        LocalTime currentTime = LocalTime.now();
//        LocalTime shutdownTime = LocalTime.of(17, 0); // 5:00 PM
//        
//        if (currentTime.isAfter(shutdownTime)) {
//            LOGGER.warning("Current time " + currentTime + " is after scheduled shutdown time " + 
//                          shutdownTime + ". Shutting down...");
//            shutdown();
//        }
//    }, 0, 5, TimeUnit.MINUTES); // Check every five minutes
//}

public void initiateShutdown() {
    if (!isShuttingDown) {
        synchronized(this) {
            if (!isShuttingDown) {
                isShuttingDown = true;
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "External shutdown request received");
                shutdown();
            }
        }
    }
}

public void shutdown() {
    ApplicationLogging.logAsync(LOGGER, Level.INFO, "Shutdown sequence initiated");
    
    // 1. Disable continuous trading first to prevent new orders
    continuousTradingEnabled = false;
    ApplicationLogging.logAsync(LOGGER, Level.INFO, "Continuous trading disabled");
    
    // 2. Cancel all outstanding orders
    cancelAllOrders();
    
    // 3. Wait a moment to allow cancel requests to be processed
    try {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Waiting for cancel requests to be processed...");
        Thread.sleep(3000); // Give the market some time to process cancels
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
    
    // 4. Unsubscribe from patterns
    try {
        if (isPatternSubscribed && depthListener != null) {
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            MkvObject obj = pm.getMkvObject(PATTERN);
            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                ((MkvPattern) obj).unsubscribe(depthListener);
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Unsubscribed from market data pattern");
            }
        }
    } catch (Exception e) {
        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error unsubscribing from pattern: " + e.getMessage(), e);
    }
    
    // 5. Shutdown all executor services
    shutdownExecutor(heartbeatScheduler, "Heartbeat scheduler");
    shutdownExecutor(scheduler, "Market recheck scheduler");
    shutdownExecutor(orderExpirationScheduler, "Order expiration scheduler");  // Added this line
    shutdownExecutor(shutdownScheduler, "Auto-shutdown scheduler");
    
    // 6. Wait briefly to allow final log messages to be processed
    try {
        Thread.sleep(2000);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
    
    // 7. Shutdown AsyncLoggingManager
    AsyncLoggingManager.getInstance().shutdown();
    
    // 8. Finally, exit the application
    ApplicationLogging.logAsync(LOGGER, Level.INFO, "Shutdown sequence completed, exiting application");
    System.exit(0);
}

public boolean isShuttingDown() {
    return isShuttingDown;
}
	/**
	 * Helper method to shutdown an executor service
	 */
	private void shutdownExecutor(ExecutorService executor, String name) {
	    if (executor != null && !executor.isShutdown()) {
	        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Shutting down " + name);
	        executor.shutdown();
	        try {
	            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
	                executor.shutdownNow();
	                ApplicationLogging.logAsync(LOGGER, Level.WARNING, name + " did not terminate in time, forcing shutdown");
	            }
	        } catch (InterruptedException e) {
	            executor.shutdownNow();
	            Thread.currentThread().interrupt();
	            ApplicationLogging.logAsync(LOGGER, Level.WARNING, "Interrupted while shutting down " + name);
	        }
	    }
	}
	
	/**
	 * Cancels all outstanding orders currently tracked by this OrderManagement instance.
	 * This is useful for shutting down the application cleanly.
	 */
	public void cancelAllOrders() {
	    ApplicationLogging.logAsync(LOGGER, Level.INFO, "Canceling all outstanding orders");
	    
	    // Create a copy of the orders map to avoid concurrent modification
	    Map<Integer, MarketOrder> ordersCopy = new HashMap<>(orders);
	    int cancelCount = 0;
	    int errorCount = 0;
	    
	    // Issue cancel requests for each active order
	    for (MarketOrder order : ordersCopy.values()) {
	        try {
	            // Extract necessary information from the original order
	            String orderId = order.getOrderId();
	            String marketSource = order.getMarketSource();
	            
	            if (orderId == null || orderId.isEmpty()) {
	                // Skip orders that don't have a market-assigned ID yet
	                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
	                    "Cannot cancel order without market ID: reqId=" + order.getMyReqId());
	                continue;
	            }
	            
	            // Get the appropriate trader ID for this venue
	            String traderId = getTraderForVenue(marketSource);
	            
	            // Send the cancel request to the market
	            ApplicationLogging.logAsync(LOGGER, Level.INFO, 
	                "Canceling order: orderId=" + orderId + 
	                ", source=" + marketSource + 
	                ", trader=" + traderId);
	            
	            // Create a cancel request for this order
	            MarketOrder cancelOrder = MarketOrder.orderCancel(
	                marketSource, 
	                traderId, 
	                orderId, 
	                this
	            );
	            
	            if (cancelOrder != null) {
	                // Track the cancel request
	                addOrder(cancelOrder);
	                cancelCount++;
	            } else {
	                ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
	                    "Failed to create cancel request for orderId=" + orderId);
	                errorCount++;
	            }
	        } catch (Exception e) {
	            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
	                "Error canceling order: " + order.getMyReqId() + ", " + e.getMessage(), e);
	            errorCount++;
	        }
	    }
	    
	    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
	        "Cancel requests issued: " + cancelCount + 
	        ", errors: " + errorCount + 
	        ", total orders: " + ordersCopy.size());
	}
}