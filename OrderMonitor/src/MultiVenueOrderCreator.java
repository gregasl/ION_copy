import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iontrading.mkv.*;
import com.iontrading.mkv.events.*;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.qos.MkvQoS;
import com.iontrading.mkv.exceptions.MkvException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.PrintStream;
import java.io.OutputStream;
import java.util.Scanner;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import org.json.JSONObject;

/**
 * Multi-venue order creator with optimized logging
 * Supported venues: FENICS_USREPO, BTEC_REPO_US, DEALERWEB_REPO
 */
public class MultiVenueOrderCreator implements MkvFunctionCallListener, MkvPlatformListener, MkvRecordListener {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiVenueOrderCreator.class);
    
    // Venue configuration class
    private static class VenueConfig {
        final String marketSource;
        final String traderId;
        final String functionSuffix = "_VCMIOrderAdd181";
        final String cancelSuffix = "_VCMIOrderDel";
        
        VenueConfig(String marketSource, String traderId) {
            this.marketSource = marketSource;
            this.traderId = traderId;
        }
        
        String getOrderFunction() {
            return marketSource + functionSuffix;
        }
        
        String getCancelFunction() {
            return marketSource + cancelSuffix;
        }
    }
    
    // Venue configurations
    private static final Map<String, VenueConfig> VENUE_CONFIGS = new HashMap<>();
    static {
        VENUE_CONFIGS.put("FENICS_USREPO", new VenueConfig("FENICS_USREPO", "frosasl1"));
        VENUE_CONFIGS.put("BTEC_REPO_US", new VenueConfig("BTEC_REPO_US", "TEST2"));
        VENUE_CONFIGS.put("DEALERWEB_REPO", new VenueConfig("DEALERWEB_REPO", "asldevtrd1"));
    }
    
    // Active venue configuration
    private static VenueConfig activeVenueConfig = null;
    
    // 将 venueStatus 设为 public，以便 RedisMessageBridge 可以访问
    public static final Map<String, Boolean> venueStatus = new ConcurrentHashMap<>();
    
    // System configuration
    private static final String SYSTEM_USER = "evan_gerhard";
    private static final String APPLICATION_ID = "automatedMarketMaking";
    
    // Order configuration
    private static final String[] TEST_INSTRUMENTS = {"912797PQ4"};
    private static final String ORDER_VERB = "Buy";
    private static final double ORDER_QUANTITY = 100.0;
    private static final double DEFAULT_ORDER_PRICE = 4.0;
    private static final String ORDER_TYPE = "Limit";
    private static final String TIME_IN_FORCE = "FAS";
    
    // Dynamic pricing
    private static double dynamicOrderPrice = DEFAULT_ORDER_PRICE;
    
    // Instance variables
    private static int reqId = 0;
    private final int myReqId;
    private String orderId;
    private final long creationTimestamp;
    private byte errCode = (byte) 0;
    private String errStr = "";
    
    // Connection management
    private final CountDownLatch connectionLatch = new CountDownLatch(1);
    private final CountDownLatch loginCheckLatch = new CountDownLatch(1);
    private boolean isConnected = false;
    
    // Components
    private static DepthListener depthListener;
    private static EnhancedOrderManager orderManager;
    
    // Redis
    private static RedisMessageBridge redisMessageBridge;
    private static final String REDIS_HOST = "cacheuat";
    private static final int REDIS_PORT = 6379;
    private static final String MARKET_DATA_CHANNEL = "ION:ION_TEST:market_data";
    private static final String ORDER_COMMAND_CHANNEL = "ION:ION_TEST:ORDER_COMMANDS";
    private static final String ORDER_RESPONSE_CHANNEL = "ION:ION_TEST:ORDER_RESPONSES";
    
    // Order tracking
    private static final Map<String, OrderDetails> orderTracking = new ConcurrentHashMap<>();
    
    // Active orders monitoring
    private static final Set<String> activeOrderCusips = new HashSet<>();
    
    // Static log time tracking
    private static final Map<String, Long> staticLogTimeTracker = new ConcurrentHashMap<>();
    
    // Market data patterns
    private static final String ORDER_PATTERN = MarketDef.ORDER_PATTERN;
    private static final String[] ORDER_FIELDS = MarketDef.ORDER_FIELDS;
    private static final String LOGIN_PATTERN = MarketDef.LOGIN_PATTERN;
    private static final String[] LOGIN_FIELDS = MarketDef.LOGIN_FIELDS;
    private static final String INSTRUMENT_PATTERN = MarketDef.INSTRUMENT_PATTERN;
    private static final String[] INSTRUMENT_FIELDS = MarketDef.INSTRUMENT_FIELDS;
    private static final String DEPTH_PATTERN = MarketDef.DEPTH_PATTERN;
    private static final String[] DEPTH_FIELDS = MarketDef.DEPTH_FIELDS;
    
    /**
     * Order details tracking class
     */
    private static class OrderDetails {
        String orderId;
        final int reqId;
        final long timestamp;
        String status = "PENDING";
        double filledQty = 0.0;
        String errorMsg = "";
        final String instrumentId;
        final String venue;
        final String cusip;
        
        OrderDetails(String orderId, int reqId, String instrumentId, String venue, String cusip) {
            this.orderId = orderId;
            this.reqId = reqId;
            this.instrumentId = instrumentId;
            this.venue = venue;
            this.cusip = cusip;
            this.timestamp = System.currentTimeMillis();
        }
        
        OrderDetails(String orderId, int reqId, String instrumentId, String venue) {
            this(orderId, reqId, instrumentId, venue, instrumentId);
        }
    }
    
    /**
     * Enhanced order manager with clean logging
     */
    private static class EnhancedOrderManager implements IOrderManager {
        private final ConcurrentHashMap<String, Best> bestPriceCache = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Long> priceUpdateTime = new ConcurrentHashMap<>();
        
        private final ConcurrentHashMap<String, GCBest> gcCashCache = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, GCBest> gcRegCache = new ConcurrentHashMap<>();
        
        private static final Set<String> TARGET_VENUES = new HashSet<>(Arrays.asList(
            "FENICS_USREPO", "BTEC_REPO_US", "DEALERWEB_REPO"
        ));
        
        @Override
        public void orderDead(MarketOrder order) {
            LOGGER.info("Order terminated: {}", order.getOrderId());
        }
        
        @Override
        public MarketOrder addOrder(String MarketSource, String TraderId, String instrId, 
                String verb, double qty, double price, String type, String tif) {
            return null;
        }
        
        @Override
        public void best(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {
            if (best != null && best.getId() != null) {
                String instrumentId = best.getId();
                
                // Silently update cache
                bestPriceCache.put(instrumentId, best);
                priceUpdateTime.put(instrumentId, System.currentTimeMillis());
                
                if (gcBestCash != null) {
                    gcCashCache.put(instrumentId, gcBestCash);
                }
                if (gcBestREG != null) {
                    gcRegCache.put(instrumentId, gcBestREG);
                }
            }
        }
        
        private boolean hasActiveOrderForInstrument(String instrumentId) {
            for (OrderDetails order : orderTracking.values()) {
                if ("PENDING".equals(order.status) || "SUBMITTED".equals(order.status) || 
                    "FILLED".equals(order.status) || "PARTIALLY_FILLED".equals(order.status)) {
                    if (instrumentId.contains(order.cusip) || order.cusip.contains(instrumentId)) {
                        return true;
                    }
                }
            }
            
            for (String cusip : activeOrderCusips) {
                if (instrumentId.contains(cusip) || cusip.contains(instrumentId)) {
                    return true;
                }
            }
            
            return false;
        }
        
        @Override
        public void mapOrderIdToReqId(String orderId, int reqId) {
            // Silent operation
        }
        
        @Override
        public void removeOrder(int reqId) {
            // Silent operation
        }
        
        @Override
        public String getApplicationId() {
            return APPLICATION_ID;
        }
        
        public Best getBestPrice(String instrumentId) {
            Best best = bestPriceCache.get(instrumentId);
            if (best != null) {
                return best;
            }
            
            for (Map.Entry<String, Best> entry : bestPriceCache.entrySet()) {
                if (entry.getKey().contains(instrumentId) || instrumentId.contains(entry.getKey())) {
                    return entry.getValue();
                }
            }
            
            return null;
        }
        
        public Map<String, Best> getAllPricesForCusip(String cusip) {
            Map<String, Best> results = new HashMap<>();
            
            for (Map.Entry<String, Best> entry : bestPriceCache.entrySet()) {
                String key = entry.getKey();
                if (key.contains(cusip)) {
                    results.put(key, entry.getValue());
                }
            }
            
            return results;
        }
        
        public long getPriceUpdateTime(String instrumentId) {
            Long time = priceUpdateTime.get(instrumentId);
            return time != null ? time : 0L;
        }
        
        public boolean isPriceStale(String instrumentId) {
            long updateTime = getPriceUpdateTime(instrumentId);
            if (updateTime == 0) return true;

            return (System.currentTimeMillis() - updateTime) > 100000; // 100 seconds
        }
    }
    
    public MultiVenueOrderCreator(int reqId) {
        this.myReqId = reqId;
        this.creationTimestamp = System.currentTimeMillis();
    }
    
    private static VenueConfig selectBestVenue() {
        for (Map.Entry<String, VenueConfig> entry : VENUE_CONFIGS.entrySet()) {
            String venueName = entry.getKey();
            VenueConfig venue = entry.getValue();
            Boolean isActive = venueStatus.get(venueName);
            
            if (Boolean.TRUE.equals(isActive)) {
                return venue;
            }
        }
        
        return VENUE_CONFIGS.values().iterator().next();
    }
    
    private static String getPriceSourceReflection(Best bestPrice, String methodName) {
        try {
            Class<?> bestClass = bestPrice.getClass();
            java.lang.reflect.Method method = bestClass.getMethod(methodName);
            String source = (String) method.invoke(bestPrice);
            return source != null && !source.isEmpty() ? source : "Unknown";
        } catch (Exception e) {
            return "Unknown";
        }
    }
    
    public static boolean cancelOrder(String orderId, String venue) {
        if (orderId == null || orderId.isEmpty() || orderId.startsWith("PENDING")) {
            LOGGER.warn("Invalid order ID for cancellation: {}", orderId);
            return false;
        }
        
        // venue 现在是必需的!
        if (venue == null || venue.trim().isEmpty()) {
            LOGGER.error("Venue is required for order cancellation");
            return false;
        }
        
        VenueConfig config = VENUE_CONFIGS.get(venue);
        if (config == null) {
            LOGGER.error("Unknown venue: {}", venue);
            return false;
        }
        
        MkvPublishManager pm = Mkv.getInstance().getPublishManager();
        String functionName = config.getCancelFunction();
        
        try {
            MkvFunction fn = pm.getMkvFunction(functionName);
            
            if (fn == null) {
                LOGGER.error("Cancel function not found: {}", functionName);
                return false;
            }
            
            reqId++;
            final int cancelReqId = reqId;
            
            MkvFunctionCallListener cancelListener = new MkvFunctionCallListener() {
                @Override
                public void onResult(MkvFunctionCallEvent event, MkvSupply supply) {
                    try {
                        String result = supply.getString(supply.firstIndex());
                        LOGGER.info("Cancel response: {}", result);
                        
                        if (result.contains("OK")) {
                            LOGGER.info("Order {} cancelled successfully", orderId);
                            updateOrderStatus(orderId, "CANCELLED");
                        } else {
                            LOGGER.error("Cancel request failed: {}", result);
                        }
                    } catch (Exception e) {
                        LOGGER.error("Error processing cancel response: ", e);
                    }
                }
                
                @Override
                public void onError(MkvFunctionCallEvent event, byte errCode, String errStr) {
                    LOGGER.error("Cancel error - Code: {} | Message: {}", errCode, errStr);
                }
            };
            
            String freeText = MarketDef.getFreeText(String.valueOf(cancelReqId), APPLICATION_ID);
            
            MkvSupply args = MkvSupplyFactory.create(new Object[] {
                config.traderId,
                orderId,
                freeText
            });

            fn.call(args, cancelListener);
            
            return true;
            
        } catch (Exception e) {
            LOGGER.error("Error cancelling order: ", e);
            return false;
        }
    }
    
    private static void updateOrderStatus(String orderId, String status) {
        for (OrderDetails details : orderTracking.values()) {
            if (orderId.equals(details.orderId)) {
                details.status = status;
                
                if ("FILLED".equals(status) || "CANCELLED".equals(status) || "REJECTED".equals(status)) {
                    activeOrderCusips.remove(details.cusip);
                    LOGGER.info("Removed {} from active monitoring (Status: {})", details.cusip, status);
                    printActiveMonitoring();
                    LOGGER.info("----");
                } else if ("SUBMITTED".equals(status)) {
                    scheduleOrderCleanup(details, 60000);
                }
                break;
            }
        }
    }
    
    private static void scheduleOrderCleanup(OrderDetails orderDetails, long timeoutMs) {
        Thread cleanupThread = new Thread(() -> {
            try {
                Thread.sleep(timeoutMs);
                
                if ("SUBMITTED".equals(orderDetails.status)) {
                    LOGGER.info("Auto-cleanup: Order {} still SUBMITTED after {} minutes, assuming filled", 
                        orderDetails.orderId, timeoutMs / 60000);
                    
                    orderDetails.status = "ASSUMED_FILLED";
                    activeOrderCusips.remove(orderDetails.cusip);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }
    
    /**
     * 显示简洁的活跃监控状态
     */
    private static void printActiveMonitoring() {
        if (activeOrderCusips.isEmpty()) {
            LOGGER.info("Active monitoring: None");
        } else {
            LOGGER.info("Active monitoring: {} CUSIPs [{}]", 
                activeOrderCusips.size(), 
                String.join(", ", activeOrderCusips));
        }
    }

    /**
     * Configure logging levels for clean output
     */
    private static void configureLogging() {
        try {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            
            // Main application - INFO
            loggerContext.getLogger("MultiVenueOrderCreator").setLevel(Level.INFO);
            loggerContext.getLogger(MultiVenueOrderCreator.class).setLevel(Level.INFO);
            
            // Order management - INFO
            loggerContext.getLogger("OrderManager").setLevel(Level.INFO);
            loggerContext.getLogger("EnhancedOrderManager").setLevel(Level.INFO);
            
            // Redis - INFO
            loggerContext.getLogger("RedisMessageBridge").setLevel(Level.INFO);
            loggerContext.getLogger(RedisMessageBridge.class).setLevel(Level.INFO);
            
            // Market data components - WARN only
            loggerContext.getLogger("DepthListener").setLevel(Level.WARN);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.DepthListener").setLevel(Level.WARN);
            loggerContext.getLogger("Instrument").setLevel(Level.WARN);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.Instrument").setLevel(Level.WARN);
            loggerContext.getLogger("Best").setLevel(Level.WARN);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.Best").setLevel(Level.WARN);
            loggerContext.getLogger("GCBest").setLevel(Level.WARN);
            loggerContext.getLogger("GCBestManager").setLevel(Level.WARN);
            
            // Unified pricing model - WARN
            loggerContext.getLogger("UnifiedPricingModel").setLevel(Level.WARN);
            
            // MKV platform - WARN
            loggerContext.getLogger("com.iontrading.mkv").setLevel(Level.WARN);
            loggerContext.getLogger("MkvConnection").setLevel(Level.WARN);
            
            // Order repository - WARN
            loggerContext.getLogger("OrderRepository").setLevel(Level.WARN);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.OrderRepository").setLevel(Level.WARN);
            
            // Root logger - INFO
            loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).setLevel(Level.INFO);
            
        } catch (Exception e) {
            LOGGER.warn("Unable to configure logging levels: {}", e.getMessage());
        }
    }
    
    /**
     * Log order operation with clean format
     */
    private static void logOrderOperation(String operation, OrderDetails details, double price) {
        LOGGER.info("--- {} Order [{}] ---", operation, LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        LOGGER.info("  Order ID: {}", details.orderId);
        LOGGER.info("  Venue: {}", details.venue);
        LOGGER.info("  Instrument: {}", details.instrumentId);
        LOGGER.info("  CUSIP: {}", details.cusip);
        LOGGER.info("  Price: {}", String.format("%.2f", price));
        LOGGER.info("  Status: {}", details.status);
        LOGGER.info("---");
        LOGGER.info("");
    }
    
    /**
     * Main application entry point
     */
    public static void main(String[] args) {
        // Configure logging first
        configureLogging();
        
        LOGGER.info("========== SYSTEM STARTUP ==========");
        
        // Build configuration string in one line
        StringBuilder configInfo = new StringBuilder("Venue: ");
        boolean first = true;
        for (Map.Entry<String, VenueConfig> entry : VENUE_CONFIGS.entrySet()) {
            if (!first) {
                configInfo.append(" | ");
            }
            configInfo.append(entry.getKey()).append("->").append(entry.getValue().traderId);
            first = false;
        }
        LOGGER.info(configInfo.toString());
        
        VenueConfig selectedVenue = null;
        int updateCounter = 0;
        
        try {
            MultiVenueOrderCreator mainInstance = new MultiVenueOrderCreator(0);
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("========== SHUTDOWN INITIATED ==========");
                if (redisMessageBridge != null) {
                    try {
                        redisMessageBridge.stop();
                        LOGGER.info("Redis bridge stopped");
                    } catch (Exception e) {
                        LOGGER.error("Error stopping Redis bridge: ", e);
                    }
                }

                try {
                    Mkv.stop();
                    LOGGER.info("MKV platform stopped");
                } catch (Exception e) {
                    LOGGER.error("Error stopping MKV platform: ", e);
                }
                
                LOGGER.info("========== SHUTDOWN COMPLETE ==========");
            }));
            
            // Initialize Redis
            initializeRedisBridge();
            
            MkvQoS qos = new MkvQoS();
            if (args.length > 0) {
                qos.setArgs(args);
            }
            
            qos.setPlatformListeners(new MkvPlatformListener[] { mainInstance });
            Mkv.start(qos);
            
            // Wait for connection
            boolean connected = mainInstance.connectionLatch.await(40, TimeUnit.SECONDS);
            if (!connected || !mainInstance.isConnected) {
                LOGGER.error("Connection timeout - unable to connect to platform");
                return;
            }
            
            // Get publish manager
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            if (pm == null) {
                LOGGER.error("Unable to obtain MkvPublishManager");
                return;
            }
            
            // Check login status
            mainInstance.subscribeToLoginStatus(pm);
            mainInstance.loginCheckLatch.await(5, TimeUnit.SECONDS);
            
            activeVenueConfig = null;
            
            // Initialize DepthListener
            PrintStream originalOut = System.out;
            PrintStream originalErr = System.err;
            PrintStream nullStream = new PrintStream(new OutputStream() {
                public void write(int b) {}
            });
            
            try {
                System.setOut(nullStream);
                System.setErr(nullStream);
                
                orderManager = new EnhancedOrderManager();
                depthListener = new DepthListener(orderManager);
                
                MkvPattern instrumentPattern = pm.getMkvPattern(INSTRUMENT_PATTERN);
                if (instrumentPattern != null) {
                    instrumentPattern.subscribe(INSTRUMENT_FIELDS, depthListener);
                }
                
                MkvPattern depthPattern = pm.getMkvPattern(DEPTH_PATTERN);
                if (depthPattern != null) {
                    depthPattern.subscribe(DEPTH_FIELDS, depthListener);
                }
                
                System.setOut(originalOut);
                System.out.print("Loading market data");
                
                for (int i = 0; i < 15; i++) {
                    Thread.sleep(1000);
                    System.out.print(".");
                    System.out.flush();
                }
                
                System.out.println(" Complete");
                Thread.sleep(2000);
                
            } finally {
                System.setOut(originalOut);
                System.setErr(originalErr);
            }
            
            LOGGER.info("Market data loaded: {} instruments", 
                depthListener != null ? depthListener.getInstrumentCount() : "Unknown");

            LOGGER.info("========== SYSTEM READY ==========");
            try {
                while (true) {
                    Thread.sleep(30000);
                    updateCounter++;
                    
                    // Monitor Redis connection silently
                    if (redisMessageBridge != null) {
                        boolean redisHealthy = redisMessageBridge.testConnection();
                        if (!redisHealthy) {
                            LOGGER.warn("Redis connection lost, attempting to reconnect...");
                            try {
                                redisMessageBridge.stop();
                                Thread.sleep(2000);
                                initializeRedisBridge();
                            } catch (Exception e) {
                                LOGGER.error("Redis reconnection failed: ", e);
                            }
                        }
                    }
                    
                    // Market summary every 10 minutes
                    if (orderManager != null && updateCounter % 20 == 0) { 
                        int activeOrders = 0;
                        for (OrderDetails order : orderTracking.values()) {
                            if ("PENDING".equals(order.status) || "SUBMITTED".equals(order.status)) {
                                activeOrders++;
                            }
                        }
                        
                        if (activeOrders > 0) {
                            LOGGER.info("========== MARKET STATUS SUMMARY ==========");
                            LOGGER.info("Active orders: {}", activeOrders);
                            LOGGER.info("Monitored CUSIPs: {}", activeOrderCusips.size());
                            LOGGER.info("==========================================");
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.info("Application interrupted, shutting down...");
                Thread.currentThread().interrupt();
            }
            
        } catch (Exception e) {
            LOGGER.error("Application error: ", e);
            
            if (redisMessageBridge != null) {
                try {
                    redisMessageBridge.stop();
                } catch (Exception ex) {
                    LOGGER.error("Failed to stop Redis bridge: ", ex);
                }
            }
            
            Mkv.stop();
            LOGGER.info("Application terminated due to error");
            System.exit(1);
        }
    }
    
    private void subscribeToLoginStatus(MkvPublishManager pm) {
        try {
            String loginRecordName = LOGIN_PATTERN + SYSTEM_USER;
            
            MkvObject obj = pm.getMkvObject(loginRecordName);
            
            if (obj != null && obj.getMkvObjectType() == MkvObjectType.RECORD) {
                MkvRecord loginRecord = (MkvRecord) obj;
                loginRecord.subscribe(LOGIN_FIELDS, this);
                checkLoginStatus(loginRecord);
            } else {
                MkvObject patternObj = pm.getMkvObject(LOGIN_PATTERN);
                
                if (patternObj != null && patternObj.getMkvObjectType() == MkvObjectType.PATTERN) {
                    MkvPattern loginPattern = (MkvPattern) patternObj;
                    loginPattern.subscribe(LOGIN_FIELDS, this);
                } else {
                    pm.addPublishListener(new MkvPublishListener() {
                        @Override
                        public void onPublish(MkvObject mkvObject, boolean published, boolean isDownloadComplete) {
                            if (published && mkvObject.getName().equals(LOGIN_PATTERN) &&
                                mkvObject.getMkvObjectType() == MkvObjectType.PATTERN) {
                                try {
                                    ((MkvPattern) mkvObject).subscribe(LOGIN_FIELDS, MultiVenueOrderCreator.this);
                                } catch (Exception e) {
                                    LOGGER.error("Error subscribing to pattern: ", e);
                                }
                            }
                        }
                        
                        @Override
                        public void onPublishIdle(String component, boolean start) {}
                        
                        @Override
                        public void onSubscribe(MkvObject obj) {}
                    });
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error subscribing to login status: ", e);
        } finally {
            new Thread(() -> {
                try {
                    Thread.sleep(3000);
                    loginCheckLatch.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }
    
    private void checkLoginStatus(MkvRecord record) {
        try {
            for (int i = 0; i < 8; i++) {
                String src = record.getValue("Src" + i).getString();
                String status = record.getValue("TStatusStr" + i).getString();
                
                if (src != null && status != null && VENUE_CONFIGS.containsKey(src)) {
                    venueStatus.put(src, "On".equals(status));
                    
                    if ("On".equals(status)) {
                        OrderRepository.getInstance().addVenueActive(src, true);
                    } else {
                        OrderRepository.getInstance().addVenueActive(src, false);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error checking login status: ", e);
        }
    }
    
    /**
     * 这个方法已经不再使用  (保留用于兼容性) 
     */
    public static MultiVenueOrderCreator createOrder(String instrumentId, String originalCusip) {
        LOGGER.error("createOrder method is deprecated. Use createOrderFromRedis with all required parameters.");
        return null;
    }
    
    /**
     * 这个方法已经不再使用  (保留用于兼容性) 
     */
    private static MultiVenueOrderCreator createOrderWithFunction(MkvFunction fn, 
           String instrumentId, String originalCusip) {
       LOGGER.error("createOrderWithFunction method is deprecated.");
       return null;
    }
   
   /**
    * 从 Redis 创建订单
    */
    public static boolean createOrderFromRedis(String cusip, String side, double quantity, double price, String venue) {
        if (depthListener == null) {
            LOGGER.error("DepthListener not initialized, cannot create order from Redis");
            return false;
        }
        
        // 验证必需参数
        if (venue == null || venue.trim().isEmpty()) {
            LOGGER.error("Venue is required but not provided");
            return false;
        }
        
        if (price <= 0) {
            LOGGER.error("Price must be positive, received: {}", price);
            return false;
        }
        
        try {
            // 验证场地是否存在
            if (!VENUE_CONFIGS.containsKey(venue)) {
                LOGGER.error("Unknown venue specified: {}", venue);
                return false;
            }
            
            VenueConfig targetVenue = VENUE_CONFIGS.get(venue);
            
            // 保存原始场地配置
            VenueConfig originalVenue = activeVenueConfig;
            activeVenueConfig = targetVenue;
            
            try {
                // 查找对应的原生instrument ID
                String nativeId = findInstrumentMapping(cusip, venue);
                if (nativeId == null) {
                    LOGGER.error("No instrument mapping found for CUSIP: {} on venue: {}", 
                        cusip, venue);
                    
                    // 提供诊断信息
                    LOGGER.info("Running diagnostic for mapping failure...");
                    diagnoseInstrumentMappings(cusip);
                    
                    return false;  // 映射失败，不添加到监控
                }
                
                // 直接使用用户提供的价格
                double finalPrice = Math.round(price * 100.0) / 100.0;
                
                // 创建订单
                MultiVenueOrderCreator orderCreator = createParameterizedOrder(
                    nativeId, cusip, side, quantity, finalPrice);
                
                // 只有在订单创建成功后才添加到活跃监控
                if (orderCreator != null) {
                    activeOrderCusips.add(cusip);
                    return true;
                } else {
                    LOGGER.error("Failed to create order for CUSIP: {}", cusip);
                    return false;
                }
                
            } finally {
                // 恢复原始场地配置
                activeVenueConfig = originalVenue;
            }
            
        } catch (Exception e) {
            LOGGER.error("Error creating order from Redis: ", e);
            return false;
        }
    }
   
   /**
    * 将 findInstrumentMapping 设为 public static 以便 RedisMessageBridge 访问
    */
   public static String findInstrumentMapping(String cusip, String venue) {
       try {
           // 直接使用传入的venue参数获取配置
           if (venue == null || venue.trim().isEmpty()) {
               LOGGER.error("Venue parameter is required for mapping");
               return null;
           }
           
           VenueConfig targetVenue = VENUE_CONFIGS.get(venue);
           if (targetVenue == null) {
               LOGGER.error("Unknown venue for mapping: {}", venue);
               return null;
           }

           // 获取所有instrument数据
           java.lang.reflect.Field f = DepthListener.class.getDeclaredField("instrumentData");
           f.setAccessible(true);
           Map<String, ?> data = (Map<String, ?>) f.get(depthListener);
           
           // 查找包含CUSIP的实际instrument ID
           for (String instrumentId : data.keySet()) {
               if (instrumentId.contains(cusip)) {
                   LOGGER.debug("Testing mapping: instrumentId={}, sourceId={}", 
                       instrumentId, targetVenue.marketSource);
                   
                   // 修正参数顺序：instrumentId, sourceId, isAON
                   String result = depthListener.getInstrumentFieldBySourceString(
                       instrumentId,               // instrumentId (如 "912797PQ4_07_22_25_07_23_25_Fixed")
                       targetVenue.marketSource,   // sourceId (如 "FENICS_USREPO")
                       false                       // isAON
                   );
                   
                   if (result != null) {
                       LOGGER.info("Found instrument mapping: {} -> {} for venue {}", 
                           instrumentId, result, venue);
                       return result;  // 找到第一个有效映射就立即返回，避免重复日志
                   }
               }
           }
           
           LOGGER.warn("No instrument mapping found for CUSIP: {} on venue: {}", cusip, venue);
           return null;
           
       } catch (Exception e) {
           LOGGER.error("Error finding instrument mapping: ", e);
           return null;
       }
   }
   
   /**
    * 列出所有包含指定CUSIP的instrument
    */
   private static void diagnoseInstrumentMappings(String cusip) {
       try {
           java.lang.reflect.Field f = DepthListener.class.getDeclaredField("instrumentData");
           f.setAccessible(true);
           Map<String, ?> data = (Map<String, ?>) f.get(depthListener);
           
           LOGGER.info("=== INSTRUMENT MAPPING DIAGNOSIS for {} ===", cusip);
           LOGGER.info("Total instruments loaded: {}", data.size());
           
           int matchCount = 0;
           for (String instrumentId : data.keySet()) {
               if (instrumentId.contains(cusip)) {
                   matchCount++;
                   LOGGER.info("Found instrument containing {}: {}", cusip, instrumentId);
                   
                   // 尝试获取这个instrument的所有venue映射
                   for (String venue : VENUE_CONFIGS.keySet()) {
                       VenueConfig config = VENUE_CONFIGS.get(venue);
                       String mapping = depthListener.getInstrumentFieldBySourceString(
                           instrumentId, config.marketSource, false);
                       if (mapping != null) {
                           LOGGER.info("  -> {} maps to: {}", venue, mapping);
                       }
                   }
               }
           }
           
           if (matchCount == 0) {
               LOGGER.warn("No instruments found containing CUSIP: {}", cusip);
               LOGGER.info("Sample instrument IDs (first 20):");
               int count = 0;
               for (String instrumentId : data.keySet()) {
                   LOGGER.info("  {}", instrumentId);
                   if (++count >= 20) break;
               }
           }
           
           LOGGER.info("=== END DIAGNOSIS ===");
           
       } catch (Exception e) {
           LOGGER.error("Error in instrument mapping diagnosis: ", e);
       }
   }
   
   /**
    * 创建参数化订单 直接使用传入的价格
    */
   private static MultiVenueOrderCreator createParameterizedOrder(String instrumentId, String originalCusip, 
           String side, double quantity, double price) {
       reqId++;
       
       if (activeVenueConfig == null) {
           LOGGER.error("No active venue configured");
           return null;
       }
       
       // 确保价格有效
       if (price <= 0) {
           LOGGER.error("Invalid price: {}", price);
           return null;
       }
       
       // 四舍五入到2位小数
       double finalPrice = Math.round(price * 100.0) / 100.0;
       
       MkvPublishManager pm = Mkv.getInstance().getPublishManager();
       String functionName = activeVenueConfig.getOrderFunction();
       
       try {
           MkvFunction fn = pm.getMkvFunction(functionName);
           
           if (fn != null) {
               return createOrderWithParameterizedFunction(fn, instrumentId, originalCusip, side, quantity, finalPrice);
           } else {
               LOGGER.error("Order function not found: {}", functionName);
               return null;
           }
       } catch (Exception e) {
           LOGGER.error("Error creating parameterized order: ", e);
           return null;
       }
   }
   
   private static MultiVenueOrderCreator createOrderWithParameterizedFunction(MkvFunction fn, 
           String instrumentId, String originalCusip, String side, double quantity, double price) {
       try {
           String freeText = MarketDef.getFreeText(String.valueOf(reqId), APPLICATION_ID);
           
           MultiVenueOrderCreator order = new MultiVenueOrderCreator(reqId);
           
           // 确保正确传递 venue 信息
           String venueName = activeVenueConfig != null ? activeVenueConfig.marketSource : "UNKNOWN";
           OrderDetails details = new OrderDetails("PENDING_" + reqId, reqId, 
               originalCusip, venueName, originalCusip);
           orderTracking.put(String.valueOf(reqId), details);
           
           MkvSupply args = MkvSupplyFactory.create(new Object[] {
               activeVenueConfig.traderId,
               instrumentId,
               side,
               Double.valueOf(price),
               Double.valueOf(quantity),
               Double.valueOf(quantity),
               ORDER_TYPE,
               TIME_IN_FORCE,
               Integer.valueOf(0),
               Integer.valueOf(0),
               "",
               freeText,
               Integer.valueOf(0),
               "",
               Double.valueOf(0)
           });
           
           fn.call(args, order);
           
           return order;
           
       } catch (Exception e) {
           LOGGER.error("Error creating parameterized order: ", e);
           return null;
       }
   }
   
   // MkvRecordListener implementation
   @Override
   public void onFullUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
       try {
           String recordName = record.getName();
           
           if (recordName.contains("CM_LOGIN") && recordName.contains(SYSTEM_USER)) {
               checkLoginStatus(record);
               loginCheckLatch.countDown();
           }
           
       } catch (Exception e) {
           LOGGER.error("Error processing record update: ", e); 
       }
   }
   
   @Override
   public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
       onFullUpdate(record, supply, isSnapshot);
   }
   
   // MkvFunctionCallListener implementation
   @Override
   public void onResult(MkvFunctionCallEvent event, MkvSupply supply) {
       try {
           String result = supply.getString(supply.firstIndex());
           
           if (result.contains("OK")) {
               String extractedOrderId = extractOrderId(result);
               
               OrderDetails details = orderTracking.get(String.valueOf(myReqId));
               if (details != null) {
                   details.status = "SUBMITTED";
                   if (extractedOrderId != null) {
                       details.orderId = extractedOrderId;
                   }
                   
                   LOGGER.info("Order response: {} (SUBMITTED)", 
                       extractedOrderId != null ? extractedOrderId : "PENDING_" + myReqId);
                   printActiveMonitoring();
                   LOGGER.info("----");
                   
                   publishOrderResponseToRedis(
                       extractedOrderId != null ? extractedOrderId : "PENDING_" + myReqId,
                       "SUBMITTED", 
                       "Order submitted successfully", 
                       details.venue  // 使用 OrderDetails 中存储的 venue
                   );
               }
           } else {
               OrderDetails details = orderTracking.get(String.valueOf(myReqId));
               if (details != null) {
                   details.status = "FAILED";
                   details.errorMsg = result;
                   
                   // 从活跃监控中移除失败的CUSIP
                   activeOrderCusips.remove(details.cusip);
                   LOGGER.info("Removed {} from active monitoring due to order failure", details.cusip);
                   printActiveMonitoring();
                   
                   // 提取详细的错误信息
                   String detailedError = extractDetailedError(result);
                   
                   publishOrderResponseToRedis(
                       "FAILED_" + myReqId,
                       "FAILED", 
                       detailedError,  // 使用详细错误
                       details.venue
                   );
               }
               
               if (result.contains("101") || result.contains("not logged in")) {
                   LOGGER.error("Error 101: User not logged in to venue {}", activeVenueConfig.marketSource);
               } else if (result.contains("Price Exceeds Current Price Band")) {
                   LOGGER.error("Price Band Error: Order price outside allowed range");
               } else if (result.contains("Invalid Instrument")) {
                   LOGGER.error("Invalid Instrument: CUSIP may not be valid for this venue");
               } else {
                   LOGGER.error("Order submission failed: {}", result);
               }
           }
           
       } catch (Exception e) {
           LOGGER.error("Error processing order response: ", e);
       }
   }
   
   /**
    * 提取详细的错误信息
    */
   private String extractDetailedError(String result) {
       if (result.contains("101") || result.contains("not logged in")) {
           return "Trader not logged in to venue " + (activeVenueConfig != null ? activeVenueConfig.marketSource : "UNKNOWN");
       } else if (result.contains("Price Exceeds Current Price Band")) {
           return "Price outside allowed range for this instrument";
       } else if (result.contains("Invalid Instrument")) {
           return "Invalid instrument ID or CUSIP for this venue";
       } else if (result.contains("Insufficient")) {
           return "Insufficient inventory or credit";
       } else {
           // 尝试提取具体的错误信息
           int errorStart = result.indexOf("Error:");
           if (errorStart != -1) {
               return result.substring(errorStart);
           }
           return "Order failed: " + result;
       }
   }
   
   private String extractOrderId(String result) {
       try {
           int idStart = result.indexOf("-Id {");
           if (idStart != -1) {
               idStart += 5;
               int idEnd = result.indexOf("}", idStart);
               if (idEnd != -1) {
                   return result.substring(idStart, idEnd);
               }
           }
       } catch (Exception e) {
           LOGGER.error("Error extracting order ID: ", e);
       }
       return null;
   }
   
   @Override
   public void onError(MkvFunctionCallEvent event, byte errCode, String errStr) {
       this.errCode = errCode;
       this.errStr = errStr;
       LOGGER.error("Order error - Code: {} | Message: {}", errCode, errStr);
       
       OrderDetails details = orderTracking.get(String.valueOf(myReqId));
       if (details != null) {
           details.status = "ERROR";
           details.errorMsg = String.format("Code %d: %s", errCode, errStr);
           
           // 从活跃监控中移除错误的CUSIP
           activeOrderCusips.remove(details.cusip);
           LOGGER.info("Removed {} from active monitoring due to order error", details.cusip);
           printActiveMonitoring();
           
           // 发送详细的错误信息到Redis
           publishOrderResponseToRedis(
               "ERROR_" + myReqId,
               "ERROR", 
               String.format("MKV Error - Code %d: %s", errCode, errStr),
               details.venue
           );
       }
   }
   
   // MkvPlatformListener implementation
   @Override
   public void onMain(MkvPlatformEvent event) {
       // Platform main event
   }
   
   @Override
   public void onComponent(com.iontrading.mkv.MkvComponent component, boolean registered) {
       String name = component.getName();
       if (VENUE_CONFIGS.containsKey(name) || name.equals("ROUTER_US") || name.equals("VMO_REPO_US")) {
           if (registered) {
               LOGGER.info("{} Registered", name);
           }
       }
   }
   
   @Override
   public void onConnect(String component, boolean connected) {
       if (connected && (component.equals("ROUTER_US") || component.contains("ROUTER"))) {
           LOGGER.info("Connected to {}", component);
           isConnected = true;
           connectionLatch.countDown();
       }
   }
   
   // Redis integration
   private static JedisPool jedisPool;
   
   private static void initializeRedisBridge() {
       try {
           redisMessageBridge = new RedisMessageBridge(REDIS_HOST, REDIS_PORT);

           if (redisMessageBridge.testConnection()) {
               redisMessageBridge.start();

               Thread heartbeatThread = new Thread(() -> {
                   while (true) {
                       try {
                           Thread.sleep(30000);
                           redisMessageBridge.publishHeartbeat();
                       } catch (InterruptedException e) {
                           Thread.currentThread().interrupt();
                           break;
                       } catch (Exception e) {
                           LOGGER.error("Heartbeat publishing failed: ", e);
                       }
                   }
               });
               heartbeatThread.setDaemon(true);
               heartbeatThread.start();
               
           } else {
               LOGGER.warn("Redis connection failed, running in standalone mode");
               redisMessageBridge = null;
           }
       } catch (Exception e) {
           LOGGER.error("Redis bridge initialization failed: ", e);
           redisMessageBridge = null;
       }
   }
   
   private static void publishMarketDataToRedis(String instrument, double bidPrice, double askPrice) {
       if (redisMessageBridge != null) {
           try {
               redisMessageBridge.publishMarketData(instrument, bidPrice, askPrice, 
                   100.0, 100.0, System.currentTimeMillis());
           } catch (Exception e) {
               // Silent failure for market data
           }
       }
   }
   
   private static void publishOrderResponseToRedis(String orderId, String status, String message, String venue) {
       if (redisMessageBridge != null) {
           try {
               redisMessageBridge.publishOrderResponse(orderId, status, message, venue, 
                   System.currentTimeMillis());
           } catch (Exception e) {
               LOGGER.debug("Failed to publish order response to Redis: ", e);
           }
       }
   }
}