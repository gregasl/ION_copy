import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iontrading.mkv.*;
import com.iontrading.mkv.events.*;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.qos.MkvQoS;
import com.iontrading.mkv.exceptions.MkvException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.PrintStream;
import java.io.OutputStream;
import java.util.Scanner;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

/**
 * Supported venues:
 * - FENICS_USREPO
 * - BTEC_REPO_US
 * - DEALERWEB_REPO
 */
public class JavaSingleOrder implements MkvFunctionCallListener, MkvPlatformListener, MkvRecordListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(JavaSingleOrder.class);

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
    
    // Venue selection priority
    private static final String[] VENUE_PRIORITY = {
        "FENICS_USREPO",
        "BTEC_REPO_US",
        "DEALERWEB_REPO"
    };
    
    // Active venue configuration
    private static VenueConfig activeVenueConfig = null;
    private static final Map<String, Boolean> venueStatus = new ConcurrentHashMap<>();
    
    // System configuration
    private static final String SYSTEM_USER = "evan_gerhard";
    private static final String APPLICATION_ID = "automatedMarketMaking";
    
    // Order configuration
    private static final String[] TEST_INSTRUMENTS = {"912797RJ8"};
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
    
    // Order tracking
    private static final Map<String, OrderDetails> orderTracking = new ConcurrentHashMap<>();
    
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
        
        OrderDetails(String orderId, int reqId, String instrumentId, String venue) {
            this.orderId = orderId;
            this.reqId = reqId;
            this.instrumentId = instrumentId;
            this.venue = venue;
            this.timestamp = System.currentTimeMillis();
        }
    }
    
    /**
     * Constructor
     * 
     * @param reqId Request ID for this instance
     */
    public JavaSingleOrder(int reqId) {
        this.myReqId = reqId;
        this.creationTimestamp = System.currentTimeMillis();
        LOGGER.debug("JavaSingleOrder initialized - ReqId: {}", reqId);
    }
    
    /**
     * Simple order manager implementation for DepthListener
     */
    private static class SimpleOrderManager implements IOrderManager {
        @Override
        public void orderDead(MarketOrder order) {
            LOGGER.info("Order terminated: {}", order.getOrderId());
        }
        
        @Override
        public MarketOrder addOrder(String MarketSource, String TraderId, String instrId, 
                String verb, double qty, double price, String type, String tif) {
            LOGGER.debug("Add order called - not implemented in simple manager");
            return null;
        }
        
        @Override
        public void best(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {
            // Simple implementation
        }
        
        @Override
        public void mapOrderIdToReqId(String orderId, int reqId) {
            LOGGER.debug("Mapping order ID {} to request ID {}", orderId, reqId);
        }
        
        @Override
        public void removeOrder(int reqId) {
            LOGGER.debug("Removing order with request ID {}", reqId);
        }
        
        @Override
        public String getApplicationId() {
            return APPLICATION_ID;
        }
    }
    
    /**
     * Select the best available venue based on login status
     * 
     * @return Selected venue configuration
     */
    private static VenueConfig selectBestVenue() {
        for (String venue : VENUE_PRIORITY) {
            Boolean isActive = venueStatus.get(venue);
            if (Boolean.TRUE.equals(isActive)) {
                VenueConfig config = VENUE_CONFIGS.get(venue);
                return config;
            }
        }
        
        LOGGER.warn("No active venues found, defaulting to: {}", VENUE_PRIORITY[0]);
        return VENUE_CONFIGS.get(VENUE_PRIORITY[0]);
    }
    
    /**
     * Cancel an existing order
     * 
     * @param orderId Order ID to cancel
     * @param venue Venue where the order was placed
     * @return true if cancellation request was sent successfully
     */
    public static boolean cancelOrder(String orderId, String venue) {
        if (orderId == null || orderId.isEmpty() || orderId.startsWith("PENDING")) {
            LOGGER.warn("Invalid order ID for cancellation: {}", orderId);
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
                        LOGGER.info("Cancel response received: {}", result);
                        
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
    
    /**
     * Update order status in tracking map
     * 
     * @param orderId Order ID to update
     * @param status New status
     */
    private static void updateOrderStatus(String orderId, String status) {
        for (OrderDetails details : orderTracking.values()) {
            if (orderId.equals(details.orderId)) {
                details.status = status;
                break;
            }
        }
    }
    
    /**
     * Configure logging levels to reduce noise
     */
    private static void configureLogging() {
        try {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            
            // Set specific logger levels
            loggerContext.getLogger("DepthListener").setLevel(Level.WARN);
            loggerContext.getLogger("Instrument").setLevel(Level.WARN);
            loggerContext.getLogger("com.iontrading.mkv").setLevel(Level.WARN);
            loggerContext.getLogger("MkvConnection").setLevel(Level.DEBUG);  // Enable MkvConnection logs for market data
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.DepthListener").setLevel(Level.WARN);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.Instrument").setLevel(Level.WARN);
            
            // Market data and repository loggers - set to DEBUG to show detailed updates
            loggerContext.getLogger("Best").setLevel(Level.ERROR);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.Best").setLevel(Level.ERROR);
            loggerContext.getLogger("GCBest").setLevel(Level.DEBUG);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.GCBest").setLevel(Level.DEBUG);
            loggerContext.getLogger("GCBestManager").setLevel(Level.DEBUG);
            loggerContext.getLogger("OrderRepository").setLevel(Level.ERROR);
            
            // Additional market data loggers
            loggerContext.getLogger("com.iontrading.mkv.GCBestManager").setLevel(Level.DEBUG);
            loggerContext.getLogger("com.iontrading.mkv.Best").setLevel(Level.ERROR);
            loggerContext.getLogger("com.iontrading.mkv.GCBest").setLevel(Level.DEBUG);
            
            // Keep main application at INFO level
            loggerContext.getLogger("JavaSingleOrder").setLevel(Level.INFO);
            loggerContext.getLogger("FENICSOrderCreatorFixed").setLevel(Level.INFO);
            
            LOGGER.debug("Logging levels configured successfully");
        } catch (Exception e) {
            LOGGER.warn("Unable to configure logging levels: {}", e.getMessage());
        }
    }
    
    /**
     * Main application entry point
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        // Configure logging first to reduce noise
        configureLogging();
        
        LOGGER.info("Configuration:");
        LOGGER.info("  System User: {}", SYSTEM_USER);
        for (Map.Entry<String, VenueConfig> entry : VENUE_CONFIGS.entrySet()) {
            LOGGER.info("  {} -> Trader: {}", entry.getKey(), entry.getValue().traderId);
        }
        LOGGER.info("  Target Instruments: {}", Arrays.toString(TEST_INSTRUMENTS));
        LOGGER.info("  Order Parameters: {} {} @ {}", ORDER_VERB, ORDER_QUANTITY, DEFAULT_ORDER_PRICE);
        LOGGER.info("");
        
        VenueConfig selectedVenue = null;
        
        try {
            // Initialize and start MKV platform
            JavaSingleOrder mainInstance = new JavaSingleOrder(0);

            LOGGER.info("Starting MKV Platform...");
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
            
            // Wait for login status check
            mainInstance.loginCheckLatch.await(5, TimeUnit.SECONDS);
            
            // Display venue status summary
            for (String venue : VENUE_PRIORITY) {
                Boolean isActive = venueStatus.get(venue);
                LOGGER.info("  {}: {}", venue, 
                    Boolean.TRUE.equals(isActive) ? "ACTIVE" : "INACTIVE");
            }
            
            // Select venue
            if (selectedVenue == null) {
                activeVenueConfig = selectBestVenue();
            } else {
                activeVenueConfig = selectedVenue;
                Boolean isActive = venueStatus.get(activeVenueConfig.marketSource);
                if (!Boolean.TRUE.equals(isActive)) {
                    LOGGER.warn("WARNING: Selected venue {} is INACTIVE", activeVenueConfig.marketSource);
                    LOGGER.warn("Orders may fail with error 101. Consider selecting an active venue.");
                }
            }
            
            LOGGER.info("Selected Venue: {} with Trader: {}", 
                activeVenueConfig.marketSource, activeVenueConfig.traderId);
            
            // Initialize DepthListener
            LOGGER.info("");

            // Suppress console output during initialization
            PrintStream originalOut = System.out;
            PrintStream originalErr = System.err;
            PrintStream nullStream = new PrintStream(new OutputStream() {
                public void write(int b) {}
            });
            
            try {
                System.setOut(nullStream);
                System.setErr(nullStream);
                
                // Initialize DepthListener
                SimpleOrderManager simpleOrderManager = new SimpleOrderManager();
                depthListener = new DepthListener(simpleOrderManager);
                
                // Subscribe to instruments
                MkvPattern instrumentPattern = pm.getMkvPattern(INSTRUMENT_PATTERN);
                if (instrumentPattern != null) {
                    instrumentPattern.subscribe(INSTRUMENT_FIELDS, depthListener);
                }
                
                // ========== DEPTH pattern  ==========
                MkvPattern depthPattern = pm.getMkvPattern(DEPTH_PATTERN);
                if (depthPattern != null) {
                    depthPattern.subscribe(DEPTH_FIELDS, depthListener);
                    LOGGER.debug("Subscribed to depth pattern for real-time prices");
                } else {
                    LOGGER.warn("Depth pattern not found: {}", DEPTH_PATTERN);
                }
                // ============================================================
                
                // Show progress
                System.setOut(originalOut);
                System.out.print("Loading market data: ");
                
                for (int i = 0; i < 15; i++) {
                    Thread.sleep(1000);
                    System.out.print(".");
                    System.out.flush();
                    
                    if (i == 5 && depthListener != null) {
                        Map<String, Object> health = depthListener.getHealthStatus();
                        Boolean isReceivingData = (Boolean) health.get("isReceivingData");
                        if (Boolean.FALSE.equals(isReceivingData)) {
                            System.out.println("\nNote: Market closed - limited data available");
                            System.out.print("Initializing: ");
                        }
                    }
                }
                
                System.out.println(" Complete");
                Thread.sleep(2000);
                
            } finally {
                System.setOut(originalOut);
                System.setErr(originalErr);
            }
            
            LOGGER.info("Instrument data loaded successfully - Total instruments: {}", 
                depthListener != null ? depthListener.getInstrumentCount() : "Unknown");
            LOGGER.info("");
            // Find instrument mapping
            String nativeId = null;
            String selectedCusip = null;
            
            // Try direct mapping first
            for (String testInstrument : TEST_INSTRUMENTS) {
                String result = depthListener.getInstrumentFieldBySourceString(
                    testInstrument, activeVenueConfig.marketSource, false);
                
                if (result != null) {
                    LOGGER.info("Found direct mapping: {} -> {}", testInstrument, result);
                    selectedCusip = testInstrument;
                    nativeId = result;
                    break;
                }
            }
            
            // Try with suffixes if direct mapping not found
            if (nativeId == null) {
                String[] suffixes = {"_C_Fixed", "_REG_Fixed"};
                
                for (String cusip : TEST_INSTRUMENTS) {
                    for (String suffix : suffixes) {
                        String testId = cusip + suffix;
                        String result = depthListener.getInstrumentFieldBySourceString(
                            testId, activeVenueConfig.marketSource, false);
                        
                        if (result != null) {
                            LOGGER.info("Found mapping with suffix: {} -> {}", testId, result);
                            selectedCusip = testId;
                            nativeId = result;
                            break;
                        }
                    }
                    if (nativeId != null) break;
                }
            }
            
            if (nativeId == null) {
                LOGGER.warn("No venue-specific mapping found for {} on {}", 
                    TEST_INSTRUMENTS[0], activeVenueConfig.marketSource);
                LOGGER.info("Using direct CUSIP as fallback");
                selectedCusip = TEST_INSTRUMENTS[0];
                nativeId = selectedCusip;
            }
            
            // Calculate dynamic price
            dynamicOrderPrice = calculateDynamicPrice(selectedCusip);
            LOGGER.info("Order price set: {} ({})", String.format("%.4f", dynamicOrderPrice), 
                "Buy".equals(ORDER_VERB) ? "Buy" : "Sell");


            JavaSingleOrder orderCreator = createOrder(nativeId, selectedCusip);

            if (orderCreator != null) {
    
                // Monitor for response
                boolean responseReceived = false;
                for (int i = 0; i < 10; i++) {
                    Thread.sleep(1000);
                    
                    OrderDetails details = orderTracking.get(String.valueOf(reqId));
                    if (details != null && !details.status.equals("PENDING")) {
                        responseReceived = true;
                        break;
                    }
                    
                    if (i % 3 == 2) {
                        System.out.print(".");
                    }
                }
                
                if (!responseReceived) {
                    LOGGER.warn("No response received within 10 seconds");
                }
                
                // Display final order status
                logOrderStatus();
                
                // Automatic order cancellation
                LOGGER.info("Automatically canceling order in 10 seconds...");
                try {
                    Thread.sleep(10000); // Wait 10 seconds before auto-cancel
                    
                    OrderDetails currentOrder = orderTracking.get(String.valueOf(reqId));
                    if (currentOrder != null && currentOrder.orderId != null && 
                        !currentOrder.orderId.startsWith("PENDING") && 
                        !"FAILED".equals(currentOrder.status)) {
                        
                        boolean cancelSuccess = cancelOrder(currentOrder.orderId, currentOrder.venue);
                        
                        if (cancelSuccess) {
                            // Success logged in cancelOrder method
                        } else {
                            LOGGER.warn("Order cancellation request failed");
                        }
                        
                        // Wait for cancellation confirmation
                        Thread.sleep(2000);
                        
                        logOrderStatus();
                    } else {
                        LOGGER.warn("Cannot cancel - order not in valid state for cancellation");
                        if (currentOrder != null) {
                            LOGGER.warn("Order status: {}, Order ID: {}", 
                                currentOrder.status, currentOrder.orderId);
                        }
                    }
                    
                } catch (Exception e) {
                    LOGGER.error("Error in automatic cancellation process: ", e);
                }
            }
            
        } catch (Exception e) {
            LOGGER.error("Application error: ", e);
        } finally {
            LOGGER.info("");
            Mkv.stop();
            LOGGER.info("Shutdown complete");
            System.exit(0);
        }
    }
    
    /**
     * Subscribe to login status updates
     * 
     * @param pm MkvPublishManager instance
     */
    private void subscribeToLoginStatus(MkvPublishManager pm) {
        try {
            String loginRecordName = LOGIN_PATTERN + SYSTEM_USER;
            LOGGER.debug("Looking for login record: {}", loginRecordName);
            
            MkvObject obj = pm.getMkvObject(loginRecordName);
            
            if (obj != null && obj.getMkvObjectType() == MkvObjectType.RECORD) {
                MkvRecord loginRecord = (MkvRecord) obj;
                loginRecord.subscribe(LOGIN_FIELDS, this);
                LOGGER.debug("Subscribed to login record: {}", loginRecordName);
                
                // Check immediate status
                checkLoginStatus(loginRecord);
            } else {
                LOGGER.debug("Login record not found, subscribing to pattern...");
                
                MkvObject patternObj = pm.getMkvObject(LOGIN_PATTERN);
                
                if (patternObj != null && patternObj.getMkvObjectType() == MkvObjectType.PATTERN) {
                    MkvPattern loginPattern = (MkvPattern) patternObj;
                    loginPattern.subscribe(LOGIN_FIELDS, this);
                    LOGGER.debug("Subscribed to login pattern: {}", LOGIN_PATTERN);
                } else {
                    LOGGER.warn("Login pattern not found: {}", LOGIN_PATTERN);
                    
                    pm.addPublishListener(new MkvPublishListener() {
                        @Override
                        public void onPublish(MkvObject mkvObject, boolean published, boolean isDownloadComplete) {
                            if (published && mkvObject.getName().equals(LOGIN_PATTERN) &&
                                mkvObject.getMkvObjectType() == MkvObjectType.PATTERN) {
                                try {
                                    ((MkvPattern) mkvObject).subscribe(LOGIN_FIELDS, JavaSingleOrder.this);
                                    LOGGER.debug("Successfully subscribed to login pattern");
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
    
    /**
     * Check login status from record
     * 
     * @param record MkvRecord containing login information
     */
    private void checkLoginStatus(MkvRecord record) {
        try {
            for (int i = 0; i < 8; i++) {
                String src = record.getValue("Src" + i).getString();
                String status = record.getValue("TStatusStr" + i).getString();
                
                if (src != null && status != null && VENUE_CONFIGS.containsKey(src)) {
                    LOGGER.debug("{} status for venue {}: {}", SYSTEM_USER, src, status);
                    
                    venueStatus.put(src, "On".equals(status));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error checking login status: ", e);
        }
    }
    
    /**
     * Calculate dynamic price for the order
     * 
     * @param cusip CUSIP identifier
     * @return Calculated price
     */
    private static double calculateDynamicPrice(String cusip) {
        LOGGER.debug("Setting price for instrument: {}", cusip);
        
        if ("Buy".equals(ORDER_VERB)) {
            double adjustedPrice = 4.38;
            return Math.round(adjustedPrice * 100.0) / 100.0; 
        } else {
            double adjustedPrice = 4.42;
            return Math.round(adjustedPrice * 100.0) / 100.0;
        }
    }
    
    /**
     * Create an order
     * 
     * @param instrumentId Instrument identifier
     * @param originalCusip Original CUSIP
     * @return JavaSingleOrder instance or null if failed
     */
    public static JavaSingleOrder createOrder(String instrumentId, String originalCusip) {
        reqId++;
        
        LOGGER.info("Creating Order:");
        LOGGER.info("  Venue: {} | Trader: {} | Instrument: {}", 
            activeVenueConfig.marketSource, activeVenueConfig.traderId, instrumentId);
        LOGGER.info("  Type: {} {} @ {} | TIF: {}", 
            ORDER_VERB, ORDER_QUANTITY, String.format("%.4f", dynamicOrderPrice), TIME_IN_FORCE);
        
        MkvPublishManager pm = Mkv.getInstance().getPublishManager();
        String functionName = activeVenueConfig.getOrderFunction();
        
        try {
            MkvFunction fn = pm.getMkvFunction(functionName);
            
            if (fn != null) {
                return createOrderWithFunction(fn, instrumentId, originalCusip);
            } else {
                LOGGER.error("Order function not found: {}", functionName);
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Error creating order: ", e);
            return null;
        }
    }
    
    /**
     * Create order using MkvFunction
     * 
     * @param fn MkvFunction for order creation
     * @param instrumentId Instrument identifier
     * @param originalCusip Original CUSIP
     * @return JavaSingleOrder instance or null if failed
     */
    private static JavaSingleOrder createOrderWithFunction(MkvFunction fn, 
            String instrumentId, String originalCusip) {
        try {
            String freeText = MarketDef.getFreeText(String.valueOf(reqId), APPLICATION_ID);
            
            JavaSingleOrder order = new JavaSingleOrder(reqId);
            
            OrderDetails details = new OrderDetails("PENDING_" + reqId, reqId, 
                originalCusip, activeVenueConfig.marketSource);
            orderTracking.put(String.valueOf(reqId), details);
            
            MkvSupply args = MkvSupplyFactory.create(new Object[] {
                activeVenueConfig.traderId,        // Trader ID
                instrumentId,                       // Instrument
                ORDER_VERB,                         // Verb
                Double.valueOf(dynamicOrderPrice),  // Price
                Double.valueOf(ORDER_QUANTITY),     // QtyShown
                Double.valueOf(ORDER_QUANTITY),     // QtyTot
                ORDER_TYPE,                         // Type
                TIME_IN_FORCE,                      // TimeInForce
                Integer.valueOf(0),                 // IsSoft
                Integer.valueOf(0),                 // Attribute
                "",                                 // CustomerInfo
                freeText,                           // FreeText
                Integer.valueOf(0),                 // StopCond
                "",                                 // StopId
                Double.valueOf(0)                   // StopPrice
            });
            
            fn.call(args, order);
            LOGGER.debug("Order function called on {} with trader {}", 
                activeVenueConfig.marketSource, activeVenueConfig.traderId);
            
            return order;
            
        } catch (Exception e) {
            LOGGER.error("Error creating order: ", e);
            return null;
        }
    }
    
    /**
     * Log current order status
     */
    private static void logOrderStatus() {
        for (OrderDetails details : orderTracking.values()) {
            LOGGER.info("Order ID: {} | Venue: {} | Status: {} | Error: {}", 
                details.orderId, details.venue, details.status, 
                details.errorMsg.isEmpty() ? "None" : details.errorMsg);
        }
    }
    
    // MkvRecordListener implementation
    @Override
    public void onFullUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
        try {
            String recordName = record.getName();
            
            if (recordName.contains("CM_LOGIN") && recordName.contains(SYSTEM_USER)) {
                LOGGER.debug("Login update received for {}", SYSTEM_USER);
                checkLoginStatus(record);
                loginCheckLatch.countDown();
            }
            else if (recordName.contains("CM_ORDER")) {
                // Process order updates if needed
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
            LOGGER.info("Order response received: {}", result);
            
            if (result.contains("OK")) {
                String extractedOrderId = extractOrderId(result);
                
                OrderDetails details = orderTracking.get(String.valueOf(myReqId));
                if (details != null) {
                    details.status = "SUBMITTED";
                    if (extractedOrderId != null) {
                        details.orderId = extractedOrderId;
                    }
                }
            } else {
                OrderDetails details = orderTracking.get(String.valueOf(myReqId));
                if (details != null) {
                    details.status = "FAILED";
                    details.errorMsg = result;
                }
                
                if (result.contains("101") || result.contains("not logged in")) {
                    LOGGER.error("Error 101: User not logged in");
                    LOGGER.error("Solution: Ensure {} is logged in to {}", 
                        SYSTEM_USER, activeVenueConfig.marketSource);
                } else if (result.contains("Price Exceeds Current Price Band")) {
                    LOGGER.error("Price Band Error: Order price {} is outside allowed range", 
                        dynamicOrderPrice);
                    LOGGER.error("Solution: Adjust price to be within market bands");
                } else {
                    LOGGER.error("Order submission failed: {}", result);
                }
            }
            
        } catch (Exception e) {
            LOGGER.error("Error processing order response: ", e);
        }
    }
    
    /**
     * Extract order ID from response
     * 
     * @param result Response string
     * @return Extracted order ID or null
     */
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
            LOGGER.info("Component {} {}", name, registered ? "registered" : "unregistered");
        }
    }
    
    @Override
    public void onConnect(String component, boolean connected) {
        if (connected && (component.equals("ROUTER_US") || component.contains("ROUTER"))) {
            LOGGER.info("Connected to component: {}", component);
            isConnected = true;
            connectionLatch.countDown();
        }
    }
}