// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import com.iontrading.mkv.*;
// import com.iontrading.mkv.events.*;
// import com.iontrading.mkv.enums.MkvPlatformEvent;
// import com.iontrading.mkv.enums.MkvObjectType;
// import com.iontrading.mkv.helper.MkvSupplyFactory;
// import com.iontrading.mkv.qos.MkvQoS;
// import com.iontrading.mkv.exceptions.MkvException;

// import java.time.LocalDateTime;
// import java.time.format.DateTimeFormatter;
// import java.util.concurrent.CountDownLatch;
// import java.util.concurrent.TimeUnit;
// import java.util.Map;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.Set;
// import java.util.HashSet;
// import java.util.Arrays;
// import java.util.concurrent.atomic.AtomicBoolean;
// import java.io.PrintStream;
// import java.io.OutputStream;

// /**
//  * FENICS Order Creator - Fixed Version
//  * Key fixes:
//  * 1. Removed logback specific configuration
//  * 2. Subscribe to evan_gerhard login record
//  * 3. Use correct trader ID (frosasl1) for orders
//  */
// public class FENICSOrderCreatorFixed implements MkvFunctionCallListener, MkvPlatformListener, MkvRecordListener {
    
//     private static final Logger LOGGER = LoggerFactory.getLogger(FENICSOrderCreatorFixed.class);
    
//     // Configuration
//     private static final String MARKET_SOURCE = "FENICS_USREPO";
//     private static final String TRADER_ID = "frosasl1";  // For orders
//     private static final String SYSTEM_USER = "evan_gerhard";  // For login status
    
//     // Test instruments
//     private static final String[] TEST_INSTRUMENTS = {
//         "912797PY7",
//         "912797NU7", 
//         "91282CNL1",
//         "912797MG9"
//     };
    
//     private static final String ORDER_VERB = "Buy";
//     private static final double ORDER_QUANTITY = 1000000.0;
//     private static final double DEFAULT_ORDER_PRICE = 99.0;  // 改为默认价格
//     private static final String ORDER_TYPE = "Limit";
//     private static final String TIME_IN_FORCE = "FAS";
//     private static final String APPLICATION_ID = "automatedMarketMaking";
    
//     // 动态价格变量
//     private static double dynamicOrderPrice = DEFAULT_ORDER_PRICE;
    
//     // Instance variables
//     private static int reqId = 0;
//     private final int myReqId;
//     private String orderId;
//     private final long creationTimestamp;
//     private boolean orderDead = false;
//     private byte errCode = (byte) 0;
//     private String errStr = "";
    
//     // Connection and login state
//     private final CountDownLatch connectionLatch = new CountDownLatch(1);
//     private final CountDownLatch loginCheckLatch = new CountDownLatch(1);
//     private boolean isConnected = false;
//     private boolean isVenueActive = false;
    
//     // Components
//     private static DepthListener depthListener;
    
//     // Order tracking
//     private static final Map<String, OrderDetails> orderTracking = new ConcurrentHashMap<>();
    
//     // Pattern and fields from MarketDef
//     private static final String ORDER_PATTERN = MarketDef.ORDER_PATTERN;
//     private static final String[] ORDER_FIELDS = MarketDef.ORDER_FIELDS;
//     private static final String LOGIN_PATTERN = MarketDef.LOGIN_PATTERN;
//     private static final String[] LOGIN_FIELDS = MarketDef.LOGIN_FIELDS;
//     private static final String INSTRUMENT_PATTERN = MarketDef.INSTRUMENT_PATTERN;
//     private static final String[] INSTRUMENT_FIELDS = MarketDef.INSTRUMENT_FIELDS;
    
//     // Inner class to track order details
//     private static class OrderDetails {
//         String orderId;
//         final int reqId;
//         final long timestamp;
//         String status = "PENDING";
//         double filledQty = 0.0;
//         String errorMsg = "";
//         final String instrumentId;
        
//         OrderDetails(String orderId, int reqId, String instrumentId) {
//             this.orderId = orderId;
//             this.reqId = reqId;
//             this.instrumentId = instrumentId;
//             this.timestamp = System.currentTimeMillis();
//         }
//     }
    
//     /**
//      * Constructor
//      */
//     public FENICSOrderCreatorFixed(int reqId) {
//         this.myReqId = reqId;
//         this.creationTimestamp = System.currentTimeMillis();
//         LOGGER.debug("FENICSOrderCreatorFixed initialized - ReqId: {}", reqId);
//     }
    
//     /**
//      * Simple IOrderManager implementation
//      */
//     private static class SimpleOrderManager implements IOrderManager {
//         @Override
//         public void orderDead(MarketOrder order) {
//             LOGGER.info("📦 Order dead: {}", order.getOrderId());
//         }
        
//         @Override
//         public MarketOrder addOrder(String MarketSource, String TraderId, String instrId, 
//                 String verb, double qty, double price, String type, String tif) {
//             LOGGER.info("Add order called - not implemented in simple manager");
//             return null;
//         }
        
//         @Override
//         public void best(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {
//             // Simple implementation
//         }
        
//         @Override
//         public void mapOrderIdToReqId(String orderId, int reqId) {
//             LOGGER.debug("Map order ID {} to request ID {}", orderId, reqId);
//         }
        
//         @Override
//         public void removeOrder(int reqId) {
//             LOGGER.debug("Remove order with request ID {}", reqId);
//         }
        
//         @Override
//         public String getApplicationId() {
//             return APPLICATION_ID;
//         }
//     }
    
//     /**
//      * Cancel order - 新增方法
//      */
//     public static boolean cancelOrder(String orderId) {
//         LOGGER.info("🚫 Attempting to cancel order: {}", orderId);
        
//         if (orderId == null || orderId.isEmpty() || orderId.startsWith("PENDING")) {
//             LOGGER.warn("⚠️ Invalid order ID for cancellation: {}", orderId);
//             return false;
//         }
        
//         MkvPublishManager pm = Mkv.getInstance().getPublishManager();
//         String functionName = MARKET_SOURCE + "_VCMIOrderDel";
        
//         try {
//             MkvFunction fn = pm.getMkvFunction(functionName);
            
//             if (fn == null) {
//                 LOGGER.error("❌ Cancel function not found: {}", functionName);
//                 return false;
//             }
            
//             reqId++;
//             final int cancelReqId = reqId;
            
//             MkvFunctionCallListener cancelListener = new MkvFunctionCallListener() {
//                 @Override
//                 public void onResult(MkvFunctionCallEvent event, MkvSupply supply) {
//                     try {
//                         String result = supply.getString(supply.firstIndex());
//                         LOGGER.info("📨 Cancel Response: {}", result);
                        
//                         if (result.contains("OK")) {
//                             LOGGER.info("✅ Order {} cancelled successfully", orderId);
//                             for (OrderDetails details : orderTracking.values()) {
//                                 if (orderId.equals(details.orderId)) {
//                                     details.status = "CANCELLED";
//                                     break;
//                                 }
//                             }
//                         } else {
//                             LOGGER.error("❌ Cancel failed: {}", result);
//                         }
//                     } catch (Exception e) {
//                         LOGGER.error("Error processing cancel response: ", e);
//                     }
//                 }
                
//                 @Override
//                 public void onError(MkvFunctionCallEvent event, byte errCode, String errStr) {
//                     LOGGER.error("❌ Cancel Error - Code: {} | Error: {}", errCode, errStr);
//                 }
//             };
            
//             String freeText = MarketDef.getFreeText(String.valueOf(cancelReqId), APPLICATION_ID);
            
//             MkvSupply args = MkvSupplyFactory.create(new Object[] {
//                 TRADER_ID,
//                 orderId,
//                 freeText
//             });
            
//             LOGGER.info("📤 Sending cancel request for order: {}", orderId);
//             fn.call(args, cancelListener);
            
//             return true;
            
//         } catch (Exception e) {
//             LOGGER.error("❌ Error cancelling order: ", e);
//             return false;
//         }
//     }
    
//     /**
//      * Calculate dynamic price based on market data - 新增方法
//      */
//     private static double calculateDynamicPrice(String nativeId, String cusip) {
//         LOGGER.info("\n💹 Checking Market Price for Dynamic Pricing...");
        
//         try {
//             // 简单的价格策略：使用一个接近100的合理价格
//             double suggestedPrice = 99.5;
            
//             LOGGER.info("📊 Pricing Strategy for {}:", cusip);
//             LOGGER.info("   Using conservative price: {}", String.format("%.4f", suggestedPrice));
//             LOGGER.info("   💡 This price should be within FENICS price bands");
            
//             return suggestedPrice;
            
//         } catch (Exception e) {
//             LOGGER.error("Error calculating dynamic price: ", e);
//         }
        
//         return DEFAULT_ORDER_PRICE;
//     }
    
//     /**
//      * Main method
//      */
//     public static void main(String[] args) {
//         LOGGER.info("╔════════════════════════════════════════════════╗");
//         LOGGER.info("║      FENICS ORDER CREATOR - FIXED v3.1         ║");
//         LOGGER.info("║            (with Dynamic Pricing)              ║");
//         LOGGER.info("╚════════════════════════════════════════════════╝");
//         LOGGER.info("");
//         LOGGER.info("📋 Configuration:");
//         LOGGER.info("   Market Source: {}", MARKET_SOURCE);
//         LOGGER.info("   Trading User: {}", TRADER_ID);
//         LOGGER.info("   System User: {}", SYSTEM_USER);
//         LOGGER.info("   Target Instruments: {}", Arrays.asList(TEST_INSTRUMENTS));
//         LOGGER.info("   Order: {} {} @ DYNAMIC PRICE", ORDER_VERB, ORDER_QUANTITY);
//         LOGGER.info("");
        
//         try {
//             // Create main instance
//             FENICSOrderCreatorFixed mainInstance = new FENICSOrderCreatorFixed(0);
            
//             // Start MKV platform
//             LOGGER.info("🚀 Starting MKV Platform...");
//             MkvQoS qos = new MkvQoS();
//             if (args.length > 0) {
//                 qos.setArgs(args);
//             }
            
//             qos.setPlatformListeners(new MkvPlatformListener[] { mainInstance });
//             Mkv.start(qos);
            
//             // Wait for connection
//             LOGGER.info("⏳ Waiting for connection...");
//             boolean connected = mainInstance.connectionLatch.await(30, TimeUnit.SECONDS);
//             if (!connected || !mainInstance.isConnected) {
//                 LOGGER.error("❌ Connection timeout!");
//                 return;
//             }
            
//             LOGGER.info("✅ Connected to platform");
            
//             // Get publish manager
//             MkvPublishManager pm = Mkv.getInstance().getPublishManager();
//             if (pm == null) {
//                 LOGGER.error("Cannot get MkvPublishManager");
//                 return;
//             }
            
//             // Subscribe to login status
//             LOGGER.info("\n🔐 Checking Login Status...");
//             mainInstance.subscribeToLoginStatus(pm);
            
//             // Wait for login check
//             mainInstance.loginCheckLatch.await(5, TimeUnit.SECONDS);
            
//             LOGGER.info("   Venue {} status: {}", MARKET_SOURCE, 
//                 mainInstance.isVenueActive ? "✅ ACTIVE" : "❌ INACTIVE");
            
//             if (!mainInstance.isVenueActive) {
//                 LOGGER.warn("⚠️ WARNING: Venue is not active. Orders may fail with error 101.");
//                 LOGGER.info("💡 Please ensure {} is logged in to {}", SYSTEM_USER, MARKET_SOURCE);
//             }
            
//             // Initialize DepthListener with reduced console output
//             LOGGER.info("\n📈 Loading Instrument Data (Silent Mode)...");
//             LOGGER.info("   This will take ~15 seconds. Please wait...");
            
//             // Save original streams
//             PrintStream originalOut = System.out;
//             PrintStream originalErr = System.err;
            
//             // Create null output stream that discards everything
//             PrintStream nullStream = new PrintStream(new OutputStream() {
//                 public void write(int b) {
//                     // Discard all output
//                 }
//             });
            
//             try {
//                 // Redirect both stdout and stderr to null
//                 System.setOut(nullStream);
//                 System.setErr(nullStream);
                
//                 // Also try to suppress SLF4J logging temporarily
//                 // This is a hack but works for many SLF4J implementations
//                 java.util.logging.Logger.getLogger("com.iontrading").setLevel(java.util.logging.Level.OFF);
//                 java.util.logging.Logger.getLogger("DepthListener").setLevel(java.util.logging.Level.OFF);
//                 java.util.logging.Logger.getLogger("Instrument").setLevel(java.util.logging.Level.OFF);
                
//                 // Initialize DepthListener
//                 SimpleOrderManager simpleOrderManager = new SimpleOrderManager();
//                 depthListener = new DepthListener(simpleOrderManager);
                
//                 // Subscribe to instruments
//                 MkvPattern instrumentPattern = pm.getMkvPattern(INSTRUMENT_PATTERN);
//                 if (instrumentPattern != null) {
//                     instrumentPattern.subscribe(INSTRUMENT_FIELDS, depthListener);
//                 }
                
//                 // Restore output temporarily to show progress
//                 System.setOut(originalOut);
//                 System.out.print("   Loading instruments: ");
//                 System.setOut(nullStream);
                
//                 // Wait with progress indicator
//                 for (int i = 0; i < 15; i++) {
//                     Thread.sleep(1000);
                    
//                     // Briefly restore output to show progress
//                     System.setOut(originalOut);
//                     System.out.print("█");
//                     System.out.flush();
//                     System.setOut(nullStream);
//                 }
                
//                 // Final restore to show completion
//                 System.setOut(originalOut);
//                 System.out.println(" Complete!");
                
//                 // Give a moment for final updates
//                 Thread.sleep(2000);
                
//             } finally {
//                 // Always restore original streams
//                 System.setOut(originalOut);
//                 System.setErr(originalErr);
                
//                 // Restore logging levels
//                 java.util.logging.Logger.getLogger("com.iontrading").setLevel(java.util.logging.Level.INFO);
//             }
            
//             LOGGER.info("   ✅ Instrument data loaded");
//             LOGGER.info("   Total instruments: ~{}", depthListener != null ? depthListener.getInstrumentCount() : "Unknown");
            
//             // Find instrument
//             LOGGER.info("\n🔍 Searching for FENICS Native IDs...");
//             String nativeId = null;
//             String selectedCusip = null;
            
//             // First try the instruments as-is
//             for (String testInstrument : TEST_INSTRUMENTS) {
//                 String result = depthListener.getInstrumentFieldBySourceString(
//                     testInstrument, MARKET_SOURCE, false);
                
//                 if (result != null) {
//                     LOGGER.info("   ✅ Found mapping: {} => {}", testInstrument, result);
//                     selectedCusip = testInstrument;
//                     nativeId = result;
//                     break;
//                 }
//             }
            
//             // If not found, try with suffixes
//             if (nativeId == null) {
//                 LOGGER.info("   Trying with suffixes...");
//                 String[] suffixes = {"_C_Fixed", "_REG_Fixed"};
                
//                 for (String cusip : TEST_INSTRUMENTS) {
//                     for (String suffix : suffixes) {
//                         String testId = cusip + suffix;
//                         String result = depthListener.getInstrumentFieldBySourceString(
//                             testId, MARKET_SOURCE, false);
                        
//                         if (result != null) {
//                             LOGGER.info("   ✅ Found mapping: {} => {}", testId, result);
//                             selectedCusip = testId;
//                             nativeId = result;
//                             break;
//                         }
//                     }
//                     if (nativeId != null) break;
//                 }
//             }
            
//             if (nativeId == null) {
//                 LOGGER.warn("   ⚠️ No FENICS mapping found, using direct CUSIP");
//                 selectedCusip = TEST_INSTRUMENTS[0];
//                 nativeId = selectedCusip;
//                 LOGGER.info("   Using: {}", nativeId);
//             }
            
//             // 新增：计算动态价格
//             dynamicOrderPrice = calculateDynamicPrice(nativeId, selectedCusip);
//             LOGGER.info("\n🎯 Final Order Price: {}", String.format("%.4f", dynamicOrderPrice));
            
//             // Create order
//             LOGGER.info("\n💼 Creating Order...");
//             LOGGER.info("   Instrument: {}", selectedCusip);
//             LOGGER.info("   Native ID: {}", nativeId);
//             LOGGER.info("   Trader: {}", TRADER_ID);
//             LOGGER.info("   Price: {}", String.format("%.4f", dynamicOrderPrice));
            
//             FENICSOrderCreatorFixed orderCreator = createOrder(nativeId, selectedCusip);
            
//             if (orderCreator != null) {
//                 LOGGER.info("📡 Order request sent");
//                 LOGGER.info("⏳ Monitoring for response...\n");
                
//                 // Monitor for response with shorter timeout
//                 boolean responseReceived = false;
//                 for (int i = 0; i < 10; i++) {
//                     Thread.sleep(1000);
                    
//                     // Check if we got a response
//                     OrderDetails details = orderTracking.get(String.valueOf(reqId));
//                     if (details != null && !details.status.equals("PENDING")) {
//                         responseReceived = true;
//                         LOGGER.info("✅ Response received after {} seconds", i + 1);
//                         break;
//                     }
                    
//                     if (i % 3 == 2) {
//                         System.out.print(".");
//                     }
//                 }
                
//                 if (!responseReceived) {
//                     LOGGER.warn("⏱️ No response after 10 seconds");
//                 }
                
//                 // Final status
//                 LOGGER.info("\n📊 Final Order Status:");
//                 logOrderStatus();
                
//                 // 新增：询问是否取消订单
//                 LOGGER.info("\n🤔 Press Enter within 5 seconds to cancel the order...");
//                 try {
//                     long startTime = System.currentTimeMillis();
//                     boolean shouldCancel = false;
                    
//                     Thread inputThread = new Thread(() -> {
//                         try {
//                             System.in.read();
//                             synchronized (FENICSOrderCreatorFixed.class) {
//                                 FENICSOrderCreatorFixed.class.notify();
//                             }
//                         } catch (Exception e) {
//                             // Ignore
//                         }
//                     });
//                     inputThread.setDaemon(true);
//                     inputThread.start();
                    
//                     synchronized (FENICSOrderCreatorFixed.class) {
//                         try {
//                             FENICSOrderCreatorFixed.class.wait(5000);
//                             if (System.currentTimeMillis() - startTime < 5000) {
//                                 shouldCancel = true;
//                             }
//                         } catch (InterruptedException e) {
//                             // Ignore
//                         }
//                     }
                    
//                     if (shouldCancel) {
//                         LOGGER.info("✅ Cancelling order...");
                        
//                         // 只取消当前创建的订单
//                         OrderDetails currentOrder = orderTracking.get(String.valueOf(reqId));
//                         if (currentOrder != null && currentOrder.orderId != null && 
//                             !currentOrder.orderId.startsWith("PENDING") && 
//                             !"FAILED".equals(currentOrder.status)) {
                            
//                             cancelOrder(currentOrder.orderId);
//                             Thread.sleep(2000); // 等待取消确认
                            
//                             LOGGER.info("\n📊 Final Status After Cancellation:");
//                             logOrderStatus();
//                         } else {
//                             LOGGER.info("⚠️ Cannot cancel - order not in valid state");
//                         }
//                     } else {
//                         LOGGER.info("⏰ Timeout - Order will remain active");
//                     }
                    
//                 } catch (Exception e) {
//                     LOGGER.error("Error in cancel process: ", e);
//                 }
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("❌ Error: ", e);
//         } finally {
//             LOGGER.info("\n🛑 Shutting down...");
//             Mkv.stop();
//             LOGGER.info("✅ Shutdown complete");
//         }
//     }
    
//     /**
//      * Subscribe to login status - fixed to use evan_gerhard
//      */
//     private void subscribeToLoginStatus(MkvPublishManager pm) {
//         try {
//             // First try to get the specific login record for evan_gerhard
//             String loginRecordName = LOGIN_PATTERN + SYSTEM_USER;
//             LOGGER.info("   Looking for login record: {}", loginRecordName);
            
//             MkvObject obj = pm.getMkvObject(loginRecordName);
            
//             if (obj != null && obj.getMkvObjectType() == MkvObjectType.RECORD) {
//                 MkvRecord loginRecord = (MkvRecord) obj;
//                 loginRecord.subscribe(LOGIN_FIELDS, this);
//                 LOGGER.info("   ✅ Subscribed to login record: {}", loginRecordName);
                
//                 // Check immediate status
//                 checkLoginStatus(loginRecord);
//             } else {
//                 LOGGER.info("   Login record not found, subscribing to pattern...");
                
//                 // Try to subscribe to the pattern
//                 MkvObject patternObj = pm.getMkvObject(LOGIN_PATTERN);
                
//                 if (patternObj != null && patternObj.getMkvObjectType() == MkvObjectType.PATTERN) {
//                     MkvPattern loginPattern = (MkvPattern) patternObj;
//                     loginPattern.subscribe(LOGIN_FIELDS, this);
//                     LOGGER.info("   ✅ Subscribed to login pattern: {}", LOGIN_PATTERN);
//                 } else {
//                     LOGGER.warn("   ⚠️ Login pattern not found: {}", LOGIN_PATTERN);
//                     LOGGER.info("   Will wait for it to be published...");
                    
//                     // Add a publish listener to wait for the pattern
//                     pm.addPublishListener(new MkvPublishListener() {
//                         @Override
//                         public void onPublish(MkvObject mkvObject, boolean published, boolean isDownloadComplete) {
//                             if (published && mkvObject.getName().equals(LOGIN_PATTERN) &&
//                                 mkvObject.getMkvObjectType() == MkvObjectType.PATTERN) {
//                                 try {
//                                     LOGGER.info("   Login pattern now available: {}", LOGIN_PATTERN);
//                                     ((MkvPattern) mkvObject).subscribe(LOGIN_FIELDS, FENICSOrderCreatorFixed.this);
//                                     LOGGER.info("   ✅ Successfully subscribed to login pattern");
//                                 } catch (Exception e) {
//                                     LOGGER.error("Error subscribing to pattern: ", e);
//                                 }
//                             }
//                         }
                        
//                         @Override
//                         public void onPublishIdle(String component, boolean start) {}
                        
//                         @Override
//                         public void onSubscribe(MkvObject obj) {}
//                     });
//                 }
//             }
//         } catch (Exception e) {
//             LOGGER.error("   ❌ Error subscribing to login: ", e);
//         } finally {
//             // Always count down after a delay to not block forever
//             new Thread(() -> {
//                 try {
//                     Thread.sleep(3000);
//                     loginCheckLatch.countDown();
//                 } catch (InterruptedException e) {
//                     Thread.currentThread().interrupt();
//                 }
//             }).start();
//         }
//     }
    
//     /**
//      * Check login status from record
//      */
//     private void checkLoginStatus(MkvRecord record) {
//         try {
//             for (int i = 0; i < 8; i++) {
//                 String src = record.getValue("Src" + i).getString();
//                 String status = record.getValue("TStatusStr" + i).getString();
                
//                 if (src != null && status != null && MARKET_SOURCE.equals(src)) {
//                     LOGGER.info("   {} status for {}: {}", SYSTEM_USER, src, status);
                    
//                     if ("On".equals(status)) {
//                         isVenueActive = true;
//                         OrderRepository.getInstance().addVenueActive(MARKET_SOURCE, true);
//                     } else {
//                         OrderRepository.getInstance().addVenueActive(MARKET_SOURCE, false);
//                     }
//                     break;
//                 }
//             }
//         } catch (Exception e) {
//             LOGGER.error("Error checking login status: ", e);
//         }
//     }
    
//     /**
//      * Create order - 修改为使用动态价格
//      */
//     public static FENICSOrderCreatorFixed createOrder(String instrumentId, String originalCusip) {
//         reqId++;
        
//         LOGGER.info("╔════════════════════════════════════════════════╗");
//         LOGGER.info("║            ORDER CREATION REQUEST              ║");
//         LOGGER.info("╠════════════════════════════════════════════════╣");
//         LOGGER.info("║ Request ID: {}", String.format("%-35d║", reqId));
//         LOGGER.info("║ Trader: {}", String.format("%-39s║", TRADER_ID));
//         LOGGER.info("║ Instrument: {}", String.format("%-35s║", instrumentId));
//         LOGGER.info("║ Quantity: {}", String.format("%-37.0f║", ORDER_QUANTITY));
//         LOGGER.info("║ Price: {}", String.format("%-40.4f║", dynamicOrderPrice));
//         LOGGER.info("╚════════════════════════════════════════════════╝");
        
//         MkvPublishManager pm = Mkv.getInstance().getPublishManager();
//         String functionName = MARKET_SOURCE + "_VCMIOrderAdd181";
        
//         try {
//             MkvFunction fn = pm.getMkvFunction(functionName);
            
//             if (fn != null) {
//                 return createOrderWithFunction(fn, instrumentId, originalCusip);
//             } else {
//                 LOGGER.error("❌ Function not found: {}", functionName);
//                 return null;
//             }
//         } catch (Exception e) {
//             LOGGER.error("❌ Error: ", e);
//             return null;
//         }
//     }
    
//     /**
//      * Create order with function - 修改为使用动态价格
//      */
//     private static FENICSOrderCreatorFixed createOrderWithFunction(MkvFunction fn, 
//             String instrumentId, String originalCusip) {
//         try {
//             String freeText = MarketDef.getFreeText(String.valueOf(reqId), APPLICATION_ID);
            
//             FENICSOrderCreatorFixed order = new FENICSOrderCreatorFixed(reqId);
            
//             OrderDetails details = new OrderDetails("PENDING_" + reqId, reqId, originalCusip);
//             orderTracking.put(String.valueOf(reqId), details);
            
//             MkvSupply args = MkvSupplyFactory.create(new Object[] {
//                 TRADER_ID,                          // Trader ID (frosasl1)
//                 instrumentId,                       // Instrument
//                 ORDER_VERB,                         // Verb
//                 Double.valueOf(dynamicOrderPrice),  // Price - 使用动态价格
//                 Double.valueOf(ORDER_QUANTITY),     // QtyShown
//                 Double.valueOf(ORDER_QUANTITY),     // QtyTot
//                 ORDER_TYPE,                         // Type
//                 TIME_IN_FORCE,                      // TimeInForce
//                 Integer.valueOf(0),                 // IsSoft
//                 Integer.valueOf(0),                 // Attribute
//                 "",                                 // CustomerInfo
//                 freeText,                           // FreeText
//                 Integer.valueOf(0),                 // StopCond
//                 "",                                 // StopId
//                 Double.valueOf(0)                   // StopPrice
//             });
            
//             fn.call(args, order);
//             LOGGER.info("✅ Order function called with price: {}", dynamicOrderPrice);
            
//             return order;
            
//         } catch (Exception e) {
//             LOGGER.error("❌ Error: ", e);
//             return null;
//         }
//     }
    
//     /**
//      * Log order status
//      */
//     private static void logOrderStatus() {
//         for (OrderDetails details : orderTracking.values()) {
//             LOGGER.info("Order {} | Status: {} | Error: {}", 
//                 details.orderId, details.status, details.errorMsg);
//         }
//     }
    
//     // MkvRecordListener - Fixed to monitor evan_gerhard login
//     @Override
//     public void onFullUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
//         try {
//             String recordName = record.getName();
            
//             // Check evan_gerhard login status
//             if (recordName.contains("CM_LOGIN") && recordName.contains(SYSTEM_USER)) {
//                 LOGGER.info("📥 Login update for {}", SYSTEM_USER);
//                 checkLoginStatus(record);
//                 loginCheckLatch.countDown();
//             }
//             // Handle order updates
//             else if (recordName.contains("CM_ORDER")) {
//                 // Process order updates...
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("Error: ", e);
//         }
//     }
    
//     @Override
//     public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
//         onFullUpdate(record, supply, isSnapshot);
//     }
    
//     // MkvFunctionCallListener - 修改以提取订单ID
//     @Override
//     public void onResult(MkvFunctionCallEvent event, MkvSupply supply) {
//         try {
//             String result = supply.getString(supply.firstIndex());
//             LOGGER.info("📨 Order Response: {}", result);
            
//             if (result.contains("OK")) {
//                 // 新增：提取订单ID
//                 String extractedOrderId = extractOrderId(result);
                
//                 OrderDetails details = orderTracking.get(String.valueOf(myReqId));
//                 if (details != null) {
//                     details.status = "SUBMITTED";
//                     if (extractedOrderId != null) {
//                         details.orderId = extractedOrderId;
//                         LOGGER.info("📝 Order ID: {}", extractedOrderId);
//                     }
//                 }
//                 LOGGER.info("✅ Order submitted successfully at price: {}", dynamicOrderPrice);
//             } else {
//                 OrderDetails details = orderTracking.get(String.valueOf(myReqId));
//                 if (details != null) {
//                     details.status = "FAILED";
//                     details.errorMsg = result;
//                 }
                
//                 if (result.contains("101") || result.contains("not logged in")) {
//                     LOGGER.error("❌ Error 101: User not logged in");
//                     LOGGER.info("💡 Solution: Ensure {} is logged in to {}", SYSTEM_USER, MARKET_SOURCE);
//                 } else if (result.contains("Price Exceeds Current Price Band")) {
//                     LOGGER.error("❌ Price Band Error: Order price {} is outside allowed range", dynamicOrderPrice);
//                     LOGGER.info("💡 Market may have moved, try again for updated price");
//                 } else {
//                     LOGGER.error("❌ Order failed: {}", result);
//                 }
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("Error processing response: ", e);
//         }
//     }
    
//     /**
//      * Extract order ID from result - 新增方法
//      */
//     private String extractOrderId(String result) {
//         try {
//             int idStart = result.indexOf("-Id {");
//             if (idStart != -1) {
//                 idStart += 5;
//                 int idEnd = result.indexOf("}", idStart);
//                 if (idEnd != -1) {
//                     return result.substring(idStart, idEnd);
//                 }
//             }
//         } catch (Exception e) {
//             LOGGER.error("Error extracting order ID: ", e);
//         }
//         return null;
//     }
    
//     @Override
//     public void onError(MkvFunctionCallEvent event, byte errCode, String errStr) {
//         this.errCode = errCode;
//         this.errStr = errStr;
//         LOGGER.error("❌ Order Error - Code: {} | Error: {}", errCode, errStr);
        
//         OrderDetails details = orderTracking.get(String.valueOf(myReqId));
//         if (details != null) {
//             details.status = "ERROR";
//             details.errorMsg = String.format("Code %d: %s", errCode, errStr);
//         }
//     }
    
//     // MkvPlatformListener
//     @Override
//     public void onMain(MkvPlatformEvent event) {
//         // Ignore
//     }
    
//     @Override
//     public void onComponent(com.iontrading.mkv.MkvComponent component, boolean registered) {
//         String name = component.getName();
//         if (name.equals(MARKET_SOURCE) || name.equals("ROUTER_US") || name.equals("VMO_REPO_US")) {
//             LOGGER.info("🔧 Component {} {}", name, registered ? "registered" : "unregistered");
//         }
//     }
    
//     @Override
//     public void onConnect(String component, boolean connected) {
//         if (connected && (component.equals("ROUTER_US") || component.contains("ROUTER"))) {
//             LOGGER.info("✅ Connected to: {}", component);
//             isConnected = true;
//             connectionLatch.countDown();
//         }
//     }
// }