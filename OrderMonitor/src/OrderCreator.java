import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionCallListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.qos.MkvQoS;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OrderCreator - Professional Order Creation and Monitoring
 * 专业的订单创建和监控系统
 * 
 * Features:
 * - Order creation via ION MKV API
 * - Real-time order status monitoring
 * - Order lifecycle tracking
 * - Professional logging and error handling
 */
public class OrderCreator implements MkvFunctionCallListener, MkvPlatformListener, MkvRecordListener {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderCreator.class);
    
    // Configuration
    private static final String[] POSSIBLE_MARKET_SOURCES = {
        "FENICS_USREPO", 
        "BTEC_REPO_US",
        "DEALERWEB_REPO"
    };
    
    private static final String TRADER_ID = "frosasl1";
    private static final String INSTRUMENT_ID = "912797RD1";
    private static final String ORDER_VERB = "Buy";
    private static final double ORDER_QUANTITY = 1.0;
    private static final double ORDER_PRICE = 100.0;
    private static final String ORDER_TYPE = "Limit";
    private static final String TIME_IN_FORCE = "Day";
    private static final String APPLICATION_ID = "OrderCreator_v1.0";
    
    // Instance variables
    private static int reqId = 0;
    private final int myReqId;
    private String orderId;
    private final long creationTimestamp;
    private boolean orderDead = false;
    private byte errCode = (byte) 0;
    private String errStr = "";
    
    // Connection state
    private final CountDownLatch connectionLatch = new CountDownLatch(1);
    private boolean isConnected = false;
    
    // Order tracking - maps orderId to order details
    private static final Map<String, OrderDetails> orderTracking = new ConcurrentHashMap<>();
    
    // Inner class to track order details
    private static class OrderDetails {
        final String orderId;
        final int reqId;
        final long timestamp;
        String status = "PENDING";
        double filledQty = 0.0;
        String errorMsg = "";
        
        OrderDetails(String orderId, int reqId) {
            this.orderId = orderId;
            this.reqId = reqId;
            this.timestamp = System.currentTimeMillis();
        }
    }
    
    /**
     * Constructor
     */
    public OrderCreator(int reqId) {
        this.myReqId = reqId;
        this.creationTimestamp = System.currentTimeMillis();
        LOGGER.info("OrderCreator initialized - ReqId: {}", reqId);
    }
    
    /**
     * Main method
     */
    public static void main(String[] args) {
        LOGGER.info("================================================");
        LOGGER.info("OrderCreator - Professional Order Management");
        LOGGER.info("Version: 1.0");
        LOGGER.info("================================================");
        
        try {
            // Create main instance
            OrderCreator mainInstance = new OrderCreator(0);
            
            // Start MKV platform
            MkvQoS qos = new MkvQoS();
            if (args.length > 0) {
                qos.setArgs(args);
                LOGGER.info("Using command line arguments: {}", String.join(" ", args));
            } else {
                LOGGER.info("Using mkv.init file configuration");
            }
            
            qos.setPlatformListeners(new MkvPlatformListener[] { mainInstance });
            Mkv.start(qos);
            LOGGER.info("MKV platform started successfully");
            
            // Wait for connection
            LOGGER.info("Waiting for connection...");
            boolean connected = mainInstance.connectionLatch.await(30, TimeUnit.SECONDS);
            if (!connected || !mainInstance.isConnected) {
                LOGGER.error("Connection timeout! Please check:");
                LOGGER.error("- mkv.cshost = 10.74.124.71");
                LOGGER.error("- mkv.csport = 20000");
                LOGGER.error("- mkv.user = mkv");
                return;
            }
            
            LOGGER.info("Connection established successfully!");
            Thread.sleep(3000); // Allow functions to publish
            
            // Subscribe to order records
            mainInstance.subscribeToOrderRecords();
            
            // Check available resources
            mainInstance.checkAvailableResources();
            
            // Create order
            OrderCreator orderCreator = createOrder();
            
            if (orderCreator != null) {
                LOGGER.info("Order creation request sent, monitoring for updates...");
                
                // Monitor for order updates
                Thread monitorThread = new Thread(() -> {
                    try {
                        for (int i = 0; i < 30; i++) { // 30 seconds monitoring
                            Thread.sleep(1000);
                            logOrderStatus();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                monitorThread.setName("OrderMonitor");
                monitorThread.start();
                monitorThread.join();
                
                // Final status
                LOGGER.info("\nFinal order status:");
                logOrderStatus();
            } else {
                LOGGER.error("Failed to create order");
            }
            
        } catch (Exception e) {
            LOGGER.error("Program error: ", e);
            e.printStackTrace();
        } finally {
            Mkv.stop();
            LOGGER.info("MKV platform stopped");
        }
    }
    
    /**
     * Check available resources
     */
    private void checkAvailableResources() {
        try {
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            if (pm == null) {
                LOGGER.error("Cannot get MkvPublishManager");
                return;
            }
            
            LOGGER.info("\nChecking available resources...");
            
            // Get function count
            try {
                int functionCount = pm.getMkvFunctions() != null ? pm.getMkvFunctions().size() : 0;
                LOGGER.info("Available functions: {}", functionCount);
                
                if (functionCount > 0 && functionCount < 50) {
                    LOGGER.info("Order-related functions:");
                    int count = 0;
                    for (Object obj : pm.getMkvFunctions()) {
                        if (obj instanceof MkvFunction) {
                            MkvFunction func = (MkvFunction) obj;
                            String funcName = func.getName();
                            
                            if (funcName.contains("Order") || funcName.contains("VCMI")) {
                                LOGGER.info("  - {}", funcName);
                            }
                        }
                        if (++count > 100) break;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error getting function list: {}", e.getMessage());
            }
            
        } catch (Exception e) {
            LOGGER.error("Error checking resources: ", e);
        }
    }
    
    /**
     * Subscribe to order records to monitor order lifecycle
     */
    private void subscribeToOrderRecords() {
        try {
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            if (pm == null) return;
            
            LOGGER.info("\nSubscribing to order records...");
            
            // Subscribe to CM_Order patterns
            String[] orderPatterns = {
                "DEALERWEB_REPO.CM_Order.*",
                "FENICS_USREPO.CM_Order.*",
                "BTEC_REPO_US.CM_Order.*",
                "CM_Order.*"
            };
            
            for (String pattern : orderPatterns) {
                try {
                    MkvRecord record = pm.getMkvRecord(pattern);
                    if (record != null) {
                        record.subscribe(this);
                        LOGGER.info("✓ Subscribed to: {}", pattern);
                    }
                } catch (Exception e) {
                    LOGGER.debug("Pattern not available: {}", pattern);
                }
            }
            
        } catch (Exception e) {
            LOGGER.error("Error subscribing to order records: ", e);
        }
    }
    
    /**
     * Log current order status
     */
    private static void logOrderStatus() {
        if (orderTracking.isEmpty()) {
            LOGGER.info("No orders being tracked");
            return;
        }
        
        LOGGER.info("\n╔══════════════════════════════════════════════════════════════╗");
        LOGGER.info("║                     ORDER STATUS SUMMARY                      ║");
        LOGGER.info("╠══════════════════════════════════════════════════════════════╣");
        
        for (OrderDetails details : orderTracking.values()) {
            LOGGER.info("║ OrderId: {} | ReqId: {} | Status: {}",
                String.format("%-20s", details.orderId),
                String.format("%-3d", details.reqId),
                String.format("%-15s", details.status));
            LOGGER.info("║ Filled: {} | Error: {}",
                String.format("%-10.2f", details.filledQty),
                details.errorMsg.isEmpty() ? "None" : details.errorMsg);
            LOGGER.info("╟──────────────────────────────────────────────────────────────╢");
        }
        LOGGER.info("╚══════════════════════════════════════════════════════════════╝\n");
    }
    
    /**
     * Create order - static factory method
     */
    public static OrderCreator createOrder() {
        reqId++;
        
        LOGGER.info("\n=== CREATING ORDER ===");
        LOGGER.info("ReqId: {}", reqId);
        LOGGER.info("Instrument: {}", INSTRUMENT_ID);
        LOGGER.info("Side: {}", ORDER_VERB);
        LOGGER.info("Quantity: {}", ORDER_QUANTITY);
        LOGGER.info("Price: {}", ORDER_PRICE);
        LOGGER.info("Type: {}", ORDER_TYPE);
        LOGGER.info("TIF: {}", TIME_IN_FORCE);
        LOGGER.info("====================");
        
        MkvPublishManager pm = Mkv.getInstance().getPublishManager();
        if (pm == null) {
            LOGGER.error("Cannot get MkvPublishManager");
            return null;
        }
        
        // Try different market sources
        for (String marketSource : POSSIBLE_MARKET_SOURCES) {
            String functionName = marketSource + "_VCMIOrderAdd181";
            
            LOGGER.info("Trying function: {}", functionName);
            
            try {
                MkvFunction fn = pm.getMkvFunction(functionName);
                
                if (fn != null) {
                    LOGGER.info("✓ Found function: {}", functionName);
                    return createOrderWithFunction(fn, marketSource);
                }
            } catch (Exception e) {
                LOGGER.debug("Function {} not available: {}", functionName, e.getMessage());
            }
        }
        
        LOGGER.error("No order functions available!");
        LOGGER.error("Please ensure:");
        LOGGER.error("1. You are connected to the correct market");
        LOGGER.error("2. You have permissions to access order functions");
        LOGGER.error("3. The market source names are correct");
        
        return null;
    }
    
    /**
     * Create order with specific function
     */
    private static OrderCreator createOrderWithFunction(MkvFunction fn, String marketSource) {
        try {
            // Create FreeText
            String freeText = createFreeText(String.valueOf(reqId), APPLICATION_ID);
            
            // Create order instance
            OrderCreator order = new OrderCreator(reqId);
            
            // Track this order
            OrderDetails details = new OrderDetails("PENDING_" + reqId, reqId);
            orderTracking.put(String.valueOf(reqId), details);
            
            // Create order arguments
            MkvSupply args = MkvSupplyFactory.create(new Object[] {
                TRADER_ID,                      // User
                INSTRUMENT_ID,                  // InstrumentId
                ORDER_VERB,                     // Verb
                Double.valueOf(ORDER_PRICE),    // Price
                Double.valueOf(ORDER_QUANTITY), // QtyShown
                Double.valueOf(ORDER_QUANTITY), // QtyTot
                ORDER_TYPE,                     // Type
                TIME_IN_FORCE,                  // TimeInForce
                Integer.valueOf(0),             // IsSoft
                Integer.valueOf(0),             // Attribute
                "",                             // CustomerInfo
                freeText,                       // FreeText
                Integer.valueOf(0),             // StopCond
                "",                             // StopId
                Double.valueOf(0)               // StopPrice
            });
            
            LOGGER.info("Calling order function...");
            
            // Call function
            fn.call(args, order);
            
            LOGGER.info("Order function called successfully");
            
            return order;
            
        } catch (Exception e) {
            LOGGER.error("Error creating order: ", e);
            return null;
        }
    }
    
    /**
     * Create FreeText field
     */
    private static String createFreeText(String userData, String freeText) {
        String separator = "\1";  // ASCII 0x01 separator
        return separator + userData + separator + freeText;
    }
    
    // MkvFunctionCallListener implementation
    @Override
    public void onResult(MkvFunctionCallEvent event, MkvSupply supply) {
        try {
            if (supply == null) {
                LOGGER.warn("Received null response - ReqId: {}", myReqId);
                return;
            }
            
            String resultString = supply.getString(supply.firstIndex());
            
            LOGGER.info("\n╔════════════════════════════════════════╗");
            LOGGER.info("║         ORDER RESPONSE RECEIVED         ║");
            LOGGER.info("╠════════════════════════════════════════╣");
            LOGGER.info("║ ReqId: {}", String.format("%-32s", myReqId) + " ║");
            LOGGER.info("║ Response: {}", String.format("%-29s", 
                resultString.length() > 29 ? resultString.substring(0, 26) + "..." : resultString) + " ║");
            
            // Parse response
            if (resultString.contains("OK")) {
                // Extract order ID
                int idIndex = resultString.indexOf("-Id {");
                if (idIndex >= 0) {
                    int startIndex = idIndex + 5;
                    int endIndex = resultString.indexOf("}", startIndex);
                    if (endIndex > startIndex) {
                        this.orderId = resultString.substring(startIndex, endIndex).trim();
                        
                        // Update tracking
                        OrderDetails details = orderTracking.get(String.valueOf(myReqId));
                        if (details != null) {
                            orderTracking.remove(String.valueOf(myReqId));
                            OrderDetails newDetails = new OrderDetails(this.orderId, myReqId);
                            newDetails.status = "SUBMITTED";
                            orderTracking.put(this.orderId, newDetails);
                        }
                        
                        LOGGER.info("║ ✓ ORDER CREATED SUCCESSFULLY!          ║");
                        LOGGER.info("║ Order ID: {}", String.format("%-28s", this.orderId) + " ║");
                        
                        // Subscribe to this specific order record
                        subscribeToSpecificOrder(this.orderId);
                    }
                }
            } else {
                LOGGER.info("║ ❌ ORDER CREATION FAILED!              ║");
                LOGGER.info("║ Error: {}", String.format("%-31s", 
                    resultString.length() > 31 ? resultString.substring(0, 28) + "..." : resultString) + " ║");
                
                OrderDetails details = orderTracking.get(String.valueOf(myReqId));
                if (details != null) {
                    details.status = "FAILED";
                    details.errorMsg = resultString;
                }
            }
            LOGGER.info("╚════════════════════════════════════════╝\n");
            
        } catch (Exception e) {
            LOGGER.error("Error processing response: ", e);
        }
    }
    
    @Override
    public void onError(MkvFunctionCallEvent event, byte errCode, String errStr) {
        this.errCode = errCode;
        this.errStr = errStr;
        this.orderDead = true;
        
        LOGGER.error("\n╔════════════════════════════════════════╗");
        LOGGER.error("║           ORDER ERROR RECEIVED          ║");
        LOGGER.error("╠════════════════════════════════════════╣");
        LOGGER.error("║ ReqId: {}", String.format("%-32s", myReqId) + " ║");
        LOGGER.error("║ Error Code: {}", String.format("%-26s", errCode) + " ║");
        LOGGER.error("║ Error: {}", String.format("%-31s", 
            errStr.length() > 31 ? errStr.substring(0, 28) + "..." : errStr) + " ║");
        LOGGER.error("╚════════════════════════════════════════╝\n");
        
        OrderDetails details = orderTracking.get(String.valueOf(myReqId));
        if (details != null) {
            details.status = "ERROR";
            details.errorMsg = errStr;
        }
    }
    
    /**
     * Subscribe to specific order record
     */
    private static void subscribeToSpecificOrder(String orderId) {
        try {
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            if (pm == null) return;
            
            LOGGER.info("Subscribing to specific order record: {}", orderId);
            
            // Try different patterns for the order record
            String[] patterns = {
                "DEALERWEB_REPO.CM_Order." + orderId,
                "FENICS_USREPO.CM_Order." + orderId,
                "BTEC_REPO_US.CM_Order." + orderId,
                "CM_Order." + orderId
            };
            
            for (String recordName : patterns) {
                try {
                    MkvRecord record = pm.getMkvRecord(recordName);
                    if (record != null) {
                        // Use a new instance for the listener
                        OrderCreator listener = new OrderCreator(0);
                        record.subscribe(listener);
                        LOGGER.info("✓ Subscribed to order record: {}", recordName);
                        break;
                    }
                } catch (Exception e) {
                    LOGGER.debug("Order record not found: {}", recordName);
                }
            }
            
        } catch (Exception e) {
            LOGGER.error("Error subscribing to order record: ", e);
        }
    }
    
    // MkvRecordListener implementation
    @Override
    public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
        // Log partial updates
        LOGGER.debug("Partial update for record: {}", record.getName());
    }
    
    @Override
    public void onFullUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
        try {
            String recordName = record.getName();
            
            // Extract order ID from record name
            String orderId = recordName.substring(recordName.lastIndexOf('.') + 1);
            OrderDetails details = orderTracking.get(orderId);
            
            // Get order fields
            String status = "";
            int active = 0;
            double qtyFilled = 0.0;
            String trader = "";
            String instrument = "";
            
            try {
                if (record.getValue("Status") != null) {
                    status = record.getValue("Status").toString();
                }
                if (record.getValue("Active") != null) {
                    active = record.getValue("Active").getInt();
                }
                if (record.getValue("QtyFilled") != null) {
                    // MkvValue doesn't have getDouble(), so we convert from string
                    String qtyStr = record.getValue("QtyFilled").toString();
                    try {
                        qtyFilled = Double.parseDouble(qtyStr);
                    } catch (NumberFormatException e) {
                        qtyFilled = 0.0;
                    }
                }
                if (record.getValue("Trader") != null) {
                    trader = record.getValue("Trader").toString();
                }
                if (record.getValue("InstrumentId") != null) {
                    instrument = record.getValue("InstrumentId").toString();
                }
            } catch (Exception e) {
                LOGGER.warn("Error reading order fields: {}", e.getMessage());
            }
            
            LOGGER.info("\n╔════════════════════════════════════════╗");
            LOGGER.info("║        ORDER RECORD UPDATE              ║");
            LOGGER.info("╠════════════════════════════════════════╣");
            LOGGER.info("║ Record: {}", String.format("%-30s", 
                recordName.length() > 30 ? "..." + recordName.substring(recordName.length() - 27) : recordName) + " ║");
            LOGGER.info("║ OrderId: {}", String.format("%-29s", orderId) + " ║");
            LOGGER.info("║ Status: {}", String.format("%-30s", status) + " ║");
            LOGGER.info("║ Active: {}", String.format("%-30s", active == 1 ? "YES" : "NO") + " ║");
            LOGGER.info("║ QtyFilled: {}", String.format("%-27.2f", qtyFilled) + " ║");
            LOGGER.info("║ Trader: {}", String.format("%-30s", trader) + " ║");
            LOGGER.info("║ Instrument: {}", String.format("%-26s", instrument) + " ║");
            
            // Update tracking
            if (details != null) {
                details.status = status;
                details.filledQty = qtyFilled;
                
                if (active == 0) {
                    details.status = "COMPLETED/DEAD";
                    LOGGER.info("║ *** ORDER IS NO LONGER ACTIVE ***      ║");
                }
            }
            
            LOGGER.info("╚════════════════════════════════════════╝\n");
            
        } catch (Exception e) {
            LOGGER.error("Error processing order update: ", e);
        }
    }
    
    // MkvPlatformListener implementation
    @Override
    public void onMain(MkvPlatformEvent event) {
        switch (event.intValue()) {
            case MkvPlatformEvent.START_code:
                LOGGER.debug("Platform event: START");
                break;
            case MkvPlatformEvent.REGISTER_code:
                LOGGER.info("Platform event: REGISTER - Component registered");
                break;
        }
    }
    
    @Override
    public void onComponent(com.iontrading.mkv.MkvComponent component, boolean registered) {
        LOGGER.info("Component event: {} {}", component.getName(), registered ? "registered" : "unregistered");
    }
    
    @Override
    public void onConnect(String component, boolean connected) {
        if (connected) {
            LOGGER.info("✓ Connected to: {}", component);
            isConnected = true;
            connectionLatch.countDown();
        } else {
            LOGGER.warn("Disconnected from: {}", component);
            isConnected = false;
        }
    }
}