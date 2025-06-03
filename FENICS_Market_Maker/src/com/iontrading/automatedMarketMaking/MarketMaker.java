package com.iontrading.automatedMarketMaking;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvPattern;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.exceptions.MkvException;

/**
 * MarketMaker implements a market making strategy for FENICS that quotes based on
 * the inside market from DEALERWEB and BTEC.
 * 
 * The strategy:
 * 1. Monitors Bid/Ask from DEALERWEB and BTEC through VMO_REPO_US
 * 2. Places orders on FENICS at Bid + Adjustment and Ask - Adjustment
 * 3. If no bid, then bid at GC_BEST_BID + Adjustment
 * 4. Updates orders when the market moves
 * 5. Hedges filled orders by trading on the venue that provided the price reference
 */
public class MarketMaker implements IOrderManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MarketMaker.class);
    
    // Add diagnostic counters
    private final AtomicInteger emptyIdCounter = new AtomicInteger(0);
    private final AtomicInteger nonOvernightCounter = new AtomicInteger(0);
    private final AtomicInteger marketUpdateCounter = new AtomicInteger(0);
    private final AtomicInteger processedUpdateCounter = new AtomicInteger(0);
    private final Map<String, AtomicInteger> instrumentUpdateCounters = new ConcurrentHashMap<>();

    // Reference to the main OrderManagement component
    private final OrderManagement orderManager;
    
    // Store our active orders by instrument ID
    private final Map<String, ActiveQuote> activeQuotes = new ConcurrentHashMap<>();
    
    // Store the trader ID to use for FENICS
    private final String fenicsTrader;
    
    // Executor for periodic tasks
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    // Track whether market making is enabled
    private volatile boolean enabled = true;
    
    private boolean isBondStaticSubscribed = false;
    private boolean isFirmPositionSubscribed = false;
    private boolean isSdsInformationSubscribed = false;

    // Configuration for the market maker
    private final MarketMakerConfig config;
    
    private final BondEligibilityListener bondEligibilityListener;
    private final Set<String> trackedInstruments = ConcurrentHashMap.newKeySet();

    private static final String BOND_STATIC_PATTERN = "USD.CM_BOND.VMO_REPO_US.";
    private static final String[] BOND_STATIC_FIELDS = {
        "Id", "Code", "Maturity"
    };
    
    private static final String FIRM_POSITION_PATTERN = "USD.IU_POSITION.VMO_REPO_US.";
    private static final String[] FIRM_POSITION_FIELDS = {
        "Id", "VirtualId", "Isin", "CalcNetExtPos", "StartDate"
    };

    private static final String SDS_INFORMATION_PATTERN = "ALL.POSITION_US.SDS.";
    private static final String[] SDS_INFORMATION_FIELDS = {
        "Id", "Code", "DateStart", "SOMA"
    };

    // Use DepthListener
    private DepthListener depthListener = null;
    
    private final Map<String, Integer> orderIdToReqIdMap = new HashMap<>();

    public String getApplicationId() {
        // Return a unique identifier for this market maker instance
        return "MarketMaker_" + System.currentTimeMillis();
        // Or if you have a specific app ID:
        // return "ION_MarketMaker_v1.0";
    }
    /**
     * Creates a new MarketMaker instance with default configuration.
     * 
     * @param orderManager The parent OrderManagement instance
     */
    public MarketMaker(OrderManagement orderManager) {
        LOGGER.info("MarketMaker.constructor", "Creating with default config, orderManager=" + orderManager);
        try {
            this.orderManager = orderManager;
            this.config = new MarketMakerConfig();
            LOGGER.info("MarketMaker initialized with default config");

            this.bondEligibilityListener = new BondEligibilityListener();

            this.fenicsTrader = orderManager.getTraderForVenue(config.getMarketSource());
            LOGGER.info("Using trader ID for {}: {}", config.getMarketSource(), fenicsTrader);
            
            // Get DepthListener from OrderManagement
            this.depthListener = OrderManagement.getDepthListener();
            if (depthListener == null) {
                LOGGER.error("CRITICAL ERROR: DepthListener is null - market data updates won't be processed properly!");
            } else {
                LOGGER.info("DepthListener obtained successfully from OrderManagement");
            }
            
            // Register for eligibility change notifications
            this.bondEligibilityListener.addEligibilityChangeListener(new EligibilityChangeListener() {
                public void onEligibilityChange(String cusip, boolean isEligible, Map<String, Object> bondData) {
                    LOGGER.info("EligibilityChangeListener.onEligibilityChange: cusip={}, isEligible={}", cusip, isEligible);
                    handleEligibilityChange(cusip, isEligible, bondData);
                    LOGGER.info("EligibilityChangeListener.onEligibilityChange: Eligibility change handled");
                }
            });

            LOGGER.info("MarketMaker initialized with bond eligibility integration");

            // Start periodic market making for eligible bonds
            scheduler.scheduleAtFixedRate(
                this::makeMarketsForEligibleBonds, 
                60, // Initial delay (seconds) 
                30, // Run every 30 seconds
                TimeUnit.SECONDS
            );
            
            // Add a diagnostic check task
            scheduler.scheduleAtFixedRate(
                this::logDiagnosticStatistics, 
                30, // Initial delay (seconds) 
                60, // Run every minute
                TimeUnit.SECONDS
            );
            
            // Register shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("MarketMaker.shutdownHook: Shutting down MarketMaker scheduler");
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                    LOGGER.error("MarketMaker.shutdownHook: Interrupted during shutdown", e);
                }
                LOGGER.info("MarketMaker.shutdownHook: Shutdown complete");
            }));

            LOGGER.info("MarketMaker.constructor: Successfully created MarketMaker instance");
        } catch (Exception e) {
            LOGGER.error("MarketMaker.constructor: Error creating MarketMaker", e);
            throw e; // Re-throw to maintain original behavior
        }
    }
    
    /**
     * Creates a new MarketMaker instance with custom configuration and default BondEligibilityListener.
     * 
     * @param orderManager The parent OrderManagement instance
     * @param config The market maker configuration
     */
    public MarketMaker(OrderManagement orderManager, MarketMakerConfig config) {
        LOGGER.info("MarketMaker.constructor: Creating with custom config, orderManager={}, config={}", orderManager, config);
        try {
            this.orderManager = orderManager;
            this.config = config;

            this.bondEligibilityListener = new BondEligibilityListener();
            
            // Subscribe to MKV data streams with detailed logging
            LOGGER.info("Subscribing to MKV data streams...");
            subscribeToBondStaticData();
            subscribeToFirmPositionData();
            subscribeToSdsInformationData();

            this.fenicsTrader = orderManager.getTraderForVenue(config.getMarketSource());
            LOGGER.info("Using trader ID for {}: {}", config.getMarketSource(), fenicsTrader);
            
            // Get DepthListener from OrderManagement
            this.depthListener = OrderManagement.getDepthListener();
            if (depthListener == null) {
                LOGGER.error("CRITICAL ERROR: DepthListener is null - market data updates won't be processed properly!");
            } else {
                LOGGER.info("DepthListener obtained successfully from OrderManagement");
            }
            
            // Register for eligibility change notifications
            this.bondEligibilityListener.addEligibilityChangeListener(new EligibilityChangeListener() {
                public void onEligibilityChange(String cusip, boolean isEligible, Map<String, Object> bondData) {
                    LOGGER.info("EligibilityChangeListener.onEligibilityChange: cusip={}, isEligible={}", cusip, isEligible);
                    handleEligibilityChange(cusip, isEligible, bondData);
                    LOGGER.info("EligibilityChangeListener.onEligibilityChange: Eligibility change handled");
                }
            });

            LOGGER.info("MarketMaker initialized with bond eligibility integration");

            // Start periodic market making for eligible bonds
            scheduler.scheduleAtFixedRate(
                this::makeMarketsForEligibleBonds, 
                60, // Initial delay (seconds) 
                30, // Run every 30 seconds
                TimeUnit.SECONDS
            );
            
            // Add a diagnostic check task
            scheduler.scheduleAtFixedRate(
                this::logDiagnosticStatistics, 
                30, // Initial delay (seconds) 
                60, // Run every minute
                TimeUnit.SECONDS
            );
            
            // Register shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("MarketMaker.shutdownHook: Shutting down MarketMaker scheduler");
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                    LOGGER.error("MarketMaker.shutdownHook: Interrupted during shutdown", e);
                }
                LOGGER.info("MarketMaker.shutdownHook: Shutdown complete");
            }));

            LOGGER.info("MarketMaker.constructor: Successfully created MarketMaker instance with custom config");
        } catch (Exception e) {
            LOGGER.error("MarketMaker.constructor: Error creating MarketMaker with custom config", e);
            throw e; // Re-throw to maintain original behavior
        }
    }

    // Add a new method to initialize MKV subscriptions after MKV is started
    public void initializeMkvSubscriptions() {
        LOGGER.info("MarketMaker.initializeMkvSubscriptions: Initializing MKV subscriptions");
        try {
            LOGGER.info("Subscribing to MKV data streams...");
            subscribeToBondStaticData();
            subscribeToFirmPositionData();
            subscribeToSdsInformationData();
            
            // Update DepthListener reference now that it should be available
            this.depthListener = OrderManagement.getDepthListener();
            if (depthListener == null) {
                LOGGER.error("CRITICAL ERROR: DepthListener is still null after MKV initialization!");
            } else {
                LOGGER.info("DepthListener obtained successfully after MKV initialization");
            }

            LOGGER.info("MarketMaker.initializeMkvSubscriptions: MKV subscriptions initialized");
        } catch (Exception e) {
            LOGGER.error("MarketMaker.initializeMkvSubscriptions: Error initializing MKV subscriptions", e);
        }
    }

    /**
     * Subscribe to bond static data
     */
    private void subscribeToBondStaticData() {
        LOGGER.info("subscribeToBondStaticData: Subscribing to bond static data");
        
        try {
            // Get the publish manager to access patterns
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            LOGGER.info("Got publish manager: {}", pm);
       
            // Look up the pattern object
            MkvObject obj = pm.getMkvObject(BOND_STATIC_PATTERN);
            LOGGER.info("Looking up pattern: {}, result: {}", BOND_STATIC_PATTERN, obj);
            
            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                // Subscribe within a synchronized block
                LOGGER.info("Found bond static pattern, subscribing: {}", (Object) BOND_STATIC_PATTERN);
                try {
                    ((MkvPattern) obj).subscribe(BOND_STATIC_FIELDS, bondEligibilityListener);
                    LOGGER.info("Subscribe call completed for bond static data");
                } catch (Exception e) {
                    LOGGER.error("Error during pattern subscription: {}", e.getMessage(), e);
                }
                
                // Mark that we've successfully subscribed
                isBondStaticSubscribed = true;

                LOGGER.info("Subscribed to bond static data: {} with {} fields",
                    BOND_STATIC_PATTERN, BOND_STATIC_FIELDS.length);

                LOGGER.info("subscribeToBondStaticData: Successfully subscribed to bond static data");
            } else {
                LOGGER.error("Bond static pattern not found: {}. MKV object: {}", 
                    BOND_STATIC_PATTERN, obj);
                LOGGER.error("subscribeToBondStaticData: Failed: Bond static pattern not found");
            }
        } catch (Exception e) {
            LOGGER.error("subscribeToBondStaticData: Error subscribing to bond static data", e);
            LOGGER.error("Error subscribing to bond static data: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Subscribe to firm position data
     */
    private void subscribeToFirmPositionData() {
        LOGGER.info("subscribeToFirmPositionData: Subscribing to firm position data");

        try {
            // Get the publish manager to access patterns
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
       
            // Look up the pattern object
            MkvObject obj = pm.getMkvObject(FIRM_POSITION_PATTERN);

            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                // Subscribe within a synchronized block
                LOGGER.info("Found firm position pattern, subscribing: {}", (Object) FIRM_POSITION_FIELDS);
                ((MkvPattern) obj).subscribe(FIRM_POSITION_FIELDS, bondEligibilityListener);
                
                // Mark that we've successfully subscribed
                isFirmPositionSubscribed = true;

                LOGGER.info("Subscribed to firm position data: {} with {} fields",
                    FIRM_POSITION_PATTERN, FIRM_POSITION_FIELDS.length);

                LOGGER.info("subscribeToFirmPositionData: Successfully subscribed to firm position data");
            } else {
                LOGGER.info("Firm position pattern not found");
                LOGGER.error("subscribeToFirmPositionData: Failed: Firm position pattern not found");
            }
        } catch (Exception e) {
            LOGGER.error("subscribeToFirmPositionData: Error subscribing to firm position data", e);
        }
    }

    /**
     * Subscribe to SDS information data
     */
    private void subscribeToSdsInformationData() {
        LOGGER.info("subscribeToSdsInformationData: Subscribing to SDS information data");

        try {
            // Get the publish manager to access patterns
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
       
            // Look up the pattern object
            MkvObject obj = pm.getMkvObject(SDS_INFORMATION_PATTERN);

            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                // Subscribe within a synchronized block
                LOGGER.info("Found SDS information pattern, subscribing: {}", (Object) SDS_INFORMATION_FIELDS);
                ((MkvPattern) obj).subscribe(SDS_INFORMATION_FIELDS, bondEligibilityListener);
                
                // Mark that we've successfully subscribed
                isSdsInformationSubscribed = true;

                LOGGER.info("Subscribed to SDS information data: {} with {} fields",
                    SDS_INFORMATION_PATTERN, SDS_INFORMATION_FIELDS.length);

                LOGGER.info("subscribeToSdsInformationData: Successfully subscribed to SDS information data");
            } else {
                LOGGER.info("SDS information pattern not found");
                LOGGER.error("subscribeToSdsInformationData: Failed: SDS information pattern not found");
            }
        } catch (Exception e) {
            LOGGER.error("subscribeToSdsInformationData: Error subscribing to SDS information data", e);
        }
    }

    /**
     * Cancel all active orders.
     * Used when disabling the market maker.
     */
    private void cancelAllOrders() {
        LOGGER.info("cancelAllOrders: Cancelling all market maker orders");

        try {
            LOGGER.info("Cancelling all market maker orders");
            int cancelledCount = 0;
            
            for (ActiveQuote quote : activeQuotes.values()) {
                MarketOrder bidOrder = quote.getBidOrder();
                MarketOrder askOrder = quote.getAskOrder();
                
                if (bidOrder != null) {
                    cancelOrder(bidOrder, quote.getCusip());
                    cancelledCount++;
                }
                
                if (askOrder != null) {
                    cancelOrder(askOrder, quote.getCusip());
                    cancelledCount++;
                }
            }

            LOGGER.info("cancelAllOrders: Cancelled " + cancelledCount + " orders");
        } catch (Exception e) {
            LOGGER.error("cancelAllOrders: Error cancelling all orders", e);
        }
    }

    /**
     * Periodically check our orders to ensure they're still valid.
     * Removes stale orders from our tracking.
     */
    private void monitorOrders() {
        LOGGER.info("monitorOrders: Starting order monitoring");
        
        if (!enabled) {
            LOGGER.info("monitorOrders: Skipping - market maker not enabled");
            return;
        }
        
        try {
            LOGGER.debug("Monitoring market maker orders - currently tracking " + 
                activeQuotes.size() + " instruments");
                
            int expiredCount = 0;
            int deadCount = 0;
            
            for (ActiveQuote quote : activeQuotes.values()) {
                MarketOrder bidOrder = quote.getBidOrder();
                MarketOrder askOrder = quote.getAskOrder();
                
                // Check bid order
                if (bidOrder != null) {
                    if (bidOrder.isExpired()) {
                        LOGGER.info("Bid order expired for {}: {}", quote.getCusip(), bidOrder.getOrderId());
                        expiredCount++;
                    } else if (bidOrder.isDead()) {
                        LOGGER.info("Bid order is dead for {}: {}", quote.getCusip(), bidOrder.getOrderId());
                        deadCount++;
                    }
                }
                
                // Check ask order
                if (askOrder != null) {
                    if (askOrder.isExpired()) {
                        LOGGER.info("Ask order expired for {}: {}", quote.getCusip(), askOrder.getOrderId());
                        expiredCount++;
                    } else if (askOrder.isDead()) {
                        LOGGER.info("Ask order is dead for {}: {}", quote.getCusip(), askOrder.getOrderId());
                        deadCount++;
                    }
                }
            }

            LOGGER.info("monitorOrders: Monitoring complete - found {} expired orders, {} dead orders", expiredCount, deadCount);
        } catch (Exception e) {
            LOGGER.error("monitorOrders: Error monitoring orders", e);
        }
    }

    /**
     * Execute a hedge trade on the reference venue.
     * This method is called when one of our orders on the market source is filled.
     */
    private void executeHedgeTrade(String bondId, String side, double size, double hedgePrice, String referenceSource) {
        LOGGER.info("executeHedgeTrade: Bond={}, Side={}, Size={}, Price={}, RefSrc={}", 
            bondId, side, size, hedgePrice, referenceSource);
        
        // Skip if auto-hedge is disabled
        if (!config.isAutoHedge()) {
            LOGGER.info("Auto-hedge disabled, skipping hedge trade");
            LOGGER.info("executeHedgeTrade: Auto-hedge disabled");
            return;
        }

        try {
            if (referenceSource == null || !isTargetVenue(referenceSource)) {
                LOGGER.info("executeHedgeTrade: Invalid reference source: {}", referenceSource);
                return;
            }

            // Get the instrument ID that corresponds to this bond ID
            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId);
            if (instrumentId == null) {
                LOGGER.info("executeHedgeTrade: No instrument ID found for bond: {}", bondId);
                return;
            }

            // Get the native instrument ID for the reference venue
            String referenceInstrument = null;
            if (depthListener != null) {
                referenceInstrument = depthListener.getInstrumentFieldBySourceString(
                    instrumentId, referenceSource, false);
            } else {
                LOGGER.warn("DepthListener is null, cannot get native instrument ID");
            }
            
            if (referenceInstrument == null) {
                LOGGER.info("executeHedgeTrade: No native instrument ID for bond: {} on venue: {}", bondId, referenceSource);
                return;
            }

            // Create a hedge trade (opposite side of our filled order)
            String hedgeSide = "Buy".equals(side) ? "Sell" : "Buy";

            // Get the trader ID for the reference venue
            String traderId = orderManager.getTraderForVenue(referenceSource);

            LOGGER.info("Executing hedge trade: {} {} {} on {} for bond {} at price {}",
                hedgeSide, size, referenceInstrument, referenceSource, bondId, hedgePrice);

            // For a market order, we use the same order type as the original algorithm
            MarketOrder hedgeOrder = orderManager.addOrder(
                referenceSource,
                traderId,
                referenceInstrument,
                hedgeSide,
                size,
                hedgePrice,
                "Limit",
                "FAS"
            );
            
            if (hedgeOrder != null) {
                LOGGER.info("Hedge trade executed successfully: {}", hedgeOrder.getOrderId());
                LOGGER.info(
                    "HEDGE_EXECUTED", 
                    hedgeOrder.getOrderId(), 
                    referenceInstrument, 
                    hedgeSide, 
                    "HEDGE", 
                    hedgePrice, 
                    size, 
                    "NEW", 
                    referenceSource, 
                    0, // Status code 
                    "Hedge for order on " + config.getMarketSource()
                );
                LOGGER.info("executeHedgeTrade: Hedge trade placed successfully");
            } else {
                LOGGER.error("Failed to execute hedge trade");
                LOGGER.info("executeHedgeTrade: Failed to place hedge order");
            }
        } catch (Exception e) {
            LOGGER.error("executeHedgeTrade: Error executing hedge trade", e);
        }
    }
        
    @Override
    public void orderDead(MarketOrder order) {
        LOGGER.info("orderDead: Order dead notification: reqId={}, orderId={}",
            order.getMyReqId(), order.getOrderId());

        try {
            LOGGER.info(
                "Order dead notification: reqId={}, orderId={}",
                order.getMyReqId(), order.getOrderId());
                
            // Find the active quote that contains this order
            for (ActiveQuote quote : activeQuotes.values()) {
                MarketOrder bidOrder = quote.getBidOrder();
                MarketOrder askOrder = quote.getAskOrder();
                
                if (bidOrder != null && bidOrder.getMyReqId() == order.getMyReqId()) {
                    LOGGER.info("Order dead was a bid for {}", quote.getCusip());
                    quote.setBidOrder(null, null, 0);
                    break;
                }
                
                if (askOrder != null && askOrder.getMyReqId() == order.getMyReqId()) {
                    LOGGER.info("Order dead was an ask for {}", quote.getCusip());
                    quote.setAskOrder(null, null, 0);
                    break;
                }
            }

            LOGGER.info("orderDead: Order dead notification processed");
        } catch (Exception e) {
            LOGGER.error("orderDead: Error processing orderDead", e);
        }
    }

    @Override
    public MarketOrder addOrder(String MarketSource, String TraderId, String instrId, 
                              String verb, double qty, double price, String type, String tif) {
        LOGGER.info("addOrder: Starting order creation: Source={}, Trader={}, Instrument={}, Side={}, Qty={}, Price={}, Type={}, TIF={}", 
            MarketSource, TraderId, instrId, verb, qty, price, type, tif);
        String.format("Source=%s, Trader=%s, Instrument=%s, Side=%s, Qty=%.2f, Price=%.4f, Type=%s, TIF=%s", 
            MarketSource, TraderId, instrId, verb, qty, price, type, tif);

        // Delegate to the main OrderManagement instance
        MarketOrder order = orderManager.addOrder(MarketSource, TraderId, instrId, verb, qty, price, type, tif);
        
        if (order != null) {
            LOGGER.info("addOrder: Order created successfully: reqId={}", order.getMyReqId());
        } else {
            LOGGER.info("addOrder: Failed to create order");
        }
        
        return order;
    }
    
    @Override
    public void best(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {
        // Increment total update counter
        int updateCount = marketUpdateCounter.incrementAndGet();

        LOGGER.info("MarketMaker.best() called with Best object: {}", best);
        // Process the market update
        String id = best.getId();
        String instrumentId = best.getInstrumentId();

        // Enhanced logging for empty ID
        if (id == null || id.isEmpty()) {
            int emptyCount = emptyIdCounter.incrementAndGet();
            
            // Only log periodically to avoid spam
            if (emptyCount % 100 == 1) {
                LOGGER.warn("Received market update with empty instrument ID, skipping (count: {})", emptyCount);
                // Log full details of the Best object for debugging
                LOGGER.debug("Empty ID Best object details: {}", best.toString());
                
                // Try to get debug info about what's in the Best object
                try {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Best object inspection: ");
                    sb.append("instrumentId=").append(best.getInstrumentId());
                    sb.append(", bid=").append(best.getBid());
                    sb.append(", ask=").append(best.getAsk());
                    sb.append(", bidSrc=").append(best.getBidSrc());
                    sb.append(", askSrc=").append(best.getAskSrc());
                    
                    LOGGER.debug(sb.toString());
                } catch (Exception e) {
                    LOGGER.error("Error inspecting Best object: {}", e.getMessage());
                }
            }
            return;
        }

        // Check for non-overnight instruments
        if (!id.endsWith("C_Fixed") && !id.endsWith("REG_Fixed")) {
            
            LOGGER.warn("Skipping non-overnight instrument: {}", id);
            // Add more information about the instrument for debugging
            LOGGER.debug("Non-overnight instrument details - ID: {}, InstrumentId: {}, Bid: {}, Ask: {}", 
                id, instrumentId, best.getBid(), best.getAsk());
            return;
        }
        
        // Count updates per instrument
        LOGGER.info("MarketMaker:  received market update within best() for instrument: {}", id);
        instrumentUpdateCounters.computeIfAbsent(id, k -> new AtomicInteger(0)).incrementAndGet();

        LOGGER.info("MarketMaker:  best() called with Best object: {}", best);
            LOGGER.info("best: Received market update for {}: bid=%.4f, ask=%.4f, bidSrc={}, askSrc={}", 
                id, best.getBid(), best.getAsk(), best.getBidSrc(), best.getAskSrc());
        

        LOGGER.info("Processing market update for instrument: {}", id);
        if (id.endsWith("C_Fixed")) {
            LOGGER.info("Processing market update for CASH instrument: {}", id);
            processMarketUpdate(best, gcBestCash);
        } else if (id.endsWith("REG_Fixed")) {
            // For REG_Fixed instruments, use REG GC rates
            LOGGER.info("Processing market update for REG instrument: {}", id);
            processMarketUpdate(best, gcBestREG);
        } else {
            LOGGER.warn("Unexpected instrument type: {}", id);
        }
        
        // Increment processed counter - we made it to processing the update
        processedUpdateCounter.incrementAndGet();

        LOGGER.info("best: Market update processed");

    }

    /**
     * Process market data updates and update our quotes accordingly.
     * Called by the DepthListener when market data changes.
     * 
     * @param best The updated best prices
     */
    public void processMarketUpdate(Best best, GCBest gcBest) {
        LOGGER.info("processMarketUpdate called with Best object: {}", best);
        LOGGER.info("GCBest object: {}", gcBest);        
        if (!enabled) {
            LOGGER.info("processMarketUpdate: Market maker not enabled, skipping");
            return;
        }
        
        try {
            String instrumentId = best.getInstrumentId();
            String bestId = best.getId(); // This should be the instrument ID from VMO.CM_INSTRUMENT
            LOGGER.info("Processing market update for bestId: {}", bestId);
            LOGGER.info("Processing market update for instrument ID: {}", instrumentId);

            if (bestId == null || bestId.isEmpty()) {
                LOGGER.info("processMarketUpdate: Empty instrument ID, skipping");
                return;
            }
            
            // Find the bond ID that maps to this instrument ID
            String bondId = null;
            
            LOGGER.info("Current bond to instrument map size: {}", 
                    bondEligibilityListener.bondToInstrumentMap.size());
                for (Map.Entry<String, String> entry : bondEligibilityListener.bondToInstrumentMap.entrySet()) {
                    LOGGER.info("Bond to instrument mapping: {} -> {}", entry.getKey(), entry.getValue());
                }
            
            for (Map.Entry<String, String> entry : bondEligibilityListener.bondToInstrumentMap.entrySet()) {
                if (entry.getValue().equals(instrumentId) || entry.getValue().equals(bestId)) {
                    bondId = entry.getKey();
                    LOGGER.info("Found bond ID mapping: {} -> {}", bondId, bestId);
                    break;
                }
            }
            
            // If we can't find a bond mapping, this instrument isn't eligible for market making
            if (bondId == null) {
                LOGGER.info("processMarketUpdate: No bond mapping found for instrument: {}", bestId);
                return;
            }

            // Check if this bond is in our tracked instruments
            if (!trackedInstruments.contains(bondId)) {
                LOGGER.info("processMarketUpdate: Bond not in tracked instruments: {}", bondId);
                return;
            }

            LOGGER.info("Processing market update for bond {} (instrument: {})", bondId, bestId);

            // Process with symmetric quoting (simplified for this example)
            processSymmetricQuoting(best, bondId);

            LOGGER.info("processMarketUpdate: Market update processed for bond: {}", bondId);
            
        } catch (Exception e) {
            LOGGER.error("Error processing market update: {}", e.getMessage(), e);
        }
    }

    /**
     * Log diagnostic statistics periodically
     */
    private void logDiagnosticStatistics() {
        try {
            LOGGER.info("DIAGNOSTIC STATISTICS:");
            LOGGER.info("  Total market updates received: {}", marketUpdateCounter.get());
            LOGGER.info("  Updates processed: {}", processedUpdateCounter.get());
            LOGGER.info("  Empty ID updates: {}", emptyIdCounter.get());
            LOGGER.info("  Non-overnight instrument updates: {}", nonOvernightCounter.get());
            LOGGER.info("  Tracked instruments: {}", trackedInstruments.size());
            LOGGER.info("  Active quotes: {}", activeQuotes.size());
            LOGGER.info("  Bond eligibility listener status:");
            LOGGER.info("    Eligible bonds: {}", bondEligibilityListener.getEligibleBonds().size());
            LOGGER.info("    Bond to instrument mappings: {}", bondEligibilityListener.bondToInstrumentMap.size());
            
            // Log top 5 instruments with most updates
            LOGGER.info("  Top instruments by update count:");
            instrumentUpdateCounters.entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue().get(), a.getValue().get()))
                .limit(5)
                .forEach(entry -> LOGGER.info("    {}: {} updates", entry.getKey(), entry.getValue().get()));
            
            // Log pattern subscription status
            LOGGER.info("  Pattern subscription status:");
            LOGGER.info("    Bond static data subscribed: {}", isBondStaticSubscribed);
            LOGGER.info("    Firm position data subscribed: {}", isFirmPositionSubscribed);
            LOGGER.info("    SDS information data subscribed: {}", isSdsInformationSubscribed);
            
            // Log depth listener status if available
            if (depthListener != null) {
                LOGGER.info("  Depth listener status: {}",
                    depthListener.isReceivingData() ? "RECEIVING DATA" : "NOT RECEIVING DATA");
                LOGGER.info("  Instruments loaded in depth listener: {}", depthListener.getInstrumentCount());
            } else {
                LOGGER.error("  Depth listener is NULL!");
            }
        } catch (Exception e) {
            LOGGER.error("Error logging diagnostic statistics", e);
        }
    }

    /**
     * Get current status for monitoring - enhanced to include diagnostic counters
     */
    public Map<String, Object> getMarketMakerStatus() {
        LOGGER.info("getMarketMakerStatus: Getting current status");

        Map<String, Object> status = new HashMap<>();
        status.put("enabled", enabled);
        status.put("trackedInstruments", trackedInstruments.size());
        status.put("activeQuotes", activeQuotes.size());
        status.put("eligibleBonds", bondEligibilityListener.getEligibleBonds().size());
        
        // Count active orders
        int activeBids = 0;
        int activeAsks = 0;
        for (ActiveQuote quote : activeQuotes.values()) {
            if (quote.getBidOrder() != null && !quote.getBidOrder().isDead()) {
                activeBids++;
            }
            if (quote.getAskOrder() != null && !quote.getAskOrder().isDead()) {
                activeAsks++;
            }
        }
        status.put("activeBids", activeBids);
        status.put("activeAsks", activeAsks);
        
        // Add diagnostic counters
        status.put("marketUpdatesReceived", marketUpdateCounter.get());
        status.put("marketUpdatesProcessed", processedUpdateCounter.get());
        status.put("emptyIdUpdates", emptyIdCounter.get());
        status.put("nonOvernightUpdates", nonOvernightCounter.get());
        
        // Add subscription status
        Map<String, Boolean> subscriptions = new HashMap<>();
        subscriptions.put("bondStatic", isBondStaticSubscribed);
        subscriptions.put("firmPosition", isFirmPositionSubscribed);
        subscriptions.put("sdsInformation", isSdsInformationSubscribed);
        status.put("subscriptions", subscriptions);
        
        // Add depth listener status
        if (depthListener != null) {
            status.put("depthListenerActive", depthListener.isReceivingData());
            status.put("instrumentsLoaded", depthListener.getInstrumentCount());
        } else {
            status.put("depthListenerActive", false);
            status.put("instrumentsLoaded", 0);
        }

        LOGGER.info("getMarketMakerStatus: Returning status with {} items", status.size());
        return status;
    }

    /**
     * ActiveQuote represents a two-sided quote for a specific instrument.
     * It tracks the bid and ask orders and their reference sources.
     */
    private static class ActiveQuote {
        private final String cusip;
        private MarketOrder bidOrder;
        private MarketOrder askOrder;
        private String bidReferenceSource;
        private String askReferenceSource;
        private double bidPrice;
        private double askPrice;
        
        public ActiveQuote(String cusip) {
            this.cusip = cusip;
        }
        
        public String getCusip() {
            return cusip;
        }
        
        public MarketOrder getBidOrder() {
            return bidOrder;
        }
        
        public void setBidOrder(MarketOrder order, String referenceSource, double price) {
            this.bidOrder = order;
            this.bidReferenceSource = referenceSource;
            this.bidPrice = price;
        }
        
        public MarketOrder getAskOrder() {
            return askOrder;
        }
        
        public void setAskOrder(MarketOrder order, String referenceSource, double price) {
            this.askOrder = order;
            this.askReferenceSource = referenceSource;
            this.askPrice = price;
        }
        
        public String getBidReferenceSource() {
            return bidReferenceSource;
        }
        
        public String getAskReferenceSource() {
            return askReferenceSource;
        }
        
        public double getBidPrice() {
            return bidPrice;
        }
        
        public double getAskPrice() {
            return askPrice;
        }

    }
    /**
     * Maps an order ID to a request ID for tracking.
     * This implementation delegates to the OrderManagement instance.
     * 
     * @param orderId The order ID assigned by the market
     * @param reqId The request ID used when creating the order
     */
    @Override
    public void mapOrderIdToReqId(String orderId, int reqId) {
       
        LOGGER.info("MarketMaker.mapOrderIdToReqId() - Mapping orderId: {} to reqId: {}", orderId, reqId);
        
        // Store the mapping in a local map for quick lookups
        orderIdToReqIdMap.put(orderId, reqId);
        
        // Also delegate to the order manager if needed
        if (orderManager != null) {
            orderManager.mapOrderIdToReqId(orderId, reqId);
        }

        LOGGER.info("mapOrderIdToReqId: Mapping complete");
    }

    /**
     * Enable or disable the market making strategy.
     * 
     * @param enabled Whether market making should be enabled
     */
    public void setEnabled(boolean enabled) {
        LOGGER.info("setEnabled: Setting enabled={}", enabled);

        boolean previousState = this.enabled;
        this.enabled = enabled;
        
        if (previousState != enabled) {
            if (enabled) {
                LOGGER.info("Market making enabled");
                LOGGER.info("MarketMaker: Market making enabled");

                // Start market making for eligible bonds
                makeMarketsForEligibleBonds();
                
                // Start order monitor
                monitorOrders();
            } else {
                LOGGER.info("Market making disabled");
                LOGGER.info("MarketMaker: Market making disabled");

                // Cancel all pending orders
                cancelAllOrders();
            }
        }

        LOGGER.info("setEnabled: Enabled state set to: {}", enabled);
    }
    
    private void handleEligibilityChange(String cusip, boolean isEligible, Map<String, Object> bondData) {
        LOGGER.info("handleEligibilityChange: Bond={} isEligible={}", cusip, isEligible);

        if (!enabled) {
            LOGGER.info("handleEligibilityChange: Market maker not enabled, ignoring change");
            return;
        }

        try {
            LOGGER.info("Bond eligibility changed: {} -> {}", cusip, (isEligible ? "ELIGIBLE" : "INELIGIBLE"));

            if (isEligible) {
                // Bond became eligible, add to tracked set and create initial markets
                trackedInstruments.add(cusip);
                
                // Try to create initial markets for this bond
                tryCreateInitialMarkets(cusip);

                LOGGER.info("handleEligibilityChange: Bond {} became eligible, initial markets created", cusip);
            } else {
                // Bond became ineligible, remove from tracked set and cancel orders
                trackedInstruments.remove(cusip);
                cancelOrdersForInstrument(cusip);
                
                // Remove from active quotes
                activeQuotes.remove(cusip);

                LOGGER.info("handleEligibilityChange: Bond {} became ineligible, orders cancelled", cusip);
            }
        } catch (Exception e) {
            LOGGER.error("Error handling eligibility change for {}: {}", cusip, e.getMessage(), e);
        }
    }

    private void makeMarketsForEligibleBonds() {
        LOGGER.info("makeMarketsForEligibleBonds: Starting periodic market making");
        
        if (!enabled) {
            LOGGER.info("makeMarketsForEligibleBonds: Market maker not enabled, skipping");
            return;
        }
        
        try {
            Set<String> eligibleBonds = bondEligibilityListener.getEligibleBonds();

            LOGGER.info("Making markets for {} eligible bonds", eligibleBonds.size());

            int marketsCreated = 0;
            int marketsUpdated = 0;
            
            // Process all eligible bonds
            for (String cusip : eligibleBonds) {
                if (!trackedInstruments.contains(cusip)) {
                    // New eligible bond, not yet tracking
                    trackedInstruments.add(cusip);
                    tryCreateInitialMarkets(cusip);
                    marketsCreated++;
                } else {
                    // Already tracking this bond, update markets if needed
                    tryCreateOrUpdateMarkets(cusip);
                    marketsUpdated++;
                }
            }
            
            // Clean up any instruments that are no longer eligible
            Set<String> instrumentsToRemove = new HashSet<>();
            for (String cusip : trackedInstruments) {
                if (!eligibleBonds.contains(cusip)) {
                    instrumentsToRemove.add(cusip);
                }
            }
            
            int marketsRemoved = 0;
            for (String cusip : instrumentsToRemove) {
                trackedInstruments.remove(cusip);
                cancelOrdersForInstrument(cusip);
                activeQuotes.remove(cusip);
                marketsRemoved++;
            }
            
            LOGGER.info("makeMarketsForEligibleBonds: Market making completed: {} created, {} updated, {} removed", 
                marketsCreated, marketsUpdated, marketsRemoved);
        } catch (Exception e) {
            LOGGER.error("makeMarketsForEligibleBonds: Error in periodic market making: {}", e.getMessage(), e);
        }
    }

    /**
     * Try to create initial markets for a newly eligible bond
     */
    private void tryCreateInitialMarkets(String bondId) {
        try {
            // Get the instrument ID that corresponds to this bond ID
            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId);
            if (instrumentId == null) {
                LOGGER.warn("No instrument ID mapping found for bond: " + bondId);
                return;
            }

            LOGGER.info("Creating initial markets for bond " + bondId + " using instrument ID " + instrumentId);

            // Get the native instrument ID for FENICS using the correct instrument ID
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    instrumentId, config.getMarketSource(), false); // Use instrumentId, not bondId
            }
            
            if (nativeInstrument == null) {
                LOGGER.warn("No instrument mapping found for instrument ID: " + instrumentId +
                    " (bond: " + bondId + ") on " + config.getMarketSource());
                return;
            }

            LOGGER.info("Found native instrument: " + nativeInstrument + " for bond " + bondId);

            // Check if we already have quotes for this bond
            ActiveQuote existingQuote = activeQuotes.get(bondId); // Still track by bondId
            if (existingQuote != null) {
                LOGGER.info("Already have quotes for bond " + bondId);
                return;
            }

            // Try to get current best prices to base our quotes on
            createDefaultMarkets(bondId, nativeInstrument);
            
        } catch (Exception e) {
            LOGGER.error("Error creating initial markets for bond " + bondId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Create default markets when no reference prices are available
     */
    private void createDefaultMarkets(String cusip, String nativeInstrument) {
        try {
            // You might want to get a reference price from bond static data or use a default
            // For now, using a placeholder approach - you'd need to implement proper pricing
            
            double referencePrice = getReferencePriceForBond(cusip);
            if (referencePrice <= 0) {
                LOGGER.warn("No reference price available for " + cusip);
                return;
            }

            // Create default spread around reference price
            double defaultSpread = config.getDefaultSpread(); // You'd add this to config
            double bidPrice = referencePrice - (defaultSpread / 2);
            double askPrice = referencePrice + (defaultSpread / 2);

            LOGGER.info("Creating default markets for " + cusip + 
                " at " + bidPrice + "/" + askPrice);

            // Place default orders
            placeOrder(cusip, nativeInstrument, "Buy", config.getMinSize(), 
                    bidPrice, "DEFAULT");
            placeOrder(cusip, nativeInstrument, "Sell", config.getMinSize(), 
                    askPrice, "DEFAULT");
                    
        } catch (Exception e) {
            LOGGER.error("Error creating default markets for " + cusip + ": " + e.getMessage(), e);
        }
    }

    /**
     * Try to create or update markets for an instrument
     */
    private void tryCreateOrUpdateMarkets(String bondId) {
        try {
            // Get the instrument ID that corresponds to this bond ID
            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId);
            if (instrumentId == null) {
                LOGGER.warn("No instrument ID mapping found for bond: " + bondId + ", skipping");
                return;
            }

            // Check if existing quotes need validation
            ActiveQuote existingQuote = activeQuotes.get(bondId);
            if (existingQuote == null) {
                tryCreateInitialMarkets(bondId);
            } else {
                // Check if existing orders are still valid
                validateExistingQuotes(bondId, existingQuote);
            }
            
        } catch (Exception e) {
            LOGGER.error("Error creating/updating markets for bond " + bondId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Validate and refresh existing quotes
     */
    private void validateExistingQuotes(String bondId, ActiveQuote quote) {
        try {
            // Check if orders are still alive
            MarketOrder bidOrder = quote.getBidOrder();
            MarketOrder askOrder = quote.getAskOrder();
            
            boolean needNewBid = (bidOrder == null || bidOrder.isExpired() || bidOrder.isDead());
            boolean needNewAsk = (askOrder == null || askOrder.isExpired() || askOrder.isDead());
            
            if (needNewBid || needNewAsk) {
                // Get the instrument ID that corresponds to this bond ID
                String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId);
                if (instrumentId == null) {
                    LOGGER.warn("Cannot refresh quotes - no instrument ID mapping for bond: " + bondId);
                    return;
                }

                String nativeInstrument = null;
                if (depthListener != null) {
                    nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                        instrumentId, config.getMarketSource(), false); // Use instrumentId
                }
                
                if (nativeInstrument == null) {
                    LOGGER.warn("Cannot refresh quotes - no instrument mapping for instrument ID: " + instrumentId + 
                        " (bond: " + bondId + ")");
                    return;
                }
                
                if (needNewBid) {
                    LOGGER.info("Refreshing bid quote for bond " + bondId);

                    double bidPrice = quote.getBidPrice();
                    if (bidPrice <= 0) {
                        bidPrice = getReferencePriceForBond(bondId) - (config.getDefaultSpread() / 2);
                    }
                    
                    if (bidPrice > 0) {
                        placeOrder(bondId, nativeInstrument, "Buy", config.getMinSize(), 
                                bidPrice, quote.getBidReferenceSource() != null ? 
                                quote.getBidReferenceSource() : "DEFAULT");
                    }
                }
                
                if (needNewAsk) {
                    LOGGER.info("Refreshing ask quote for bond " + bondId);

                    double askPrice = quote.getAskPrice();
                    if (askPrice <= 0) {
                        askPrice = getReferencePriceForBond(bondId) + (config.getDefaultSpread() / 2);
                    }
                    
                    if (askPrice > 0) {
                        placeOrder(bondId, nativeInstrument, "Sell", config.getMinSize(), 
                                askPrice, quote.getAskReferenceSource() != null ? 
                                quote.getAskReferenceSource() : "DEFAULT");
                    }
                }
            }
            
        } catch (Exception e) {
            LOGGER.error("Error validating quotes for bond " + bondId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Get reference price for a bond (you'll need to implement this)
     */
    private double getReferencePriceForBond(String cusip) {
        // This is where you'd implement your pricing logic
        // Could come from:
        // - Last traded price
        // - Bond static data (par value, etc.)
        // - External pricing service
        // - Previous day's close
        // For now, returning a placeholder
        
        try {
            // You might get this from bond static data
            Map<String, Object> bondData = bondEligibilityListener.getBondData(cusip);
            if (bondData != null) {
                // Look for any price fields in the static data
                Object lastPrice = bondData.get("LastPrice");
                if (lastPrice instanceof Number) {
                    return ((Number) lastPrice).doubleValue();
                }
                
                // Fallback to par value if available
                Object parValue = bondData.get("ParValue");
                if (parValue instanceof Number) {
                    return ((Number) parValue).doubleValue();
                }
            }
            
            // Default fallback price (you'd want something more sophisticated)
            return 100.0; // Par value assumption
            
        } catch (Exception e) {
            LOGGER.warn("Error getting reference price for " + cusip + ": " + e.getMessage(), e);
            return 100.0; // Default fallback
        }
    }

    @Override
    public void removeOrder(int reqId) {
        LOGGER.info("removeOrder: Removing order with reqId: {}", reqId);
        
        try {
            LOGGER.info("MarketMaker.removeOrder() - Removing order with reqId: {}", reqId);
            
            // Find and remove the order from our active quotes
            for (ActiveQuote quote : activeQuotes.values()) {
                MarketOrder bidOrder = quote.getBidOrder();
                MarketOrder askOrder = quote.getAskOrder();
                
                if (bidOrder != null && bidOrder.getMyReqId() == reqId) {
                    LOGGER.info("Removed bid order for {}: reqId={}", quote.getCusip(), reqId);
                    quote.setBidOrder(null, null, 0);
                    break;
                }
                
                if (askOrder != null && askOrder.getMyReqId() == reqId) {
                    LOGGER.info("Removed ask order for {}: reqId={}", quote.getCusip(), reqId);
                    quote.setAskOrder(null, null, 0);
                    break;
                }
            }
            
            // Remove from our local mapping
            orderIdToReqIdMap.values().removeIf(id -> id == reqId);
            
            // Delegate to the main OrderManagement instance if needed
            if (orderManager != null) {
                orderManager.removeOrder(reqId);
            }
            
            LOGGER.info("removeOrder: Order removed successfully");
        } catch (Exception e) {
            LOGGER.error("removeOrder: Error removing order with reqId {}: {}", reqId, e.getMessage(), e);
        }
    }

    /**
     * Cancel all orders for a specific instrument
     */
    private void cancelOrdersForInstrument(String cusip) {
        try {
            ActiveQuote quote = activeQuotes.get(cusip);
            if (quote != null) {
                LOGGER.info("Cancelling orders for ineligible instrument: " + cusip);

                MarketOrder bidOrder = quote.getBidOrder();
                MarketOrder askOrder = quote.getAskOrder();
                
                if (bidOrder != null) {
                    cancelOrder(bidOrder, cusip);
                }
                
                if (askOrder != null) {
                    cancelOrder(askOrder, cusip);
                }
                
                // Remove from tracking
                activeQuotes.remove(cusip);
            }
        } catch (Exception e) {
            LOGGER.error("Error cancelling orders for " + cusip + ": " + e.getMessage(), e);
        }
    }

    /**
     * Get whether market making is currently enabled.
     * 
     * @return true if enabled, false if disabled
     */
    public boolean isEnabled() {
        // Simple getter, no need for detailed logging
        return enabled;
    }

    /**
     * Gets the current configuration.
     * 
     * @return The market maker configuration
     */
    public MarketMakerConfig getConfig() {
        // Simple getter, no need for detailed logging
        return config;
    }
    
    /**
     * Update configuration
     */
    public void updateConfiguration(MarketMakerConfig newConfig) {
        LOGGER.info("updateConfiguration: Updating configuration: {}", newConfig);

        // Note: This should be implemented to safely update config
        // For now, logging that update was requested
        LOGGER.warn("Configuration update requested but not implemented safely");

        LOGGER.info("updateConfiguration: Configuration update not implemented");
    }

    /**
     * Get configuration summary for monitoring
     */
    public Map<String, Object> getConfigSummary() {
        LOGGER.info("getConfigSummary: Getting configuration summary");

        Map<String, Object> summary = new HashMap<>();
        summary.put("enabled", enabled);
        summary.put("marketSource", config.getMarketSource());
        summary.put("minSize", config.getMinSize());
        summary.put("bidAdjustment", config.getBidAdjustment());
        summary.put("askAdjustment", config.getAskAdjustment());
        summary.put("autoHedge", config.isAutoHedge());
        summary.put("activeQuotes", activeQuotes.size());
        summary.put("trackedInstruments", trackedInstruments.size());

        LOGGER.info("getConfigSummary: Returning summary with {} items", summary.size());
        return summary;
    }

    /**
     * Cancel an existing order.
     * 
     * @param order The order to cancel
     * @param cusip The instrument CUSIP (for tracking)
     */
    private void cancelOrder(MarketOrder order, String cusip) {
        try {
            if (order == null || order.getOrderId() == null) {
                return;
            }

            LOGGER.info("Cancelling order: orderId=" + order.getOrderId() + 
                ", reqId=" + order.getMyReqId() + 
                ", cusip=" + cusip);
                
            String traderId = orderManager.getTraderForVenue(config.getMarketSource());
            
            MarketOrder cancelOrder = MarketOrder.orderCancel(
                config.getMarketSource(), 
                traderId, 
                order.getOrderId(), 
                this
            );
            
            if (cancelOrder != null) {
                LOGGER.info("Cancel request sent: orderId=" + order.getOrderId());
            } else {
                LOGGER.warn("Failed to cancel order: orderId=" + order.getOrderId());
            }
        } catch (Exception e) {
            LOGGER.error("Error cancelling order: " + e.getMessage(), e);
        }
    }

    public void shutdown() {
        LOGGER.info("Shutting down market maker");
        setEnabled(false);
        cancelAllOrders();
    }

    public boolean isRunning() {
        return enabled;
    }

    public void startAutomatedMarketMaking() {
        setEnabled(true);
        LOGGER.info("Automated market making started");
    }

    public void stopAutomatedMarketMaking() {
        setEnabled(false);
        LOGGER.info("Automated market making stopped");
    }

    /**
     * Check if a venue is a target venue for hedging
     * @param venue The venue to check
     * @return true if it's a target venue, false otherwise
     */
    private boolean isTargetVenue(String venue) {
        LOGGER.info("isTargetVenue: Checking if venue is targeted: {}", venue);
        if (venue == null) {
            LOGGER.info("isTargetVenue: Result: false (venue is null)");
            return false;
        }
        boolean result = config.getTargetVenuesSet().contains(venue);
        LOGGER.info("isTargetVenue: Result: {} for venue: {}", result, venue);
        return result;
    }
    
    /**
     * Enhanced order placement with better error handling and validation
     * @param cusip The CUSIP of the instrument
     * @param nativeInstrument The native instrument ID
     * @param verb The order direction ("Buy" or "Sell")
     * @param size The order size
     * @param price The order price
     * @param referenceSource The source of the reference price
     */
    private void placeOrder(String cusip, String nativeInstrument, String verb, 
                        double size, double price, String referenceSource) {
        LOGGER.info("placeOrder: CUSIP={}, Native={}, Side={}, Size={}, Price={}, Source={}", 
            cusip, nativeInstrument, verb, size, price, referenceSource);
        
        try {
            // Additional validation before placing order
            if (size <= 0) {
                LOGGER.warn("Invalid order size: {}, must be positive", size);
                LOGGER.info("placeOrder: Failed: Invalid order size");
                return;
            }

            if (price <= 0 || price < config.getMinPrice() || price > config.getMaxPrice()) {
                LOGGER.warn("Invalid order price: {} (min={}, max={})", 
                    price, config.getMinPrice(), config.getMaxPrice());
                LOGGER.info("placeOrder: Failed: Invalid order price");
                return;
            }

            // Check if we're within trading hours for default markets
            if ("DEFAULT".equals(referenceSource) && !config.isDefaultMarketMakingAllowed()) {
                LOGGER.info("Default market making not allowed at this time");
                LOGGER.info("placeOrder: Failed: Default market making not allowed");
                return;
            }

            LOGGER.info("Placing {} order on {}: {} ({}), size={}, price={}, reference={}", 
                verb, config.getMarketSource(), nativeInstrument, cusip, size, price, referenceSource);
            
            MarketOrder order = orderManager.addOrder(
                config.getMarketSource(), 
                fenicsTrader, 
                nativeInstrument, 
                verb, 
                size, 
                price, 
                config.getOrderType(), 
                config.getTimeInForce()
            );
            
            if (order != null) {
                LOGGER.info("Order placed successfully: reqId={}", order.getMyReqId());
                
                // Update the appropriate quote
                ActiveQuote quote = activeQuotes.computeIfAbsent(cusip, ActiveQuote::new);
                
                if ("Buy".equals(verb)) {
                    quote.setBidOrder(order, referenceSource, price);
                    LOGGER.info("MarketMaker: Bid quote updated for {}: price=%.4f, source={}, reqId={}", 
                        cusip, price, referenceSource, order.getMyReqId());
                } else {
                    quote.setAskOrder(order, referenceSource, price);
                    LOGGER.info("MarketMaker: Ask quote updated for {}: price=%.4f, source={}, reqId={}", 
                        cusip, price, referenceSource, order.getMyReqId());
                }

                // Now also track this instrument
                trackedInstruments.add(cusip);
                LOGGER.info("placeOrder: Order placed successfully");
            } else {
                LOGGER.error("Failed to place {} order for {}", verb, cusip);
                LOGGER.info("placeOrder: Failed to place order");
            }
        } catch (Exception e) {
            LOGGER.error("placeOrder: Error placing order for {}", cusip, e);
        }
    }

    /**
     * Process normal symmetric quoting when both sides can be hedged
     * @param best The updated best prices
     * @param bondId The bond ID
     */
    private void processSymmetricQuoting(Best best, String bondId) {
        LOGGER.info("processSymmetricQuoting: Processing symmetric quoting for bond: {}", bondId);

        try {
            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId);
            // if (instrumentId == null) {
            //     LOGGER.warn("No instrument ID found for bond: {}", bondId);
            //     LOGGER.debug(instrumentId);("processSymmetricQuoting:  Failed: No instrument ID found for bond");
            //     return;
            // }

            // Get the best bid and ask prices and sources
            double bestBid = best.getBid();
            double bestAsk = best.getAsk();
            String bidSource = best.getBidSrc();
            String askSource = best.getAskSrc();
            LOGGER.info("Bid Source: {}, Ask Source: {}", bidSource, askSource);
            // Log the best prices
            LOGGER.info("Best prices for bond {}: bid={}, ask={}", bondId, bestBid, bestAsk);

            // Skip if we don't have valid prices or sources
            if (bestBid <= 0 || bestAsk <= 0 || bidSource == null || askSource == null) {
                LOGGER.info("Skipping symmetric quoting - invalid prices or sources: " +
                    "bid={}, ask={}, bidSrc={}, askSrc={}",
                    bestBid, bestAsk, bidSource, askSource);
                return;
            }
            
            // Check if either bid or ask is from one of our target venues
            boolean validBidSource = isTargetVenue(bidSource);
            boolean validAskSource = isTargetVenue(askSource);
            
            if (!validBidSource && !validAskSource) {
                LOGGER.debug("Skipping symmetric quoting - no valid source venues");
                return;
            }

            LOGGER.info("Valid symmetric market data for bond {}: bid={}/{}, ask={}/{}",
                bondId, bestBid, bidSource, bestAsk, askSource);

            // Calculate our quoting prices
            double ourBidPrice = 0;
            double ourAskPrice = 0;
            String referenceBidSource = null;
            String referenceAskSource = null;
            
            if (validBidSource) {
                ourBidPrice = bestBid + config.getBidAdjustment();
                referenceBidSource = bidSource;
                LOGGER.debug("Valid bid source, our bid price will be: {}", ourBidPrice);
            }
            
            if (validAskSource) {
                ourAskPrice = bestAsk - config.getAskAdjustment();
                referenceAskSource = askSource;
                LOGGER.debug("Valid ask source, our ask price will be: {}", ourAskPrice);
            }
            
            // Enforce minimum spread if both sides are valid
            if (validBidSource && validAskSource) {
                double currentSpread = ourAskPrice - ourBidPrice;
                if (currentSpread < config.getMinSpread()) {
                    // Adjust prices to meet minimum spread
                    double midpoint = (ourBidPrice + ourAskPrice) / 2;
                    ourBidPrice = midpoint - (config.getMinSpread() / 2);
                    ourAskPrice = midpoint + (config.getMinSpread() / 2);
                    LOGGER.info("Enforcing minimum spread of {}, adjusted prices: bid={}, ask={}",
                        config.getMinSpread(), ourBidPrice, ourAskPrice);
                }
            }
            
            // Validate that our prices are reasonable
            if (validBidSource && (ourBidPrice <= 0 || ourBidPrice < config.getMinPrice() || ourBidPrice > config.getMaxPrice())) {
                LOGGER.warn("Invalid calculated bid price: {} (min={}, max={})", 
                    ourBidPrice, config.getMinPrice(), config.getMaxPrice());
                validBidSource = false;
            }
            
            if (validAskSource && (ourAskPrice <= 0 || ourAskPrice < config.getMinPrice() || ourAskPrice > config.getMaxPrice())) {
                LOGGER.warn("Invalid calculated ask price: {} (min={}, max={})", 
                    ourAskPrice, config.getMinPrice(), config.getMaxPrice());
                validAskSource = false;
            }

            // Get the native instrument ID using the correct instrument ID
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    instrumentId, config.getMarketSource(), false);
                LOGGER.debug("Looking up native instrument for {} on {}: {}",
                    instrumentId, config.getMarketSource(), nativeInstrument);
            }
                
            if (nativeInstrument == null) {
                LOGGER.warn("No native instrument ID found for {} on {}", 
                    instrumentId, config.getMarketSource());
                LOGGER.info("processSymmetricQuoting: Failed: No native instrument ID found");
                return;
            }

            LOGGER.info("Using native instrument: {} for bond {}", nativeInstrument, bondId);

            // Check if we already have a quote for this bond
            ActiveQuote existingQuote = activeQuotes.get(bondId);
            
            if (existingQuote == null) {
                // Create new quote entries
                LOGGER.info("Creating new quotes for bond: {}", bondId);
                
                if (validBidSource) {
                    placeOrder(bondId, nativeInstrument, "Buy", config.getMinSize(), 
                        ourBidPrice, referenceBidSource);
                }
                
                if (validAskSource) {
                    placeOrder(bondId, nativeInstrument, "Sell", config.getMinSize(), 
                        ourAskPrice, referenceAskSource);
                }
                
                LOGGER.info("processSymmetricQuoting: New quotes created for bond: {}", bondId);
            } else {
                // Update existing quotes
                updateExistingSymmetricQuotes(existingQuote, bondId, nativeInstrument,
                    validBidSource, ourBidPrice, referenceBidSource,
                    validAskSource, ourAskPrice, referenceAskSource);
                
                LOGGER.info("processSymmetricQuoting: Updated existing quotes for bond: {}", bondId);
            }
            
        } catch (Exception e) {
            LOGGER.error("Error processing symmetric quoting for bond {}: {}", bondId, e.getMessage(), e);
        }
    }
    
    /**
     * Update existing symmetric quotes with price change detection and order management
     */
    private void updateExistingSymmetricQuotes(ActiveQuote existingQuote, String bondId, String nativeInstrument,
                                            boolean validBidSource, double ourBidPrice, String referenceBidSource,
                                            boolean validAskSource, double ourAskPrice, String referenceAskSource) {
        LOGGER.info("updateExistingSymmetricQuotes: Updating quotes for bond: {}", bondId);
        
        try {
            // Define minimum price change threshold to avoid excessive order churn
            final double MIN_PRICE_CHANGE_THRESHOLD = 0.005; // Half a penny
            
            // Update bid if needed
            if (validBidSource && ourBidPrice > 0) {
                MarketOrder currentBid = existingQuote.getBidOrder();
                double currentBidPrice = existingQuote.getBidPrice();
                
                // Check if we need to update the bid order
                boolean updateBid = currentBid == null || 
                                    currentBid.isDead() || 
                                    Math.abs(currentBidPrice - ourBidPrice) >= MIN_PRICE_CHANGE_THRESHOLD ||
                                    !referenceBidSource.equals(existingQuote.getBidReferenceSource());
                
                if (updateBid) {
                    // Cancel existing order if present
                    if (currentBid != null && !currentBid.isDead()) {
                        cancelOrder(currentBid, bondId);
                    }
                    
                    // Place new order
                    placeOrder(bondId, nativeInstrument, "Buy", config.getMinSize(), 
                        ourBidPrice, referenceBidSource);
                    
                    LOGGER.info("Updated bid for {}: price={}, source={}", 
                        bondId, ourBidPrice, referenceBidSource);
                } else {
                    LOGGER.debug("Bid unchanged for {}: price={}, change={}", 
                        bondId, currentBidPrice, Math.abs(currentBidPrice - ourBidPrice));
                }
            } else if (existingQuote.getBidOrder() != null && !existingQuote.getBidOrder().isDead()) {
                // Cancel bid side if it's no longer valid
                LOGGER.info("Cancelling bid for {} as source is no longer valid", bondId);
                cancelOrder(existingQuote.getBidOrder(), bondId);
            }
            
            // Update ask if needed
            if (validAskSource && ourAskPrice > 0) {
                MarketOrder currentAsk = existingQuote.getAskOrder();
                double currentAskPrice = existingQuote.getAskPrice();
                
                // Check if we need to update the ask order
                boolean updateAsk = currentAsk == null || 
                                    currentAsk.isDead() || 
                                    Math.abs(currentAskPrice - ourAskPrice) >= MIN_PRICE_CHANGE_THRESHOLD ||
                                    !referenceAskSource.equals(existingQuote.getAskReferenceSource());
                
                if (updateAsk) {
                    // Cancel existing order if present
                    if (currentAsk != null && !currentAsk.isDead()) {
                        cancelOrder(currentAsk, bondId);
                    }
                    
                    // Place new order
                    placeOrder(bondId, nativeInstrument, "Sell", config.getMinSize(), 
                        ourAskPrice, referenceAskSource);
                    
                    LOGGER.info("Updated ask for {}: price={}, source={}", 
                        bondId, ourAskPrice, referenceAskSource);
                } else {
                    LOGGER.debug("Ask unchanged for {}: price={}, change={}", 
                        bondId, currentAskPrice, Math.abs(currentAskPrice - ourAskPrice));
                }
            } else if (existingQuote.getAskOrder() != null && !existingQuote.getAskOrder().isDead()) {
                // Cancel ask side if it's no longer valid
                LOGGER.info("Cancelling ask for {} as source is no longer valid", bondId);
                cancelOrder(existingQuote.getAskOrder(), bondId);
            }

            LOGGER.info("updateExistingSymmetricQuotes: Quotes updated for bond: {}", bondId);

        } catch (Exception e) {
            LOGGER.error("updateExistingSymmetricQuotes: Error updating symmetric quotes for bond {}: {}", bondId, e.getMessage(), e);
        }
    }
}