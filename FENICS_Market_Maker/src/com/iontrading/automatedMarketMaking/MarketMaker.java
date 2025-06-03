package com.iontrading.automatedMarketMaking;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

    // Reference to the main OrderManagement component
    private final OrderManagement orderManager;
    
    // Store our active orders by instrument ID
    private final Map<String, ActiveQuote> activeQuotes = new ConcurrentHashMap<>();
    
    // Store the trader ID to use for FENICS
    private final String fenicsTrader;
    
    // Executor for periodic tasks
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    // Track whether market making is enabled
    private volatile boolean enabled = false;
    
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

    /**
     * Creates a new MarketMaker instance with default configuration.
     * 
     * @param orderManager The parent OrderManagement instance
     */
    public MarketMaker(OrderManagement orderManager) {
        this(orderManager, new MarketMakerConfig());
    }
    
    /**
     * Creates a new MarketMaker instance with custom configuration and default BondEligibilityListener.
     * 
     * @param orderManager The parent OrderManagement instance
     * @param config The market maker configuration
     */
    public MarketMaker(OrderManagement orderManager, MarketMakerConfig config) {
        this.orderManager = orderManager;
        this.config = config;

        this.bondEligibilityListener = new BondEligibilityListener();
        // Subscribe to MKV data streams
        subscribeToBondStaticData();
        subscribeToFirmPositionData();
        subscribeToSdsInformationData();

        this.fenicsTrader = orderManager.getTraderForVenue(config.getMarketSource());
        
        // Get DepthListener from OrderManagement
        this.depthListener = OrderManagement.getDepthListener();
        
        // Register for eligibility change notifications
        this.bondEligibilityListener.addEligibilityChangeListener(new EligibilityChangeListener() {
            public void onEligibilityChange(String cusip, boolean isEligible, Map<String, Object> bondData) {
                handleEligibilityChange(cusip, isEligible, bondData);
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
        
        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down MarketMaker scheduler");
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
    }

    /**
     * Subscribe to bond static data
     */
    private void subscribeToBondStaticData() {
        try {
            // Get the publish manager to access patterns
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
       
            // Look up the pattern object
            MkvObject obj = pm.getMkvObject(BOND_STATIC_PATTERN);
            
            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
            // Subscribe within a synchronized block
                LOGGER.info("Found bond static pattern, subscribing: {}", (Object) BOND_STATIC_PATTERN);
                ((MkvPattern) obj).subscribe(BOND_STATIC_FIELDS, bondEligibilityListener);
                // Mark that we've successfully subscribed
                isBondStaticSubscribed = true;

                LOGGER.info("Subscribed to bond static data: {} with {} fields",
                    BOND_STATIC_PATTERN, BOND_STATIC_FIELDS.length);            
            } else {
                LOGGER.info("Bond static pattern not found");
            }
        } catch (MkvException e) {
            LOGGER.error("Error subscribing to bond static data: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Subscribe to firm position data
     */
    private void subscribeToFirmPositionData() {
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
            } else {
                LOGGER.info("Firm position pattern not found");
            }
        } catch (MkvException e) {
            LOGGER.error("Error subscribing to firm position data: {}", e.getMessage(), e);
        }
    }
    
        /**
     * Subscribe to firm position data
     */
    private void subscribeToSdsInformationData() {
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
            } else {
                LOGGER.info("SDS information pattern not found");
            }
        } catch (MkvException e) {
            LOGGER.error("Error subscribing to SDS information data: {}", e.getMessage(), e);
        }
    }

        /**
     * Cancel all active orders.
     * Used when disabling the market maker.
     */
    private void cancelAllOrders() {
        LOGGER.info("Cancelling all market maker orders");
        
        for (ActiveQuote quote : activeQuotes.values()) {
            MarketOrder bidOrder = quote.getBidOrder();
            MarketOrder askOrder = quote.getAskOrder();
            
            if (bidOrder != null) {
                cancelOrder(bidOrder, quote.getCusip());
            }
            
            if (askOrder != null) {
                cancelOrder(askOrder, quote.getCusip());
            }
        }
    }

    /**
     * Periodically check our orders to ensure they're still valid.
     * Removes stale orders from our tracking.
     */
    private void monitorOrders() {
        if (!enabled) {
            return;
        }
        
        try {
            LOGGER.debug("Monitoring market maker orders - currently tracking " + 
                activeQuotes.size() + " instruments");
                
            for (ActiveQuote quote : activeQuotes.values()) {
                // Check bid order
                MarketOrder bidOrder = quote.getBidOrder();
                if (bidOrder != null && bidOrder.isExpired()) {
                    LOGGER.info("Bid order expired: cusip=" + quote.getCusip() + 
                        ", reqId=" + bidOrder.getMyReqId());
                    quote.setBidOrder(null, null, 0.0);
                }
                
                // Check ask order
                MarketOrder askOrder = quote.getAskOrder();
                if (askOrder != null && askOrder.isExpired()) {
                    LOGGER.info("Ask order expired: cusip=" + quote.getCusip() + 
                        ", reqId=" + askOrder.getMyReqId());
                    quote.setAskOrder(null, null, 0.0);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error monitoring orders: " + e.getMessage(), e);
        }
    }

    /**
     * Execute a hedge trade on the reference venue.
     * This method is called when one of our orders on the market source is filled.
     */
    private void executeHedgeTrade(String bondId, String side, double size, double hedgePrice, String referenceSource) {
        // Skip if auto-hedge is disabled
        if (!config.isAutoHedge()) {
            LOGGER.info("Auto-hedge disabled, skipping hedge trade");
            return;
        }

        try {
            if (referenceSource == null || !isTargetVenue(referenceSource)) {
                LOGGER.warn("Cannot hedge on invalid venue: {}", referenceSource);
                return;
            }
            
            // Get the instrument ID that corresponds to this bond ID
            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId);
            if (instrumentId == null) {
                LOGGER.warn("No instrument ID found for bond: {}", bondId);
                return;
            }
            
            // Get the native instrument ID for the reference venue
            String referenceInstrument = null;
            if (depthListener != null) {
                referenceInstrument = depthListener.getInstrumentFieldBySourceString(
                    instrumentId, referenceSource, false);
            } else {
                LOGGER.warn("DepthListener not available for instrument mapping");
            }
            
            if (referenceInstrument == null) {
                LOGGER.warn("No native instrument ID found for bond {} on venue {}", 
                            bondId, referenceSource);
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
                LOGGER.info("Hedge order created successfully: reqId={}", hedgeOrder.getMyReqId());
            } else {
                LOGGER.error("Failed to create hedge order");
            }
        } catch (Exception e) {
            LOGGER.error("Error executing hedge trade: {}", e.getMessage(), e);
        }
    }
        
    @Override
    public void orderDead(MarketOrder order) {
        try {
            LOGGER.info(
                "Order dead notification: reqId={}, orderId={}",
                order.getMyReqId(), order.getOrderId());
                
            // Find the active quote that contains this order
            for (ActiveQuote quote : activeQuotes.values()) {
                // Check bid order
                if (order.equals(quote.getBidOrder())) {
                    // If order was filled, execute hedge trade
                    boolean wasFilled = (order.getQtyFilled() > 0);
                    
                    if (wasFilled) {
                        LOGGER.info("Bid order filled: cusip={}, size={}, price={}",
                            quote.getCusip(), order.getQtyFilled(), order.getPrice());
                        
                        // Execute hedge trade on the bid reference source
                        executeHedgeTrade(
                            quote.getCusip(), 
                            "Buy", 
                            order.getQtyFilled(),
                            order.getPrice(),  
                            quote.getBidReferenceSource()
                        );
                    }
                    
                    // Clear the bid order reference
                    quote.setBidOrder(null, null, 0.0);
                    break;
                }
                
                // Check ask order
                if (order.equals(quote.getAskOrder())) {
                    // If order was filled, execute hedge trade
                    boolean wasFilled = (order.getQtyFilled() > 0);
                    
                    if (wasFilled) {
                        LOGGER.info("Ask order filled: cusip={}, size={}, price={}",
                            quote.getCusip(), order.getQtyFilled(), order.getPrice());
                        
                        // Execute hedge trade on the ask reference source
                        executeHedgeTrade(
                            quote.getCusip(), 
                            "Sell", 
                            order.getQtyFilled(),
                            order.getPrice(),
                            quote.getAskReferenceSource()
                        );
                    }
                    
                    // Clear the ask order reference
                    quote.setAskOrder(null, null, 0.0);
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error processing orderDead: {}", e.getMessage(), e);
        }
    }
    @Override
    public MarketOrder addOrder(String MarketSource, String TraderId, String instrId, 
                              String verb, double qty, double price, String type, String tif) {
        // Delegate to the main OrderManagement instance
        return orderManager.addOrder(MarketSource, TraderId, instrId, verb, qty, price, type, tif);
    }
    
    @Override
    public void best(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {
        // Process the market update
        String id = best.getId();
        if(id == null || id.isEmpty()) {
            LOGGER.warn("Received market update with empty instrument ID, skipping");
            return;
        }

        if (!id.endsWith(".C_Fixed") && !id.endsWith(".REG_Fixed")){
            LOGGER.debug("Skipping non-overnight instrument: " + id);
            return;
        }
        if (id.endsWith(".C_Fixed")) {
            processMarketUpdate(best, gcBestCash);
        } else if (id.endsWith(".REG_Fixed")) {
            processMarketUpdate(best, gcBestREG);
        } else {
            LOGGER.warn("Received market update for unsupported instrument type: " + id);
        }
        
    }
    
    @Override
    public String getApplicationId() {
        return "FENICS_MarketMaker";
    }
    
        /**
     * Gets the current configuration.
     * 
     * @return The market maker configuration
     */
    public MarketMakerConfig getConfig() {
        return config;
    }
    

    /**
     * Update configuration
     */
    public void updateConfiguration(MarketMakerConfig newConfig) {
        // Note: This should be implemented to safely update config
        // For now, logging that update was requested
        LOGGER.warn("Configuration update requested but not implemented safely");
    }

    /**
     * Get configuration summary for monitoring
     */
    public Map<String, Object> getConfigSummary() {
        Map<String, Object> summary = new HashMap<>();
        summary.put("enabled", enabled);
        summary.put("marketSource", config.getMarketSource());
        summary.put("minSize", config.getMinSize());
        summary.put("bidAdjustment", config.getBidAdjustment());
        summary.put("askAdjustment", config.getAskAdjustment());
        summary.put("autoHedge", config.isAutoHedge());
        summary.put("activeQuotes", activeQuotes.size());
        summary.put("trackedInstruments", trackedInstruments.size());
        return summary;
    }

    /**
     * Get whether market making is currently enabled.
     * 
     * @return true if enabled, false if disabled
     */
    public boolean isEnabled() {
        return enabled;
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
    }

    /**
     * Enable or disable the market making strategy.
     * 
     * @param enabled Whether market making should be enabled
     */
    public void setEnabled(boolean enabled) {
        boolean previousState = this.enabled;
        this.enabled = enabled;
        
        if (previousState != enabled) {
            if (enabled) {
                LOGGER.info("Market maker enabled");
            } else {
                LOGGER.info("Market maker disabled - cancelling all orders");
                cancelAllOrders();
            }
        }
    }
    
    private void handleEligibilityChange(String cusip, boolean isEligible, Map<String, Object> bondData) {
        if (!enabled) {
            return;
        }
        try {
            LOGGER.info("Bond eligibility changed: " + cusip + " -> " + 
                (isEligible ? "ELIGIBLE" : "INELIGIBLE"));

            if (isEligible) {
                // Add to our tracking set
                trackedInstruments.add(cusip);

                // Try to create initial markets if we have reference data
                tryCreateInitialMarkets(cusip);
            } else {
                // Remove from tracking and cancel any existing orders
                trackedInstruments.remove(cusip);
                cancelOrdersForInstrument(cusip);
            }
        } catch (Exception e) {
            LOGGER.error("Error handling eligibility change for " + cusip + ": " + e.getMessage(), e);
        }
    }

    private void makeMarketsForEligibleBonds() {
        if (!enabled) {
            return;
        }
        
        try {
            Set<String> eligibleBonds = bondEligibilityListener.getEligibleBonds();

            LOGGER.info("Making markets for " + eligibleBonds.size() + " eligible bonds");

            for (String cusip : eligibleBonds) {
                // Add to tracking if not already there
                trackedInstruments.add(cusip);
                
                // Try to create or update markets
                tryCreateOrUpdateMarkets(cusip);
            }
            
            // Clean up any instruments that are no longer eligible
            Set<String> instrumentsToRemove = new HashSet<>();
            for (String cusip : trackedInstruments) {
                if (!eligibleBonds.contains(cusip)) {
                    instrumentsToRemove.add(cusip);
                }
            }
            
            for (String cusip : instrumentsToRemove) {
                trackedInstruments.remove(cusip);
                cancelOrdersForInstrument(cusip);
            }
        } catch (Exception e) {
            LOGGER.error("Error in periodic market making: " + e.getMessage(), e);
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
     * Get current status for monitoring
     */
    public Map<String, Object> getMarketMakerStatus() {
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
        
        return status;
    }

    /**
     * Process market data updates and update our quotes accordingly.
     * Called by the DepthListener when market data changes.
     * 
     * @param best The updated best prices
     */
    public void processMarketUpdate(Best best, GCBest gcBest) {
        if (!enabled) {
            return;
        }
        
        try {
            String instrumentId = best.getInstrumentId();
            String bestId = best.getId(); // This should be the instrument ID from VMO.CM_INSTRUMENT
            
            if (bestId == null || bestId.isEmpty()) {
                return;
            }
            
            // Find the bond ID that maps to this instrument ID
            String bondId = null;
            for (Map.Entry<String, String> entry : bondEligibilityListener.bondToInstrumentMap.entrySet()) {
                if (bestId.equals(entry.getValue())) {
                    bondId = entry.getKey();
                    break;
                }
            }
            
            // If we can't find a bond mapping, this instrument isn't eligible for market making
            if (bondId == null) {
                LOGGER.debug("No bond mapping found for instrument ID: " + bestId + ", skipping market update");
                return;
            }

            // Check if this bond is in our tracked instruments
            if (!trackedInstruments.contains(bondId)) {
                LOGGER.debug("Bond " + bondId + " not in tracked instruments, skipping market update");
                return;
            }

            LOGGER.info("Processing market update for bond " + bondId + " (instrument: " + bestId + ")");

            // Skip if this is not a CASH instrument (if cashOnly is true)
            boolean isCash = bestId.contains("C_Fixed");
            if (config.isCashOnly() && !isCash) {
                LOGGER.debug("Skipping non-CASH instrument: " + bestId);
                return;
            }
            
            // Get the best bid and ask prices and sources
            double bestBid = 0.0;
            double bestAsk = 0.0;
            String bidSource = null;
            String askSource = null;
            try {
                if (best == null) {
                    LOGGER.warn("Best object is null for bond " + bondId);
                    return;
                }
                bestBid = 0.0;
                bestAsk = 0.0;
                bidSource = null;
                askSource = null;
                
                // Safely extract bid data, continue if null
                if (best.getBid() > 0) {
                    bestBid = best.getBid();
                    bidSource = best.getBidSrc();
                }
                
                // Safely extract ask data, continue if null
                if (best.getAsk() > 0) {
                    bestAsk = best.getAsk();
                    askSource = best.getAskSrc();
                }
                
                // If we have no prices from best, check GC best
                if ((bestBid <= 0 || bestAsk <= 0) && gcBest != null) {
                    GCLevelResult gcLevel = gcBest.getGCLevel();
                    if (gcLevel != null) {
                        if (bestBid <= 0 && gcLevel.getBidPrice() != null && gcLevel.getBidPrice() > 0) {
                            bestBid = gcLevel.getBidPrice();
                            bidSource = "GC_BEST";
                        }
                        if (bestAsk <= 0 && gcLevel.getAskPrice() != null && gcLevel.getAskPrice() > 0) {
                            bestAsk = gcLevel.getAskPrice();
                            askSource = "GC_BEST";
                        }
                    }
                }

                LOGGER.debug("Market data for {}: bid={}/{}, ask={}/{}", 
                    bondId, bestBid, bidSource, bestAsk, askSource);
            } catch (Exception e) {
                LOGGER.error("Error getting best prices for " + bondId + ": " + e.getMessage(), e);
                return;
            }

            // Pre-validate that we could hedge if needed
            boolean canHedgeOnBid = canExecuteHedgeOnVenue(bidSource, bestId, "Sell", bestBid);
            boolean canHedgeOnAsk = canExecuteHedgeOnVenue(askSource, bestId, "Buy", bestAsk);

            // Only create markets if we can hedge them
            if (!canHedgeOnBid && !canHedgeOnAsk) {
                LOGGER.debug("Skipping market making for " + bondId + " - cannot hedge on either side due to self-match");
                return;
            }

            if (!canHedgeOnBid) {
                LOGGER.info("Not quoting bid side for " + bondId + " - cannot hedge due to self-match on " + bidSource);
                // Only quote ask side
                processAsymmetricQuoting(best, bondId, false, true);
                return;
            }
            
            if (!canHedgeOnAsk) {
                LOGGER.info("Not quoting ask side for " + bondId + " - cannot hedge due to self-match on " + askSource);
                // Only quote bid side
                processAsymmetricQuoting(best, bondId, true, false);
                return;
            }
            
            // If we can hedge both sides, proceed with normal market making
            processSymmetricQuoting(best, bondId);
            
        } catch (Exception e) {
            LOGGER.error("Error processing enhanced market update: " + e.getMessage(), e);
        }
    }

    /**
     * Check if we can execute a hedge on a specific venue without self-match
     * Note: Since VenueTraderListener was removed, this is a simplified version
     */
    private boolean canExecuteHedgeOnVenue(String venue, String instrumentId, String hedgeSide, double price) {
        try {
            if (!isTargetVenue(venue)) {
                LOGGER.debug("Venue {} is not a target venue", venue);
                return false;
            }
            
            // Get native instrument
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    instrumentId, venue, false); // Assume non-AON for validation
            }
            
            if (nativeInstrument == null) {
                LOGGER.debug("No native instrument mapping found for {} on venue {}", instrumentId, venue);
                return false;
            }
            
            // Get the current Best data for this instrument to check for self-match
            Best currentBest = orderManager.getLatestBestByInstrument().get(instrumentId);
            if (currentBest == null) {
                LOGGER.debug("No current best data available for instrument {}", instrumentId);
                return true; // No data means no self-match risk
            }
            
            // Check if we would be hedging against our own price
            if ("Buy".equals(hedgeSide)) {
                // We want to buy (hedge a sell), check if the ask is ours
                String askSource = currentBest.getAskSrc();
                if (venue.equals(askSource)) {
                    int askStatus = currentBest.getAskStatus();
                    boolean isOurAsk = currentBest.isMinePrice(askStatus);
                    if (isOurAsk) {
                        LOGGER.info("Cannot hedge BUY on {} - ask price belongs to us (self-match prevention)", venue);
                        return false;
                    }
                }
            } else if ("Sell".equals(hedgeSide)) {
                // We want to sell (hedge a buy), check if the bid is ours
                String bidSource = currentBest.getBidSrc();
                if (venue.equals(bidSource)) {
                    int bidStatus = currentBest.getBidStatus();
                    boolean isOurBid = currentBest.isMinePrice(bidStatus);
                    if (isOurBid) {
                        LOGGER.info("Cannot hedge SELL on {} - bid price belongs to us (self-match prevention)", venue);
                        return false;
                    }
                }
            }
            
            LOGGER.debug("Hedge feasibility check passed for venue: {} instrument: {}, side: {} - no self-match detected", 
                venue, nativeInstrument, hedgeSide);
            return true;

        } catch (Exception e) {
            LOGGER.warn("Error checking hedge feasibility for venue {} instrument {} side {}: {}", 
                venue, instrumentId, hedgeSide, e.getMessage());
            return false;
        }
    }
    /**
     * Process asymmetric quoting when only one side can be hedged
     */
    private void processAsymmetricQuoting(Best best, String bondId, boolean quoteBid, boolean quoteAsk) {
        // Implementation for selective single-sided quoting
        // This allows market making even when only one side can be safely hedged

        LOGGER.info("Processing asymmetric quoting for " + bondId +
            " (bid=" + quoteBid + ", ask=" + quoteAsk + ")");

        // Get existing quote
        ActiveQuote existingQuote = activeQuotes.get(bondId);
        if (existingQuote == null) {
            existingQuote = new ActiveQuote(bondId);
            activeQuotes.put(bondId, existingQuote);
        }
        
        // Cancel the side we can't hedge and keep the side we can
        if (!quoteBid && existingQuote.getBidOrder() != null) {
            cancelOrder(existingQuote.getBidOrder(), bondId);
        }
        
        if (!quoteAsk && existingQuote.getAskOrder() != null) {
            cancelOrder(existingQuote.getAskOrder(), bondId);
        }
        
        // Place new orders only for the side we can hedge
        String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId);
        if (instrumentId == null) return;
        
        String nativeInstrument = null;
        if (depthListener != null) {
            nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                instrumentId, config.getMarketSource(), false);
        }
        if (nativeInstrument == null) return;
        
        if (quoteBid) {
            double bidPrice = best.getBid() + config.getBidAdjustment();
            placeOrder(bondId, nativeInstrument, "Buy", config.getMinSize(), bidPrice, best.getBidSrc());
        }
        
        if (quoteAsk) {
            double askPrice = best.getAsk() - config.getAskAdjustment();
            placeOrder(bondId, nativeInstrument, "Sell", config.getMinSize(), askPrice, best.getAskSrc());
        }
    }
    
    /**
     * Update asymmetric quotes with intelligent order management
     */
    private void updateAsymmetricQuotes(ActiveQuote existingQuote, String bondId, String nativeInstrument,
                                    boolean validBidData, double ourBidPrice, String referenceBidSource,
                                    boolean validAskData, double ourAskPrice, String referenceAskSource,
                                    boolean shouldQuoteBid, boolean shouldQuoteAsk) {
        try {
            final double MIN_PRICE_CHANGE_THRESHOLD = 0.005; // Half a penny
            
            // Handle BID side
            if (shouldQuoteBid && validBidData) {
                // We want to quote bid and have valid data
                MarketOrder existingBid = existingQuote.getBidOrder();
                boolean needNewBid = false;
                String bidUpdateReason = "";
                
                if (existingBid == null) {
                    needNewBid = true;
                    bidUpdateReason = "No existing bid order for asymmetric quoting";
                } else if (existingBid.isDead() || existingBid.isExpired()) {
                    needNewBid = true;
                    bidUpdateReason = "Existing asymmetric bid is dead/expired";
                } else if (Math.abs(existingBid.getPrice() - ourBidPrice) >= MIN_PRICE_CHANGE_THRESHOLD) {
                    needNewBid = true;
                    bidUpdateReason = "Asymmetric bid price change: old=" + existingBid.getPrice() + 
                                    ", new=" + ourBidPrice + ", diff=" + Math.abs(existingBid.getPrice() - ourBidPrice);
                } else if (!referenceBidSource.equals(existingQuote.getBidReferenceSource())) {
                    needNewBid = true;
                    bidUpdateReason = "Asymmetric bid reference source changed: old=" + 
                                    existingQuote.getBidReferenceSource() + ", new=" + referenceBidSource;
                }
                
                if (needNewBid) {
                    LOGGER.info("Updating asymmetric bid for bond " + bondId + ": " + bidUpdateReason);

                    // Cancel existing bid if present
                    if (existingBid != null && !existingBid.isDead()) {
                        cancelOrder(existingBid, bondId);
                    }
                    
                    // Place new bid
                    placeAsymmetricOrder(bondId, nativeInstrument, "Buy", config.getMinSize(), 
                                    ourBidPrice, referenceBidSource, "BID");
                } else {
                    LOGGER.debug("Keeping existing asymmetric bid for bond " + bondId + 
                        "Keeping existing asymmetric bid for bond " + bondId + 
                        " (price diff: " + Math.abs(existingBid.getPrice() - ourBidPrice) + ")");
                }
            } else {
                // We don't want to quote bid or don't have valid data - cancel existing bid
                MarketOrder existingBid = existingQuote.getBidOrder();
                if (existingBid != null && !existingBid.isDead()) {
                    String reason = !shouldQuoteBid ? "not hedgeable" : "invalid data";
                    LOGGER.info("Cancelling asymmetric bid for bond " + bondId + " - " + reason);
                    cancelOrder(existingBid, bondId);
                }
            }
            
            // Handle ASK side
            if (shouldQuoteAsk && validAskData) {
                // We want to quote ask and have valid data
                MarketOrder existingAsk = existingQuote.getAskOrder();
                boolean needNewAsk = false;
                String askUpdateReason = "";
                
                if (existingAsk == null) {
                    needNewAsk = true;
                    askUpdateReason = "No existing ask order for asymmetric quoting";
                } else if (existingAsk.isDead() || existingAsk.isExpired()) {
                    needNewAsk = true;
                    askUpdateReason = "Existing asymmetric ask is dead/expired";
                } else if (Math.abs(existingAsk.getPrice() - ourAskPrice) >= MIN_PRICE_CHANGE_THRESHOLD) {
                    needNewAsk = true;
                    askUpdateReason = "Asymmetric ask price change: old=" + existingAsk.getPrice() + 
                                    ", new=" + ourAskPrice + ", diff=" + Math.abs(existingAsk.getPrice() - ourAskPrice);
                } else if (!referenceAskSource.equals(existingQuote.getAskReferenceSource())) {
                    needNewAsk = true;
                    askUpdateReason = "Asymmetric ask reference source changed: old=" + 
                                    existingQuote.getAskReferenceSource() + ", new=" + referenceAskSource;
                }
                
                if (needNewAsk) {
                    LOGGER.info("Updating asymmetric ask for bond " + bondId + ": " + askUpdateReason);

                    // Cancel existing ask if present
                    if (existingAsk != null && !existingAsk.isDead()) {
                        cancelOrder(existingAsk, bondId);
                    }
                    
                    // Place new ask
                    placeAsymmetricOrder(bondId, nativeInstrument, "Sell", config.getMinSize(), 
                                    ourAskPrice, referenceAskSource, "ASK");
                } else {
                    LOGGER.debug("Keeping existing asymmetric ask for bond " + bondId + 
                        " (price diff: " + Math.abs(existingAsk.getPrice() - ourAskPrice) + ")");
                }
            } else {
                // We don't want to quote ask or don't have valid data - cancel existing ask
                MarketOrder existingAsk = existingQuote.getAskOrder();
                if (existingAsk != null && !existingAsk.isDead()) {
                    String reason = !shouldQuoteAsk ? "not hedgeable" : "invalid data";
                    LOGGER.info("Cancelling asymmetric ask for bond " + bondId + " - " + reason);
                    cancelOrder(existingAsk, bondId);
                }
            }
            
            // Log the final asymmetric state
            boolean hasBid = existingQuote.getBidOrder() != null && !existingQuote.getBidOrder().isDead();
            boolean hasAsk = existingQuote.getAskOrder() != null && !existingQuote.getAskOrder().isDead();

            LOGGER.info("Asymmetric quote state for " + bondId + ": hasBid=" + hasBid +
                ", hasAsk=" + hasAsk + " (intended: bid=" + shouldQuoteBid + ", ask=" + shouldQuoteAsk + ")");
                
        } catch (Exception e) {
            LOGGER.error("Error updating asymmetric quotes for bond " + bondId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Cancel all existing quotes for a bond
     */
    private void cancelExistingQuotes(String bondId) {
        try {
            ActiveQuote existingQuote = activeQuotes.get(bondId);
            if (existingQuote != null) {
                LOGGER.info("Cancelling all existing quotes for bond " + bondId);

                MarketOrder bidOrder = existingQuote.getBidOrder();
                MarketOrder askOrder = existingQuote.getAskOrder();
                
                if (bidOrder != null && !bidOrder.isDead()) {
                    cancelOrder(bidOrder, bondId);
                }
                
                if (askOrder != null && !askOrder.isDead()) {
                    cancelOrder(askOrder, bondId);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error cancelling existing quotes for " + bondId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Place an asymmetric order with enhanced logging
     */
    private void placeAsymmetricOrder(String cusip, String nativeInstrument, String verb, 
                                    double size, double price, String referenceSource, String side) {
        try {
            // Additional validation for asymmetric orders
            if (size <= 0) {
                LOGGER.warn("Invalid asymmetric order size for " + cusip + " " + side + ": " + size);
                return;
            }

            if (price <= 0 || price < config.getMinPrice() || price > config.getMaxPrice()) {
                LOGGER.warn("Invalid asymmetric order price for " + cusip + " " + side + ": " + price);
                return;
            }

            // Check trading hours for default markets
            if ("DEFAULT".equals(referenceSource) && !config.isDefaultMarketMakingAllowed()) {
                LOGGER.info("Skipping asymmetric " + side + " order outside allowed hours for " + cusip);
                return;
            }

            LOGGER.info("Placing ASYMMETRIC " + side + " (" + verb + ") order on " + config.getMarketSource() + 
                ": " + nativeInstrument + " (" + cusip + "), size=" + size + ", price=" + price + 
                ", reference=" + referenceSource);

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
                // Store the order in our active quotes map
                ActiveQuote quote = activeQuotes.computeIfAbsent(cusip, 
                    k -> new ActiveQuote(cusip));
                
                if ("Buy".equals(verb)) {
                    quote.setBidOrder(order, referenceSource, price);
                    LOGGER.info("Asymmetric BID order placed successfully: reqId=" + order.getMyReqId() + 
                        ", orderId=" + order.getOrderId() + ", " + verb + " " + nativeInstrument + 
                        " (hedgeable on " + referenceSource + ")");
                } else {
                    quote.setAskOrder(order, referenceSource, price);
                    LOGGER.info("Asymmetric ASK order placed successfully: reqId=" + order.getMyReqId() + 
                        ", orderId=" + order.getOrderId() + ", " + verb + " " + nativeInstrument + 
                        " (hedgeable on " + referenceSource + ")");
                }
            } else {
                LOGGER.warn("Failed to place asymmetric " + side + " order on " + config.getMarketSource() + 
                    ": " + nativeInstrument + " (" + cusip + ")");
            }
        } catch (Exception e) {
            LOGGER.error("Error placing asymmetric " + side + " order for " + cusip + ": " + e.getMessage(), e);
        }
    }

    /**
     * Check if a venue is a target venue for hedging
     */
    private boolean isTargetVenue(String venue) {
        if (venue == null) return false;
        return config.getTargetVenuesSet().contains(venue);
    }
    
    /**
     * Process normal symmetric quoting when both sides can be hedged
     */
    private void processSymmetricQuoting(Best best, String bondId) {
        try {
            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId);
            if (instrumentId == null) {
                LOGGER.warn("No instrument ID mapping for bond: " + bondId);
                return;
            }

            // Get the best bid and ask prices and sources
            double bestBid = best.getBid();
            double bestAsk = best.getAsk();
            String bidSource = best.getBidSrc();
            String askSource = best.getAskSrc();
            
            // Skip if we don't have valid prices or sources
            if (bestBid <= 0 || bestAsk <= 0 || bidSource == null || askSource == null) {
                LOGGER.debug("Invalid prices or sources for " + bondId + ": bid=" + bestBid +
                    ", ask=" + bestAsk + ", bidSrc=" + bidSource + ", askSrc=" + askSource);
                return;
            }
            
            // Check if either bid or ask is from one of our target venues
            boolean validBidSource = isTargetVenue(bidSource);
            boolean validAskSource = isTargetVenue(askSource);
            
            if (!validBidSource && !validAskSource) {
                LOGGER.trace("No valid target venues for " + bondId + ": bidSrc=" + bidSource +
                    ", askSrc=" + askSource);
                return;
            }

            LOGGER.info("Valid symmetric market data for bond " + bondId + ": bid=" + bestBid + "/" + bidSource +
                ", ask=" + bestAsk + "/" + askSource);

            // Calculate our quoting prices
            double ourBidPrice = 0;
            double ourAskPrice = 0;
            String referenceBidSource = null;
            String referenceAskSource = null;
            
            if (validBidSource) {
                ourBidPrice = bestBid - config.getBidAdjustment();
                referenceBidSource = bidSource;
            }
            
            if (validAskSource) {
                ourAskPrice = bestAsk + config.getAskAdjustment();
                referenceAskSource = askSource;
            }
            
            // Enforce minimum spread if both sides are valid
            if (validBidSource && validAskSource) {
                double spread = ourAskPrice - ourBidPrice;
                if (spread < config.getMinSpread()) {
                    double adjustment = (config.getMinSpread() - spread) / 2;
                    ourBidPrice -= adjustment;
                    ourAskPrice += adjustment;

                    LOGGER.info("Adjusted prices for minimum spread: bid=" + ourBidPrice +
                        ", ask=" + ourAskPrice + ", spread=" + (ourAskPrice - ourBidPrice));
                }
            }
            
            // Validate that our prices are reasonable
            if (validBidSource && (ourBidPrice <= 0 || ourBidPrice < config.getMinPrice() || ourBidPrice > config.getMaxPrice())) {
                LOGGER.warn("Invalid bid price calculated for " + bondId + ": " + ourBidPrice);
                validBidSource = false;
            }
            
            if (validAskSource && (ourAskPrice <= 0 || ourAskPrice < config.getMinPrice() || ourAskPrice > config.getMaxPrice())) {
                LOGGER.warn("Invalid ask price calculated for " + bondId + ": " + ourAskPrice);
                validAskSource = false;
            }

            // Get the native instrument ID using the correct instrument ID
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    instrumentId, config.getMarketSource(), false);
            }
                
            if (nativeInstrument == null) {
                LOGGER.warn("No instrument mapping found for instrument ID: " + instrumentId + 
                    " (bond: " + bondId + ") on " + config.getMarketSource());
                return;
            }

            LOGGER.info("Using native instrument: " + nativeInstrument + " for bond " + bondId);

            // Check if we already have a quote for this bond
            ActiveQuote existingQuote = activeQuotes.get(bondId);
            
            if (existingQuote == null) {
                LOGGER.info("Creating new symmetric quotes for bond " + bondId);

                // Create new quotes if we don't have any
                if (validBidSource && ourBidPrice > 0) {
                    placeOrder(bondId, nativeInstrument, "Buy", config.getMinSize(), ourBidPrice, referenceBidSource);
                }
                
                if (validAskSource && ourAskPrice > 0) {
                    placeOrder(bondId, nativeInstrument, "Sell", config.getMinSize(), ourAskPrice, referenceAskSource);
                }
            } else {
                // Update existing quotes if price has changed significantly
                updateExistingSymmetricQuotes(existingQuote, bondId, nativeInstrument, 
                    validBidSource, ourBidPrice, referenceBidSource,
                    validAskSource, ourAskPrice, referenceAskSource);
            }
            
        } catch (Exception e) {
            LOGGER.error("Error processing symmetric quoting for bond " + bondId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Update existing symmetric quotes with price change detection and order management
     */
    private void updateExistingSymmetricQuotes(ActiveQuote existingQuote, String bondId, String nativeInstrument,
                                            boolean validBidSource, double ourBidPrice, String referenceBidSource,
                                            boolean validAskSource, double ourAskPrice, String referenceAskSource) {
        try {
            // Define minimum price change threshold to avoid excessive order churn
            final double MIN_PRICE_CHANGE_THRESHOLD = 0.005; // Half a penny
            
            // Update bid if needed
            if (validBidSource && ourBidPrice > 0) {
                MarketOrder existingBid = existingQuote.getBidOrder();
                boolean needNewBid = false;
                String bidUpdateReason = "";
                
                if (existingBid == null) {
                    needNewBid = true;
                    bidUpdateReason = "No existing bid order";
                } else if (existingBid.isDead() || existingBid.isExpired()) {
                    needNewBid = true;
                    bidUpdateReason = "Existing bid is dead/expired";
                } else if (Math.abs(existingBid.getPrice() - ourBidPrice) >= MIN_PRICE_CHANGE_THRESHOLD) {
                    needNewBid = true;
                    bidUpdateReason = "Price change exceeds threshold: old=" + existingBid.getPrice() + 
                                    ", new=" + ourBidPrice + ", diff=" + Math.abs(existingBid.getPrice() - ourBidPrice);
                } else {
                    // Check if the reference source has changed
                    if (!referenceBidSource.equals(existingQuote.getBidReferenceSource())) {
                        needNewBid = true;
                        bidUpdateReason = "Reference source changed: old=" + existingQuote.getBidReferenceSource() + 
                                        ", new=" + referenceBidSource;
                    }
                }
                
                if (needNewBid) {
                    LOGGER.info("Updating bid for bond " + bondId + ": " + bidUpdateReason);

                    // Cancel existing bid if present
                    if (existingBid != null && !existingBid.isDead()) {
                        cancelOrder(existingBid, bondId);
                    }
                    
                    // Place new bid
                    placeOrder(bondId, nativeInstrument, "Buy", config.getMinSize(), ourBidPrice, referenceBidSource);
                } else {
                    LOGGER.debug("Keeping existing bid for bond " + bondId + " (price diff: " + 
                        Math.abs(existingBid.getPrice() - ourBidPrice) + ")");
                }
            } else {
                // No valid bid source - cancel existing bid if any
                MarketOrder existingBid = existingQuote.getBidOrder();
                if (existingBid != null && !existingBid.isDead()) {
                    LOGGER.info("Cancelling bid for bond " + bondId + " - no valid bid source");
                    cancelOrder(existingBid, bondId);
                }
            }
            
            // Update ask if needed
            if (validAskSource && ourAskPrice > 0) {
                MarketOrder existingAsk = existingQuote.getAskOrder();
                boolean needNewAsk = false;
                String askUpdateReason = "";
                
                if (existingAsk == null) {
                    needNewAsk = true;
                    askUpdateReason = "No existing ask order";
                } else if (existingAsk.isDead() || existingAsk.isExpired()) {
                    needNewAsk = true;
                    askUpdateReason = "Existing ask is dead/expired";
                } else if (Math.abs(existingAsk.getPrice() - ourAskPrice) >= MIN_PRICE_CHANGE_THRESHOLD) {
                    needNewAsk = true;
                    askUpdateReason = "Price change exceeds threshold: old=" + existingAsk.getPrice() + 
                                    ", new=" + ourAskPrice + ", diff=" + Math.abs(existingAsk.getPrice() - ourAskPrice);
                } else {
                    // Check if the reference source has changed
                    if (!referenceAskSource.equals(existingQuote.getAskReferenceSource())) {
                        needNewAsk = true;
                        askUpdateReason = "Reference source changed: old=" + existingQuote.getAskReferenceSource() + 
                                        ", new=" + referenceAskSource;
                    }
                }
                
                if (needNewAsk) {
                    LOGGER.info("Updating ask for bond " + bondId + ": " + askUpdateReason);

                    // Cancel existing ask if present
                    if (existingAsk != null && !existingAsk.isDead()) {
                        cancelOrder(existingAsk, bondId);
                    }
                    
                    // Place new ask
                    placeOrder(bondId, nativeInstrument, "Sell", config.getMinSize(), ourAskPrice, referenceAskSource);
                } else {
                    LOGGER.debug("Keeping existing ask for bond " + bondId + " (price diff: " + 
                        Math.abs(existingAsk.getPrice() - ourAskPrice) + ")");
                }
            } else {
                // No valid ask source - cancel existing ask if any
                MarketOrder existingAsk = existingQuote.getAskOrder();
                if (existingAsk != null && !existingAsk.isDead()) {
                    LOGGER.info("Cancelling ask for bond " + bondId + " - no valid ask source");
                    cancelOrder(existingAsk, bondId);
                }
            }
            
        } catch (Exception e) {
            LOGGER.error("Error updating symmetric quotes for bond " + bondId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Enhanced order placement with better error handling and validation
     */
    private void placeOrder(String cusip, String nativeInstrument, String verb, 
                        double size, double price, String referenceSource) {
        try {
            // Additional validation before placing order
            if (size <= 0) {
                LOGGER.warn("Invalid order size for " + cusip + ": " + size);
                return;
            }

            if (price <= 0 || price < config.getMinPrice() || price > config.getMaxPrice()) {
                LOGGER.warn("Invalid order price for " + cusip + ": " + price);
                return;
            }

            // Check if we're within trading hours for default markets
            if ("DEFAULT".equals(referenceSource) && !config.isDefaultMarketMakingAllowed()) {
                LOGGER.info("Skipping default market order outside allowed hours for " + cusip);
                return;
            }

            LOGGER.info("Placing " + verb + " order on " + config.getMarketSource() + ": " + 
                nativeInstrument + " (" + cusip + "), size=" + size + ", price=" + price + 
                ", reference=" + referenceSource);
            
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
                // Store the order in our active quotes map
                ActiveQuote quote = activeQuotes.computeIfAbsent(cusip, 
                    k -> new ActiveQuote(cusip));
                
                if ("Buy".equals(verb)) {
                    quote.setBidOrder(order, referenceSource, price);
                    LOGGER.info("Bid order placed successfully: reqId=" + order.getMyReqId() + 
                        ", orderId=" + order.getOrderId() + ", " + verb + " " + nativeInstrument);
                } else {
                    quote.setAskOrder(order, referenceSource, price);
                    LOGGER.info("Ask order placed successfully: reqId=" + order.getMyReqId() + 
                        ", orderId=" + order.getOrderId() + ", " + verb + " " + nativeInstrument);
                }
            } else {
                LOGGER.warn(
                    "Failed to place order on " + config.getMarketSource() + 
                    ": " + nativeInstrument + " (" + cusip + ")");
            }
        } catch (Exception e) {
            LOGGER.error("Error placing order for " + cusip + ": " + e.getMessage(), e);
        }
    }

    /**
     * Removes an order from tracking based on the request ID
     * 
     * @param reqId The request ID to remove
     */
    @Override
    public void removeOrder(int reqId) {
        LOGGER.info("MarketMaker.removeOrder() - Removing order with reqId: {}", reqId);
        
        // Find the quote containing this order
        for (Map.Entry<String, ActiveQuote> entry : activeQuotes.entrySet()) {
            ActiveQuote quote = entry.getValue();
            
            // Check if this quote has the order
            MarketOrder bidOrder = quote.getBidOrder();
            MarketOrder askOrder = quote.getAskOrder();
            
            if (bidOrder != null && bidOrder.getMyReqId() == reqId) {
                LOGGER.info("Found and removing bid order with reqId {} for {}", reqId, entry.getKey());
                quote.setBidOrder(null, null, 0.0);
                return;
            }
            
            if (askOrder != null && askOrder.getMyReqId() == reqId) {
                LOGGER.info("Found and removing ask order with reqId {} for {}", reqId, entry.getKey());
                quote.setAskOrder(null, null, 0.0);
                return;
            }
        }
        
        LOGGER.warn("No order found with reqId {}", reqId);
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
}