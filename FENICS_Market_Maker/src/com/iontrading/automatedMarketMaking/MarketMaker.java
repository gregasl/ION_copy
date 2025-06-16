package com.iontrading.automatedMarketMaking;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iontrading.OSP.packets.i;
import com.iontrading.janino.p.t;
import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvPattern;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import java.io.ObjectInputFilter;

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

    private final Map<String, Boolean> termCodeActiveStatus = new ConcurrentHashMap<>();

    // Store our active orders by instrument ID
    private final Map<String, ActiveQuote> activeQuotes = new ConcurrentHashMap<>();

    // cache venue minimums for quick access
    private final Map<String, Double> venueMinimumCache = new ConcurrentHashMap<>();

    // Store the trader ID to use for FENICS
    private final String fenicsTrader;
    
    // Executor for periodic tasks
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    // Track whether market making is enabled
    private volatile boolean enabled = true;

    private final Object subscriptionLock = new Object();
    private boolean isBondStaticSubscribed = false;
    private boolean isFirmPositionSubscribed = false;
    private boolean isSdsInformationSubscribed = false;
    private boolean isMfaInformationSubscribed = false;

    // Configuration for the market maker
    private MarketMakerConfig config;
    
    private final BondEligibilityListener bondEligibilityListener;
    private final Set<String> trackedInstruments = ConcurrentHashMap.newKeySet();

    private static final String BOND_STATIC_PATTERN = "USD.CM_BOND.VMO_REPO_US.";
    private static final String[] BOND_STATIC_FIELDS = {
        "Id", "DateMaturity"
    };
    
    private static final String FIRM_POSITION_PATTERN = "USD.IU_POSITION.VMO_REPO_US.";
    private static final String[] FIRM_POSITION_FIELDS = {
        "Id", "VirtualId", "Isin", "CalcNetExtPos", "StartCode", "StartDate"
    };

    private static final String SDS_INFORMATION_PATTERN = "ALL.POSITION_US.SDS.";
    private static final String[] SDS_INFORMATION_FIELDS = {
        "Id", "Code", "DateStart", "SOMA"
    };

    private static final String MFA_INFORMATION_PATTERN = "ALL.STATISTICS.MFA.";
    private static final String[] MFA_INFORMATION_FIELDS = {
        "Id", "DateMaturity", "RateAvg", "SpreadGCAvg", "Term", "VirtualInstrumentId", "VolumeTotal"
    };

    private volatile boolean shutdownHandled = false;

    // Use DepthListener
    private DepthListener depthListener = null;
    
    private final Map<String, Integer> orderIdToReqIdMap = new HashMap<>();

    private final Map<String, String> instrumentToBondCache = new ConcurrentHashMap<>();

    private final Map<String, Long> lastOrderUpdateTime = new ConcurrentHashMap<>();
    private static final long MIN_UPDATE_INTERVAL_MS = 2000; // 500 milliseconds minimum between updates

    public String getApplicationId() {
        // Return a unique identifier for this market maker instance
        return "MarketMaker";
    }
    /**
     * Creates a new MarketMaker instance with default configuration.
     * 
     * @param orderManager The parent OrderManagement instance
     */

    public MarketMaker(OrderManagement orderManager) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("MarketMaker.constructor", "Creating with default config, orderManager=" + orderManager);
        }
        try {
            // Initialize with default configuration
            this.orderManager = orderManager;
            this.config = new MarketMakerConfig.Builder()
                .setAutoEnabled(true)
                .setQuoteUpdateIntervalSeconds(15)
                .setMarketSource("FENICS_USREPO")
                .setTargetVenues("BTEC_REPO_US", "DEALERWEB_REPO")
                .setRegMarketHours(LocalTime.of(8, 30), LocalTime.of(17, 0))
                .setCashMarketHours(LocalTime.of(7, 0), LocalTime.of(11, 55))
                .build();
            initializeMarketSchedule();
            this.bondEligibilityListener = new BondEligibilityListener();

            this.fenicsTrader = orderManager.getTraderForVenue(config.getMarketSource());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Using trader ID for {}: {}", config.getMarketSource(), fenicsTrader);
            }

            // Get DepthListener from OrderManagement
            this.depthListener = OrderManagement.getDepthListener();
            if (depthListener == null) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("CRITICAL ERROR: DepthListener is null - market data updates won't be processed properly!");
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("DepthListener obtained successfully from OrderManagement");
                }
            }

            // Register for eligibility change notifications
            this.bondEligibilityListener.addEligibilityChangeListener(new EligibilityChangeListener() {
                public void onEligibilityChange(String Id, boolean isEligible, Map<String, Object> bondData) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("EligibilityChangeListener.onEligibilityChange: Id={}, isEligible={}", Id, isEligible);
                    }
                    handleEligibilityChange(Id, isEligible, bondData);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("EligibilityChangeListener.onEligibilityChange: Eligibility change handled");
                    }
                }
            });

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("MarketMaker initialized with bond eligibility integration");
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("MarketMaker.constructor: Successfully created MarketMaker instance");
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("MarketMaker.constructor: Error creating MarketMaker", e);
            }
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
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("MarketMaker.constructor: Creating with custom config, orderManager={}, config={}", orderManager, config);
        }
        try {
            this.orderManager = orderManager;
            this.config = config;
            
            initializeMarketSchedule();
            this.bondEligibilityListener = new BondEligibilityListener();
            
            // Subscribe to MKV data streams with detailed logging
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Subscribing to MKV data streams...");
            }
            subscribeToBondStaticData();
            subscribeToFirmPositionData();
            subscribeToSdsInformationData();
            subscribeToMfaInformationData();

            this.fenicsTrader = orderManager.getTraderForVenue(config.getMarketSource());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Using trader ID for {}: {}", config.getMarketSource(), fenicsTrader);
            }

            // Get DepthListener from OrderManagement
            this.depthListener = OrderManagement.getDepthListener();
            if (depthListener == null) {
                if (LOGGER.isErrorEnabled()) {
                    if (LOGGER.isErrorEnabled()) {
                        LOGGER.error("CRITICAL ERROR: DepthListener is null - market data updates won't be processed properly!");
                    }
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("DepthListener obtained successfully from OrderManagement");
                }
            }

            // Register for eligibility change notifications
            this.bondEligibilityListener.addEligibilityChangeListener(new EligibilityChangeListener() {
                public void onEligibilityChange(String Id, boolean isEligible, Map<String, Object> bondData) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("EligibilityChangeListener.onEligibilityChange: Id={}, isEligible={}", Id, isEligible);
                    }
                    handleEligibilityChange(Id, isEligible, bondData);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("EligibilityChangeListener.onEligibilityChange: Eligibility change handled");
                    }
                }
            });

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("MarketMaker initialized with bond eligibility integration");
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("MarketMaker.constructor: Successfully created MarketMaker instance with custom config");
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("MarketMaker.constructor: Error creating MarketMaker with custom config", e);
            }
            throw e; // Re-throw to maintain original behavior
        }
    }

    /**
     * Initialize market making schedule based on configured market hours
     */
    private void initializeMarketSchedule() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Initializing market schedule for term codes");
        }

        // Initialize initial status for each term code based on current time
        updateTermCodeActiveStatus();
        
        // Log current market status for startup diagnostics
        boolean cashActive = termCodeActiveStatus.getOrDefault("C", false);
        boolean regActive = termCodeActiveStatus.getOrDefault("REG", false);
        
        LOGGER.info("Current market status at startup: Cash={}, REG={}", 
            cashActive ? "OPEN" : "CLOSED", regActive ? "OPEN" : "CLOSED");

        // Calculate delays for periodic market making based on actual market hours
        long cashMarketMakingDelay = calculateMarketMakingDelay("C");
        long regMarketMakingDelay = calculateMarketMakingDelay("REG");    

        LOGGER.info("Market making delays: Cash starts in {} seconds, REG starts in {} seconds", 
            cashMarketMakingDelay, regMarketMakingDelay);

        // Schedule periodic market making with market-hours-aware delays
        scheduler.scheduleAtFixedRate(
            () -> {
                // Safety check - only make markets if term code is active
                if (termCodeActiveStatus.getOrDefault("C", false) && enabled) {
                    makeMarketsForEligibleBonds("C");
                }
            }, 
            cashMarketMakingDelay, // Wait until Cash market opens (or start immediately if already open)
            config.getQuoteUpdateIntervalSeconds(),
            TimeUnit.SECONDS
        );

        scheduler.scheduleAtFixedRate(
            () -> {
                // Safety check - only make markets if term code is active
                if (termCodeActiveStatus.getOrDefault("REG", false) && enabled) {
                    makeMarketsForEligibleBonds("REG");
                }
            }, 
            regMarketMakingDelay, // Wait until REG market opens (or start immediately if already open)
            config.getQuoteUpdateIntervalSeconds(),
            TimeUnit.SECONDS
        );
        
        // Schedule daily market open/close events (these handle the termCodeActiveStatus flags)
        scheduler.scheduleAtFixedRate(
            () -> startTermCodeMarketMaking("C"), 
            getSecondsUntilTime(getCashMarketOpenTime()),
            24 * 60 * 60, // Every 24 hours
            TimeUnit.SECONDS
        );
        
        scheduler.scheduleAtFixedRate(
            () -> stopTermCodeMarketMaking("C"), 
            getSecondsUntilTime(getCashMarketCloseTime()),
            24 * 60 * 60, // Every 24 hours
            TimeUnit.SECONDS
        );
        
        scheduler.scheduleAtFixedRate(
            () -> startTermCodeMarketMaking("REG"), 
            getSecondsUntilTime(getRegMarketOpenTime()),
            24 * 60 * 60, // Every 24 hours
            TimeUnit.SECONDS
        );
        
        scheduler.scheduleAtFixedRate(
            () -> stopTermCodeMarketMaking("REG"), 
            getSecondsUntilTime(getRegMarketCloseTime()),
            24 * 60 * 60, // Every 24 hours
            TimeUnit.SECONDS
        );
        
        // Check market status every minute for edge cases
        scheduler.scheduleAtFixedRate(
            this::updateTermCodeActiveStatus,
            60, // Start after 1 minute
            60, // Check every minute
            TimeUnit.SECONDS
        );
        
        // Add the diagnostic check task
        scheduler.scheduleAtFixedRate(
            this::logDiagnosticStatistics, 
            30, // Initial delay (seconds) 
            60, // Run every minute
            TimeUnit.SECONDS
        );
        
        LOGGER.info("Market schedule initialized - C: {}-{}, REG: {}-{}", 
            getCashMarketOpenTime(), getCashMarketCloseTime(),
            getRegMarketOpenTime(), getRegMarketCloseTime());
    }

    /**
     * Calculate delay until market making should start for a specific term code
     * Returns immediate start (10 seconds) if market is already open, 
     * or seconds until market opens if currently closed
     */
    private long calculateMarketMakingDelay(String termCode) {
        LocalTime now = LocalTime.now();
        LocalTime marketOpen;
        
        if ("C".equals(termCode)) {
            marketOpen = getCashMarketOpenTime();
        } else if ("REG".equals(termCode)) {
            marketOpen = getRegMarketOpenTime();
        } else {
            LOGGER.warn("Unknown term code: {}, defaulting to 60 second delay", termCode);
            return 60; // Default 1 minute delay for unknown term codes
        }
        
        // If we're already past market open time today, start in 10 seconds
        if (now.isAfter(marketOpen)) {
            LOGGER.info("{} market is already open, starting market making in 10 seconds", termCode);
            return 10;
        }
        
        // Calculate seconds until market opens
        long secondsUntilOpen = Duration.between(now, marketOpen).getSeconds();
        
        LOGGER.info("{} market opens at {}, starting market making in {} seconds ({} minutes)", 
            termCode, marketOpen, secondsUntilOpen, secondsUntilOpen / 60);
        
        return secondsUntilOpen;
    }

    /**
     * Calculate seconds until a specified time today or tomorrow
     */
    private long getSecondsUntilTime(LocalTime targetTime) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime target = LocalDateTime.of(now.toLocalDate(), targetTime);
        
        // If target time is earlier today, schedule for tomorrow
        if (now.toLocalTime().isAfter(targetTime)) {
            target = target.plusDays(1);
        }
        
        return java.time.Duration.between(now, target).getSeconds();
    }

    private LocalTime getCashMarketOpenTime() {
        // Convert from pre-computed minutes back to LocalTime
        int minutes = config.getCashMarketOpenMinutes();
        return LocalTime.of(minutes / 60, minutes % 60);
    }

    private LocalTime getCashMarketCloseTime() {
        int minutes = config.getCashMarketCloseMinutes();
        return LocalTime.of(minutes / 60, minutes % 60);
    }

    private LocalTime getRegMarketOpenTime() {
        int minutes = config.getRegMarketOpenMinutes();
        return LocalTime.of(minutes / 60, minutes % 60);
    }

    private LocalTime getRegMarketCloseTime() {
        int minutes = config.getRegMarketCloseMinutes();
        return LocalTime.of(minutes / 60, minutes % 60);
    }

    /**
     * Start market making for a specific term code
     */
    private void startTermCodeMarketMaking(String termCode) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting market making for term code: {}", termCode);
        }

        boolean wasActive = termCodeActiveStatus.getOrDefault(termCode, false);
        termCodeActiveStatus.put(termCode, true);
        
        if (!wasActive && enabled) {
            // Only create markets if global enabled flag is true
            makeMarketsForEligibleBonds(termCode);        }
    }

    /**
     * Stop market making for a specific term code
     */
    private void stopTermCodeMarketMaking(String termCode) {
        LOGGER.info("Stopping market making for term code: {}", termCode);
        
        termCodeActiveStatus.put(termCode, false);
        
        // Cancel all orders for this term code
        cancelAllOrders(termCode);
    }

    /**
     * Update active status for all term codes based on current time
     */
    private void updateTermCodeActiveStatus() {
        // Use the pre-computed market status from config for better performance
        config.marketHours();
        boolean cashActive = config.isDuringCashHours();
        boolean regActive = config.isDuringRegHours();

        boolean cashWasActive = termCodeActiveStatus.getOrDefault("C", false);
        boolean regWasActive = termCodeActiveStatus.getOrDefault("REG", false);
        
        termCodeActiveStatus.put("C", cashActive);
        termCodeActiveStatus.put("REG", regActive);
        
        // Handle state changes
        if (cashActive != cashWasActive) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Cash market status changed to: {}", cashActive ? "ACTIVE" : "INACTIVE");
            }
            if (!cashActive) {
                cancelAllOrders("C");
            }
        }
        
        // Handle market open - trigger one-time market making (scheduler will continue)
        if (!cashWasActive && cashActive && enabled) {
            LOGGER.info("Cash market opened - triggering initial C market making");
            scheduler.schedule(() -> {
                try {
                    makeMarketsForEligibleBonds("C");
                    LOGGER.info("Initial C market making completed - scheduler will continue regular updates");
                } catch (Exception e) {
                    LOGGER.error("Error in initial C market making: {}", e.getMessage(), e);
                }
            }, 5, TimeUnit.SECONDS);
        }

        if (regActive != regWasActive) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("REG market status changed to: {}", regActive ? "ACTIVE" : "INACTIVE");
            }
            if (!regActive) {
                cancelAllOrders("REG");
            }
        }
    }

    // Add a new method to initialize MKV subscriptions after MKV is started
    public void initializeMkvSubscriptions() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("MarketMaker.initializeMkvSubscriptions: Initializing MKV subscriptions");
        }

        try {
            // Get the publish manager
            final MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            
            // Try immediate subscription first
            boolean bondSubscribed = subscribeToBondStaticData();
            boolean firmSubscribed = subscribeToFirmPositionData();
            boolean sdsSubscribed = subscribeToSdsInformationData();
            boolean mfaSubscribed = subscribeToMfaInformationData();

            // If any subscription failed, set up a listener for pattern discovery
            if (!bondSubscribed || !firmSubscribed || !sdsSubscribed || !mfaSubscribed) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Some patterns not available yet, setting up a publish listener");
                }

                // Create a single shared listener for all patterns
                MkvPublishListener patternListener = new MkvPublishListener() {
                    @Override
                    public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
                        // Only proceed if this is a pattern publication
                        if (pub_unpub && mkvObject.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                            String name = mkvObject.getName();
                            
                            // Try to subscribe to the pattern if it's one we're looking for
                            if (!isBondStaticSubscribed && BOND_STATIC_PATTERN.equals(name)) {
                                trySubscribeToPattern(BOND_STATIC_PATTERN, BOND_STATIC_FIELDS,
                                    bondEligibilityListener, mkvObject, pm, this);
                            }
                            else if (!isFirmPositionSubscribed && FIRM_POSITION_PATTERN.equals(name)) {
                                trySubscribeToPattern(FIRM_POSITION_PATTERN, FIRM_POSITION_FIELDS,
                                    bondEligibilityListener, mkvObject, pm, this);
                            }
                            else if (!isSdsInformationSubscribed && SDS_INFORMATION_PATTERN.equals(name)) {
                                trySubscribeToPattern(SDS_INFORMATION_PATTERN, SDS_INFORMATION_FIELDS,
                                    bondEligibilityListener, mkvObject, pm, this);
                            }
                            else if (!isMfaInformationSubscribed && MFA_INFORMATION_PATTERN.equals(name)) {
                                trySubscribeToPattern(MFA_INFORMATION_PATTERN, MFA_INFORMATION_FIELDS,
                                    bondEligibilityListener, mkvObject, pm, this);
                            }
                        }
                    }

                    @Override
                    public void onPublishIdle(String component, boolean start) {
                        // At idle time, check for patterns again
                        if (!isBondStaticSubscribed) {
                            MkvObject obj = pm.getMkvObject(BOND_STATIC_PATTERN);
                            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                                trySubscribeToPattern(BOND_STATIC_PATTERN, BOND_STATIC_FIELDS,
                                    bondEligibilityListener, obj, pm, this);
                            }
                        }
                        
                        if (!isFirmPositionSubscribed) {
                            MkvObject obj = pm.getMkvObject(FIRM_POSITION_PATTERN);
                            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                                trySubscribeToPattern(FIRM_POSITION_PATTERN, FIRM_POSITION_FIELDS,
                                    bondEligibilityListener, obj, pm, this);
                            }
                        }
                        
                        if (!isSdsInformationSubscribed) {
                            MkvObject obj = pm.getMkvObject(SDS_INFORMATION_PATTERN);
                            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                                trySubscribeToPattern(SDS_INFORMATION_PATTERN, SDS_INFORMATION_FIELDS,
                                    bondEligibilityListener, obj, pm, this);
                            }
                        }
                        
                        if (!isMfaInformationSubscribed) {
                            MkvObject obj = pm.getMkvObject(MFA_INFORMATION_PATTERN);
                            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                                trySubscribeToPattern(MFA_INFORMATION_PATTERN, MFA_INFORMATION_FIELDS,
                                    bondEligibilityListener, obj, pm, this);
                            }
                        }

                    }
                    
                    @Override
                    public void onSubscribe(MkvObject mkvObject) {
                        // Not needed
                    }
                };
                
                // Register the listener
                pm.addPublishListener(patternListener);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Pattern discovery listener registered");
                }

                // Also set up a timeout to eventually give up
                Thread timeoutThread = new Thread(() -> {
                    try {
                        // Wait for up to 3 minutes
                        Thread.sleep(3 * 60 * 1000);
                        
                        // Check if we're still waiting for patterns
                        boolean stillWaiting = false;
                        synchronized (subscriptionLock) {
                            stillWaiting = !isBondStaticSubscribed || 
                                        !isFirmPositionSubscribed || 
                                        !isSdsInformationSubscribed || 
                                        !isMfaInformationSubscribed;
                            
                            if (stillWaiting) {
                                if (LOGGER.isWarnEnabled()) {
                                    LOGGER.warn("Pattern subscription timeout reached. Status: Bond={}, Firm={}, SDS={}, MFA={}",
                                        isBondStaticSubscribed, isFirmPositionSubscribed, isSdsInformationSubscribed, isMfaInformationSubscribed);
                                }

                                // Remove the listener to avoid leaks
                                pm.removePublishListener(patternListener);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        if (LOGGER.isErrorEnabled()) {
                            LOGGER.error("Error in pattern subscription timeout thread", e);
                        }
                    }
                });

                timeoutThread.setDaemon(true);
                timeoutThread.setName("PatternSubscriptionTimeout");
                timeoutThread.start();
            }
            
            // Update DepthListener reference
            this.depthListener = OrderManagement.getDepthListener();
            if (depthListener == null) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("CRITICAL ERROR: DepthListener is still null after MKV initialization!");
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("DepthListener obtained successfully after MKV initialization");
                }
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("MKV subscriptions initialized - will continue discovery asynchronously");
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error setting up MKV subscriptions", e);
            }
        }
    }

    /**
     * Subscribe to bond static data
     */
    private boolean subscribeToBondStaticData() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("subscribeToBondStaticData: Subscribing to bond static data");
        }

        try {
            // Get the publish manager
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Got publish manager: {}", pm);
            }
    
            // Look up the pattern object
            MkvObject obj = pm.getMkvObject(BOND_STATIC_PATTERN);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Looking up pattern: {}, result: {}", BOND_STATIC_PATTERN, obj);
            }
            
            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                synchronized (subscriptionLock) {
                    // Check again inside synchronized block
                    if (isBondStaticSubscribed) {
                        return true;
                    }

                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Found bond static pattern, subscribing: {}", BOND_STATIC_PATTERN);
                    }
                    ((MkvPattern) obj).subscribe(BOND_STATIC_FIELDS, bondEligibilityListener);

                    isBondStaticSubscribed = true;
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Successfully subscribed to bond static data: {} with {} fields",
                            BOND_STATIC_PATTERN, BOND_STATIC_FIELDS.length);
                    }
                    return true;
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Bond static pattern not found: {}. MKV object: {}", BOND_STATIC_PATTERN, obj);
                }
                return false;
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("subscribeToBondStaticData: Failed: {}", e.getMessage(), e);
            }
            return false;
        }
    }
    
    /**
     * Subscribe to firm position data
     */
    private boolean subscribeToFirmPositionData() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("subscribeToFirmPositionData: Subscribing to firm position data");
        }

        try {
            // Get the publish manager to access patterns
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Got publish manager: {}", pm);
            }

            // Look up the pattern object
            MkvObject obj = pm.getMkvObject(FIRM_POSITION_PATTERN);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Looking up pattern: {}, result: {}", FIRM_POSITION_PATTERN, obj);
            }

            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                synchronized (subscriptionLock) {
                    // Check again inside synchronized block
                    if (isFirmPositionSubscribed) {
                        return true;
                    }

                    // Subscribe within a synchronized block
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Found firm position pattern, subscribing: {}", (Object) FIRM_POSITION_FIELDS);
                    }
                    ((MkvPattern) obj).subscribe(FIRM_POSITION_FIELDS, bondEligibilityListener);

                    // Mark that we've successfully subscribed
                    isFirmPositionSubscribed = true;
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Successfully subscribed to firm position data: {} with {} fields",
                            FIRM_POSITION_PATTERN, FIRM_POSITION_FIELDS.length);
                    }
                    return true;
                } 
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Firm position pattern not found: {}. MKV object: {}", FIRM_POSITION_PATTERN, obj);
                }
                return false;
            }
        } catch (Exception e) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("subscribeToFirmPositionData: Error subscribing to firm position data", e);
            }
            return false;
        }
    }

    /**
     * Subscribe to SDS information data
     */
    private boolean subscribeToSdsInformationData() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("subscribeToSdsInformationData: Subscribing to SDS information data");
        }

        try {
            // Get the publish manager to access patterns
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Got publish manager: {}", pm);
            }

            // Look up the pattern object
            MkvObject obj = pm.getMkvObject(SDS_INFORMATION_PATTERN);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Looking up pattern: {}, result: {}", SDS_INFORMATION_PATTERN, obj);
            }

            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                synchronized (this.subscriptionLock) {
                    // Check again inside synchronized block
                    if (isSdsInformationSubscribed) {
                        return true;
                    
                    }
                    // Subscribe within a synchronized block
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Found SDS information pattern, subscribing: {}", (Object) SDS_INFORMATION_FIELDS);
                    }
                    ((MkvPattern) obj).subscribe(SDS_INFORMATION_FIELDS, bondEligibilityListener);

                    // Mark that we've successfully subscribed
                    isSdsInformationSubscribed = true;
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Subscribed to SDS information data: {} with {} fields",
                            SDS_INFORMATION_PATTERN, SDS_INFORMATION_FIELDS.length);
                    }
                    return true;
                }
            } else {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("subscribeToSdsInformationData: Failed: SDS information pattern not found");
                }
                return false;
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("subscribeToSdsInformationData: Error subscribing to SDS information data", e);
            }
            return false;
        }
    }


    /**
     * Subscribe to MFA information data
     */
    private boolean subscribeToMfaInformationData() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("subscribeToMfaInformationData: Subscribing to MFA information data");
        }

        try {
            // Get the publish manager to access patterns
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Got publish manager: {}", pm);
            }

            // Look up the pattern object
            MkvObject obj = pm.getMkvObject(MFA_INFORMATION_PATTERN);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Looking up pattern: {}, result: {}", MFA_INFORMATION_PATTERN, obj);
            }

            if (obj != null && obj.getMkvObjectType().equals(MkvObjectType.PATTERN)) {
                synchronized (this.subscriptionLock) {
                    // Check again inside synchronized block
                    if (isMfaInformationSubscribed) {
                        return true;
                    
                    }
                    // Subscribe within a synchronized block
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Found MFA information pattern, subscribing: {}", (Object) MFA_INFORMATION_FIELDS);
                    }
                    ((MkvPattern) obj).subscribe(MFA_INFORMATION_FIELDS, bondEligibilityListener);

                    // Mark that we've successfully subscribed
                    isMfaInformationSubscribed = true;

                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Subscribed to MFA information data: {} with {} fields",
                            MFA_INFORMATION_PATTERN, MFA_INFORMATION_FIELDS.length);
                    }
                    return true;
                }
            } else {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("subscribeToMfaInformationData: Failed: MFA information pattern not found");
                }
                return false;
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("subscribeToMfaInformationData: Error subscribing to MFA information data", e);
            }
            return false;
        }
    }

    /**
     * Helper method to safely subscribe to a pattern when it becomes available
     */
    private boolean trySubscribeToPattern(String patternName, String[] fields, MkvRecordListener listener, 
                                        MkvObject mkvObject, MkvPublishManager pm, MkvPublishListener publishListener) {
        synchronized (subscriptionLock) {
            // Determine which subscription flag to check
            boolean isAlreadySubscribed = false;
            if (BOND_STATIC_PATTERN.equals(patternName)) {
                isAlreadySubscribed = isBondStaticSubscribed;
            } else if (FIRM_POSITION_PATTERN.equals(patternName)) {
                isAlreadySubscribed = isFirmPositionSubscribed;
            } else if (SDS_INFORMATION_PATTERN.equals(patternName)) {
                isAlreadySubscribed = isSdsInformationSubscribed;
            } else if (MFA_INFORMATION_PATTERN.equals(patternName)) {
                isAlreadySubscribed = isMfaInformationSubscribed;
            }

            // Check if already subscribed
            if (isAlreadySubscribed) {
                return true;
            }
            
            try {
                LOGGER.info("Pattern found, subscribing to: {}", patternName);

                ((MkvPattern) mkvObject).subscribe(fields, listener);
                
                // Update subscription flag
                if (BOND_STATIC_PATTERN.equals(patternName)) {
                    isBondStaticSubscribed = true;
                } else if (FIRM_POSITION_PATTERN.equals(patternName)) {
                    isFirmPositionSubscribed = true;
                } else if (SDS_INFORMATION_PATTERN.equals(patternName)) {
                    isSdsInformationSubscribed = true;
                } else if (MFA_INFORMATION_PATTERN.equals(patternName)) {
                    isMfaInformationSubscribed = true;
                }

               if (LOGGER.isInfoEnabled()) {
                   LOGGER.info("Successfully subscribed to pattern: {}", patternName);
               }

                // Remove the listener if we're done with all subscriptions
                if (isBondStaticSubscribed && isFirmPositionSubscribed && isSdsInformationSubscribed && isMfaInformationSubscribed) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("All patterns subscribed, removing publish listener");
                    }
                    pm.removePublishListener(publishListener);
                }

                return true;
            } catch (Exception e) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Error subscribing to pattern {}: {}", patternName, e.getMessage(), e);
                }
                return false;
            }
        }
    }

    public Map<String, ActiveQuote> getActiveQuotes() {
        return Collections.unmodifiableMap(activeQuotes); // Return unmodifiable to prevent external modification
    }

    /**
     * Cancel all orders for instruments with a specific term code
     */
    private void cancelAllOrders(String termCode) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Cancelling all orders for term code: {}", termCode);
        }

        List<String> bondsToCancel = new ArrayList<>();

        // Find all bonds with that term code that have active orders
        for (String Id : trackedInstruments) {
            if (activeQuotes.containsKey(Id) && Id.endsWith(termCode + "_Fixed")) {
                bondsToCancel.add(Id);
            }
        }
        
        // Cancel orders for each bond
        int cancelCount = 0;
        for (String Id : bondsToCancel) {
            try {
                cancelOrdersForInstrument(Id);
                cancelCount++;
            } catch (Exception e) {
                LOGGER.error("Error cancelling orders for {}: {}", Id, e.getMessage(), e);
            }
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Cancelled orders for {} bonds with term code: {}", cancelCount, termCode);
        }
    }
    /**
     * Periodically check our orders to ensure they're still valid.
     * Removes stale orders from our tracking.
     */
    private void monitorOrders() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("monitorOrders: Starting order monitoring");
        }

        if (!enabled) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("monitorOrders: Skipping - market maker not enabled");
            }
            return;
        }
        
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Monitoring market maker orders - currently tracking " + 
                    activeQuotes.size() + " instruments");
            }

            int expiredCount = 0;
            int deadCount = 0;
            
            for (ActiveQuote quote : activeQuotes.values()) {
                MarketOrder bidOrder = quote.getBidOrder();
                MarketOrder askOrder = quote.getAskOrder();
                
                // Check bid order
                if (bidOrder != null) {
                    if (bidOrder.isExpired()) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Bid order expired for {}: {}", quote.getId(), bidOrder.getOrderId());
                        }
                        expiredCount++;
                    } else if (bidOrder.isDead()) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Bid order is dead for {}: {}", quote.getId(), bidOrder.getOrderId());
                        }
                        deadCount++;
                    }
                }
                
                // Check ask order
                if (askOrder != null) {
                    if (askOrder.isExpired()) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Ask order expired for {}: {}", quote.getId(), askOrder.getOrderId());
                        }
                        expiredCount++;
                    } else if (askOrder.isDead()) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Ask order is dead for {}: {}", quote.getId(), askOrder.getOrderId());
                        }
                        deadCount++;
                    }
                }
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("monitorOrders: Monitoring complete - found {} expired orders, {} dead orders", expiredCount, deadCount);
            }
        } catch (Exception e) {
            LOGGER.error("monitorOrders: Error monitoring orders", e);
        }
    }
            
    @Override
    public void orderDead(MarketOrder order) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("orderDead: Order dead notification: reqId={}, orderId={}",
                order.getMyReqId(), order.getOrderId());
        }

        try {
            // Find the active quote that contains this order
            for (Map.Entry<String, ActiveQuote> entry : activeQuotes.entrySet()) {
                String id = entry.getKey();
                ActiveQuote quote = entry.getValue();
                
                MarketOrder bidOrder = quote.getBidOrder();
                MarketOrder askOrder = quote.getAskOrder();
                
                if (bidOrder != null && bidOrder.getMyReqId() == order.getMyReqId()) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Order dead was a bid for {}", id);
                    }
                    // Update all relevant properties
                    quote.setBidOrder(null, null, 0);
                    quote.setBidActive(false);
                    quote.setGcBasedBid(false); 
                    quote.setMarketBasedBid(false);
                    
                    LOGGER.info("Bid marked dead for {}, cleared all bid properties", id);
                    
                    break;
                }
                
                if (askOrder != null && askOrder.getMyReqId() == order.getMyReqId()) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Order dead was an ask for {}", id);
                    }
                    // Update all relevant properties
                    quote.setAskOrder(null, null, 0);
                    quote.setAskActive(false);
                    quote.setGcBasedAsk(false);
                    quote.setMarketBasedAsk(false);
                    
                    LOGGER.info("Ask marked dead for {}, cleared all ask properties", id);
                    
                    break;
                }
            }

            // Remove any quotes that are now empty (both bid and ask are null)
            activeQuotes.entrySet().removeIf(entry -> {
                ActiveQuote quote = entry.getValue();
                boolean isEmpty = quote.getBidOrder() == null && quote.getAskOrder() == null;
                if (isEmpty) {
                    LOGGER.info("Removing empty quote for {}", entry.getKey());
                }
                return isEmpty;
            });

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("orderDead: Order dead notification processed");
            }
        } catch (Exception e) {
            LOGGER.error("orderDead: Error processing orderDead", e);
        }
    }

    @Override
    public MarketOrder addOrder(String MarketSource, String TraderId, String instrId, 
                              String verb, double qty, double price, String type, String tif) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("addOrder: Starting order creation: Source={}, Trader={}, Instrument={}, Side={}, Qty={}, Price={}, Type={}, TIF={}", 
                MarketSource, TraderId, instrId, verb, qty, price, type, tif);
        }

        // Delegate to the main OrderManagement instance
        MarketOrder order = orderManager.addOrder(MarketSource, TraderId, instrId, verb, qty, price, type, tif);
        
        if (order != null) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("addOrder: Order created successfully: reqId={}", order.getMyReqId());
            }
        } else {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("addOrder: Failed to create order");
            }
        }

        return order;
    }
    
    @Override
    public void best(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {
        // Increment total update counter
        String id = best.getId();
        if (id == null) {
            emptyIdCounter.incrementAndGet();
            return;
        }

        if (!id.endsWith("C_Fixed") && !id.endsWith("REG_Fixed")) {
            nonOvernightCounter.incrementAndGet();
            return;
        }
        
        if (!enabled) return;
        // Batch update counters to reduce atomic operations
        int updates = marketUpdateCounter.incrementAndGet();
        instrumentUpdateCounters.computeIfAbsent(id, k -> new AtomicInteger(0)).incrementAndGet();
        
        boolean significantCashGcChange = false;
        boolean significantRegGcChange = false;

        // synchronized(gcBestLock) {
        //     // Check for Cash GC changes
        //     if (gcBestCash != null) {
        //         if (latestGcBestCash == null) {
        //             significantCashGcChange = true;
        //         } else {
                    
        //             GCLevelResult latestGcBestCash = gcBestCash.getGCLevel();
        //             GCLevelResult thisLatestGcBestCash = this.latestGcBestCash.getGCLevel();
        //             if (latestGcBestCash == null || thisLatestGcBestCash == null) {
        //                 significantCashGcChange = true; // If we can't compare, treat as significant change
        //             } else {
        //                 // Compare bid and ask prices for significant changes
        //             // Detect significant changes (e.g., > 1 bp)
        //             significantCashGcChange = 
        //                 (latestGcBestCash.getBidPrice() != null && 
        //                  thisLatestGcBestCash.getBidPrice() != null && 
        //                  Math.abs(latestGcBestCash.getBidPrice() - thisLatestGcBestCash.getBidPrice()) >= 0.02) || 
        //                 (latestGcBestCash.getAskPrice() != null && 
        //                  thisLatestGcBestCash.getAskPrice() != null && 
        //                  Math.abs(latestGcBestCash.getAskPrice() - thisLatestGcBestCash.getAskPrice()) >= 0.02);
        //             }
        //         this.latestGcBestCash = gcBestCash;
        //         this.latestCashGcRate = cash_gc;
        //         }
        //     }
            
            // Check for REG GC changes
        //     if (gcBestREG != null) {
        //         if (latestGcBestREG == null) {
        //             significantRegGcChange = true;
        //         } else {
        //             // Detect significant changes (e.g., > 0.5 bp)
        //             GCLevelResult latestGcBestREG = gcBestREG.getGCLevel();
        //             GCLevelResult thisLatestGcBestREG = this.latestGcBestREG.getGCLevel();
        //             if (latestGcBestREG == null || thisLatestGcBestREG == null) {
        //                 significantRegGcChange = true; // If we can't compare, treat as significant change
        //             } else {
        //                 // Compare bid and ask prices for significant changes
        //                 // Detect significant changes (e.g., > 1 bp)
        //             significantRegGcChange = 
        //                 (latestGcBestREG.getBidPrice() != null && 
        //                  thisLatestGcBestREG.getBidPrice() != null && 
        //                  Math.abs(latestGcBestREG.getBidPrice() - thisLatestGcBestREG.getBidPrice()) >= 0.02) || 
        //                 (latestGcBestREG.getAskPrice() != null && 
        //                  thisLatestGcBestREG.getAskPrice() != null && 
        //                  Math.abs(latestGcBestREG.getAskPrice() - thisLatestGcBestREG.getAskPrice()) >= 0.02);
        //         }
        //         this.latestGcBestREG = gcBestREG;
        //         this.latestRegGcRate = reg_gc;
        //         }
        //     }
        // }

        // Process based on term code - avoid string operations
        boolean isCash = id.endsWith("C_Fixed");
        processMarketUpdate(best, isCash ? GCBestManager.getInstance().getCashGCBest() : GCBestManager.getInstance().getRegGCBest());

        processedUpdateCounter.incrementAndGet();
        
        // Only log every 1000 updates to reduce I/O
        if (updates % 1000 == 0) {
            LOGGER.info("Processed {} market updates", updates);
        }
    }

    /**
     * Process market data updates using the unified pricing model.
     * Called by the DepthListener when market data changes.
     * 
     * @param best The updated best prices
     * @param gcBest The GC best prices
     */
    public void processMarketUpdate(Best best, GCBest gcBest) {
        long updateCount = marketUpdateCounter.get();
        
        if (updateCount % 1000 == 0) {
            LOGGER.debug("Processed {} market updates", updateCount);
        }
        
        try {
            String Id = best.getId(); // This should be the instrument ID from VMO.CM_INSTRUMENT

            if (Id == null || Id.isEmpty()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("processMarketUpdate: Empty best ID, skipping");
                }
                return;
            }
            // Determine term code from instrument ID
            String termCode;
            if (Id.endsWith("C_Fixed")) {
                termCode = "C";
            } else if (Id.endsWith("REG_Fixed")) {
                termCode = "REG";
            } else {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("processMarketUpdate: Unsupported instrument type: {}", Id);
                }
                nonOvernightCounter.incrementAndGet();
                return;
            }
            
            // Check if this instrument is eligible for market making
            boolean isEligible = bondEligibilityListener.isIdEligible(Id);
            
            // If not eligible, remove from tracked instruments and stop processing
            if (!isEligible) {
                if (trackedInstruments.remove(Id)) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("processMarketUpdate: Instrument {} is not eligible, removed from tracking", Id);
                    }
                    // Cancel any existing orders for this instrument
                    cancelOrdersForInstrument(Id);
                }
                return;
            }
            
            // If eligible but not tracked, add to tracking
            if (!trackedInstruments.contains(Id)) {
                trackedInstruments.add(Id);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("processMarketUpdate: Added eligible instrument {} to tracking", Id);
                }
            }
            
            // Don't proceed if market maker is disabled
            if (!enabled) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("processMarketUpdate: Market maker disabled, skipping update for {}", Id);
                }
                return;
            }
            
            // Check if market hours are enforced and if we're within market hours
            if (config.isEnforceMarketHours()) {
                boolean isMarketOpen = termCodeActiveStatus.getOrDefault(termCode, false);
                if (!isMarketOpen) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("processMarketUpdate: {} market closed, skipping update", termCode);
                    }
                    return;
                }
            }
            // Get existing quote status if any
            ActiveQuote existingQuote = activeQuotes.get(Id);
            LOGGER.debug("processMarketUpdate: Existing quote for {}: {}", Id, existingQuote);

            String previousBidSource = null;
            String previousAskSource = null;
            if (existingQuote != null) {
                previousBidSource = existingQuote.getBidReferenceSource();
                previousAskSource = existingQuote.getAskReferenceSource();
            }

            boolean bidFromTargetVenue = isTargetVenue(best.getBidSrc());
            boolean askFromTargetVenue = isTargetVenue(best.getAskSrc());
            
            // Skip processing for minimal market movements that don't affect us
            boolean targetSourceDisappeared = false;
            if (existingQuote != null) {
                // Check if we previously had market-based quotes that would be affected
                boolean hadMarketBasedBid = existingQuote.isMarketBasedBid() && 
                    isTargetVenue(previousBidSource);
                boolean hadMarketBasedAsk = existingQuote.isMarketBasedAsk() && 
                    isTargetVenue(previousAskSource);
                    
                // Detect if a target venue that was previously providing prices disappeared
                if ((hadMarketBasedBid && !bidFromTargetVenue) || 
                    (hadMarketBasedAsk && !askFromTargetVenue)) {
                    
                        targetSourceDisappeared = true;
                    }
                }

            // MODIFIED LOGIC: Only skip if we have no relevant data changes
            if (!bidFromTargetVenue && !askFromTargetVenue && !targetSourceDisappeared) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Skipping update - neither side from target venue: {}/{}", 
                        best.getBidSrc(), best.getAskSrc());
                }
                return;
            }
            
            boolean hasActiveBid = false;
            boolean hasActiveAsk = false;

            if (existingQuote != null) {
                // Use the ActiveQuote's knowledge of actual order status
                hasActiveBid = existingQuote.isBidActive();
                hasActiveAsk = existingQuote.isAskActive();
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Existing quote status for {}: bidActive={}, askActive={}, bidAge={}ms, askAge={}ms", 
                        Id, hasActiveBid, hasActiveAsk,
                        hasActiveBid ? existingQuote.getBidAge() : 0,
                        hasActiveAsk ? existingQuote.getAskAge() : 0);
                }
            }
            
            // Get the native instrument ID
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    Id, config.getMarketSource(), false);
            }
            
            if (nativeInstrument == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("No native instrument ID found for {}", Id);
                }
                return;
            }
            
            // Use unified pricing model to calculate prices
            PricingDecision decision = calculateUnifiedPrices(Id, termCode, best, gcBest);
            
            // Apply pricing decision
            if (existingQuote == null) {
                // Create new quotes if we have valid prices
                if (decision.hasBid || decision.hasAsk) {
                    existingQuote = new ActiveQuote(Id);
                    activeQuotes.put(Id, existingQuote);

                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Creating new quotes for bond: {}", Id);
                    }
                }
            }
            
            if (existingQuote != null) {
                // Update existing quotes
                if (decision.hasBid) {
                    // Check if we need to update bid
                    double currentBidPrice = existingQuote.getBidPrice();
                    boolean updateBid = !hasActiveBid || 
                                    Math.abs(currentBidPrice - decision.bidPrice) >= 0.02 ||
                                    !Objects.equals(existingQuote.getBidReferenceSource(), decision.bidSource);
                    
                    // Add backoff check
                    if (updateBid && existingQuote.isBidInBackoff()) {
                        long remainingMs = existingQuote.getBidBackoffUntilTime() - System.currentTimeMillis();
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Skipping bid update for {} due to backoff ({}ms remaining)", 
                                Id, remainingMs);
                        }
                        updateBid = false;
                    }

                    if (updateBid) {
                        MarketOrder updatedBid = updateOrder(
                            Id, nativeInstrument, "Buy", decision.bidPrice, 
                            decision.bidSource, hasActiveBid ? existingQuote.getBidOrder() : null);
                        
                        if (updatedBid != null) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Updated bid for {}: price={}, source={}, isGcBased={}", 
                                    Id, decision.bidPrice, decision.bidSource, decision.isGcBasedBid);
                            }
                        }
                    }
                } else if (hasActiveBid) {
                    // Cancel bid if we have no valid price
                    cancelOrder(existingQuote.getBidOrder(), Id);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Cancelled bid for {} - no valid pricing reference", Id);
                    }
                }
                
                if (decision.hasAsk) {
                    // Check if we need to update ask
                    double currentAskPrice = existingQuote.getAskPrice();
                    boolean updateAsk = !hasActiveAsk || 
                                    Math.abs(currentAskPrice - decision.askPrice) >= 0.02 ||
                                    !Objects.equals(existingQuote.getAskReferenceSource(), decision.askSource);
                    
                    // Add backoff check
                    if (updateAsk && existingQuote.isAskInBackoff()) {
                        long remainingMs = existingQuote.getAskBackoffUntilTime() - System.currentTimeMillis();
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Skipping ask update for {} due to backoff ({}ms remaining)", 
                                Id, remainingMs);
                        }
                        updateAsk = false;
                    }
                    
                    if (updateAsk) {
                        MarketOrder updatedAsk = updateOrder(
                            Id, nativeInstrument, "Sell", decision.askPrice, 
                            decision.askSource, hasActiveAsk ? existingQuote.getAskOrder() : null);
                        
                        if (updatedAsk != null) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Updated ask for {}: price={}, source={}, isGcBased={}", 
                                    Id, decision.askPrice, decision.askSource, decision.isGcBasedAsk);
                            }
                        }
                    }
                } else if (hasActiveAsk) {
                    // Cancel ask if we have no valid price
                    cancelOrder(existingQuote.getAskOrder(), Id);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Cancelled ask for {} - no valid pricing reference", Id);
                    }
                }
            }
            instrumentUpdateCounters.computeIfAbsent(Id, k -> new AtomicInteger(0)).incrementAndGet();
            processedUpdateCounter.incrementAndGet();
        } catch (Exception e) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Error processing market update: {}", e.getMessage(), e);
        }
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
            LOGGER.info("    Bond to instrument mappings: {}", bondEligibilityListener.bondToInstrumentMaps.size());
            
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
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("getMarketMakerStatus: Retrieving market maker status");
        }

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
        subscriptions.put("mfaInformation", isMfaInformationSubscribed);
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
     * Updates the status of an order in its associated ActiveQuote
     * 
     * @param Id The Id
     * @param side The order side ("Buy" or "Sell")
     * @param isActive Whether the order is active
     */
    public void updateOrderStatus(String Id, String side, boolean isActive) {
        if (Id == null || side == null) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("updateOrderStatus: Missing required parameters: orderId={}, side={}", 
                    Id, side);
            }
            return;
        }
        
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("updateOrderStatus: Updating status for order {}/{} to {}", 
                    Id, side, isActive ? "ACTIVE" : "INACTIVE");
            }
            
            // First try direct lookup by instrumentId
            ActiveQuote quote = activeQuotes.get(Id);
            
            if (quote != null) {
                // Found quote directly by instrumentId
                if ("Buy".equals(side)) {
                    quote.setBidActive(isActive);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Updated bid status for {} to {}", 
                            quote.getId(), isActive ? "ACTIVE" : "INACTIVE");
                    }
                } else if ("Sell".equals(side)) {
                    quote.setAskActive(isActive);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Updated ask status for {} to {}", 
                            quote.getId(), isActive ? "ACTIVE" : "INACTIVE");
                    }
                }
                return;
            }

            // Find the ActiveQuote with this order
            for (ActiveQuote existingQuote : activeQuotes.values()) {
                MarketOrder bidOrder = existingQuote.getBidOrder();
                MarketOrder askOrder = existingQuote.getAskOrder();

                // Check if the bid order matches
                if ("Buy".equals(side) && bidOrder != null && Id.equals(bidOrder.getInstrId())) {
                    // Update the bid status
                    existingQuote.setBidActive(isActive);
                    
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Updated bid status for {}: Id={}, active={}", 
                            existingQuote.getId(), Id, isActive);
                    }
                    return;
                }
                
                // Check if the ask order matches
                if ("Sell".equals(side) && askOrder != null && Id.equals(askOrder.getInstrId())) {
                    // Update the ask status
                    existingQuote.setAskActive(isActive);
                    
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Updated ask status for {}: Id={}, active={}", 
                            existingQuote.getId(), Id, isActive);
                    }
                    return;
                }
            }
            
            // If we get here, we didn't find the order in any ActiveQuote
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("No ActiveQuote found containing order {}/{}, creating one", Id, side);
                // Create a new quote for this instrument if we couldn't find it
                ActiveQuote newQuote = new ActiveQuote(Id);
                activeQuotes.put(Id, newQuote);
                
                // Set the appropriate side's status
                if ("Buy".equals(side)) {
                    newQuote.setBidActive(isActive);
                } else if ("Sell".equals(side)) {
                    newQuote.setAskActive(isActive);
                }
                LOGGER.info("Created new ActiveQuote for {}", Id);
            }

            // If we get here, we didn't find the order in any ActiveQuote
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("No ActiveQuote found containing order {}/{}", Id, side);
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error updating order status for {}/{}: {}", 
                    Id, side, e.getMessage(), e);
            }
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
       
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("MarketMaker.mapOrderIdToReqId() - Mapping orderId: {} to reqId: {}", orderId, reqId);
        }

        // Store the mapping in a local map for quick lookups
        orderIdToReqIdMap.put(orderId, reqId);
        
        // Also delegate to the order manager if needed
        if (orderManager != null) {
            orderManager.mapOrderIdToReqId(orderId, reqId);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("mapOrderIdToReqId: Mapped orderId {} to reqId {}", orderId, reqId);
        }
    }

    /**
     * Enable or disable the market making strategy.
     * 
     * @param enabled Whether market making should be enabled
     */
    public void setEnabled(boolean enabled) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("setEnabled: Current enabled state: {}", this.enabled);
        }

        boolean previousState = this.enabled;
        this.enabled = enabled;
        
        if (previousState != enabled) {
            if (enabled) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Market making enabled");
                    LOGGER.info("MarketMaker: Market making enabled");
                }

                // Start market making for eligible bonds
                if (termCodeActiveStatus.get("C")){
                    makeMarketsForEligibleBonds("C");
                }

                if (termCodeActiveStatus.get("REG")){
                    makeMarketsForEligibleBonds("REG");
                }
                
                // Start order monitor
                monitorOrders();
            } else {
               if (LOGGER.isInfoEnabled()) {
                   LOGGER.info("Market making disabled");
                   LOGGER.info("MarketMaker: Market making disabled");
               }

                // Cancel all pending orders
                cancelAllOrders(5000);
            }
        }

       if (LOGGER.isInfoEnabled()) {
           LOGGER.info("setEnabled: Enabled state set to: {}", enabled);
       }
    }

    private void handleEligibilityChange(String Id, boolean isEligible, Map<String, Object> bondData) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("handleEligibilityChange: Bond={} isEligible={}", Id, isEligible);
        }

        if (Id == null) {
            LOGGER.error("handleEligibilityChange: Id is null, ignoring eligibility change");
            return;
        }

        if (!enabled) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("handleEligibilityChange: Market maker not enabled, ignoring change");
            }
            return;
        }

        try {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Bond eligibility changed: {} -> {}", Id, (isEligible ? "ELIGIBLE" : "INELIGIBLE"));
            }

            if (isEligible) {
                // Bond became eligible, add to tracked set and create initial markets
                trackedInstruments.add(Id);
                String termCode = (String) bondData.get("termCode");
                // Try to create initial markets for this bond
                tryCreateOrUpdateMarkets(Id);

            } else {
                // Bond became ineligible, remove from tracked set and cancel orders
                trackedInstruments.remove(Id);
                cancelOrdersForInstrument(Id);
                
                // Remove from active quotes
                activeQuotes.remove(Id);

                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("handleEligibilityChange: Bond {} became ineligible, orders cancelled", Id);
                }
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error handling eligibility change for {}: {}", Id, e.getMessage(), e);
            }
        }
    }

    private void makeMarketsForEligibleBonds(String termCode) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("makeMarketsForEligibleBonds: Starting periodic market making");
        }

        if (!enabled) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("makeMarketsForEligibleBonds: Market maker not enabled, skipping");
            }
            return;
        }
        
        String suffix = termCode + "_Fixed";

        try {
            Set<String> eligibleBonds = bondEligibilityListener.getEligibleBonds();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Making markets for {} eligible bonds", eligibleBonds.size());
            }

            int marketsCreated = 0;
            int marketsUpdated = 0;
            int marketsSkipped = 0;
            
            // Process all eligible bonds
            for (String Id : eligibleBonds) {
                LOGGER.debug("makeMarketsForEligibleBonds: Processing Id={}", Id);
                if (!Id.endsWith(suffix)){
                    return;
                }
                // Check if we're already tracking this instrument
                if (Id == null || Id.isEmpty()) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("makeMarketsForEligibleBonds: Empty bond ID, skipping");
                    }
                    continue;
                }
                if (!trackedInstruments.contains(Id)) {
                    // New eligible bond, not yet tracking
                    LOGGER.debug("makeMarketsForEligibleBonds: New eligible bond detected, Id={}", Id);
                    trackedInstruments.add(Id);
                    int i = tryCreateOrUpdateMarkets(Id);
                    marketsCreated+=i;
                }
            }
            // Clean up any instruments that are no longer eligible
            Set<String> instrumentsToRemove = new HashSet<>();
            for (String Id : trackedInstruments) {
                if (!eligibleBonds.contains(Id)) {
                    instrumentsToRemove.add(Id);
                }
            }
            
            int marketsRemoved = 0;
            for (String Id : instrumentsToRemove) {
                trackedInstruments.remove(Id);
                cancelOrdersForInstrument(Id);
                activeQuotes.remove(Id);
                marketsRemoved++;
            }
           if (LOGGER.isInfoEnabled()) {
               LOGGER.info("makeMarketsForEligibleBonds: Market making completed: {} created, {} updated, {} skipped, {} removed",
                   marketsCreated, marketsUpdated, marketsSkipped, marketsRemoved);
           }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("makeMarketsForEligibleBonds: Error in periodic market making: {}", e.getMessage(), e);
            }
        }
    }

        /**
     * Try to create or update markets for an instrument using the unified pricing model
     */
    private int tryCreateOrUpdateMarkets(String Id) {
        try {
            if (Id == null) {
                LOGGER.error("tryCreateOrUpdateMarkets: Id is null, cannot create markets");
                return -1;
            }
            
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("tryCreateOrUpdateMarkets: Attempting to create/update markets for Id={}", Id);
            }

            // Get term code from instrument ID
            String termCode;
            if (Id.endsWith("C_Fixed")) {
                termCode = "C";
                LOGGER.debug("tryCreateOrUpdateMarkets: Detected term code C for Id={}", Id);
            } else if (Id.endsWith("REG_Fixed")) {
                termCode = "REG";
                LOGGER.debug("tryCreateOrUpdateMarkets: Detected term code REG for Id={}", Id);
            } else {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("tryCreateOrUpdateMarkets: Unsupported instrument type: {}", Id);
                }
                return -1;
            }
            

            // Get the native instrument ID
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    Id, config.getMarketSource(), false);
                LOGGER.debug("tryCreateOrUpdateMarkets: Native instrument for Id={} is {}", Id, nativeInstrument);
                return -1;
            }
            
            if (nativeInstrument == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("No native instrument ID found for {}", Id);
                }
                trackedInstruments.remove(Id);
                return -1;
            }
            
            // Get the appropriate GC reference data
            GCBest gcBest = "C".equals(termCode) ? GCBestManager.getInstance().getCashGCBest() : GCBestManager.getInstance().getRegGCBest();

            // Use unified pricing model (with no market data available)
            PricingDecision decision = calculateUnifiedPrices(Id, termCode, null, gcBest);

            // Check if existing quotes need validation
            ActiveQuote existingQuote = activeQuotes.get(Id);
            if (existingQuote == null) {
                // Create new quotes if we have valid prices
                if (decision.hasBid || decision.hasAsk) {
                    existingQuote = new ActiveQuote(Id);
                    activeQuotes.put(Id, existingQuote);
                    
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Creating initial markets for bond: {}", Id);
                    }
                    
                    if (decision.hasBid) {
                        placeOrder(Id, nativeInstrument, "Buy", decision.bidPrice, decision.bidSource);
                    }
                    
                    // if (decision.hasAsk) {
                    //     placeOrder(Id, nativeInstrument, "Sell", decision.askPrice, decision.askSource);
                    // }
                } else {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("No valid pricing available for new bond: {}", Id);
                    }
                }
            } else {
                // Check if any existing orders are dead and need replacement
                MarketOrder bidOrder = existingQuote.getBidOrder();
                MarketOrder askOrder = existingQuote.getAskOrder();
                
                boolean bidActive = (bidOrder != null && !bidOrder.isDead());
                boolean askActive = (askOrder != null && !askOrder.isDead());
                
                // Only update sides that need attention
                if (!bidActive && decision.hasBid) {
                    placeOrder(Id, nativeInstrument, "Buy", decision.bidPrice, decision.bidSource);
                }
                
                if (!askActive && decision.hasAsk) {
                    placeOrder(Id, nativeInstrument, "Sell", decision.askPrice, decision.askSource);
                }
            }
            return 1;
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error creating/updating markets for bond {}: {}", Id, e.getMessage(), e);
            }
            return -1;
        }
    }

    // private double getReferencePriceForBond(String Id, String side) {
    //     double lastGCRate = 0.0;
    //     GCBest lastGCBest = null;
    //     if (Id.endsWith("C_Fixed")) {
    //         synchronized(gcBestLock) {
    //             lastGCBest = latestGcBestCash;
    //             lastGCRate = latestCashGcRate;
    //         }
    //     } else if (Id.endsWith("REG_Fixed")) {
    //         synchronized(gcBestLock) {
    //             lastGCBest = latestGcBestREG;
    //             lastGCRate = latestRegGcRate;
    //         }
    //     }

    //     GCLevelResult gcLevel = lastGCBest.getGCLevel();

    //     if (side.equals("Buy")){
    //         double bid = 0;
    //         if (gcLevel.getBidPrice() == null || lastGCRate <= 0) {
    //             LOGGER.warn("No valid GC rate available for Buy side on bond: " + Id);
    //             return -9999; // No valid rate, cannot quote
    //         } else if (gcLevel.getBidPrice() != null) {
    //             bid = gcLevel.getBidPrice();
    //             if (bid == 0 && lastGCRate > 0) {
    //                 if (LOGGER.isInfoEnabled()) {
    //                     LOGGER.info("No valid bid available, using last GC traded value for pricing on Buy side for bond: " + Id);
    //                 }
    //                 return lastGCRate + 20;
    //             } else if (bid != 0) {
    //                 if (LOGGER.isInfoEnabled()) {
    //                     LOGGER.info("Using last GC bid value for pricing on Buy side for bond: " + Id);
    //                 }
    //                 return bid + 1;
    //             }
    //         }
    //     }
        
    //     double gcAsk = 0.0;
    //     if (side.equals("Sell")){
    //         double ask = 0.0;
    //         if (gcLevel.getAskPrice() == null || lastGCRate <= 0) {
    //             if (LOGGER.isWarnEnabled()) {
    //                 LOGGER.warn("No valid GC rate available for Sell side on bond: " + Id);
    //             }
    //             gcAsk = -9999; // No valid rate, cannot quote
    //         } else if (gcLevel.getAskPrice() != null) {
    //             ask = gcLevel.getAskPrice();
    //             if (ask == 0 && lastGCRate > 0) {
    //                 if (LOGGER.isInfoEnabled()) {
    //                     LOGGER.info("Using last GC traded value for pricing on Sell side for bond: " + Id);
    //                 }
    //                 gcAsk = lastGCRate;
    //             } else if (ask != 0) {
    //                 if (LOGGER.isInfoEnabled()) {
    //                     LOGGER.info("Using last GC ask value for pricing on Sell side for bond: " + Id);
    //                 }
    //                 gcAsk = ask;
    //             }
    //         }
    //     }

    //     double spread = 0.0;
    //     try {
    //         String cusip = Id.substring(0, Math.min(Id.length(), 9));
    //         String termCode = Id.endsWith("C_Fixed") ? "C" : "REG";
    //         // Get bond data from the eligibility listener
    //         Map<String, Object> bondData = bondEligibilityListener.getBondData(cusip);
    //         if (bondData != null) {
    //             // Try to get MFA rate information
    //             Object mfaData = bondData.get("mfaData");
    //             if (mfaData != null && mfaData instanceof Map) {
    //                 @SuppressWarnings("unchecked")
    //                 Map<String, Object> mfaInfo = (Map<String, Object>) mfaData;

    //                 Map<String, Object> cToday = getMfaRecord(mfaInfo, "MFA_" + cusip + "_C_Fixed_TODAY");
    //                 Map<String, Object> cYest = getMfaRecord(mfaInfo, "MFA_" + cusip + "_C_Fixed_YEST");
    //                 Map<String, Object> regToday = getMfaRecord(mfaInfo, "MFA_" + cusip + "_REG_Fixed_TODAY");
    //                 Map<String, Object> regYest = getMfaRecord(mfaInfo, "MFA_" + cusip + "_REG_Fixed_YEST");
    //                 if (termCode.equals("C")) {
    //                     if (cToday != null) {
    //                         Object rateAvg = cToday.get("SpreadGCAvg");
    //                             if (rateAvg != null) {
    //                                 try {
    //                                     spread = 2 * Double.parseDouble(rateAvg.toString());
    //                                     if (LOGGER.isInfoEnabled()) {
    //                                         LOGGER.info("Using MFA GC Spread for {}: {}", cusip, spread);
    //                                     }
    //                                 } catch (NumberFormatException e) {
    //                                     if (LOGGER.isWarnEnabled()) {
    //                                         LOGGER.warn("Invalid MFA rate format for {}: {}", cusip, rateAvg);
    //                                     }
    //                                 }
    //                             }
    //                         } else if (cYest != null) {
    //                             Object rateAvg = cYest.get("SpreadGCAvg");
    //                             if (rateAvg != null) {
    //                                 try {
    //                                     spread = 2 * Double.parseDouble(rateAvg.toString());
    //                                     if (LOGGER.isInfoEnabled()) {
    //                                         LOGGER.info("Using MFA GC Spread for {}: {}", cusip, spread);
    //                                     }
    //                                 } catch (NumberFormatException e) {
    //                                     if (LOGGER.isWarnEnabled()) {
    //                                         LOGGER.warn("Invalid MFA rate format for {}: {}", cusip, rateAvg);
    //                                     }
    //                                 }
    //                             }
    //                         } else if (regYest != null) {
    //                             Object rateAvg = regYest.get("SpreadGCAvg");
    //                             if (rateAvg != null) {
    //                                 try {
    //                                     spread = 2 * Double.parseDouble(rateAvg.toString());
    //                                     if (LOGGER.isInfoEnabled()) {
    //                                         LOGGER.info("Using MFA GC Spread for {}: {}", cusip, spread);
    //                                     }
    //                                 } catch (NumberFormatException e) {
    //                                     if (LOGGER.isWarnEnabled()) {
    //                                         LOGGER.warn("Invalid MFA rate format for {}: {}", cusip, rateAvg);
    //                                     }
    //                                 }
    //                             }
    //                         }
    //                         if (spread > 0) {
    //                             if (LOGGER.isInfoEnabled()) {
    //                                 LOGGER.info("Using calculated spread for {}: {}", cusip, spread);
    //                             }
    //                             return gcAsk - spread;
    //                         }
    //                     } else if (termCode.equals("REG")){
    //                         if (regToday != null) {
    //                             Object rateAvg = regToday.get("SpreadGCAvg");
    //                             if (rateAvg != null) {
    //                                 try {
    //                                     spread = 2 * Double.parseDouble(rateAvg.toString());
    //                                     if (LOGGER.isInfoEnabled()) {
    //                                         LOGGER.info("Using MFA GC Spread for {}: {}", cusip, spread);
    //                                     }
    //                                 } catch (NumberFormatException e) {
    //                                     if (LOGGER.isWarnEnabled()) {
    //                                         LOGGER.warn("Invalid MFA rate format for {}: {}", cusip, rateAvg);
    //                                     }
    //                                 }
    //                             }
    //                         } else if (cToday != null) {
    //                             Object rateAvg = cToday.get("SpreadGCAvg");
    //                             if (rateAvg != null) {
    //                                 try {
    //                                     spread = 2 * Double.parseDouble(rateAvg.toString());
    //                                     if (LOGGER.isInfoEnabled()) {
    //                                         LOGGER.info("Using MFA GC Spread for {}: {}", cusip, spread);
    //                                     }
    //                                 } catch (NumberFormatException e) {
    //                                     if (LOGGER.isWarnEnabled()) {
    //                                         LOGGER.warn("Invalid MFA rate format for {}: {}", cusip, rateAvg);
    //                                     }
    //                                 }
    //                             }
    //                         }
    //                         if (spread > 0) {
    //                             if (LOGGER.isInfoEnabled()) {
    //                                 LOGGER.info("Using calculated spread for {}: {}", cusip, spread);
    //                             }
    //                             return gcAsk - spread;
    //                         }
    //                     }
    //                 } 
    //             } else {
    //                 if (LOGGER.isWarnEnabled()) {
    //                     LOGGER.warn("No bond data available for {} to calculate reference price", cusip);
    //                 }
    //                 return -9999; // No bond data, cannot quote
    //             }
    //         } catch (Exception e) {
    //             if (LOGGER.isWarnEnabled()) {
    //                 LOGGER.warn("Error accessing MFA data for {}: {}", Id, e.getMessage());
    //             }
    //         }
    //     return -9999;
    // }

    // /**
    //  * Helper method to safely get MFA record
    //  */
    // @SuppressWarnings("unchecked")
    // private Map<String, Object> getMfaRecord(Map<String, Object> mfaInfo, String key) {
    //     if (mfaInfo == null) return null;
    //     Object obj = mfaInfo.get(key);
    //     if (obj instanceof Map) {
    //         return (Map<String, Object>) obj;
    //     }
    //     return null;
    // }

    @Override
    public void removeOrder(int reqId) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("removeOrder: Removing order with reqId: {}", reqId);
        }

        try {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("MarketMaker.removeOrder() - Removing order with reqId: {}", reqId);
            }

            // Find and remove the order from our active quotes
            for (ActiveQuote quote : activeQuotes.values()) {
                MarketOrder bidOrder = quote.getBidOrder();
                MarketOrder askOrder = quote.getAskOrder();
                
                if (bidOrder != null && bidOrder.getMyReqId() == reqId) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Removed bid order for {}: reqId={}", quote.getId(), reqId);
                    }
                    quote.setBidOrder(null, null, 0);
                    break;
                }
                
                if (askOrder != null && askOrder.getMyReqId() == reqId) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Removed ask order for {}: reqId={}", quote.getId(), reqId);
                    }
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

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("removeOrder: Order removed successfully");
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("removeOrder: Error removing order with reqId {}: {}", reqId, e.getMessage(), e);
            }
        }
    }

    /**
     * Cancel all orders for a specific instrument
     */
    private void cancelOrdersForInstrument(String cusip) {
        try {
            ActiveQuote quote = activeQuotes.get(cusip);
            if (quote != null) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Cancelling orders for ineligible instrument: {}", cusip);
                }

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
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error cancelling orders for {}: {}", cusip, e.getMessage(), e);
            }
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
     * Get configuration summary for monitoring
     */
    public Map<String, Object> getConfigSummary() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("getConfigSummary: Getting configuration summary");
        }

        Map<String, Object> summary = new HashMap<>();
        summary.put("enabled", enabled);
        summary.put("marketSource", config.getMarketSource());
        summary.put("minSize", config.getMinSize());
        summary.put("autoHedge", config.isAutoHedge());
        summary.put("activeQuotes", activeQuotes.size());
        summary.put("trackedInstruments", trackedInstruments.size());

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("getConfigSummary: Returning summary with {} items", summary.size());
        }
        return summary;
    }

    /**
     * Updates the configuration for this market maker.
     * 
     * @param newConfig The new configuration to apply
     */
    public void updateConfig(MarketMakerConfig newConfig) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("updateConfig: Updating market maker configuration");
        }
        
        if (newConfig == null) {
            throw new IllegalArgumentException("New configuration cannot be null");
        }
        
        MarketMakerConfig oldConfig = this.config;
        this.config = newConfig;
        
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Configuration updated: autoEnabled={} -> {}, minSize={} -> {}", 
                oldConfig.isAutoEnabled(), newConfig.isAutoEnabled(),
                oldConfig.getMinSize(), newConfig.getMinSize());
        }
        
        // Apply the new configuration settings
        // If auto-enabled is being turned on, we may need to start trading
        if (newConfig.isAutoEnabled() && !isRunning()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Starting automated market making due to config update");
            }
            startAutomatedMarketMaking();
        } 
        // If auto-enabled is being turned off, we may need to stop trading
        else if (!newConfig.isAutoEnabled() && isRunning()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Stopping automated market making due to config update");
            }
            stopAutomatedMarketMaking();
        }
        
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("updateConfig: Configuration update completed");
        }
    }

    /**
     * Prepares the market maker for shutdown without making MKV calls.
     * First phase of the two-phase shutdown process.
     */
    public void prepareForShutdown() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Preparing market maker for shutdown");
        }

        // First disable to prevent new orders
        this.enabled = false;
        
        // Shutdown scheduler first to prevent any new order placements
        if (scheduler != null) {
            try {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Shutting down market maker scheduler");
                }
                scheduler.shutdown();

                if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                    List<Runnable> pendingTasks = scheduler.shutdownNow();
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Force-terminated scheduler with {} pending tasks", 
                            pendingTasks.size());
                    }
                }
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Error shutting down scheduler: {}", e.getMessage());
                }
            }
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Market maker prepared for shutdown");
        }
    }

    /**
     * Completes the shutdown process after the main application
     * has finished its critical MKV operations.
     * Second phase of the two-phase shutdown process.
     */
    public void completeShutdown() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completing market maker shutdown");
        }

        try {
            // Clear all data structures
            activeQuotes.clear();
            trackedInstruments.clear();
            orderIdToReqIdMap.clear();
            instrumentUpdateCounters.clear();
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error during final market maker cleanup: {}", e.getMessage(), e);
            }
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Market maker shutdown completed");
        }
    }

    /**
     * Performs a complete shutdown of the market maker component.
     * Disables the market maker and cancels all orders if possible.
     */
    public void shutdown() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Shutting down market maker");
        }

        // Check if shutdown has already been handled by the shutdown hook
        if (!markShutdownHandled()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Shutdown already handled by shutdown hook, skipping");
            }
            return;
        }

        // First prepare for shutdown (disable and stop scheduler)
        prepareForShutdown();
        
        try {
            // Use the thread-safe Mkv access pattern with proper synchronization
            Mkv mkv = null;
            MkvPublishManager pm = null;
            boolean mkvAvailable = false;
            
            // Get the MKV instance and publish manager safely
            try {
                synchronized (Mkv.class) {  // Synchronize on Mkv class to prevent concurrent shutdown
                    try {
                        mkv = Mkv.getInstance();
                        if (mkv != null) {
                            try {
                                pm = mkv.getPublishManager();
                                mkvAvailable = (pm != null);
                            } catch (IllegalStateException e) {
                                LOGGER.warn("MKV API is stopping or stopped, cannot access PublishManager");
                            }
                        }
                    } catch (IllegalStateException e) {
                        if (LOGGER.isWarnEnabled()) {
                            LOGGER.warn("MKV API is already stopped, cannot get instance");
                        }
                    }
                }
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Error checking MKV availability: {}", e.getMessage());
                    }
                }
            }

            if (mkvAvailable) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("MKV still available, cancelling all active quotes");
                }
                try {
                    // Cancel orders with a short timeout
                    boolean success = cancelAllOrders(3000);
                    if (!success) {
                        if (LOGGER.isWarnEnabled()) {
                            LOGGER.warn("Not all orders cancelled successfully before timeout");
                        }
                    }
                } catch (Exception e) {
                    if (LOGGER.isErrorEnabled()) {
                        LOGGER.error("Error during order cancellation: {}", e.getMessage(), e);
                    }
                }
            } else {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("MKV no longer available, skipping order cancellation");
                }
            }
            
            // Complete the shutdown process
            completeShutdown();
            
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error during market maker shutdown: {}", e.getMessage(), e);
            }
        }
    }

    // Add a method to check if shutdown is already handled
    private boolean markShutdownHandled() {
        synchronized (this) {
            if (shutdownHandled) {
                return false;
            }
            shutdownHandled = true;
            return true;
        }
    }

    public boolean isRunning() {
        return enabled;
    }

    public void startAutomatedMarketMaking() {
        setEnabled(true);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Automated market making started");
        }
    }

    public void stopAutomatedMarketMaking() {
        setEnabled(false);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Automated market making stopped");
        }
    }

    /**
     * Check if a venue is a target venue for hedging
     * @param venue The venue to check
     * @return true if it's a target venue, false otherwise
     */
    public boolean isTargetVenue(String venue) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("isTargetVenue: Checking if venue is targeted: {}", venue);
        }
        if (venue == null) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("isTargetVenue: Result: false (venue is null)");
            }
            return false;
        }
        boolean result = config.getTargetVenuesSet().contains(venue);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("isTargetVenue: Result: {} for venue: {}", result, venue);
        }
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
                        double price, String referenceSource) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("placeOrder: CUSIP={}, Native={}, Side={}, Price={}, Source={}", 
                cusip, nativeInstrument, verb, price, referenceSource);
        }

        try {

            // Check if we're within trading hours for default markets
            if ("DEFAULT".equals(referenceSource) && !config.isDefaultMarketMakingAllowed()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Default market making not allowed at this time");
                }
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("placeOrder: Failed: Default market making not allowed");
                }
                return;
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Placing {} order on {}: {} ({}), size={}, price={}, reference={}", 
                    verb, config.getMarketSource(), nativeInstrument, cusip, price, referenceSource);
            }

            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(cusip, 
                nativeInstrument.contains("_C_") ? "C" : "REG");
            double size = config.getMinSize(); // Default to minimum size
            if (depthListener != null && instrumentId != null) {
                double venueMinimum = getCachedVenueMinimum(instrumentId, config.getMarketSource());

                if (venueMinimum > 0) {
                    // Also check hedge venue minimum if auto-hedge enabled
                    if (config.isAutoHedge() && referenceSource != null && isTargetVenue(referenceSource)) {
                        double hedgeMinimum = getCachedVenueMinimum(instrumentId, referenceSource);
                        // Use the larger of the two minimums
                        size = Math.max(size, Math.max(venueMinimum, hedgeMinimum));
                    } else {
                        // Just ensure we meet the trading venue minimum
                        size = Math.max(size, venueMinimum);
                    }
                    
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Adjusted order size for {}: venue minimum={}, final size={}", 
                            cusip, venueMinimum, size);
                    }
                } else {
                    size = venueMinimum;
                }
            }

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
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Order placed successfully: reqId={}", order.getMyReqId());
                }
                // Update the appropriate quote
                ActiveQuote quote = activeQuotes.computeIfAbsent(cusip, ActiveQuote::new);
                
                if ("Buy".equals(verb)) {
                // Update the ActiveQuote with bid information
                    quote.setBidOrder(order, referenceSource, price);
                    
                    // Set source flags explicitly
                    quote.setGcBasedBid(referenceSource != null && referenceSource.startsWith("GC_"));
                    quote.setMarketBasedBid(referenceSource != null && 
                                        !referenceSource.startsWith("GC_") && 
                                        !referenceSource.equals("DEFAULT"));
                    
                    // Explicitly set active status
                    quote.setBidActive(true);
                    
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Bid quote updated for {}: price={}, source={}, reqId={}, isGcBased={}, isMarketBased={}", 
                            cusip, price, referenceSource, order.getMyReqId(),
                            quote.isGcBasedBid(), quote.isMarketBasedBid());
                    }
                } else {
                // Update the ActiveQuote with ask information
                    quote.setAskOrder(order, referenceSource, price);
                    
                    // Set source flags explicitly
                    quote.setGcBasedAsk(referenceSource != null && referenceSource.startsWith("GC_"));
                    quote.setMarketBasedAsk(referenceSource != null && 
                                        !referenceSource.startsWith("GC_") && 
                                        !referenceSource.equals("DEFAULT"));
                    
                    // Explicitly set active status
                    quote.setAskActive(true);
                    
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Ask quote updated for {}: price={}, source={}, reqId={}, isGcBased={}, isMarketBased={}", 
                            cusip, price, referenceSource, order.getMyReqId(),
                            quote.isGcBasedAsk(), quote.isMarketBasedAsk());
                    }
                }

                // Now also track this instrument
                trackedInstruments.add(cusip);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("placeOrder: Order placed successfully");
                }
            } else {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Failed to place {} order for {}", verb, cusip);
                }
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("placeOrder: Failed to place order");
                }
            }
        } catch (Exception e) {
            LOGGER.error("placeOrder: Error placing order for {}", cusip, e);
        }
    }

    /**
     * Updates an existing order with new parameters, or places a new order if none exists
     * 
     * @param bondId The bond CUSIP
     * @param nativeInstrument The native instrument ID for the venue
     * @param side The order side ("Buy" or "Sell")
     * @param size Order size
     * @param price New order price
     * @param referenceSource Reference source for the price
     * @param existingOrder Existing order to update (can be null)
     * @return The updated or new MarketOrder
     */
    private MarketOrder updateOrder(String bondId, String nativeInstrument, String side, 
                                double price, String referenceSource,
                                MarketOrder existingOrder) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("updateOrder: CUSIP={}, Side={}, Price={}, Source={}, HasExisting={}", 
                bondId, side, price, referenceSource, (existingOrder != null));
        }

        ActiveQuote quote = activeQuotes.get(bondId);
        // Check if the quote is in backoff
        if (quote != null) {
            boolean inBackoff = "Buy".equals(side) ? quote.isBidInBackoff() : quote.isAskInBackoff();
            if (inBackoff) {
                long backoffUntil = "Buy".equals(side) ? quote.getBidBackoffUntilTime() : quote.getAskBackoffUntilTime();
                long remainingMs = backoffUntil - System.currentTimeMillis();
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Skip update due to backoff for {}/{}: {}ms remaining", 
                        bondId, side, remainingMs);
                }
                return existingOrder; // Skip update during backoff period
            }
        }

        // Rate limit updates to prevent spamming the venue
        long currentTime = System.currentTimeMillis();
        Long lastUpdate = lastOrderUpdateTime.get(bondId + ":" + side);
        if (lastUpdate != null && (currentTime - lastUpdate < MIN_UPDATE_INTERVAL_MS)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Rate limiting update for {}/{}: last update was {}ms ago", 
                    bondId, side, (currentTime - lastUpdate));
            }
            return existingOrder; // Skip update, return existing order
        }

        try {
            // Get venue-specific size using cached lookup instead of passed-in size parameter
            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId, 
                nativeInstrument.contains("_C_") ? "C" : "REG");
            
            double effectiveSize = config.getMinSize(); // Fallback
            
            if (instrumentId != null) {
                // Use cached venue minimum lookup
                double venueMinimum = getCachedVenueMinimum(instrumentId, config.getMarketSource());
                
                // Also check hedge venue minimum if applicable
                if (config.isAutoHedge() && referenceSource != null && 
                    !referenceSource.equals("DEFAULT") && !referenceSource.equals("GC_FALLBACK") && 
                    isTargetVenue(referenceSource)) {
                    
                    // Use cached lookup for hedge venue
                    double hedgeMinimum = getCachedVenueMinimum(instrumentId, referenceSource);
                    
                    effectiveSize = Math.max(venueMinimum, hedgeMinimum);
                    
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Update order size for {}: venue_min={}, hedge_min={}, final={}", 
                            bondId, venueMinimum, hedgeMinimum, effectiveSize);
                    }
                } else {
                    effectiveSize = venueMinimum;
                    
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Update order size for {}: venue_min={}, final={}", 
                            bondId, venueMinimum, effectiveSize);
                    }
                }
            }

            // Additional validation
            if (effectiveSize <= 0) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Invalid effective order size: {}, using config default", effectiveSize);
                }
                effectiveSize = config.getMinSize();
            }

            // If we have an existing order that's still active, update it
            if (existingOrder != null && !existingOrder.isDead()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Updating existing {} order: orderId={}, oldPrice={}, newPrice={}, size={}M", 
                        side, existingOrder.getOrderId(), existingOrder.getPrice(), price, effectiveSize / 1_000_000);
                }

                MarketOrder updatedOrder = MarketOrder.orderUpdate(
                    config.getMarketSource(), 
                    fenicsTrader,
                    existingOrder.getOrderId(),
                    nativeInstrument, 
                    side, 
                    effectiveSize, // Use cached venue-specific size
                    price, 
                    this
                );
                
                if (updatedOrder != null) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Order updated successfully: {}, size={}M", 
                            updatedOrder.getOrderId(), effectiveSize / 1_000_000);
                    }
                    
                    // Update the ActiveQuote with complete information
                    if ("Buy".equals(side)) {
                        quote.setBidOrder(updatedOrder, referenceSource, price);
                        quote.setGcBasedBid(referenceSource != null && referenceSource.startsWith("GC_"));
                        quote.setMarketBasedBid(referenceSource != null && 
                                            !referenceSource.startsWith("GC_") && 
                                            !referenceSource.equals("DEFAULT"));
                        quote.setBidActive(true);
                        quote.resetBidFailure();
                        
                        LOGGER.info("Updated bid for {}: price={}, source={}, isGcBased={}, isMarketBased={}", 
                            bondId, price, referenceSource, quote.isGcBasedBid(), quote.isMarketBasedBid());
                    } else {
                        quote.setAskOrder(updatedOrder, referenceSource, price);
                        quote.setGcBasedAsk(referenceSource != null && referenceSource.startsWith("GC_"));
                        quote.setMarketBasedAsk(referenceSource != null && 
                                            !referenceSource.startsWith("GC_") && 
                                            !referenceSource.equals("DEFAULT"));
                        quote.setAskActive(true);
                        quote.resetAskFailure();
                        
                        LOGGER.info("Updated ask for {}: price={}, source={}, isGcBased={}, isMarketBased={}", 
                            bondId, price, referenceSource, quote.isGcBasedAsk(), quote.isMarketBasedAsk());
                    }
                    
                    lastOrderUpdateTime.put(bondId + ":" + side, currentTime);
                    return updatedOrder;
                } else {
                    if (LOGGER.isErrorEnabled()) {
                        LOGGER.error("Failed to update {} order for {}", side, bondId);
                    }
                    
                    // Increment failure counter and apply backoff
                    if ("Buy".equals(side)) {
                        quote.incrementBidFailure();
                        long backoffMs = quote.getBidBackoffUntilTime() - System.currentTimeMillis();
                        LOGGER.warn("Update failed for {}/{}. Backing off for {}ms (failures: {})",
                            bondId, side, backoffMs, quote.getBidUpdateFailureCount());
                    } else {
                        quote.incrementAskFailure();
                        long backoffMs = quote.getAskBackoffUntilTime() - System.currentTimeMillis();
                        LOGGER.warn("Update failed for {}/{}. Backing off for {}ms (failures: {})",
                            bondId, side, backoffMs, quote.getAskUpdateFailureCount());
                    }
                    
                    // If update fails, attempt to cancel and place new
                    lastOrderUpdateTime.put(bondId + ":" + side, currentTime);
                    cancelOrder(existingOrder, bondId);
                }
            }
            
            // If no existing order or update failed, place a new order
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Placing new {} order on {}: {} ({}), size={}M, price={}", 
                    side, config.getMarketSource(), nativeInstrument, bondId, effectiveSize / 1_000_000, price);
            }

            MarketOrder newOrder = orderManager.addOrder(
                config.getMarketSource(), 
                fenicsTrader, 
                nativeInstrument, 
                side, 
                effectiveSize, // Use cached venue-specific size
                price, 
                config.getOrderType(), 
                config.getTimeInForce()
            );
            
            if (newOrder != null) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("New order placed successfully: reqId={}, size={}M", 
                        newOrder.getMyReqId(), effectiveSize / 1_000_000);
                }
                
                // Update ActiveQuote with full information for the new order
                if ("Buy".equals(side)) {
                    quote.setBidOrder(newOrder, referenceSource, price);
                    quote.setGcBasedBid(referenceSource != null && referenceSource.startsWith("GC_"));
                    quote.setMarketBasedBid(referenceSource != null && 
                                        !referenceSource.startsWith("GC_") && 
                                        !referenceSource.equals("DEFAULT"));
                    quote.setBidActive(true);
                    quote.resetBidFailure();
                    
                    LOGGER.info("New bid created for {}: price={}, source={}, isGcBased={}, isMarketBased={}", 
                        bondId, price, referenceSource, quote.isGcBasedBid(), quote.isMarketBasedBid());
                } else {
                    quote.setAskOrder(newOrder, referenceSource, price);
                    quote.setGcBasedAsk(referenceSource != null && referenceSource.startsWith("GC_"));
                    quote.setMarketBasedAsk(referenceSource != null && 
                                        !referenceSource.startsWith("GC_") && 
                                        !referenceSource.equals("DEFAULT"));
                    quote.setAskActive(true);
                    quote.resetAskFailure();
                    
                    LOGGER.info("New ask created for {}: price={}, source={}, isGcBased={}, isMarketBased={}", 
                        bondId, price, referenceSource, quote.isGcBasedAsk(), quote.isMarketBasedAsk());
                }
                
                lastOrderUpdateTime.put(bondId + ":" + side, currentTime);
                return newOrder;
            } else {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Failed to place new {} order for {}", side, bondId);
                }
                // Increment failure counter and apply backoff
                if (quote != null) {
                    if ("Buy".equals(side)) {
                        quote.incrementBidFailure();
                        long backoffMs = quote.getBidBackoffUntilTime() - System.currentTimeMillis();
                        LOGGER.warn("New order failed for {}/{}. Backing off for {}ms (failures: {})",
                            bondId, side, backoffMs, quote.getBidUpdateFailureCount());
                    } else {
                        quote.incrementAskFailure();
                        long backoffMs = quote.getAskBackoffUntilTime() - System.currentTimeMillis();
                        LOGGER.warn("New order failed for {}/{}. Backing off for {}ms (failures: {})",
                            bondId, side, backoffMs, quote.getAskUpdateFailureCount());
                    }
                }
                lastOrderUpdateTime.put(bondId + ":" + side, currentTime);
                return null;
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error updating/placing order for {}: {}", bondId, e.getMessage(), e);
            }
            // Increment failure counter and apply backoff even for exceptions
            if (quote != null) {
                if ("Buy".equals(side)) {
                    quote.incrementBidFailure();
                } else {
                    quote.incrementAskFailure();
                }
            }
            lastOrderUpdateTime.put(bondId + ":" + side, currentTime);
            return null;
        }
    }
    
    /**
     * Cancel all active orders.
     * Delegates to OrderManagement for the actual cancellation process.
     * Used when disabling the market maker.
     * 
     * @param timeoutMs Maximum time to wait for cancellations (in milliseconds)
     * @return true if all orders were successfully cancelled, false otherwise
     */
    public boolean cancelAllOrders(long timeoutMs) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("cancelAllOrders: Delegating cancellation of all market maker orders to OrderManagement");
        }

        // Disable market maker to prevent new orders
        this.enabled = false;
        
        // Delegate to OrderManagement
        if (orderManager != null) {
            boolean result = orderManager.cancelMarketMakerOrders(timeoutMs);
            
            if (result) {
                // Clear our tracking maps since all orders are cancelled
                activeQuotes.clear();
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("All orders successfully cancelled, cleared quote tracking");
                }
            }
            
            return result;
        } else {
            LOGGER.error("OrderManagement is null, cannot cancel orders");
            return false;
        }
    }

    /**
     * Cancel an order with error handling and logging
     * 
     * @param order The order to cancel
     * @param instrumentId The instrument ID for the order (for logging)
     * @return true if cancel request was successfully sent, false otherwise
     */
    private void cancelOrder(MarketOrder order, String Id) {
        if (order == null) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("cancelOrder: Cannot cancel null order for instrument: {}", Id);
            }
        }
        
        try {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Cancelling order: Id={}, orderId={}, reqId={}", 
                    Id, order.getOrderId(), order.getMyReqId());
            }
            
            // Check if the order is already dead/cancelled
            if (order.isDead()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Order already dead/cancelled: {}", order.getOrderId());
                }
            
                // Update the ActiveQuote even for already dead orders
                ActiveQuote quote = activeQuotes.get(Id);
                if (quote != null) {
                    // Determine if it's a bid or ask order
                    if (quote.getBidOrder() != null && quote.getBidOrder().getMyReqId() == order.getMyReqId()) {
                        quote.setBidOrder(null, null, 0);
                        quote.setBidActive(false);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Marked dead bid order for {}: reqId={}", Id, order.getMyReqId());
                        }
                    } else if (quote.getAskOrder() != null && quote.getAskOrder().getMyReqId() == order.getMyReqId()) {
                        quote.setAskOrder(null, null, 0);
                        quote.setAskActive(false);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Marked dead ask order for {}: reqId={}", Id, order.getMyReqId());
                        }
                    }
                }
                return;
            }
            String marketSource = order.getMarketSource();
            String traderId = orderManager.getTraderForVenue(marketSource);
            String orderId = order.getOrderId();

            // Send the cancel request
            MarketOrder cancelOrder = order.orderCancel(marketSource, traderId, orderId, orderManager);

            if (cancelOrder != null) {
                // Track the cancel request
                removeOrder(order.getMyReqId());
                // Update the ActiveQuote
                ActiveQuote quote = activeQuotes.get(Id);
                if (quote != null) {
                    // Determine if it's a bid or ask order
                    if (quote.getBidOrder() != null && quote.getBidOrder().getMyReqId() == order.getMyReqId()) {
                        quote.setBidOrder(null, null, 0);
                        quote.setBidActive(false);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Cancelled bid order for {}: reqId={}", Id, order.getMyReqId());
                        }
                    } else if (quote.getAskOrder() != null && quote.getAskOrder().getMyReqId() == order.getMyReqId()) {
                        quote.setAskOrder(null, null, 0);
                        quote.setAskActive(false);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Cancelled ask order for {}: reqId={}", Id, order.getMyReqId());
                        }
                    }
                }
                // Log to machine-readable format
                ApplicationLogging.logOrderUpdate(
                    "AUTO_CANCEL", 
                    order.getMyReqId(),
                    order.getOrderId(),
                    "Order expired after " + (order.getAgeMillis() / 1000) + " seconds"
                );
            }
        } catch ( Exception e ) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error cancelling order for {}: {}", Id, e.getMessage(), e);
            }
        }
    }
    /**
     * Unified pricing model that considers all available data sources
     * and applies a consistent strategy to determine quote prices.
     * 
     * @param bondId The bond ID
     * @param termCode The term code (C or REG)
     * @param best Current market data
     * @param gcBest Current GC reference data
     * @return A PricingDecision object containing calculated prices and sources
     */
    private PricingDecision calculateUnifiedPrices(String bondId, String termCode, 
                                                Best best, GCBest gcBest) {
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("calculateUnifiedPrices: Starting unified pricing for bond: {}, termCode: {}", 
                bondId, termCode);
        }
        
        // Initialize pricing decision
        PricingDecision decision = new PricingDecision();
        
        try {
            // Get spread adjustment from config
            double spreadAdjustment = config.getDefaultIntraMarketSpread();
            
            // Step 1: Evaluate market data quality
            double referenceBid = 0;
            double referenceAsk = 0;
            String bidSource = null;
            String askSource = null;
            
            if (best != null) {
                referenceBid = best.getBid();
                referenceAsk = best.getAsk();
                bidSource = best.getBidSrc();
                askSource = best.getAskSrc();
            }
            
            boolean validBidSource = bidSource != null && isTargetVenue(bidSource) && referenceBid > 0;
            boolean validAskSource = askSource != null && isTargetVenue(askSource) && referenceAsk > 0;
            
            // Step 2: Evaluate GC data quality
            double gcBidRate = 0;
            double gcAskRate = 0;
            boolean validGcBid = false;
            boolean validGcAsk = false;
            
            if (gcBest != null) {
                GCLevelResult gcLevel = gcBest.getGCLevel();
                if (gcLevel !=  null) {
                    gcBidRate = gcLevel.getBidPrice() != null ? gcLevel.getBidPrice() : 0;
                    gcAskRate = gcLevel.getAskPrice() != null ? gcLevel.getAskPrice() : 0;
                    validGcBid = gcBidRate > 0;
                    validGcAsk = gcAskRate > 0;
                }
            }
            
            // Step 3: Look up MFA spread data for bond
            // double mfaSpread = getMfaSpreadForBond(bondId, termCode);
            
            // Step 4: Calculate bid price using hierarchy of sources
            if (validBidSource && validGcBid) {
                // Use the lower of market data or GC as reference with adjustment
                double lowerBid = Math.min(referenceBid, gcBidRate);
                decision.bidPrice = lowerBid + spreadAdjustment;
                decision.bidSource = (lowerBid == referenceBid) ? bidSource : "GC_" + termCode;
                decision.hasBid = true;
                decision.isMarketBasedBid = (lowerBid == referenceBid);
                decision.isGcBasedBid = (lowerBid == gcBidRate);
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Calculated bid using minimum of market/GC: min({}, {}) + {} = {}", 
                        referenceBid, gcBidRate, spreadAdjustment, decision.bidPrice);
                }
            } else if (validBidSource) {
                // Only market data available
                decision.bidPrice = referenceBid + spreadAdjustment;
                decision.bidSource = bidSource;
                decision.hasBid = true;
                decision.isMarketBasedBid = true;
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Calculated bid using market data: {} + {} = {}", 
                        referenceBid, spreadAdjustment, decision.bidPrice);
                }
            } else if (validGcBid) {
                // Only GC data available
                decision.bidPrice = gcBidRate + spreadAdjustment;
                decision.bidSource = "GC_" + termCode;
                decision.hasBid = true;
                decision.isGcBasedBid = true;
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Calculated bid using GC data: {} + {} = {}", 
                        gcBidRate, spreadAdjustment, decision.bidPrice);
                }
            } else {
                // No valid reference for bid
                decision.hasBid = false;
            }
            
            // Step 5: Calculate ask price using ONLY market data - NEVER use GC for ask
            if (validAskSource) {
                // ONLY use market data for ask side
                decision.askPrice = referenceAsk - spreadAdjustment;
                decision.askSource = askSource;
                decision.hasAsk = true;
                decision.isMarketBasedAsk = true;
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Calculated ask using market data: {} - {} = {}", 
                        referenceAsk, spreadAdjustment, decision.askPrice);
                }
            } else {
                // No valid market reference for ask - DO NOT use GC
                decision.hasAsk = false;
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("No valid market reference for ask, skipping ask (never using GC)");
                }
            }
            
            // CRITICAL: Ensure ask price is always higher than bid price
            if (decision.hasBid && decision.hasAsk) {
                // If bid ≥ ask (inverted quotes), adjust ask to be higher than bid
                if (decision.bidPrice <= decision.askPrice) {
                    // Apply minimum spread (e.g., 2bp)
                    double minSpread = 0.02;
                    double midPrice = (decision.bidPrice + decision.askPrice) / 2;
                    
                    // Adjust both sides around the mid-price
                    decision.bidPrice = midPrice + (minSpread / 2);
                    decision.askPrice = midPrice - (minSpread / 2);
                    
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Fixed inverted quotes for {}: bid={}, ask={}, mid={}, new bid={}, new ask={}", 
                            bondId, decision.bidPrice, decision.askPrice, midPrice, decision.bidPrice, decision.askPrice);
                    }
                }
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error calculating unified prices for bond {}: {}", 
                    bondId, e.getMessage(), e);
            }
        }
        
        return decision;
    }

    /**
     * Represents a pricing decision from the unified model
     */
    private static class PricingDecision {
        boolean hasBid = false;
        boolean hasAsk = false;
        double bidPrice = 0;
        double askPrice = 0;
        String bidSource = null;
        String askSource = null;

        boolean isGcBasedBid = false;
        boolean isGcBasedAsk = false;
        boolean isMarketBasedBid = false;
        boolean isMarketBasedAsk = false;
    }

    // /**
    //  * Gets the MFA spread for a bond
    //  * @param bondId The bond ID
    //  * @param termCode The term code
    //  * @return The MFA spread or 0 if not available
    //  */
    // private double getMfaSpreadForBond(String bondId, String termCode) {
    //     try {
    //         // Extract CUSIP from bond ID (first 9 characters)
    //         String cusip = bondId.substring(0, Math.min(bondId.length(), 9));
            
    //         // Get bond data from the eligibility listener
    //         Map<String, Object> bondData = bondEligibilityListener.getBondData(cusip);
    //         if (bondData != null) {
    //             // Try to get MFA rate information
    //             Object mfaData = bondData.get("mfaData");
    //             if (mfaData != null && mfaData instanceof Map) {
    //                 @SuppressWarnings("unchecked")
    //                 Map<String, Object> mfaInfo = (Map<String, Object>) mfaData;
                    
    //                 // Try different records in priority order
    //                 Map<String, Object> primaryRecord = getMfaRecord(mfaInfo, 
    //                     "MFA_" + cusip + "_" + termCode + "_Fixed_TODAY");
                    
    //                 Map<String, Object> secondaryRecord = getMfaRecord(mfaInfo,
    //                     "MFA_" + cusip + "_" + termCode + "_Fixed_YEST");
                    
    //                 Map<String, Object> fallbackRecord = null;
    //                 if ("C".equals(termCode)) {
    //                     fallbackRecord = getMfaRecord(mfaInfo, "MFA_" + cusip + "_REG_Fixed_TODAY");
    //                 } else {
    //                     fallbackRecord = getMfaRecord(mfaInfo, "MFA_" + cusip + "_C_Fixed_TODAY");
    //                 }
                    
    //                 // Try each record in priority order
    //                 double spread = extractMfaSpread(primaryRecord);
    //                 if (spread <= 0) {
    //                     spread = extractMfaSpread(secondaryRecord);
    //                 }
    //                 if (spread <= 0) {
    //                     spread = extractMfaSpread(fallbackRecord);
    //                 }
                    
    //                 if (spread > 0) {
    //                     if (LOGGER.isDebugEnabled()) {
    //                         LOGGER.debug("Using MFA spread for {}: {}", cusip, spread);
    //                     }
    //                     return spread;
    //                 }
    //             }
    //         }
    //     } catch (Exception e) {
    //         if (LOGGER.isWarnEnabled()) {
    //             LOGGER.warn("Error accessing MFA data for {}: {}", bondId, e.getMessage());
    //         }
    //     }
        
    //     return 0;
    // }

    // /**
    //  * Extract MFA spread from a record
    //  */
    // private double extractMfaSpread(Map<String, Object> record) {
    //     if (record == null) return 0;
        
    //     Object spreadObj = record.get("SpreadGCAvg");
    //     if (spreadObj != null) {
    //         try {
    //             double spread = Double.parseDouble(spreadObj.toString());
    //             // Double the spread as we're using it for a bid-ask spread
    //             return 2 * spread;
    //         } catch (NumberFormatException e) {
    //             if (LOGGER.isWarnEnabled()) {
    //                 LOGGER.warn("Invalid MFA spread format: {}", spreadObj);
    //             }
    //         }
    //     }
        
    //     return 0;
    // }

    private double getCachedVenueMinimum(String instrumentId, String venue) {
        String cacheKey = instrumentId + ":" + venue;
        return venueMinimumCache.computeIfAbsent(cacheKey, k -> 
        depthListener.getMinimumQuantityBySource(instrumentId, venue, config.getMinSize()));
    }
    
}