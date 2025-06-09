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
    
    private volatile GCBest latestGcBestCash;
    private volatile GCBest latestGcBestREG;
    private volatile double latestCashGcRate;
    private volatile double latestRegGcRate;
    private final Object gcBestLock = new Object();

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
    private static final long MIN_UPDATE_INTERVAL_MS = 1000; // 5 seconds minimum between updates

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
     * Cancel all active orders.
     * Used when disabling the market maker.
     */
    public boolean cancelAllOrders(long timeoutMs) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("cancelAllOrders: Cancelling all market maker orders");
        }

        // Disable market maker to prevent new orders
        this.enabled = false;
        
        // Check if we have any active quotes to cancel
        if (activeQuotes.isEmpty()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("No active quotes to cancel");
            }
            return true;
        }

        int totalOrderCount = 0;
        
        // Count the active orders that need cancellation
        for (ActiveQuote quote : activeQuotes.values()) {
            if (quote.getBidOrder() != null && !quote.getBidOrder().isDead()) {
                totalOrderCount++;
            }
            if (quote.getAskOrder() != null && !quote.getAskOrder().isDead()) {
                totalOrderCount++;
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Found {} active orders to cancel", totalOrderCount);
        }

        if (totalOrderCount == 0) {
            return true;
        }
        
        // Begin the cancellation process
        try {
            // Check MKV availability first
            boolean mkvAvailable = false;
            try {
                MkvPublishManager pm = Mkv.getInstance().getPublishManager();
                mkvAvailable = (pm != null);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Error checking MKV availability: {}", e.getMessage());
                }
                return false;
            }

            if (!mkvAvailable) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("MKV not available, cannot cancel orders properly");
                }
                return false;
            }
            
            // Start tracking cancelled orders
            final Set<String> pendingCancellations = ConcurrentHashMap.newKeySet();
            int cancelRequestsSent = 0;
            
            // Send cancel requests for all active orders
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Sending cancel requests for all active orders");
            }
            for (ActiveQuote quote : new ArrayList<>(activeQuotes.values())) {
                MarketOrder bidOrder = quote.getBidOrder();
                if (bidOrder != null && !bidOrder.isDead() && bidOrder.getOrderId() != null) {
                    pendingCancellations.add(bidOrder.getOrderId());
                    try {
                        LOGGER.info("Cancelling bid order: {}", bidOrder.getOrderId());
                        cancelOrder(bidOrder, quote.getId());
                        cancelRequestsSent++;
                    } catch (Exception e) {
                        if (LOGGER.isErrorEnabled()) {
                            LOGGER.error("Error cancelling bid order {}: {}", 
                                bidOrder.getOrderId(), e.getMessage());
                        }
                    }
                }

                MarketOrder askOrder = quote.getAskOrder();
                if (askOrder != null && !askOrder.isDead() && askOrder.getOrderId() != null) {
                    pendingCancellations.add(askOrder.getOrderId());
                    try {
                        LOGGER.info("Cancelling ask order: {}", askOrder.getOrderId());
                        cancelOrder(askOrder, quote.getId());
                        cancelRequestsSent++;
                    } catch (Exception e) {
                        if (LOGGER.isErrorEnabled()) {
                            LOGGER.error("Error cancelling ask order {}: {}", 
                                askOrder.getOrderId(), e.getMessage());
                        }
                    }
                }
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Sent {} cancel requests, waiting for confirmation...", cancelRequestsSent);
            }

            // Now wait until timeout or all orders are cancelled
            long startTime = System.currentTimeMillis();
            long endTime = startTime + timeoutMs;
            
            while (!pendingCancellations.isEmpty() && System.currentTimeMillis() < endTime) {
                // Check if any of the pending cancellations are no longer in the active quotes
                for (Iterator<String> it = pendingCancellations.iterator(); it.hasNext();) {
                    String orderId = it.next();
                    boolean stillActive = false;
                    
                    // Check if this order is still in any active quote
                    for (ActiveQuote quote : activeQuotes.values()) {
                        MarketOrder bidOrder = quote.getBidOrder();
                        MarketOrder askOrder = quote.getAskOrder();
                        
                        if ((bidOrder != null && orderId.equals(bidOrder.getOrderId())) ||
                            (askOrder != null && orderId.equals(askOrder.getOrderId()))) {
                            stillActive = true;
                            break;
                        }
                    }
                    
                    if (!stillActive) {
                        // Order is no longer active, remove from pending list
                        it.remove();
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Order cancellation confirmed: {}", orderId);
                        }
                    }
                }

                // Log progress every second
                if (System.currentTimeMillis() - startTime > 1000) {
                    startTime = System.currentTimeMillis(); // Reset for next interval
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Waiting for {} order cancellations to complete...", 
                            pendingCancellations.size());
                    }
                }

                // Sleep briefly to avoid CPU spinning
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Interrupted while waiting for order cancellations");
                    }
                    break;
                }
            }
            
            boolean success = pendingCancellations.isEmpty();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Order cancellation completed: {} of {} orders confirmed cancelled{}",
                    cancelRequestsSent - pendingCancellations.size(),
                    cancelRequestsSent,
                    success ? "" : " (TIMEOUT OCCURRED)");
            }

            return success;
            
        } catch (Exception e) {
            LOGGER.error("Error in cancelAllOrders: {}", e.getMessage(), e);
            return false;
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
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(
                "Order dead notification: reqId={}, orderId={}",
                order.getMyReqId(), order.getOrderId());
            }
            // Find the active quote that contains this order
            for (ActiveQuote quote : activeQuotes.values()) {
                MarketOrder bidOrder = quote.getBidOrder();
                MarketOrder askOrder = quote.getAskOrder();
                
                if (bidOrder != null && bidOrder.getMyReqId() == order.getMyReqId()) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Order dead was a bid for {}", quote.getId());
                    }
                    quote.setBidOrder(null, null, 0);
                }
                
                if (askOrder != null && askOrder.getMyReqId() == order.getMyReqId()) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Order dead was an ask for {}", quote.getId());
                    }
                    quote.setAskOrder(null, null, 0);
                }
            }

            // Remove any quotes that are now empty
            activeQuotes.values().removeIf(quote -> quote.getBidOrder() == null && quote.getAskOrder() == null);

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

        synchronized(gcBestLock) {
            // Check for Cash GC changes
            if (gcBestCash != null) {
                if (latestGcBestCash == null) {
                    significantCashGcChange = true;
                } else {
                    // Detect significant changes (e.g., > 1 bp)
                    significantCashGcChange = 
                        Math.abs(gcBestCash.getBid() - latestGcBestCash.getBid()) >= 0.02 || 
                        Math.abs(gcBestCash.getAsk() - latestGcBestCash.getAsk()) >= 0.02;
                }
                this.latestGcBestCash = gcBestCash;
                this.latestCashGcRate = cash_gc;
            }
            
            // Check for REG GC changes
            if (gcBestREG != null) {
                if (latestGcBestREG == null) {
                    significantRegGcChange = true;
                } else {
                    // Detect significant changes (e.g., > 0.5 bp)
                    significantRegGcChange = 
                        Math.abs(gcBestREG.getBid() - latestGcBestREG.getBid()) >= 0.02 || 
                        Math.abs(gcBestREG.getAsk() - latestGcBestREG.getAsk()) >= 0.02;
                }
                this.latestGcBestREG = gcBestREG;
                this.latestRegGcRate = reg_gc;
            }
        }

        // Process based on term code - avoid string operations
        boolean isCash = id.endsWith("C_Fixed");
        processMarketUpdate(best, isCash ? gcBestCash : gcBestREG);
        
        // If GC rates changed significantly, update all GC-based quotes
        if (significantCashGcChange) {
            LOGGER.info("Significant Cash GC rate change detected, updating GC-based quotes");
            onGcRateUpdate("C", gcBestCash);
        }
        
        if (significantRegGcChange) {
            LOGGER.info("Significant REG GC rate change detected, updating GC-based quotes");
            onGcRateUpdate("REG", gcBestREG);
        }

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
            String id = best.getId(); // This should be the instrument ID from VMO.CM_INSTRUMENT
            
            if (id == null || id.isEmpty()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("processMarketUpdate: Empty best ID, skipping");
                }
                return;
            }
            
            // Check if this bond is in our tracked instruments
            if (!trackedInstruments.contains(id)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("processMarketUpdate: Bond not in tracked instruments: {}", id);
                }
                return;
            }
            
            // Get existing quote status if any
            ActiveQuote existingQuote = activeQuotes.get(id);
            
            // Skip processing for minimal market movements that don't affect us
            if (best != null && existingQuote != null) {
                boolean bidFromTargetVenue = isTargetVenue(best.getBidSrc());
                boolean askFromTargetVenue = isTargetVenue(best.getAskSrc());
                
                // If neither side is from a venue we care about, skip processing
                if (!bidFromTargetVenue && !askFromTargetVenue) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Skipping update - neither side from target venue: {}/{}", 
                            best.getBidSrc(), best.getAskSrc());
                    }
                    return;
                }
                
                // If prices haven't moved significantly, skip processing
                if ((bidFromTargetVenue && existingQuote.isMarketBasedBid() && 
                    Math.abs(best.getBid() - existingQuote.getBidPrice()) < 0.01) ||
                    (askFromTargetVenue && existingQuote.isMarketBasedAsk() && 
                    Math.abs(best.getAsk() - existingQuote.getAskPrice()) < 0.01)) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Skipping update - price change below threshold");
                    }
                    return;
                }
            }

            // Determine term code from instrument ID
            String termCode;
            if (id.endsWith("C_Fixed")) {
                termCode = "C";
            } else if (id.endsWith("REG_Fixed")) {
                termCode = "REG";
            } else {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("processMarketUpdate: Unsupported instrument type: {}", id);
                }
                return;
            }
            
            boolean hasActiveBid = false;
            boolean hasActiveAsk = false;
            
            if (existingQuote != null) {
                MarketOrder bidOrder = existingQuote.getBidOrder();
                MarketOrder askOrder = existingQuote.getAskOrder();
                
                hasActiveBid = (bidOrder != null && !bidOrder.isDead());
                hasActiveAsk = (askOrder != null && !askOrder.isDead());
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Existing quote status for {}: bidActive={}, askActive={}", 
                        id, hasActiveBid, hasActiveAsk);
                }
            }
            
            // Get the native instrument ID
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    id, config.getMarketSource(), false);
            }
            
            if (nativeInstrument == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("No native instrument ID found for {}", id);
                }
                return;
            }
            
            // Use unified pricing model to calculate prices
            PricingDecision decision = calculateUnifiedPrices(id, termCode, best, gcBest);
            
            // Apply pricing decision
            if (existingQuote == null) {
                // Create new quotes if we have valid prices
                if (decision.hasBid || decision.hasAsk) {
                    existingQuote = new ActiveQuote(id);
                    activeQuotes.put(id, existingQuote);
                    
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Creating new quotes for bond: {}", id);
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
                                id, remainingMs);
                        }
                        updateBid = false;
                    }

                    if (updateBid) {
                        MarketOrder updatedBid = updateOrder(
                            id, nativeInstrument, "Buy", decision.bidPrice, 
                            decision.bidSource, hasActiveBid ? existingQuote.getBidOrder() : null);
                        
                        if (updatedBid != null) {
                            existingQuote.setBidOrder(updatedBid, decision.bidSource, decision.bidPrice);
                            // Transfer the source type flags
                            existingQuote.isGcBasedBid = decision.isGcBasedBid;
                            existingQuote.isMarketBasedBid = decision.isMarketBasedBid;
                            
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Updated bid for {}: price={}, source={}, isGcBased={}", 
                                    id, decision.bidPrice, decision.bidSource, decision.isGcBasedBid);
                            }
                        }
                    }
                } else if (hasActiveBid) {
                    // Cancel bid if we have no valid price
                    cancelOrder(existingQuote.getBidOrder(), id);
                    existingQuote.setBidOrder(null, null, 0);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Cancelled bid for {} - no valid pricing reference", id);
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
                                id, remainingMs);
                        }
                        updateAsk = false;
                    }
                    
                    if (updateAsk) {
                        MarketOrder updatedAsk = updateOrder(
                            id, nativeInstrument, "Sell", decision.askPrice, 
                            decision.askSource, hasActiveAsk ? existingQuote.getAskOrder() : null);
                        
                        if (updatedAsk != null) {
                            existingQuote.setAskOrder(updatedAsk, decision.askSource, decision.askPrice);
                            // Transfer the source type flags
                            existingQuote.isGcBasedAsk = decision.isGcBasedAsk;
                            existingQuote.isMarketBasedAsk = decision.isMarketBasedAsk;
                            
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Updated ask for {}: price={}, source={}, isGcBased={}", 
                                    id, decision.askPrice, decision.askSource, decision.isGcBasedAsk);
                            }
                        }
                    }
                } else if (hasActiveAsk) {
                    // Cancel ask if we have no valid price
                    cancelOrder(existingQuote.getAskOrder(), id);
                    existingQuote.setAskOrder(null, null, 0);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Cancelled ask for {} - no valid pricing reference", id);
                    }
                }
            }
            
            processedUpdateCounter.incrementAndGet();
            
        } catch (Exception e) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Error processing market update: {}", e.getMessage(), e);
        }
    }
}

/**
 * Handle GC rate updates
 * Called when GC rates change significantly
 */
public void onGcRateUpdate(String termCode, GCBest newGcBest) {
    if (newGcBest == null || !enabled) {
        return;
    }
    
    if (LOGGER.isInfoEnabled()) {
        LOGGER.info("GC rate update for {}: bid={}, ask={}", 
            termCode, newGcBest.getBid(), newGcBest.getAsk());
    }
    
    // Store the updated GC rates
    if ("C".equals(termCode)) {
        synchronized(gcBestLock) {
            latestGcBestCash = newGcBest;
            latestCashGcRate = newGcBest.getBid(); // Or some average/mid if preferred
        }
    } else if ("REG".equals(termCode)) {
        synchronized(gcBestLock) {
            latestGcBestREG = newGcBest;
            latestRegGcRate = newGcBest.getBid(); // Or some average/mid if preferred
        }
    } else {
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn("Unknown term code in GC update: {}", termCode);
        }
        return;
    }
    
    // Only update GC-based orders
    updateGcBasedQuotesOnly(termCode, newGcBest);
}

/**
 * Update only GC-based quotes when GC rates change
 * @param termCode The term code (C or REG)
 * @param gcBest The new GC reference data
 */
public void updateGcBasedQuotesOnly(String termCode, GCBest gcBest) {
    if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Updating only GC-based quotes for term code: {}", termCode);
    }
    
    if (!enabled || gcBest == null) {
        return;
    }
    
    int updatedBids = 0;
    int updatedAsks = 0;
    
    for (Map.Entry<String, ActiveQuote> entry : activeQuotes.entrySet()) {
        String bondId = entry.getKey();
        ActiveQuote quote = entry.getValue();
        
        // Check if this is for the right term code
        if (bondId.endsWith(termCode + "_Fixed")) {
            // Only process quotes that are GC-based
            if (quote.isGcBasedBid() || quote.isGcBasedAsk()) {
                // Get the instrument ID that corresponds to this bond ID
                String nativeInstrument = null;
                if (depthListener != null) {
                    nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                        bondId, config.getMarketSource(), false);
                }
                
                if (nativeInstrument == null) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("No native instrument ID found for {}", bondId);
                    }
                    continue;
                }
                
                // Recalculate prices based only on GC data
                PricingDecision decision = calculateGcOnlyPrices(bondId, termCode, gcBest);
                
                // Update only the sides that are GC-based
                if (decision.hasBid && quote.isGcBasedBid()) {
                    MarketOrder bidOrder = quote.getBidOrder();
                    boolean hasActiveBid = (bidOrder != null && !bidOrder.isDead());
                    
                    // Check if price changed enough to update
                    double currentPrice = quote.getBidPrice();
                    boolean needsUpdate = !hasActiveBid || 
                                         Math.abs(currentPrice - decision.bidPrice) >= 0.02;
                    
                    if (needsUpdate) {
                        MarketOrder updatedBid = updateOrder(
                            bondId, nativeInstrument, "Buy", decision.bidPrice, 
                            decision.bidSource, hasActiveBid ? bidOrder : null);
                        
                        if (updatedBid != null) {
                            quote.setBidOrder(updatedBid, decision.bidSource, decision.bidPrice);
                            updatedBids++;
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Updated GC-based bid for {}: {} -> {}", 
                                    bondId, currentPrice, decision.bidPrice);
                            }
                        }
                    }
                }
                
                // Handle ask side similarly
                if (decision.hasAsk && quote.isGcBasedAsk()) {
                    MarketOrder askOrder = quote.getAskOrder();
                    boolean hasActiveAsk = (askOrder != null && !askOrder.isDead());
                    
                    // Check if price changed enough to update
                    double currentPrice = quote.getAskPrice();
                    boolean needsUpdate = !hasActiveAsk || 
                                         Math.abs(currentPrice - decision.askPrice) >= 0.02;
                    
                    if (needsUpdate) {
                        MarketOrder updatedAsk = updateOrder(
                            bondId, nativeInstrument, "Sell", decision.askPrice, 
                            decision.askSource, hasActiveAsk ? askOrder : null);
                        
                        if (updatedAsk != null) {
                            quote.setAskOrder(updatedAsk, decision.askSource, decision.askPrice);
                            updatedAsks++;
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Updated GC-based ask for {}: {} -> {}", 
                                    bondId, currentPrice, decision.askPrice);
                            }
                        }
                    }
                }
            }
        }
    }
    
    if (LOGGER.isInfoEnabled()) {
        LOGGER.info("GC-based update completed for {}: updated {} bids, {} asks", 
            termCode, updatedBids, updatedAsks);
    }
}

/**
 * Calculate prices based only on GC data
 */
private PricingDecision calculateGcOnlyPrices(String bondId, String termCode, GCBest gcBest) {
    PricingDecision decision = new PricingDecision();
    
    try {
        double spreadAdjustment = config.getDefaultIntraMarketSpread();
        
        // Only use GC bid/ask rates
        double gcBidRate = gcBest.getBid();
        double gcAskRate = gcBest.getAsk();
        
        // Calculate bid price if GC bid is available
        if (gcBidRate > 0) {
            decision.bidPrice = gcBidRate + spreadAdjustment;
            decision.bidSource = "GC_" + termCode;
            decision.hasBid = true;
            decision.isGcBasedBid = true;
        }
        
        // Calculate ask price if GC ask is available
        if (gcAskRate > 0) {
            decision.askPrice = gcAskRate - spreadAdjustment;
            decision.askSource = "GC_" + termCode;
            decision.hasAsk = true;
            decision.isGcBasedAsk = true;
        }
        
        // Check for inverted quotes
        if (decision.hasBid && decision.hasAsk && decision.bidPrice <= decision.askPrice) {
            double midPrice = (decision.bidPrice + decision.askPrice) / 2;
            double minSpread = 0.02; // 2bp minimum spread
            
            decision.bidPrice = midPrice + (minSpread / 2);
            decision.askPrice = midPrice - (minSpread / 2);
        }
    } catch (Exception e) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Error calculating GC-only prices: {}", e.getMessage(), e);
        }
    }
    
    return decision;
}

    // /**
    //  * Process market data updates and update our quotes accordingly.
    //  * Called by the DepthListener when market data changes.
    //  * 
    //  * @param best The updated best prices
    //  */
    // public void processMarketUpdate(Best best, GCBest gcBest) {
    //     long updateCount = marketUpdateCounter.get();
        
    //     if (updateCount % 1000 == 0) {
    //         LOGGER.debug("Processed {} market updates", updateCount);
    //     }
    //     try {
    //         //String instrumentId = best.getInstrumentId();
    //         String Id = best.getId(); // This should be the instrument ID from VMO.CM_INSTRUMENT

    //         if (Id == null || Id.isEmpty()) {
    //             if (LOGGER.isDebugEnabled()) {
    //                 LOGGER.debug("processMarketUpdate: Empty best ID, skipping");
    //             }
    //             return;
    //         }


    //         // Check if this bond is in our tracked instruments
    //         if (!trackedInstruments.contains(Id)) {
    //             if (LOGGER.isInfoEnabled()) {
    //                 LOGGER.info("processMarketUpdate: Bond not in tracked instruments: {}", Id);
    //             }
    //             return;
    //         }

    //         // Check if we already have active quotes for this bond
    //         ActiveQuote existingQuote = activeQuotes.get(Id);
    //         boolean hasActiveBid = false;
    //         boolean hasActiveAsk = false;
            
    //         if (existingQuote != null) {
    //             // Check if we have active orders that are still alive
    //             MarketOrder bidOrder = existingQuote.getBidOrder();
    //             MarketOrder askOrder = existingQuote.getAskOrder();
                
    //             hasActiveBid = (bidOrder != null && !bidOrder.isDead());
    //             hasActiveAsk = (askOrder != null && !askOrder.isDead());
    //             if (LOGGER.isInfoEnabled()) {
    //                 LOGGER.info("Existing quote status for bond {}: activeBid={}, activeAsk={}", 
    //                     Id, hasActiveBid, hasActiveAsk);
    //             }
    //         }

    //         if (LOGGER.isDebugEnabled()) {
    //             LOGGER.debug("processMarketUpdate:  Processing market update for bond: {}", Id);
    //         }
            
    //         String termCode;
    //         if (Id.endsWith("C_Fixed")) {
    //             termCode = "C";
    //         } else if (Id.endsWith("REG_Fixed")) {
    //             termCode = "REG";
    //         } else {
    //             if (LOGGER.isWarnEnabled()) {
    //                 LOGGER.warn("processMarketUpdate: Unsupported instrument type for bond: {}", Id);
    //             }
    //             return; // Unsupported instrument type
    //         }
            
    //         // Determine if we have sufficient market data for symmetric quoting
    //         boolean hasValidMarketData = (best != null && 
    //                                     (isTargetVenue(best.getBidSrc()) || isTargetVenue(best.getAskSrc())));
    
    //         // Determine if we have sufficient GC data for GC-based quoting
    //         boolean hasValidGCData = (gcBest != null && (gcBest.getBid() > 0 || gcBest.getAsk() > 0));

    //         if (hasValidMarketData) {
    //             processSymmetricQuoting(best, termCode, Id, hasActiveBid, hasActiveAsk);
    //         }

    //         if (hasValidGCData) {
    //             processGCQuoting(gcBest, termCode, Id, hasActiveBid, hasActiveAsk);
    //         }

    //         if (LOGGER.isDebugEnabled()) {
    //             LOGGER.debug("processMarketUpdate: Market update processed for bond: {}", Id);
    //         }

    //     } catch (Exception e) {
    //         if (LOGGER.isErrorEnabled()) {
    //             LOGGER.error("Error processing market update: {}", e.getMessage(), e);
    //         }
    //     }
    // }

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
     * ActiveQuote represents a two-sided quote for a specific instrument.
     * It tracks the bid and ask orders and their reference sources.
     */
    public static class ActiveQuote {
        private final String Id;
        private MarketOrder bidOrder;
        private MarketOrder askOrder;
        private String bidReferenceSource;
        private String askReferenceSource;
        private double bidPrice;
        private double askPrice;
        private int bidUpdateFailureCount = 0;
        private int askUpdateFailureCount = 0;
        private long bidBackoffUntilTime = 0;
        private long askBackoffUntilTime = 0;

        private boolean isGcBasedBid = false;
        private boolean isGcBasedAsk = false;
        private boolean isMarketBasedBid = false;
        private boolean isMarketBasedAsk = false;

        public ActiveQuote(String Id) { this.Id = Id; }
       
        public String getId() { return Id;  }
        public MarketOrder getBidOrder() { return bidOrder; }
        public MarketOrder getAskOrder() { return askOrder;  }

        public void setBidOrder(MarketOrder order, String referenceSource, double price) {
            this.bidOrder = order;
            this.bidReferenceSource = referenceSource;
            this.bidPrice = price;
            this.isGcBasedBid = referenceSource != null && referenceSource.startsWith("GC_");
            this.isMarketBasedBid = referenceSource != null && !isGcBasedBid && 
                                !referenceSource.equals("DEFAULT");
        }
        
        public void setAskOrder(MarketOrder order, String referenceSource, double price) {
            this.askOrder = order;
            this.askReferenceSource = referenceSource;
            this.askPrice = price;
            this.isGcBasedAsk = referenceSource != null && referenceSource.startsWith("GC_");
            this.isMarketBasedAsk = referenceSource != null && !isGcBasedAsk && 
                                !referenceSource.equals("DEFAULT");
        }

        public String getBidReferenceSource() { return bidReferenceSource; }
        public String getAskReferenceSource() { return askReferenceSource; }
        public double getBidPrice() { return bidPrice; }
        public double getAskPrice() { return askPrice; }
        public boolean isGcBasedBid() { return isGcBasedBid; }
        public boolean isGcBasedAsk() { return isGcBasedAsk; }
        public boolean isMarketBasedBid() { return isMarketBasedBid; }
        public boolean isMarketBasedAsk() { return isMarketBasedAsk; }

        public int getBidUpdateFailureCount() { return bidUpdateFailureCount; }
        public int getAskUpdateFailureCount() { return askUpdateFailureCount; }
        public long getBidBackoffUntilTime() { return bidBackoffUntilTime; }
        public long getAskBackoffUntilTime() { return askBackoffUntilTime; }
        
        public void incrementBidFailure() {
            bidUpdateFailureCount++;
            // Calculate exponential backoff time (in milliseconds)
            long backoffMs = Math.min(300_000, (long)(1000 * Math.pow(2, bidUpdateFailureCount)));
            bidBackoffUntilTime = System.currentTimeMillis() + backoffMs;
        }
        
        public void incrementAskFailure() {
            askUpdateFailureCount++;
            // Calculate exponential backoff time (in milliseconds)
            long backoffMs = Math.min(300_000, (long)(1000 * Math.pow(2, askUpdateFailureCount)));
            askBackoffUntilTime = System.currentTimeMillis() + backoffMs;
        }
        
        public void resetBidFailure() {
            bidUpdateFailureCount = 0;
            bidBackoffUntilTime = 0;
        }
        
        public void resetAskFailure() {
            askUpdateFailureCount = 0;
            askBackoffUntilTime = 0;
        }
        
        public boolean isBidInBackoff() {
            return System.currentTimeMillis() < bidBackoffUntilTime;
        }
        
        public boolean isAskInBackoff() {
            return System.currentTimeMillis() < askBackoffUntilTime;
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
                    trackedInstruments.add(Id);
                    tryCreateOrUpdateMarkets(Id);
                    marketsCreated++;
                } else {
                    // Already tracking this bond - only update if necessary
                    ActiveQuote existingQuote = activeQuotes.get(Id);
                    if (existingQuote == null) {
                        // We're tracking the instrument but don't have quotes - create them
                        // Log the CUSIP we're about to create markets for
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Creating initial markets for Id: {}", Id);
                        }
                        tryCreateInitialMarkets(Id);
                        marketsUpdated++;
                    } else {
                        // Check if orders are still active
                        MarketOrder bidOrder = existingQuote.getBidOrder();
                        MarketOrder askOrder = existingQuote.getAskOrder();
                        
                        boolean bidActive = (bidOrder != null && !bidOrder.isDead());
                        boolean askActive = (askOrder != null && !askOrder.isDead());
                        
                        if (!bidActive && !askActive) {
                            // Both sides need refresh, use the full validator
                            if (!existingQuote.isBidInBackoff() && !existingQuote.isAskInBackoff()) {
                                validateExistingQuotes(Id, existingQuote);
                                marketsUpdated++;
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info("Refreshed both sides for {}", Id);
                                }
                            } else {
                                // Handle sides individually to respect backoff
                                if (!existingQuote.isBidInBackoff()) {
                                    validateSingleSideQuote(Id, existingQuote, "Buy");
                                    marketsUpdated++;
                                    if (LOGGER.isInfoEnabled()) {
                                        LOGGER.info("Refreshed bid only for {} (ask in backoff)", Id);
                                    }
                                }
                                if (!existingQuote.isAskInBackoff()) {
                                    validateSingleSideQuote(Id, existingQuote, "Sell");
                                    marketsUpdated++;
                                    if (LOGGER.isInfoEnabled()) {
                                        LOGGER.info("Refreshed ask only for {} (bid in backoff)", Id);
                                    }
                                }
                            }
                        } else if (!bidActive) {
                            // Only refresh bid if not in backoff
                            if (!existingQuote.isBidInBackoff()) {
                                validateSingleSideQuote(Id, existingQuote, "Buy");
                                marketsUpdated++;
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info("Refreshed bid only for {}", Id);
                                }
                            } else {
                                marketsSkipped++;
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info("Skipped bid refresh for {} due to backoff", Id);
                                }
                            }
                        } else if (!askActive) {
                            // Only refresh ask if not in backoff
                            if (!existingQuote.isAskInBackoff()) {
                                validateSingleSideQuote(Id, existingQuote, "Sell");
                                marketsUpdated++;
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info("Refreshed ask only for {}", Id);
                                }
                            } else {
                                marketsSkipped++;
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info("Skipped ask refresh for {} due to backoff", Id);
                                }
                            }
                        } else {
                            // Both sides are active, skip
                            marketsSkipped++;
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Skipping market refresh for {}: both sides active", Id);
                            }
                        } 
                    }
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
     * Try to create initial markets for a newly eligible bond
     */
    private void tryCreateInitialMarkets(String Id) {
        try {
            if (Id == null) {
                LOGGER.warn("No instrument ID mapping found for bond: " + Id);
                return;
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Creating initial markets for instrument ID " + Id);
            }

            // Get the native instrument ID for FENICS using the correct instrument ID
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    Id, config.getMarketSource(), false); // Use instrumentId, not bondId
            }
            
            if (nativeInstrument == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("No instrument mapping found for instrument ID: " + Id +
                        " on " + config.getMarketSource());
                }
                return;
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Found native instrument: " + nativeInstrument + " for bond " + Id);
            }

            // Check if we already have quotes for this bond
            ActiveQuote existingQuote = activeQuotes.get(Id); // Still track by bondId
            if (existingQuote != null) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Already have quotes for bond " + Id);
                }
                return;
            }

            // Try to get current best prices to base our quotes on
            createDefaultMarkets(Id, nativeInstrument);

        } catch (Exception e) {
            LOGGER.error("Error creating initial markets for bond " + Id + ": " + e.getMessage(), e);
        }
    }

    /**
     * Create default markets when no reference prices are available
     */
    private void createDefaultMarkets(String Id, String nativeInstrument) {
        try {

            double bidPrice = getReferencePriceForBond(Id, "Buy");
            double askPrice = getReferencePriceForBond(Id, "Sell");

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Creating default markets for " + Id + 
                    " at " + bidPrice + "/" + askPrice);
            }

            // Place default orders
            placeOrder(Id, nativeInstrument, "Buy", bidPrice, "DEFAULT");
            placeOrder(Id, nativeInstrument, "Sell", askPrice, "DEFAULT");

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Default markets created for " + Id + 
                    ": Bid=" + bidPrice + ", Ask=" + askPrice);
            }

        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error creating default markets for " + Id + ": " + e.getMessage(), e);
            }
        }
    }

        /**
     * Try to create or update markets for an instrument using the unified pricing model
     */
    private void tryCreateOrUpdateMarkets(String id) {
        try {
            if (id == null) {
                LOGGER.error("tryCreateOrUpdateMarkets: id is null, cannot create markets");
                return;
            }
            
            // Get term code from instrument ID
            String termCode;
            if (id.endsWith("C_Fixed")) {
                termCode = "C";
            } else if (id.endsWith("REG_Fixed")) {
                termCode = "REG";
            } else {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("tryCreateOrUpdateMarkets: Unsupported instrument type: {}", id);
                }
                return;
            }
            
            // Get the native instrument ID
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    id, config.getMarketSource(), false);
            }
            
            if (nativeInstrument == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("No native instrument ID found for {}", id);
                }
                return;
            }
            
            // Get the appropriate GC reference data
            GCBest gcBest = "C".equals(termCode) ? getLatestGcBestCash() : getLatestGcBestREG();
            
            // Use unified pricing model (with no market data available)
            PricingDecision decision = calculateUnifiedPrices(id, termCode, null, gcBest);
            
            // Check if existing quotes need validation
            ActiveQuote existingQuote = activeQuotes.get(id);
            if (existingQuote == null) {
                // Create new quotes if we have valid prices
                if (decision.hasBid || decision.hasAsk) {
                    existingQuote = new ActiveQuote(id);
                    activeQuotes.put(id, existingQuote);
                    
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Creating initial markets for bond: {}", id);
                    }
                    
                    if (decision.hasBid) {
                        placeOrder(id, nativeInstrument, "Buy", decision.bidPrice, decision.bidSource);
                    }
                    
                    if (decision.hasAsk) {
                        placeOrder(id, nativeInstrument, "Sell", decision.askPrice, decision.askSource);
                    }
                } else {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("No valid pricing available for new bond: {}", id);
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
                    placeOrder(id, nativeInstrument, "Buy", decision.bidPrice, decision.bidSource);
                }
                
                if (!askActive && decision.hasAsk) {
                    placeOrder(id, nativeInstrument, "Sell", decision.askPrice, decision.askSource);
                }
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error creating/updating markets for bond {}: {}", id, e.getMessage(), e);
            }
        }
    }

    // /**
    //  * Try to create or update markets for an instrument
    //  */
    // private void tryCreateOrUpdateMarkets(String Id) {
    //     try {
    //         if (Id == null) {
    //            LOGGER.error("tryCreateOrUpdateMarkets: Id is null, cannot create markets");
    //             return;
    //         }
            
    //         // Get the instrument ID that corresponds to this bond ID
    //         if (Id == null) {
    //             if (LOGGER.isWarnEnabled()) {
    //                 LOGGER.warn("No instrument ID mapping found for bond: {}, skipping", Id);
    //             }
    //             return;
    //         }

    //         // Check if existing quotes need validation
    //         ActiveQuote existingQuote = activeQuotes.get(Id);
    //         if (existingQuote == null) {
    //             tryCreateInitialMarkets(Id);
    //         } else {
    //             // Check if any existing orders are dead and need replacement
    //             MarketOrder bidOrder = existingQuote.getBidOrder();
    //             MarketOrder askOrder = existingQuote.getAskOrder();
                
    //             boolean bidActive = (bidOrder != null && !bidOrder.isDead());
    //             boolean askActive = (askOrder != null && !askOrder.isDead());
                
    //             // Only validate if at least one side needs attention
    //             if (!bidActive || !askActive) {
    //                 if (LOGGER.isInfoEnabled()) {
    //                     LOGGER.info("Validating quotes for {}: bidActive={}, askActive={}", 
    //                         Id, bidActive, askActive);
    //                 }
    //                 validateExistingQuotes(Id, existingQuote);
    //             } else {
    //                 if (LOGGER.isDebugEnabled()) {
    //                     LOGGER.debug("Both sides active for {}, skipping update", Id);
    //                 }
    //             }
    //         }
    //     } catch (Exception e) {
    //         if (LOGGER.isErrorEnabled()) {
    //             LOGGER.error("Error creating/updating markets for bond {}: {}", Id, e.getMessage(), e);
    //         }
    //     }
    // }


    /**
     * Validate and refresh a specific side of a quote
     */
    private void validateSingleSideQuote(String Id, ActiveQuote quote, String side) {
        try {
            // Check if order is still alive
            MarketOrder order = "Buy".equals(side) ? quote.getBidOrder() : quote.getAskOrder();
            boolean needNewOrder = (order == null || order.isExpired() || order.isDead());
            
            // Check if we're in backoff period
            boolean inBackoff = "Buy".equals(side) ? quote.isBidInBackoff() : quote.isAskInBackoff();
            if (inBackoff) {
                long backoffUntil = "Buy".equals(side) ? quote.getBidBackoffUntilTime() : quote.getAskBackoffUntilTime();
                long remainingMs = backoffUntil - System.currentTimeMillis();
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Skip refresh due to backoff for {}/{}: {}ms remaining", 
                        Id, side, remainingMs);
                }
                return;
            }
            
            if (needNewOrder) {
                // Get the native instrument ID that corresponds to this bond ID
                String nativeInstrument = null;
                if (depthListener != null) {
                    nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                        Id, config.getMarketSource(), false);
                }
                
                if (nativeInstrument == null) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Cannot refresh {} quote - no instrument mapping for: {}", side, Id);
                    }
                    return;
                }

                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Refreshing {} quote for bond {}", side, Id);
                }

                double price;
                String source;
                if ("Buy".equals(side)) {
                    price = quote.getBidPrice();
                    source = quote.getBidReferenceSource();
                    if (price <= 0) {
                        price = getReferencePriceForBond(Id, side);
                        source = "DEFAULT";
                    }
                } else {
                    price = quote.getAskPrice();
                    source = quote.getAskReferenceSource();
                    if (price <= 0) {
                        price = getReferencePriceForBond(Id, side);
                        source = "DEFAULT";
                    }
                }
                
                if (price > 0) {
                    placeOrder(Id, nativeInstrument, side, price, source != null ? source : "DEFAULT");
                }
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error validating {} quote for bond {}: {}", side, Id, e.getMessage(), e);
            }
        }
    }

    /**
     * Validate and refresh existing quotes
     */
    private void validateExistingQuotes(String Id, ActiveQuote quote) {
        try {
            // Check if orders are still alive
            MarketOrder bidOrder = quote.getBidOrder();
            MarketOrder askOrder = quote.getAskOrder();
            
            boolean needNewBid = (bidOrder == null || bidOrder.isExpired() || bidOrder.isDead());
            boolean needNewAsk = (askOrder == null || askOrder.isExpired() || askOrder.isDead());
            
            if (needNewBid || needNewAsk) {
                // Get the instrument ID that corresponds to this bond ID
                if (Id == null) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Cannot refresh quotes - no instrument ID mapping for bond: " + Id);
                    }
                    return;
                }

                String nativeInstrument = null;
                if (depthListener != null) {
                    nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                        Id, config.getMarketSource(), false); // Use instrumentId
                }
                
                if (nativeInstrument == null) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Cannot refresh quotes - no instrument mapping for instrument ID: " + Id +
                            " (bond: " + Id + ")");
                    }
                    return;
                }

                if (needNewBid) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Refreshing bid quote for bond " + Id);
                    }

                    double bidPrice = quote.getBidPrice();
                    if (bidPrice <= 0) {
                        bidPrice = getReferencePriceForBond(Id, "Buy");
                    }
                    
                    if (bidPrice > 0) {
                        placeOrder(Id, nativeInstrument, "Buy",
                                bidPrice, quote.getBidReferenceSource() != null ?
                                quote.getBidReferenceSource() : "DEFAULT");
                    }
                }
                
                if (needNewAsk) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Refreshing ask quote for bond " + Id);
                    }

                    double askPrice = quote.getAskPrice();
                    if (askPrice <= 0) {
                        askPrice = getReferencePriceForBond(Id, "Sell");
                    }
                    
                    if (askPrice > 0) {
                        placeOrder(Id, nativeInstrument, "Sell",
                                askPrice, quote.getAskReferenceSource() != null ?
                                quote.getAskReferenceSource() : "DEFAULT");
                    }
                }
            }
            
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error validating quotes for bond " + Id + ": " + e.getMessage(), e);
            }
        }
    }

    private double getReferencePriceForBond(String Id, String side) {
        double lastGCRate = 0.0;
        GCBest lastGCBest = null;
        if (Id.endsWith("C_Fixed")) {
            synchronized(gcBestLock) {
                lastGCBest = latestGcBestCash;
                lastGCRate = latestCashGcRate;
            }
        } else if (Id.endsWith("REG_Fixed")) {
            synchronized(gcBestLock) {
                lastGCBest = latestGcBestREG;
                lastGCRate = latestRegGcRate;
            }
        }
        
        if (side.equals("Buy")){
            double bid = 0;
            if (lastGCBest == null || lastGCRate <= 0) {
                LOGGER.warn("No valid GC rate available for Buy side on bond: " + Id);
                return -9999; // No valid rate, cannot quote
            } else if (lastGCBest != null) {
                bid = lastGCBest.getBid();
                if (bid == 0 && lastGCRate > 0) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("No valid bid available, using last GC traded value for pricing on Buy side for bond: " + Id);
                    }
                    return lastGCRate + 20;
                } else if (bid != 0) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Using last GC bid value for pricing on Buy side for bond: " + Id);
                    }
                    return bid + 1;
                }
            }
        }
        
        double gcAsk = 0.0;
        if (side.equals("Sell")){
            double ask = 0.0;
            if (lastGCBest == null || lastGCRate <= 0) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("No valid GC rate available for Sell side on bond: " + Id);
                }
                gcAsk = -9999; // No valid rate, cannot quote
            } else if (lastGCBest != null) {
                ask = lastGCBest.getAsk();
                if (ask == 0 && lastGCRate > 0) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Using last GC traded value for pricing on Sell side for bond: " + Id);
                    }
                    gcAsk = lastGCRate;
                } else if (ask != 0) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Using last GC ask value for pricing on Sell side for bond: " + Id);
                    }
                    gcAsk = ask;
                }
            }
        }

        double spread = 0.0;
        try {
            String cusip = Id.substring(0, Math.min(Id.length(), 9));
            String termCode = Id.endsWith("C_Fixed") ? "C" : "REG";
            // Get bond data from the eligibility listener
            Map<String, Object> bondData = bondEligibilityListener.getBondData(cusip);
            if (bondData != null) {
                // Try to get MFA rate information
                Object mfaData = bondData.get("mfaData");
                if (mfaData != null && mfaData instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> mfaInfo = (Map<String, Object>) mfaData;

                    Map<String, Object> cToday = getMfaRecord(mfaInfo, "MFA_" + cusip + "_C_Fixed_TODAY");
                    Map<String, Object> cYest = getMfaRecord(mfaInfo, "MFA_" + cusip + "_C_Fixed_YEST");
                    Map<String, Object> regToday = getMfaRecord(mfaInfo, "MFA_" + cusip + "_REG_Fixed_TODAY");
                    Map<String, Object> regYest = getMfaRecord(mfaInfo, "MFA_" + cusip + "_REG_Fixed_YEST");
                    if (termCode.equals("C")) {
                        if (cToday != null) {
                            Object rateAvg = cToday.get("SpreadGCAvg");
                                if (rateAvg != null) {
                                    try {
                                        spread = 2 * Double.parseDouble(rateAvg.toString());
                                        if (LOGGER.isInfoEnabled()) {
                                            LOGGER.info("Using MFA GC Spread for {}: {}", cusip, spread);
                                        }
                                    } catch (NumberFormatException e) {
                                        if (LOGGER.isWarnEnabled()) {
                                            LOGGER.warn("Invalid MFA rate format for {}: {}", cusip, rateAvg);
                                        }
                                    }
                                }
                            } else if (cYest != null) {
                                Object rateAvg = cYest.get("SpreadGCAvg");
                                if (rateAvg != null) {
                                    try {
                                        spread = 2 * Double.parseDouble(rateAvg.toString());
                                        if (LOGGER.isInfoEnabled()) {
                                            LOGGER.info("Using MFA GC Spread for {}: {}", cusip, spread);
                                        }
                                    } catch (NumberFormatException e) {
                                        if (LOGGER.isWarnEnabled()) {
                                            LOGGER.warn("Invalid MFA rate format for {}: {}", cusip, rateAvg);
                                        }
                                    }
                                }
                            } else if (regYest != null) {
                                Object rateAvg = regYest.get("SpreadGCAvg");
                                if (rateAvg != null) {
                                    try {
                                        spread = 2 * Double.parseDouble(rateAvg.toString());
                                        if (LOGGER.isInfoEnabled()) {
                                            LOGGER.info("Using MFA GC Spread for {}: {}", cusip, spread);
                                        }
                                    } catch (NumberFormatException e) {
                                        if (LOGGER.isWarnEnabled()) {
                                            LOGGER.warn("Invalid MFA rate format for {}: {}", cusip, rateAvg);
                                        }
                                    }
                                }
                            }
                            if (spread > 0) {
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info("Using calculated spread for {}: {}", cusip, spread);
                                }
                                return gcAsk - spread;
                            }
                        } else if (termCode.equals("REG")){
                            if (regToday != null) {
                                Object rateAvg = regToday.get("SpreadGCAvg");
                                if (rateAvg != null) {
                                    try {
                                        spread = 2 * Double.parseDouble(rateAvg.toString());
                                        if (LOGGER.isInfoEnabled()) {
                                            LOGGER.info("Using MFA GC Spread for {}: {}", cusip, spread);
                                        }
                                    } catch (NumberFormatException e) {
                                        if (LOGGER.isWarnEnabled()) {
                                            LOGGER.warn("Invalid MFA rate format for {}: {}", cusip, rateAvg);
                                        }
                                    }
                                }
                            } else if (cToday != null) {
                                Object rateAvg = cToday.get("SpreadGCAvg");
                                if (rateAvg != null) {
                                    try {
                                        spread = 2 * Double.parseDouble(rateAvg.toString());
                                        if (LOGGER.isInfoEnabled()) {
                                            LOGGER.info("Using MFA GC Spread for {}: {}", cusip, spread);
                                        }
                                    } catch (NumberFormatException e) {
                                        if (LOGGER.isWarnEnabled()) {
                                            LOGGER.warn("Invalid MFA rate format for {}: {}", cusip, rateAvg);
                                        }
                                    }
                                }
                            }
                            if (spread > 0) {
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info("Using calculated spread for {}: {}", cusip, spread);
                                }
                                return gcAsk - spread;
                            }
                        }
                    } 
                } else {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("No bond data available for {} to calculate reference price", cusip);
                    }
                    return -9999; // No bond data, cannot quote
                }
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Error accessing MFA data for {}: {}", Id, e.getMessage());
                }
            }
        return -9999;
    }

    /**
     * Helper method to safely get MFA record
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> getMfaRecord(Map<String, Object> mfaInfo, String key) {
        if (mfaInfo == null) return null;
        Object obj = mfaInfo.get(key);
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        }
        return null;
    }

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
                    quote.setBidOrder(order, referenceSource, price);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("MarketMaker: Bid quote updated for {}: price=%.4f, source={}, reqId={}", 
                            cusip, price, referenceSource, order.getMyReqId());
                    }
                } else {
                    quote.setAskOrder(order, referenceSource, price);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("MarketMaker: Ask quote updated for {}: price=%.4f, source={}, reqId={}", 
                            cusip, price, referenceSource, order.getMyReqId());
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
                    // Reset failure counter on success
                    if (quote != null) {
                        if ("Buy".equals(side)) {
                            quote.resetBidFailure();
                        } else {
                            quote.resetAskFailure();
                        }
                    }
                    lastOrderUpdateTime.put(bondId + ":" + side, currentTime);
                    return updatedOrder;
                } else {
                    if (LOGGER.isErrorEnabled()) {
                        LOGGER.error("Failed to update {} order for {}", side, bondId);
                    }
                                    // Increment failure counter and apply backoff
                    if (quote != null) {
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
                // Reset failure counter on success
                if (quote != null) {
                    if ("Buy".equals(side)) {
                        quote.resetBidFailure();
                    } else {
                        quote.resetAskFailure();
                    }
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

            // Check if MKV is still available in a more robust way
            try {
                // Check both Mkv instance and PublishManager availability
                Mkv mkv = Mkv.getInstance();
                if (mkv == null) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Cannot cancel order {} - MKV not available", order.getOrderId());
                    }
                    return;
                }

                MkvPublishManager pm;
                try {
                    pm = mkv.getPublishManager();
                    if (pm == null) {
                        if (LOGGER.isWarnEnabled()) {
                            LOGGER.warn("Cannot cancel order {} - MKV PublishManager not available", order.getOrderId());
                        }
                        return;
                    }
                } catch (IllegalStateException e) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Cannot cancel order {} - MKV API is stopped", order.getOrderId());
                    }
                    return;
                }
            } catch (IllegalStateException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Cannot cancel order {} - MKV API is stopped", order.getOrderId());
                }
                return;
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Cancelling order: orderId={}, reqId={}, cusip={}", 
                    order.getOrderId(), order.getMyReqId(), cusip);
            }

            String traderId = orderManager.getTraderForVenue(config.getMarketSource());

            try {
                MarketOrder cancelOrder = MarketOrder.orderCancel(
                    config.getMarketSource(), 
                    traderId, 
                    order.getOrderId(), 
                    this
                );
                
                if (cancelOrder != null) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Cancel request sent: orderId={}", order.getOrderId());
                    }
                } else {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Failed to cancel order: orderId={}", order.getOrderId());
                    }
                }
            } catch (IllegalStateException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Cannot cancel order {} - MKV API is stopped", order.getOrderId());
                }
                return;
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error cancelling order: {}", e.getMessage(), e);
            }
            return;
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
                gcBidRate = gcBest.getBid();
                gcAskRate = gcBest.getAsk();
                validGcBid = gcBidRate > 0;
                validGcAsk = gcAskRate > 0;
            }
            
            // Step 3: Look up MFA spread data for bond
            double mfaSpread = getMfaSpreadForBond(bondId, termCode);
            
            // Step 4: Calculate bid price using hierarchy of sources
            if (validBidSource) {
                // Primary: Use market data (venue) as reference with adjustment
                decision.bidPrice = referenceBid + spreadAdjustment;
                decision.bidSource = bidSource;
                decision.hasBid = true;
                decision.isMarketBasedBid = true;
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Calculated bid using market data: {} + {} = {}", 
                        referenceBid, spreadAdjustment, decision.bidPrice);
                }
            } else if (validGcBid) {
                // Secondary: Use GC bid as reference with adjustment
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
            
            // Step 5: Calculate ask price using hierarchy of sources
            if (validAskSource) {
                // Primary: Use market data (venue) as reference with adjustment
                decision.askPrice = referenceAsk - spreadAdjustment;
                decision.askSource = askSource;
                decision.hasAsk = true;
                decision.isMarketBasedAsk = true;
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Calculated ask using market data: {} - {} = {}", 
                        referenceAsk, spreadAdjustment, decision.askPrice);
                }
            } else if (validGcAsk) {
                // Secondary: Use GC ask as reference with adjustment
                decision.askPrice = gcAskRate - spreadAdjustment;
                decision.askSource = "GC_" + termCode;
                decision.hasAsk = true;
                decision.isGcBasedAsk = true;
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Calculated ask using GC data: {} - {} = {}", 
                        gcAskRate, spreadAdjustment, decision.askPrice);
                }
            } else if (decision.hasBid && mfaSpread > 0) {
                // If we have a valid bid and MFA spread, derive ask from bid + spread
                decision.askPrice = decision.bidPrice + mfaSpread;
                decision.askSource = decision.bidSource + "_MFA_DERIVED";
                decision.hasAsk = true;
                decision.isGcBasedAsk = decision.isGcBasedBid; // Inherit source type
                decision.isMarketBasedAsk = decision.isMarketBasedBid;
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Derived ask using bid + MFA spread: {} + {} = {}", 
                        decision.bidPrice, mfaSpread, decision.askPrice);
                }
            } else {
                // No valid reference for ask
                decision.hasAsk = false;
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

    /**
     * Gets the MFA spread for a bond
     * @param bondId The bond ID
     * @param termCode The term code
     * @return The MFA spread or 0 if not available
     */
    private double getMfaSpreadForBond(String bondId, String termCode) {
        try {
            // Extract CUSIP from bond ID (first 9 characters)
            String cusip = bondId.substring(0, Math.min(bondId.length(), 9));
            
            // Get bond data from the eligibility listener
            Map<String, Object> bondData = bondEligibilityListener.getBondData(cusip);
            if (bondData != null) {
                // Try to get MFA rate information
                Object mfaData = bondData.get("mfaData");
                if (mfaData != null && mfaData instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> mfaInfo = (Map<String, Object>) mfaData;
                    
                    // Try different records in priority order
                    Map<String, Object> primaryRecord = getMfaRecord(mfaInfo, 
                        "MFA_" + cusip + "_" + termCode + "_Fixed_TODAY");
                    
                    Map<String, Object> secondaryRecord = getMfaRecord(mfaInfo,
                        "MFA_" + cusip + "_" + termCode + "_Fixed_YEST");
                    
                    Map<String, Object> fallbackRecord = null;
                    if ("C".equals(termCode)) {
                        fallbackRecord = getMfaRecord(mfaInfo, "MFA_" + cusip + "_REG_Fixed_TODAY");
                    } else {
                        fallbackRecord = getMfaRecord(mfaInfo, "MFA_" + cusip + "_C_Fixed_TODAY");
                    }
                    
                    // Try each record in priority order
                    double spread = extractMfaSpread(primaryRecord);
                    if (spread <= 0) {
                        spread = extractMfaSpread(secondaryRecord);
                    }
                    if (spread <= 0) {
                        spread = extractMfaSpread(fallbackRecord);
                    }
                    
                    if (spread > 0) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Using MFA spread for {}: {}", cusip, spread);
                        }
                        return spread;
                    }
                }
            }
        } catch (Exception e) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Error accessing MFA data for {}: {}", bondId, e.getMessage());
            }
        }
        
        return 0;
    }

    /**
     * Extract MFA spread from a record
     */
    private double extractMfaSpread(Map<String, Object> record) {
        if (record == null) return 0;
        
        Object spreadObj = record.get("SpreadGCAvg");
        if (spreadObj != null) {
            try {
                double spread = Double.parseDouble(spreadObj.toString());
                // Double the spread as we're using it for a bid-ask spread
                return 2 * spread;
            } catch (NumberFormatException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Invalid MFA spread format: {}", spreadObj);
                }
            }
        }
        
        return 0;
    }

    // /**
    //  * Process normal symmetric quoting when both sides can be hedged
    //  * @param best The updated best prices
    //  * @param bondId The bond ID
    //  */
    // private void processSymmetricQuoting(Best best, String bondId, String termCode, boolean hasActiveBid, boolean hasActiveAsk) {
    //     if (LOGGER.isInfoEnabled()) {
    //         LOGGER.info("processSymmetricQuoting: Starting symmetric quoting for bond: {}, termCode: {}, hasActiveBid: {}, hasActiveAsk: {}", 
    //             bondId, termCode, hasActiveBid, hasActiveAsk);
    //     }

    //     try {
    //         String Id = best.getId();
    //         if (LOGGER.isInfoEnabled()) {
    //             LOGGER.info("processSymmetricQuoting: Instrument ID for bond: {}", Id);
    //         }
    //         // if (instrumentId == null) {
    //         //     LOGGER.warn("No instrument ID found for bond: {}", bondId);
    //         //     LOGGER.debug(instrumentId);("processSymmetricQuoting:  Failed: No instrument ID found for bond");
    //         //     return;
    //         // }

    //         // Get the best bid and ask prices and sources
    //         double referenceBid = best.getBid();
    //         double referenceAsk = best.getAsk();
    //         String bidSource = best.getBidSrc();
    //         String askSource = best.getAskSrc();

    //         // Check if either bid or ask is from one of our target venues
    //         boolean validBidSource = isTargetVenue(bidSource) && referenceBid > 0;
    //         boolean validAskSource = isTargetVenue(askSource) && referenceAsk > 0;
            
    //         if (!validBidSource && !validAskSource) {
    //             if (LOGGER.isDebugEnabled()) {
    //                 LOGGER.debug("Skipping symmetric quoting - no valid source venues");
    //             }
    //             return;
    //         }

    //         // Calculate our quoting prices
    //         GCBest gcBest = "C".equals(termCode) ? getLatestGcBestCash() : getLatestGcBestREG();
            
    //         // If we're missing either side, defer to the more conservative logic
    //         if (!validAskSource) {
    //             if (LOGGER.isDebugEnabled()) {
    //                 LOGGER.debug("Missing valid sources (bid: {}, ask: {}) - deferring to conservative pricing", 
    //                     validBidSource, validAskSource);
    //             }
    //             // Defer to the more conservative tryCreateOrUpdateMarkets logic
    //             tryCreateOrUpdateMarkets(Id);
    //             return;
    //         }

    //         // Calculate our quote prices with spread
    //         double spreadAdjustment = config.getDefaultIntraMarketSpread(); // 1bp spread
    //         double ourBidPrice;
    //         if (gcBest != null && gcBest.getBid() > 0) {
    //             ourBidPrice = Math.min(referenceBid, gcBest.getBid()) + spreadAdjustment;
    //         } else {
    //             // Fallback if gcBest is null or bid is 0
    //             ourBidPrice = referenceBid + spreadAdjustment;
    //             if (LOGGER.isDebugEnabled()) {
    //                 LOGGER.debug("Using fallback bid calculation (no GC reference): {} + {}", 
    //                     referenceBid, spreadAdjustment);
    //             }
    //         }
    //         double ourAskPrice = referenceAsk - spreadAdjustment;
            
    //         // Validate minimum spread
    //         if (ourBidPrice > 0 && ourAskPrice > 0 && ((ourAskPrice - ourBidPrice) > config.getDefaultIntraMarketSpread())) {
    //             LOGGER.debug("Spread too tight for {}: bid={}, ask={}", bondId, ourBidPrice, ourAskPrice);
    //             return;
    //         }

    //         // Get the native instrument ID using the correct instrument ID
    //         String nativeInstrument = null;
    //         if (depthListener != null) {
    //             nativeInstrument = depthListener.getInstrumentFieldBySourceString(
    //                 Id, config.getMarketSource(), false);
    //             if (LOGGER.isDebugEnabled()) {
    //                 LOGGER.debug("Looking up native instrument for {} on {}: {}",
    //                 Id, config.getMarketSource(), nativeInstrument);
    //             }
    //         }

    //         if (nativeInstrument == null) {
    //             if (LOGGER.isWarnEnabled()) {
    //                 LOGGER.warn("No native instrument ID found for {} on {}", 
    //                     Id, config.getMarketSource());
    //             }
    //             if (LOGGER.isInfoEnabled()) {
    //                 LOGGER.info("processSymmetricQuoting: Failed: No native instrument ID found");
    //             }
    //             return;
    //         }

    //         if (LOGGER.isDebugEnabled()) {
    //             LOGGER.debug("Using native instrument: {} for bond {}", nativeInstrument, bondId);
    //         }

    //         // Check if we already have a quote for this bond
    //         ActiveQuote existingQuote = activeQuotes.get(bondId);
            
    //         if (existingQuote == null) {
    //             // Create new quote entries
    //             if (LOGGER.isInfoEnabled()) {
    //                 LOGGER.info("Creating new quotes for bond: {}", bondId);
    //             }

    //             if (validBidSource) {
    //                 //GC + 1 fallback for bids
    //                 placeOrder(bondId, nativeInstrument, "Buy",
    //                     ourBidPrice, bidSource != null ? bidSource : "GC_FALLBACK");
    //             }
                
    //             if (validAskSource) {
    //                 //only quote this way if we have a valid venue
    //                 placeOrder(bondId, nativeInstrument, "Sell",
    //                     ourAskPrice, askSource);
    //             }

    //             if (LOGGER.isDebugEnabled()) {
    //                 LOGGER.debug("processSymmetricQuoting: New quotes created for bond: {}", bondId);
    //             }
    //         } else {
    //             // Update existing quotes
    //         // Only update quotes that need updating based on price changes or dead orders
    //             updateExistingSymmetricQuotesWithDupeCheck(
    //                 existingQuote, bondId, termCode, nativeInstrument,
    //                 validBidSource, ourBidPrice, bidSource, hasActiveBid,
    //                 validAskSource, ourAskPrice, askSource, hasActiveAsk);
    //             if (LOGGER.isDebugEnabled()) {
    //                 LOGGER.debug("processSymmetricQuoting: Updated existing quotes for bond: {}", bondId);
    //             }
    //         }
    //     } catch (Exception e) {
    //         if (LOGGER.isErrorEnabled()) {
    //             LOGGER.error("Error processing symmetric quoting for bond {}: {}", bondId, e.getMessage(), e);
    //         }
    //     }
    // }    
    // /**
    //  * Update existing symmetric quotes with price change detection and order management
    //  */
    // private void updateExistingSymmetricQuotesWithDupeCheck(
    //     ActiveQuote existingQuote, String bondId, String termCode, String nativeInstrument,
    //     boolean validBidSource, double ourBidPrice, String referenceBidSource, boolean hasActiveBid,
    //     boolean validAskSource, double ourAskPrice, String referenceAskSource, boolean hasActiveAsk) {
    //     if (LOGGER.isDebugEnabled()) {
    //         LOGGER.debug("updateExistingSymmetricQuotes: Updating quotes for bond: {}", bondId);
    //     }

    //     try {
    //         // Define minimum price change threshold to avoid excessive order churn
    //         final double MIN_PRICE_CHANGE_THRESHOLD = 0.01; // Basis point
            
    //         // Handle ASK side first - STRICT: Must have valid market source
    //         if (validAskSource && ourAskPrice > 0) {
    //             MarketOrder currentAsk = existingQuote.getAskOrder();
    //             double currentAskPrice = existingQuote.getAskPrice();
                
    //             // Check if we need to update the ask order
    //             boolean updateAsk = !hasActiveAsk || 
    //                             Math.abs(currentAskPrice - ourAskPrice) >= MIN_PRICE_CHANGE_THRESHOLD ||
    //                             (referenceAskSource != null && 
    //                             !referenceAskSource.equals(existingQuote.getAskReferenceSource()));
                
    //             if (updateAsk) {
    //                 MarketOrder updatedAsk = updateOrder(bondId, nativeInstrument, "Sell", 
    //                     ourAskPrice, referenceAskSource, 
    //                     hasActiveAsk ? currentAsk : null);
                    
    //                 if (updatedAsk != null) {
    //                     existingQuote.setAskOrder(updatedAsk, referenceAskSource, ourAskPrice);
    //                     if (LOGGER.isInfoEnabled()) {
    //                         LOGGER.info("Market-driven ask updated for {}: price={}, source={}", 
    //                             bondId, ourAskPrice, referenceAskSource);
    //                     }
    //                 }
    //             } else {
    //                 if (LOGGER.isDebugEnabled()) {
    //                     LOGGER.debug("Ask unchanged for {}: price={}, change={}", 
    //                         bondId, currentAskPrice, Math.abs(currentAskPrice - ourAskPrice));
    //                 }
    //             }
    //         } else if (hasActiveAsk) {
    //             // CRITICAL: No valid ask source - cancel existing ask (no fallback for offers)
    //             if (LOGGER.isInfoEnabled()) {
    //                 LOGGER.info("Cancelling ask for {} - no valid market source available (NO FALLBACK FOR OFFERS)", bondId);
    //             }
    //             cancelOrder(existingQuote.getAskOrder(), bondId);
    //             existingQuote.setAskOrder(null, null, 0);
    //         }

    //         boolean bidNeedsUpdate = false;
    //         double finalBidPrice = 0;
    //         String finalBidSource = null;

    //         // Handle BID side - FLEXIBLE: Allow fallback pricing
    //         if (validBidSource && ourBidPrice > 0) {
    //             // Use market-driven bid pricing
    //             finalBidPrice = ourBidPrice;
    //             finalBidSource = referenceBidSource;
    //             bidNeedsUpdate = true;
    //         } else {
    //             // Fallback to GC pricing for bids (borrowing money is lower risk)
    //             GCBest gcBest = "C".equals(termCode) ? getLatestGcBestCash() : getLatestGcBestREG();
    //             if (gcBest != null && gcBest.getBid() > 0) {
    //                 double spreadAdjustment = config.getDefaultIntraMarketSpread();
    //                 finalBidPrice = gcBest.getBid() + spreadAdjustment;
    //                 finalBidSource = "GC_FALLBACK";
    //                 bidNeedsUpdate = true;
                    
    //                 if (LOGGER.isDebugEnabled()) {
    //                     LOGGER.debug("Using GC fallback for bid on {}: {} + {} = {}", 
    //                         bondId, gcBest.getBid(), spreadAdjustment, finalBidPrice);
    //                 }
    //             } else if (hasActiveBid) {
    //                 // No valid bid source and no GC fallback - cancel existing bid
    //                 if (LOGGER.isInfoEnabled()) {
    //                     LOGGER.info("Cancelling bid for {} - no valid sources available", bondId);
    //                 }
    //                 cancelOrder(existingQuote.getBidOrder(), bondId);
    //                 existingQuote.setBidOrder(null, null, 0);
    //             }
    //         }

    //         // Update bid if we have a valid price
    //         if (bidNeedsUpdate && finalBidPrice > 0) {
    //             MarketOrder currentBid = existingQuote.getBidOrder();
    //             double currentBidPrice = existingQuote.getBidPrice();
                
    //             // Only update if:
    //             // 1. No active order exists, OR
    //             // 2. Price change is significant, OR
    //             // 3. Reference source has changed
    //             boolean updateBid = !hasActiveBid || 
    //                             Math.abs(currentBidPrice - finalBidPrice) >= MIN_PRICE_CHANGE_THRESHOLD ||
    //                             (finalBidSource != null && 
    //                             !finalBidSource.equals(existingQuote.getBidReferenceSource()));
                
    //             if (updateBid) {
    //                 MarketOrder updatedBid = updateOrder(bondId, nativeInstrument, "Buy", 
    //                     finalBidPrice, finalBidSource, 
    //                     hasActiveBid ? currentBid : null);
                    
    //                 if (updatedBid != null) {
    //                     existingQuote.setBidOrder(updatedBid, finalBidSource, finalBidPrice);
    //                     if (LOGGER.isInfoEnabled()) {
    //                         LOGGER.info("Bid updated for {}: price={}, source={}", 
    //                             bondId, finalBidPrice, finalBidSource);
    //                     }
    //                 }
    //             } else {
    //                 if (LOGGER.isDebugEnabled()) {
    //                     LOGGER.debug("Bid unchanged for {}: price={}, change={}", 
    //                         bondId, currentBidPrice, Math.abs(currentBidPrice - finalBidPrice));
    //                 }
    //             }
    //         }

    //        if (LOGGER.isInfoEnabled()) {
    //            LOGGER.info("updateExistingSymmetricQuotes: Quotes updated for bond: {}", bondId);
    //        }

    //     } catch (Exception e) {
    //         if (LOGGER.isErrorEnabled()) {
    //             LOGGER.error("updateExistingSymmetricQuotes: Error updating symmetric quotes for bond {}: {}", 
    //                 bondId, e.getMessage(), e);
    //         }
    //     }
    // }

    // /**
    //  * Process GC-based quoting when market data is available from GCBest
    //  * @param best The updated best prices (can be null if using pure GC quoting)
    //  * @param bondId The bond ID
    //  * @param termCode The term code (C or REG)
    //  * @param hasActiveBid Whether we already have an active bid
    //  * @param hasActiveAsk Whether we already have an active ask
    //  */
    // private void processGCQuoting(Best best, String bondId, String termCode, boolean hasActiveBid, boolean hasActiveAsk) {
    //     if (LOGGER.isInfoEnabled()) {
    //         LOGGER.info("processGCQuoting: Starting GC-based quoting for bond: {}, termCode: {}, hasActiveBid: {}, hasActiveAsk: {}", 
    //             bondId, termCode, hasActiveBid, hasActiveAsk);
    //     }

    //     try {
    //         String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId, termCode);
    //         if (LOGGER.isInfoEnabled()) {
    //             LOGGER.info("processGCQuoting: Instrument ID for bond: {}", instrumentId);
    //         }
            
    //         if (instrumentId == null) {
    //             LOGGER.warn("No instrument ID found for bond: {}", bondId);
    //             return;
    //         }

    //         // Get the appropriate GC reference source and data based on term code
    //         GCBest gcBest;
    //         String gcSource;
    //         if ("C".equals(termCode)) {
    //             gcBest = getLatestGcBestCash();
    //             gcSource = "GC_CASH";
    //         } else {
    //             gcBest = getLatestGcBestREG();
    //             gcSource = "GC_REG";
    //         }
            
    //         // Check if we have valid GC reference data
    //         if (gcBest == null) {
    //             if (LOGGER.isDebugEnabled()) {
    //                 LOGGER.debug("No GC reference data available for term code: {}", termCode);
    //             }
    //             return;
    //         }
            
    //         // Get the GC bid and ask rates
    //         double gcBidRate = gcBest.getBid();
    //         double gcAskRate = gcBest.getAsk();
            
    //         if (LOGGER.isDebugEnabled()) {
    //             LOGGER.debug("GC reference rates for {}: bid={}, ask={}", termCode, gcBidRate, gcAskRate);
    //         }
            
    //         // Calculate our quote prices with spread
    //         double spreadAdjustment = config.getDefaultIntraMarketSpread(); // Default spread (e.g., 1bp)
            
    //         // Calculate our bid (GCBest.bid + 1bp)
    //             double ourBidPrice;
    //         if (gcBest != null && gcBest.getBid() > 0) {
    //             ourBidPrice = gcBest.getBid() + spreadAdjustment;
    //         } else {
    //             // Fallback if gcBest is null or bid is 0
    //             return;
    //         }
            
    //         // Calculate our ask (GCBest.ask - 1bp)
    //         double ourAskPrice = gcAskRate > 0 ? gcAskRate - spreadAdjustment : 0;
            
    //         if (LOGGER.isDebugEnabled()) {
    //             LOGGER.debug("Calculated quote prices for {}: ourBid={}, ourAsk={}", 
    //                 bondId, ourBidPrice, ourAskPrice);
    //         }
            
    //         // Validate calculated prices - we don't quote if prices would cross
    //         if (ourBidPrice > 0 && ourAskPrice > 0 && ourBidPrice >= ourAskPrice) {
    //             LOGGER.warn("GC quote prices would cross for bond {}: bid={}, ask={}", 
    //                 bondId, ourBidPrice, ourAskPrice);
    //             return;
    //         }
            
    //         // Get the native instrument ID using the correct instrument ID
    //         String nativeInstrument = null;
    //         if (depthListener != null) {
    //             nativeInstrument = depthListener.getInstrumentFieldBySourceString(
    //                 instrumentId, config.getMarketSource(), false);
    //             if (LOGGER.isDebugEnabled()) {
    //                 LOGGER.debug("Native instrument for market making: {}", nativeInstrument);
    //             }
    //         }

    //         if (nativeInstrument == null) {
    //             if (LOGGER.isWarnEnabled()) {
    //                 LOGGER.warn("No native instrument ID found for {}", instrumentId);
    //             }
    //             return;
    //         }

    //         // Check if we already have a quote for this bond
    //         ActiveQuote existingQuote = activeQuotes.get(bondId);
            
    //         if (existingQuote == null) {
    //             // Create new quote entries if we have valid prices
    //             if (LOGGER.isInfoEnabled()) {
    //                 LOGGER.info("Creating new GC-based quotes for bond: {}", bondId);
    //             }

    //             // Only place bid if we have a valid GC bid reference
    //             if (ourBidPrice > 0) {
    //                 placeOrder(bondId, nativeInstrument, "Buy", ourBidPrice, gcSource);
    //             }
                
    //             // Only place ask if we have a valid GC ask reference
    //             if (ourAskPrice > 0) {
    //                 placeOrder(bondId, nativeInstrument, "Sell", ourAskPrice, gcSource);
    //             }
    //         } else {
    //             // Update existing quotes - only if prices have changed sufficiently
    //             updateExistingGCQuotes(
    //                 existingQuote, bondId, termCode, nativeInstrument,
    //                 ourBidPrice > 0, ourBidPrice, gcSource, hasActiveBid,
    //                 ourAskPrice > 0, ourAskPrice, gcSource, hasActiveAsk
    //             );
    //         }
    //     } catch (Exception e) {
    //         if (LOGGER.isErrorEnabled()) {
    //             LOGGER.error("Error processing GC-based quoting for bond {}: {}", bondId, e.getMessage(), e);
    //         }
    //     }
    // }

    // /**
    //  * Update existing quotes based on GC reference with price change detection
    //  */
    // private void updateExistingGCQuotes(
    //     ActiveQuote existingQuote, String bondId, String termCode, String nativeInstrument,
    //     boolean validBidRef, double ourBidPrice, String bidRefSource, boolean hasActiveBid,
    //     boolean validAskRef, double ourAskPrice, String askRefSource, boolean hasActiveAsk) {
        
    //     if (LOGGER.isDebugEnabled()) {
    //         LOGGER.debug("updateExistingGCQuotes: Updating GC-based quotes for bond: {}", bondId);
    //     }

    //     try {
    //         // Define minimum price change threshold to avoid excessive order churn
    //         final double MIN_PRICE_CHANGE_THRESHOLD = 0.01; // 1 basis point
            
    //         // Handle BID side
    //         if (validBidRef && ourBidPrice > 0) {
    //             MarketOrder currentBid = existingQuote.getBidOrder();
    //             double currentBidPrice = existingQuote.getBidPrice();
                
    //             // Check if we need to update the bid order
    //             boolean updateBid = !hasActiveBid || 
    //                             Math.abs(currentBidPrice - ourBidPrice) >= MIN_PRICE_CHANGE_THRESHOLD ||
    //                             (bidRefSource != null && 
    //                             !bidRefSource.equals(existingQuote.getBidReferenceSource()));
                
    //             if (updateBid) {
    //                 MarketOrder updatedBid = updateOrder(bondId, nativeInstrument, "Buy", 
    //                     ourBidPrice, bidRefSource, currentBid);
                    
    //                 if (updatedBid != null) {
    //                     existingQuote.setBidOrder(updatedBid, bidRefSource, ourBidPrice);
    //                     if (LOGGER.isInfoEnabled()) {
    //                         LOGGER.info("Updated GC-based bid for {}: price={}, source={}", 
    //                             bondId, ourBidPrice, bidRefSource);
    //                     }
    //                 }
    //             } else {
    //                 if (LOGGER.isDebugEnabled()) {
    //                     LOGGER.debug("No need to update GC-based bid for {}: current={}, new={}, diff={}", 
    //                         bondId, currentBidPrice, ourBidPrice, 
    //                         Math.abs(currentBidPrice - ourBidPrice));
    //                 }
    //             }
    //         } else if (!validBidRef && hasActiveBid) {
    //             // Cancel bid if we have no valid reference price
    //             MarketOrder currentBid = existingQuote.getBidOrder();
    //             if (currentBid != null) {
    //                 cancelOrder(currentBid, bondId);
    //                 if (LOGGER.isInfoEnabled()) {
    //                     LOGGER.info("Cancelled bid for {} due to missing GC reference", bondId);
    //                 }
    //             }
    //         }

    //         // Handle ASK side
    //         if (validAskRef && ourAskPrice > 0) {
    //             MarketOrder currentAsk = existingQuote.getAskOrder();
    //             double currentAskPrice = existingQuote.getAskPrice();
                
    //             // Check if we need to update the ask order
    //             boolean updateAsk = !hasActiveAsk || 
    //                             Math.abs(currentAskPrice - ourAskPrice) >= MIN_PRICE_CHANGE_THRESHOLD ||
    //                             (askRefSource != null && 
    //                             !askRefSource.equals(existingQuote.getAskReferenceSource()));
                
    //             if (updateAsk) {
    //                 MarketOrder updatedAsk = updateOrder(bondId, nativeInstrument, "Sell", 
    //                     ourAskPrice, askRefSource, currentAsk);
                    
    //                 if (updatedAsk != null) {
    //                     existingQuote.setAskOrder(updatedAsk, askRefSource, ourAskPrice);
    //                     if (LOGGER.isInfoEnabled()) {
    //                         LOGGER.info("Updated GC-based ask for {}: price={}, source={}", 
    //                             bondId, ourAskPrice, askRefSource);
    //                     }
    //                 }
    //             } else {
    //                 if (LOGGER.isDebugEnabled()) {
    //                     LOGGER.debug("No need to update GC-based ask for {}: current={}, new={}, diff={}", 
    //                         bondId, currentAskPrice, ourAskPrice, 
    //                         Math.abs(currentAskPrice - ourAskPrice));
    //                 }
    //             }
    //         } else if (!validAskRef && hasActiveAsk) {
    //             // Cancel ask if we have no valid reference price
    //             MarketOrder currentAsk = existingQuote.getAskOrder();
    //             if (currentAsk != null) {
    //                 cancelOrder(currentAsk, bondId);
    //                 if (LOGGER.isInfoEnabled()) {
    //                     LOGGER.info("Cancelled ask for {} due to missing GC reference", bondId);
    //                 }
    //             }
    //         }

    //     } catch (Exception e) {
    //         LOGGER.error("Error updating GC-based quotes for bond {}: {}", bondId, e.getMessage(), e);
    //     }
    // }

    /**
     * Gets the latest GCBest for Cash
     * @return The latest GCBest for Cash, may be null if not yet received
     */
    public GCBest getLatestGcBestCash() {
        synchronized(gcBestLock) {
            return latestGcBestCash;
        }
    }

    /**
     * Gets the latest GCBest for REG
     * @return The latest GCBest for REG, may be null if not yet received
     */
    public GCBest getLatestGcBestREG() {
        synchronized(gcBestLock) {
            return latestGcBestREG;
        }
    }

    /**
     * Gets the latest Cash GC rate
     * @return The latest Cash GC rate, 0 if not yet received
     */
    public double getLatestCashGcRate() {
        synchronized(gcBestLock) {
            return latestCashGcRate;
        }
    }

    /**
     * Gets the latest REG GC rate
     * @return The latest REG GC rate, 0 if not yet received
     */
    public double getLatestRegGcRate() {
        synchronized(gcBestLock) {
            return latestRegGcRate;
        }
    }

    private double getCachedVenueMinimum(String instrumentId, String venue) {
        String cacheKey = instrumentId + ":" + venue;
        return venueMinimumCache.computeIfAbsent(cacheKey, k -> 
        depthListener.getMinimumQuantityBySource(instrumentId, venue, config.getMinSize()));
    }
    
}