package com.iontrading.automatedMarketMaking;

import java.util.Map;
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
    private final MarketMakerConfig config;
    
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
                public void onEligibilityChange(String cusip, boolean isEligible, Map<String, Object> bondData) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("EligibilityChangeListener.onEligibilityChange: cusip={}, isEligible={}", cusip, isEligible);
                    }
                    handleEligibilityChange(cusip, isEligible, bondData);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("EligibilityChangeListener.onEligibilityChange: Eligibility change handled");
                    }
                }
            });

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("MarketMaker initialized with bond eligibility integration");
            }

            // Start periodic market making for eligible bonds
            scheduler.scheduleAtFixedRate(
                () -> makeMarketsForEligibleBonds("C"), 
                5, // Initial delay (seconds) 
                config.getQuoteUpdateIntervalSeconds(), // Run every 30 seconds
                TimeUnit.SECONDS
            );

                        // Start periodic market making for eligible bonds
            scheduler.scheduleAtFixedRate(
                () -> makeMarketsForEligibleBonds("REG"), 
                5, // Initial delay (seconds) 
                config.getQuoteUpdateIntervalSeconds(), // Run every 30 seconds
                TimeUnit.SECONDS
            );
            
            // Add a diagnostic check task
            scheduler.scheduleAtFixedRate(
                this::logDiagnosticStatistics, 
                30, // Initial delay (seconds) 
                60, // Run every minute
                TimeUnit.SECONDS
            );


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
                public void onEligibilityChange(String cusip, boolean isEligible, Map<String, Object> bondData) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("EligibilityChangeListener.onEligibilityChange: cusip={}, isEligible={}", cusip, isEligible);
                    }
                    handleEligibilityChange(cusip, isEligible, bondData);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("EligibilityChangeListener.onEligibilityChange: Eligibility change handled");
                    }
                }
            });

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("MarketMaker initialized with bond eligibility integration");
            }

            // Start periodic market making for eligible bonds
            scheduler.scheduleAtFixedRate(
                () -> makeMarketsForEligibleBonds("C"), 
                5, // Initial delay (seconds) 
                config.getQuoteUpdateIntervalSeconds(), // Run every 30 seconds
                TimeUnit.SECONDS
            );

            // Start periodic market making for eligible bonds
            scheduler.scheduleAtFixedRate(
                () -> makeMarketsForEligibleBonds("REG"), 
                5, // Initial delay (seconds) 
                config.getQuoteUpdateIntervalSeconds(), // Run every 30 seconds
                TimeUnit.SECONDS
            );

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
        
        // Schedule daily start/stop tasks
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
        
        // Also schedule a check every minute to handle any timing issues
        scheduler.scheduleAtFixedRate(
            this::updateTermCodeActiveStatus,
            60, // Start after 1 minute
            60, // Check every minute
            TimeUnit.SECONDS
        );
        
        LOGGER.info("Market schedule initialized - C: {}-{}, REG: {}-{}", 
            getCashMarketOpenTime(), getCashMarketCloseTime(),
            getRegMarketOpenTime(), getRegMarketCloseTime());
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
            if (cashActive && enabled) {
                makeMarketsForEligibleBonds("C");
            } else if (!cashActive) {
                cancelAllOrders("C");
            }
        }
        
        if (regActive != regWasActive) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("REG market status changed to: {}", regActive ? "ACTIVE" : "INACTIVE");
            }
            if (regActive && enabled) {
                makeMarketsForEligibleBonds("REG");
            } else if (!regActive) {
                cancelAllOrders("REG");
            }
        }
    }

    /**
     * Check if time is within a range (handles overnight ranges)
     */
    private boolean isTimeInRange(LocalTime time, LocalTime start, LocalTime end) {
        if (start.isAfter(end)) {
            // Overnight range (e.g., 22:00-06:00)
            return !time.isAfter(end) || !time.isBefore(start);
        } else {
            // Same-day range
            return !time.isBefore(start) && !time.isAfter(end);
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
        for (String cusip : trackedInstruments) {
            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(cusip, termCode);
            if (instrumentId != null && activeQuotes.containsKey(cusip)) {
                bondsToCancel.add(cusip);
            }
        }
        
        // Cancel orders for each bond
        int cancelCount = 0;
        for (String cusip : bondsToCancel) {
            try {
                cancelOrdersForInstrument(cusip);
                cancelCount++;
            } catch (Exception e) {
                LOGGER.error("Error cancelling orders for {}: {}", cusip, e.getMessage(), e);
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
                        cancelOrder(bidOrder, quote.getCusip());
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
                        cancelOrder(askOrder, quote.getCusip());
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
                            LOGGER.info("Bid order expired for {}: {}", quote.getCusip(), bidOrder.getOrderId());
                        }
                        expiredCount++;
                    } else if (bidOrder.isDead()) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Bid order is dead for {}: {}", quote.getCusip(), bidOrder.getOrderId());
                        }
                        deadCount++;
                    }
                }
                
                // Check ask order
                if (askOrder != null) {
                    if (askOrder.isExpired()) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Ask order expired for {}: {}", quote.getCusip(), askOrder.getOrderId());
                        }
                        expiredCount++;
                    } else if (askOrder.isDead()) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Ask order is dead for {}: {}", quote.getCusip(), askOrder.getOrderId());
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
                        LOGGER.info("Order dead was a bid for {}", quote.getCusip());
                    }
                    quote.setBidOrder(null, null, 0);
                    break;
                }
                
                if (askOrder != null && askOrder.getMyReqId() == order.getMyReqId()) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Order dead was an ask for {}", quote.getCusip());
                    }
                    quote.setAskOrder(null, null, 0);
                    break;
                }
            }

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
        String.format("Source=%s, Trader=%s, Instrument=%s, Side=%s, Qty=%.2f, Price=%.4f, Type=%s, TIF=%s", 
            MarketSource, TraderId, instrId, verb, qty, price, type, tif);

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

        synchronized(gcBestLock) {
            this.latestGcBestCash = gcBestCash;
            this.latestGcBestREG = gcBestREG;
            this.latestCashGcRate = cash_gc;
            this.latestRegGcRate = reg_gc;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("MarketMaker.best() called with Best object: {}", best);
        }
        // Process the market update
        String id = best.getId();
        String instrumentId = best.getInstrumentId();

        // Enhanced logging for empty ID
        if (id == null || id.isEmpty()) {
            int emptyCount = emptyIdCounter.incrementAndGet();
            
            // Only log periodically to avoid spam
            if (emptyCount % 100 == 1) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Received market update with empty instrument ID, skipping (count: {})", emptyCount);
                }
                // Log full details of the Best object for debugging
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Empty ID Best object details: {}", best.toString());
                }

                // Try to get debug info about what's in the Best object
                try {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Best object inspection: ");
                    sb.append("instrumentId=").append(best.getInstrumentId());
                    sb.append(", bid=").append(best.getBid());
                    sb.append(", ask=").append(best.getAsk());
                    sb.append(", bidSrc=").append(best.getBidSrc());
                    sb.append(", askSrc=").append(best.getAskSrc());
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(sb.toString());
                }
                } catch (Exception e) {
                    if (LOGGER.isErrorEnabled()) {
                        LOGGER.error("Error inspecting Best object: {}", e.getMessage());
                    }
                }
            }
            return;
        }

        // Check for non-overnight instruments
        if (!id.endsWith("C_Fixed") && !id.endsWith("REG_Fixed")) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Skipping non-overnight instrument: {}", id);
            }
            // Add more information about the instrument for debugging
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Non-overnight instrument details - ID: {}, InstrumentId: {}, Bid: {}, Ask: {}", 
                    id, instrumentId, best.getBid(), best.getAsk());
            }
            return;
        }
        
        // Count updates per instrument
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("MarketMaker:  received market update within best() for instrument: {}", id);
        }
        instrumentUpdateCounters.computeIfAbsent(id, k -> new AtomicInteger(0)).incrementAndGet();
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("MarketMaker:  update count for instrument {}: {}", id, instrumentUpdateCounters.get(id).get());
            LOGGER.debug("MarketMaker:  best() called with Best object: {}", best);
            LOGGER.debug("best: Received market update for {}: bid={}, ask={}, bidSrc={}, askSrc={}", 
                id, best.getBid(), best.getAsk(), best.getBidSrc(), best.getAskSrc());
            LOGGER.debug("MarketMaker:  Processing market update for instrument: {}", id);
        }
        if (id.endsWith("C_Fixed")) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("MarketMaker:  Processing market update for CASH instrument");
            }
            processMarketUpdate(best, gcBestCash);
        } else if (id.endsWith("REG_Fixed")) {
            // For REG_Fixed instruments, use REG GC rates
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("MarketMaker:  Processing market update for REG instrument");
            }
            processMarketUpdate(best, gcBestREG);
        } else {
            LOGGER.warn("MarketMaker:  Unexpected instrument type: {}", id);
        }

        // Increment processed counter - we made it to processing the update
        processedUpdateCounter.incrementAndGet();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("best: Market update processed");
        }

    }

    /**
     * Process market data updates and update our quotes accordingly.
     * Called by the DepthListener when market data changes.
     * 
     * @param best The updated best prices
     */
    public void processMarketUpdate(Best best, GCBest gcBest) {
    
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("processMarketUpdate:  called with Best object: {}", best);
        }
        if (!enabled) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("processMarketUpdate: Market maker not enabled, skipping");
            }
            return;
        }

        try {
            String instrumentId = best.getInstrumentId();
            String bestId = best.getId(); // This should be the instrument ID from VMO.CM_INSTRUMENT
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("processMarketUpdate:  Processing market update for bestId: {}", bestId);
                LOGGER.debug("processMarketUpdate:  Processing market update for instrument ID: {}", instrumentId);
            }

            if (bestId == null || bestId.isEmpty()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("processMarketUpdate: Empty best ID, skipping");
                }
                return;
            }

            if (instrumentId == null || instrumentId.isEmpty()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("processMarketUpdate: Empty instrument ID, skipping");
                }
                return;
            }

            // Find the bond ID that maps to this instrument ID
            String bondId = null;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Current bond to instrument map size: {}", 
                    bondEligibilityListener.bondToInstrumentMaps.size());
            }

            for (Map.Entry<String, Map<String, String>> entry : bondEligibilityListener.bondToInstrumentMaps.entrySet()) {
                for (Map.Entry<String, String> innerEntry : entry.getValue().entrySet()) {
                    if (innerEntry.getValue().equals(instrumentId) || innerEntry.getValue().equals(bestId)) {
                        bondId = entry.getKey();
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("processMarketUpdate: Found bond mapping for instrument ID {}: bondId={}", 
                                instrumentId, bondId);
                        }
                        break;
                    }
                }
                if (bondId != null) {
                    break;
                }
            }
            
            // If we can't find a bond mapping, this instrument isn't eligible for market making
            if (bondId == null) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("processMarketUpdate: Bond mapping not found for instrument ID: {}", instrumentId);
                }
                return;
            }

            // Check if this bond is in our tracked instruments
            if (!trackedInstruments.contains(bondId)) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("processMarketUpdate: Bond not in tracked instruments: {}", bondId);
                }
                return;
            }


            // Check if we already have active quotes for this bond
            ActiveQuote existingQuote = activeQuotes.get(bondId);
            boolean hasActiveBid = false;
            boolean hasActiveAsk = false;
            
            if (existingQuote != null) {
                // Check if we have active orders that are still alive
                MarketOrder bidOrder = existingQuote.getBidOrder();
                MarketOrder askOrder = existingQuote.getAskOrder();
                
                hasActiveBid = (bidOrder != null && !bidOrder.isDead());
                hasActiveAsk = (askOrder != null && !askOrder.isDead());
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Existing quote status for bond {}: activeBid={}, activeAsk={}", 
                        bondId, hasActiveBid, hasActiveAsk);
                }
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("processMarketUpdate:  Processing market update for bond: {}", bondId);
            }
            
            String termCode;
            if (instrumentId.endsWith("C_Fixed")) {
                termCode = "C";
            } else if (instrumentId.endsWith("REG_Fixed")) {
                termCode = "REG";
            } else {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("processMarketUpdate: Unsupported instrument type for bond: {}", bondId);
                }
                return; // Unsupported instrument type
            }
            // Process with symmetric quoting (simplified for this example)
            processSymmetricQuoting(best, termCode, bondId, hasActiveBid, hasActiveAsk);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("processMarketUpdate: Market update processed for bond: {}", bondId);
            }

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
     * ActiveQuote represents a two-sided quote for a specific instrument.
     * It tracks the bid and ask orders and their reference sources.
     */
    public static class ActiveQuote {
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

    private void handleEligibilityChange(String cusip, boolean isEligible, Map<String, Object> bondData) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("handleEligibilityChange: Bond={} isEligible={}", cusip, isEligible);
        }

        if (!enabled) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("handleEligibilityChange: Market maker not enabled, ignoring change");
            }
            return;
        }

        try {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Bond eligibility changed: {} -> {}", cusip, (isEligible ? "ELIGIBLE" : "INELIGIBLE"));
            }

            if (isEligible) {
                // Bond became eligible, add to tracked set and create initial markets
                trackedInstruments.add(cusip);
                String termCode = (String) bondData.get("termCode");
                // Try to create initial markets for this bond
                tryCreateOrUpdateMarkets(cusip, termCode);

            } else {
                // Bond became ineligible, remove from tracked set and cancel orders
                trackedInstruments.remove(cusip);
                cancelOrdersForInstrument(cusip);
                
                // Remove from active quotes
                activeQuotes.remove(cusip);

                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("handleEligibilityChange: Bond {} became ineligible, orders cancelled", cusip);
                }
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error handling eligibility change for {}: {}", cusip, e.getMessage(), e);
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
        
        try {
            Set<String> eligibleBonds = bondEligibilityListener.getEligibleBonds();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Making markets for {} eligible bonds", eligibleBonds.size());
            }

            int marketsCreated = 0;
            int marketsUpdated = 0;
            int marketsSkipped = 0;
            
            // Process all eligible bonds
            for (String cusip : eligibleBonds) {
                // Check if we're already tracking this instrument
                if (!trackedInstruments.contains(cusip)) {
                    // New eligible bond, not yet tracking
                    trackedInstruments.add(cusip);
                    tryCreateOrUpdateMarkets(cusip, termCode);
                    marketsCreated++;
                } else {
                    // Already tracking this bond - only update if necessary
                    ActiveQuote existingQuote = activeQuotes.get(cusip);
                    if (existingQuote == null) {
                        // We're tracking the instrument but don't have quotes - create them
                        tryCreateInitialMarkets(cusip, termCode);
                        marketsUpdated++;
                    } else {
                        // Check if orders are still active
                        MarketOrder bidOrder = existingQuote.getBidOrder();
                        MarketOrder askOrder = existingQuote.getAskOrder();
                        
                        boolean bidActive = (bidOrder != null && !bidOrder.isDead());
                        boolean askActive = (askOrder != null && !askOrder.isDead());
                        
                        if (!bidActive || !askActive) {
                            // Only refresh if one side is dead
                            validateExistingQuotes(cusip, termCode,existingQuote);
                            marketsUpdated++;
                        } else {
                            // Both sides are active, skip
                            marketsSkipped++;
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Skipping market refresh for {}: both sides active", cusip);
                            }
                        }
                    }
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
    private void tryCreateInitialMarkets(String bondId, String termCode) {
        try {
            // Get the instrument ID that corresponds to this bond ID
            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId, termCode);
            if (instrumentId == null) {
                LOGGER.warn("No instrument ID mapping found for bond: " + bondId);
                return;
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Creating initial markets for bond " + bondId + " using instrument ID " + instrumentId);
            }

            // Get the native instrument ID for FENICS using the correct instrument ID
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    instrumentId, config.getMarketSource(), false); // Use instrumentId, not bondId
            }
            
            if (nativeInstrument == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("No instrument mapping found for instrument ID: " + instrumentId +
                        " (bond: " + bondId + ") on " + config.getMarketSource());
                }
                return;
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Found native instrument: " + nativeInstrument + " for bond " + bondId);
            }

            // Check if we already have quotes for this bond
            ActiveQuote existingQuote = activeQuotes.get(bondId); // Still track by bondId
            if (existingQuote != null) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Already have quotes for bond " + bondId);
                }
                return;
            }

            // Try to get current best prices to base our quotes on
            createDefaultMarkets(termCode, bondId, nativeInstrument);
            
        } catch (Exception e) {
            LOGGER.error("Error creating initial markets for bond " + bondId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Create default markets when no reference prices are available
     */
    private void createDefaultMarkets(String termCode, String cusip, String nativeInstrument) {
        try {
            
            double bidPrice = getReferencePriceForBond(cusip, termCode, "Buy");
            double askPrice = getReferencePriceForBond(cusip, termCode, "Sell");

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Creating default markets for " + cusip + 
                    " at " + bidPrice + "/" + askPrice);
            }

            // Place default orders
            placeOrder(cusip, nativeInstrument, "Buy", config.getMinSize(), 
                    bidPrice, "DEFAULT");
            placeOrder(cusip, nativeInstrument, "Sell", config.getMinSize(), 
                    askPrice, "DEFAULT");

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Default markets created for " + cusip + 
                    ": Bid=" + bidPrice + ", Ask=" + askPrice);
            }

        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error creating default markets for " + cusip + ": " + e.getMessage(), e);
            }
        }
    }

    /**
     * Try to create or update markets for an instrument
     */
    private void tryCreateOrUpdateMarkets(String bondId, String termCode) {
        try {
            // Get the instrument ID that corresponds to this bond ID
            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId, termCode);
            if (instrumentId == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("No instrument ID mapping found for bond: {}, skipping", bondId);
                }
                return;
            }

            // Check if existing quotes need validation
            ActiveQuote existingQuote = activeQuotes.get(bondId);
            if (existingQuote == null) {
                tryCreateInitialMarkets(bondId, termCode);
            } else {
                // Check if any existing orders are dead and need replacement
                MarketOrder bidOrder = existingQuote.getBidOrder();
                MarketOrder askOrder = existingQuote.getAskOrder();
                
                boolean bidActive = (bidOrder != null && !bidOrder.isDead());
                boolean askActive = (askOrder != null && !askOrder.isDead());
                
                // Only validate if at least one side needs attention
                if (!bidActive || !askActive) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Validating quotes for {}: bidActive={}, askActive={}", 
                            bondId, bidActive, askActive);
                    }
                    validateExistingQuotes(bondId, termCode, existingQuote);
                } else {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Both sides active for {}, skipping update", bondId);
                    }
                }
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error creating/updating markets for bond {}: {}", bondId, e.getMessage(), e);
            }
        }
    }

    /**
     * Validate and refresh existing quotes
     */
    private void validateExistingQuotes(String bondId, String termCode, ActiveQuote quote) {
        try {
            // Check if orders are still alive
            MarketOrder bidOrder = quote.getBidOrder();
            MarketOrder askOrder = quote.getAskOrder();
            
            boolean needNewBid = (bidOrder == null || bidOrder.isExpired() || bidOrder.isDead());
            boolean needNewAsk = (askOrder == null || askOrder.isExpired() || askOrder.isDead());
            
            if (needNewBid || needNewAsk) {
                // Get the instrument ID that corresponds to this bond ID
                String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId, termCode);
                if (instrumentId == null) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Cannot refresh quotes - no instrument ID mapping for bond: " + bondId);
                    }
                    return;
                }

                String nativeInstrument = null;
                if (depthListener != null) {
                    nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                        instrumentId, config.getMarketSource(), false); // Use instrumentId
                }
                
                if (nativeInstrument == null) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Cannot refresh quotes - no instrument mapping for instrument ID: " + instrumentId + 
                            " (bond: " + bondId + ")");
                    }
                    return;
                }

                if (needNewBid) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Refreshing bid quote for bond " + bondId);
                    }

                    double bidPrice = quote.getBidPrice();
                    if (bidPrice <= 0) {
                        bidPrice = getReferencePriceForBond(bondId, termCode, "Buy");
                    }
                    
                    if (bidPrice > 0) {
                        placeOrder(bondId, nativeInstrument, "Buy", config.getMinSize(), 
                                bidPrice, quote.getBidReferenceSource() != null ? 
                                quote.getBidReferenceSource() : "DEFAULT");
                    }
                }
                
                if (needNewAsk) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Refreshing ask quote for bond " + bondId);
                    }

                    double askPrice = quote.getAskPrice();
                    if (askPrice <= 0) {
                        askPrice = getReferencePriceForBond(bondId, termCode, "Sell");
                    }
                    
                    if (askPrice > 0) {
                        placeOrder(bondId, nativeInstrument, "Sell", config.getMinSize(), 
                                askPrice, quote.getAskReferenceSource() != null ? 
                                quote.getAskReferenceSource() : "DEFAULT");
                    }
                }
            }
            
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error validating quotes for bond " + bondId + ": " + e.getMessage(), e);
            }
        }
    }

    private double getReferencePriceForBond(String cusip, String termCode, String side) {
        double lastGCRate = 0.0;
        GCBest lastGCBest = null;
        if (termCode.equals("C")) {
            synchronized(gcBestLock) {
                lastGCBest = latestGcBestCash;
                lastGCRate = latestCashGcRate;
            }
        } else if (termCode.equals("REG")) {
            synchronized(gcBestLock) {
                lastGCBest = latestGcBestREG;
                lastGCRate = latestRegGcRate;
            }
        }
        
        if (side.equals("Buy")){
            double bid = 0;
            if (lastGCBest == null || lastGCRate <= 0) {
                LOGGER.warn("No valid GC rate available for Buy side on bond: " + cusip);
                return -9999; // No valid rate, cannot quote
            } else if (lastGCBest != null) {
                bid = lastGCBest.getBid();
                if (bid == 0 && lastGCRate > 0) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("No valid bid available, using last GC traded value for pricing on Buy side for bond: " + cusip);
                    }
                    return lastGCRate + 20;
                } else if (bid != 0) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Using last GC bid value for pricing on Buy side for bond: " + cusip);
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
                    LOGGER.warn("No valid GC rate available for Sell side on bond: " + cusip);
                }
                gcAsk = -9999; // No valid rate, cannot quote
            } else if (lastGCBest != null) {
                ask = lastGCBest.getAsk();
                if (ask == 0 && lastGCRate > 0) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Using last GC traded value for pricing on Sell side for bond: " + cusip);
                    }
                    gcAsk = lastGCRate;
                } else if (ask != 0) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Using last GC ask value for pricing on Sell side for bond: " + cusip);
                    }
                    gcAsk = ask;
                }
            }
        }

        double spread = 0.0;
        try {
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
                    LOGGER.warn("Error accessing MFA data for {}: {}", cusip, e.getMessage());
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
                        LOGGER.info("Removed bid order for {}: reqId={}", quote.getCusip(), reqId);
                    }
                    quote.setBidOrder(null, null, 0);
                    break;
                }
                
                if (askOrder != null && askOrder.getMyReqId() == reqId) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Removed ask order for {}: reqId={}", quote.getCusip(), reqId);
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
    private boolean isTargetVenue(String venue) {
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
                        double size, double price, String referenceSource) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("placeOrder: CUSIP={}, Native={}, Side={}, Size={}, Price={}, Source={}", 
                cusip, nativeInstrument, verb, size, price, referenceSource);
        }

        try {
            // Additional validation before placing order
            if (size <= 0) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Invalid order size: {}, must be positive", size);
                }
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("placeOrder: Failed: Invalid order size");
                }
                return;
            }

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
                    verb, config.getMarketSource(), nativeInstrument, cusip, size, price, referenceSource);
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
                                double size, double price, String referenceSource,
                                MarketOrder existingOrder) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("updateOrder: CUSIP={}, Side={}, Size={}, Price={}, Source={}, HasExisting={}", 
                bondId, side, size, price, referenceSource, (existingOrder != null));
        }

        try {
            // Additional validation before placing/updating order
            if (size <= 0) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Invalid order size: {}, must be positive", size);
                }
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("updateOrder: Failed: Invalid order size");
                }
                return null;
            }

            // If we have an existing order that's still active, update it
            if (existingOrder != null && !existingOrder.isDead()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Updating existing {} order: orderId={}, oldPrice={}, newPrice={}", 
                        side, existingOrder.getOrderId(), existingOrder.getPrice(), price);
                }

                MarketOrder updatedOrder = MarketOrder.orderUpdate(
                    config.getMarketSource(), 
                    fenicsTrader,
                    existingOrder.getOrderId(),
                    nativeInstrument, 
                    side, 
                    size, 
                    price, 
                    this  // OrderManager instance
                );
                
                if (updatedOrder != null) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Order updated successfully: {}", updatedOrder.getOrderId());
                    }
                    return updatedOrder;
                } else {
                    if (LOGGER.isErrorEnabled()) {
                        LOGGER.error("Failed to update {} order for {}", side, bondId);
                    }
                    // If update fails, attempt to cancel and place new
                    cancelOrder(existingOrder, bondId);
                }
            }
            
            // If no existing order or update failed, place a new order
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Placing new {} order on {}: {} ({}), size={}, price={}", 
                    side, config.getMarketSource(), nativeInstrument, bondId, size, price);
            }

            MarketOrder newOrder = orderManager.addOrder(
                config.getMarketSource(), 
                fenicsTrader, 
                nativeInstrument, 
                side, 
                size, 
                price, 
                config.getOrderType(), 
                config.getTimeInForce()
            );
            
            if (newOrder != null) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("New order placed successfully: reqId={}", newOrder.getMyReqId());
                }
                return newOrder;
            } else {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Failed to place new {} order for {}", side, bondId);
                }
                return null;
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("updateOrder: Error updating/placing order for {}: {}", bondId, e.getMessage(), e);
            }
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
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error cancelling order: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * Process normal symmetric quoting when both sides can be hedged
     * @param best The updated best prices
     * @param bondId The bond ID
     */
    private void processSymmetricQuoting(Best best, String bondId, String termCode, boolean hasActiveBid, boolean hasActiveAsk) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("processSymmetricQuoting: Starting symmetric quoting for bond: {}, termCode: {}, hasActiveBid: {}, hasActiveAsk: {}", 
                bondId, termCode, hasActiveBid, hasActiveAsk);
        }

        try {
            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(bondId, termCode);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("processSymmetricQuoting: Instrument ID for bond: {}", instrumentId);
            }
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
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Bid Source: {}, Ask Source: {}", bidSource, askSource);
            }
            // Log the best prices
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Best prices for bond {}: bid={}, ask={}", bondId, bestBid, bestAsk);
            }

            // Skip if we don't have valid prices or sources
            if (bestBid <= 0 || bestAsk <= 0 || bidSource == null || askSource == null) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Skipping symmetric quoting - invalid prices or sources: " +
                        "bid={}, ask={}, bidSrc={}, askSrc={}",
                    bestBid, bestAsk, bidSource, askSource);
                return;
                }
            }   
            // Check if either bid or ask is from one of our target venues
            boolean validBidSource = isTargetVenue(bidSource);
            boolean validAskSource = isTargetVenue(askSource);
            
            if (!validBidSource && !validAskSource) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Skipping symmetric quoting - no valid source venues");
                }
                return;
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Valid symmetric market data for bond {}: bid={}/{}, ask={}/{}",
                    bondId, bestBid, bidSource, bestAsk, askSource);
            }

            // Calculate our quoting prices
            double ourBidPrice = 0;
            double ourAskPrice = 0;
            String referenceBidSource = null;
            String referenceAskSource = null;
            
            if (validBidSource) {
                ourBidPrice = bestBid + 0.01;
                referenceBidSource = bidSource;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Valid bid source, our bid price will be: {}", ourBidPrice);
                }
            }

            if (validAskSource) {
                ourAskPrice = bestAsk - 0.01;
                referenceAskSource = askSource;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Valid ask source, our ask price will be: {}", ourAskPrice);
                }
            }
            
            // // Enforce minimum spread if both sides are valid
            // if (validBidSource && validAskSource) {
            //     double currentSpread = ourAskPrice - ourBidPrice;
            //     if (currentSpread < config.getMinSpread()) {
            //         // Adjust prices to meet minimum spread
            //         double midpoint = (ourBidPrice + ourAskPrice) / 2;
            //         ourBidPrice = midpoint - (config.getMinSpread() / 2);
            //         ourAskPrice = midpoint + (config.getMinSpread() / 2);
            //         LOGGER.info("Enforcing minimum spread of {}, adjusted prices: bid={}, ask={}",
            //             config.getMinSpread(), ourBidPrice, ourAskPrice);
            //     }
            // }
            
            // Validate that our prices are reasonable
            // if (validBidSource && (ourBidPrice <= 0 || ourBidPrice < config.getMinPrice() || ourBidPrice > config.getMaxPrice())) {
            //     LOGGER.warn("Invalid calculated bid price: {} (min={}, max={})", 
            //         ourBidPrice, config.getMinPrice(), config.getMaxPrice());
            //     validBidSource = false;
            // }
            
            // if (validAskSource && (ourAskPrice <= 0 || ourAskPrice < config.getMinPrice() || ourAskPrice > config.getMaxPrice())) {
            //     LOGGER.warn("Invalid calculated ask price: {} (min={}, max={})", 
            //         ourAskPrice, config.getMinPrice(), config.getMaxPrice());
            //     validAskSource = false;
            // }
            //
            // Get the native instrument ID using the correct instrument ID
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    instrumentId, config.getMarketSource(), false);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Looking up native instrument for {} on {}: {}",
                    instrumentId, config.getMarketSource(), nativeInstrument);
                }
            }

            if (nativeInstrument == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("No native instrument ID found for {} on {}", 
                        instrumentId, config.getMarketSource());
                }
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("processSymmetricQuoting: Failed: No native instrument ID found");
                }
                return;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Using native instrument: {} for bond {}", nativeInstrument, bondId);
            }

            // Check if we already have a quote for this bond
            ActiveQuote existingQuote = activeQuotes.get(bondId);
            
            if (existingQuote == null) {
                // Create new quote entries
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Creating new quotes for bond: {}", bondId);
                }

                if (validBidSource) {
                    placeOrder(bondId, nativeInstrument, "Buy", config.getMinSize(),
                        ourBidPrice, referenceBidSource);
                }
                
                if (validAskSource) {
                    placeOrder(bondId, nativeInstrument, "Sell", config.getMinSize(), 
                        ourAskPrice, referenceAskSource);
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("processSymmetricQuoting: New quotes created for bond: {}", bondId);
                }
            } else {
                // Update existing quotes
            // Only update quotes that need updating based on price changes or dead orders
                updateExistingSymmetricQuotesWithDupeCheck(
                    existingQuote, bondId, nativeInstrument,
                    validBidSource, ourBidPrice, referenceBidSource, hasActiveBid,
                    validAskSource, ourAskPrice, referenceAskSource, hasActiveAsk);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("processSymmetricQuoting: Updated existing quotes for bond: {}", bondId);
                }
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error processing symmetric quoting for bond {}: {}", bondId, e.getMessage(), e);
            }
        }
    }    
    /**
     * Update existing symmetric quotes with price change detection and order management
     */
    private void updateExistingSymmetricQuotesWithDupeCheck(
        ActiveQuote existingQuote, String bondId, String nativeInstrument,
        boolean validBidSource, double ourBidPrice, String referenceBidSource, boolean hasActiveBid,
        boolean validAskSource, double ourAskPrice, String referenceAskSource, boolean hasActiveAsk) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("updateExistingSymmetricQuotes: Updating quotes for bond: {}", bondId);
        }

        try {
            // Define minimum price change threshold to avoid excessive order churn
            final double MIN_PRICE_CHANGE_THRESHOLD = 0.01; // Basis point
            
            // Update bid if needed
            if (validBidSource && ourBidPrice > 0) {
                MarketOrder currentBid = existingQuote.getBidOrder();
                double currentBidPrice = existingQuote.getBidPrice();
                
                // Only update if:
                // 1. No active order exists, OR
                // 2. Price change is significant, OR
                // 3. Reference source has changed
                boolean updateBid = !hasActiveBid || 
                                Math.abs(currentBidPrice - ourBidPrice) >= MIN_PRICE_CHANGE_THRESHOLD ||
                                (referenceBidSource != null && 
                                !referenceBidSource.equals(existingQuote.getBidReferenceSource()));
                
                if (updateBid) {
                    // Use the updateOrder wrapper to handle the order update/placement
                    MarketOrder updatedBid = updateOrder(bondId, nativeInstrument, "Buy", 
                        config.getMinSize(), ourBidPrice, referenceBidSource, 
                        hasActiveBid ? currentBid : null);
                    
                    if (updatedBid != null) {
                        existingQuote.setBidOrder(updatedBid, referenceBidSource, ourBidPrice);
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Bid quote updated for {}: price={}, source={}", 
                                bondId, ourBidPrice, referenceBidSource);
                        }
                    }
                } else {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Bid unchanged for {}: price={}, change={}", 
                            bondId, currentBidPrice, Math.abs(currentBidPrice - ourBidPrice));
                    }
                }
            } else if (existingQuote.getBidOrder() != null && hasActiveBid) {
                // Cancel bid side if it's no longer valid
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Cancelling bid for {} as source is no longer valid", bondId);
                }
                cancelOrder(existingQuote.getBidOrder(), bondId);
            }

            // Update ask if needed
            if (validAskSource && ourAskPrice > 0) {
                MarketOrder currentAsk = existingQuote.getAskOrder();
                double currentAskPrice = existingQuote.getAskPrice();
                
                // Check if we need to update the ask order
                boolean updateAsk = !hasActiveAsk || 
                                Math.abs(currentAskPrice - ourAskPrice) >= MIN_PRICE_CHANGE_THRESHOLD ||
                                (referenceAskSource != null && 
                                !referenceAskSource.equals(existingQuote.getAskReferenceSource()));
                
                if (updateAsk) {
                    // Use the updateOrder wrapper to handle the order update/placement
                    MarketOrder updatedAsk = updateOrder(bondId, nativeInstrument, "Sell", 
                        config.getMinSize(), ourAskPrice, referenceAskSource, 
                        hasActiveAsk ? currentAsk : null);
                    
                    if (updatedAsk != null) {
                        existingQuote.setAskOrder(updatedAsk, referenceAskSource, ourAskPrice);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Ask quote updated for {}: price={}, source={}", 
                                bondId, ourAskPrice, referenceAskSource);
                        }
                    }
                } else {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Ask unchanged for {}: price={}, change={}", 
                            bondId, currentAskPrice, Math.abs(currentAskPrice - ourAskPrice));
                    }
                }
            } else if (existingQuote.getAskOrder() != null && hasActiveAsk) {
                // Cancel ask side if it's no longer valid
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Cancelling ask for {} as source is no longer valid", bondId);
                }
                cancelOrder(existingQuote.getAskOrder(), bondId);
            }

           if (LOGGER.isInfoEnabled()) {
               LOGGER.info("updateExistingSymmetricQuotes: Quotes updated for bond: {}", bondId);
           }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("updateExistingSymmetricQuotes: Error updating symmetric quotes for bond {}: {}", 
                    bondId, e.getMessage(), e);
            }
        }
    }

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
}