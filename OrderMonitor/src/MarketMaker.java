
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

    // Reference to the main OrderManagement component
    private final OrderManagement orderManager;

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
    
    // private final Map<String, Integer> orderIdToReqIdMap = new HashMap<>();

    private final OrderRepository orderRepository = OrderRepository.getInstance();

    private static final long MIN_UPDATE_INTERVAL_MS = 2000; // 500 milliseconds minimum between updates

    public String getApplicationId() {
        // Return a unique identifier for this market maker instance
        return "automatedMarketMaking";
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
                .setCashMarketHours(LocalTime.of(7, 0), LocalTime.of(17, 0))
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
    public MarketMaker(OrderManagement orderManager, MarketMakerConfig config,  BondEligibilityListener bondEligibilityListener) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("MarketMaker.constructor: Creating with custom config, orderManager={}, config={}", orderManager, config);
        }
        try {
            this.orderManager = orderManager;
            this.config = config;
            
            initializeMarketSchedule();
            this.bondEligibilityListener = bondEligibilityListener;

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
        boolean cashActive = orderRepository.isTermCodeActive("C");
        boolean regActive = orderRepository.isTermCodeActive("REG");

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
                if (orderRepository.isTermCodeActive("C") && enabled) {
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
                if (orderRepository.isTermCodeActive("REG") && enabled) {
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

        boolean wasActive = orderRepository.isTermCodeActive(termCode);
        orderRepository.setTermCodeStatus(termCode, true);

        if (!wasActive && enabled) {
            // Only create markets if global enabled flag is true
            makeMarketsForEligibleBonds(termCode); 
        }
    }

    /**
     * Stop market making for a specific term code
     */
    private void stopTermCodeMarketMaking(String termCode) {
        LOGGER.info("Stopping market making for term code: {}", termCode);

        orderRepository.setTermCodeStatus(termCode, false);

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

        boolean cashWasActive = orderRepository.isTermCodeActive("C");
        boolean regWasActive = orderRepository.isTermCodeActive("REG");

        orderRepository.setTermCodeStatus("C", cashActive);
        orderRepository.setTermCodeStatus("REG", regActive);

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

    // public Map<String, ActiveQuote> getActiveQuotes() {
    //     return Collections.unmodifiableMap(activeQuotes); // Return unmodifiable to prevent external modification
    // }

    public Map<String, ActiveQuote> getActiveQuotes() {
        // Create a copy to prevent external modification
        Map<String, ActiveQuote> result = new HashMap<>();
        for (ActiveQuote quote : orderRepository.getAllActiveQuotes()) {
            result.put(quote.getId(), quote);
        }
        return Collections.unmodifiableMap(result);
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
        for (String Id : orderRepository.getTrackedInstruments()) {
            if (orderRepository.findQuote(Id) != null && Id.endsWith(termCode + "_Fixed")) {
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
                    orderRepository.getAllActiveQuotes().size() + " instruments");
            }

            int expiredCount = 0;
            int deadCount = 0;
            
            for (ActiveQuote quote : orderRepository.getAllActiveQuotes()) {
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
            for (ActiveQuote quote : orderRepository.getAllActiveQuotes()) {
                String id = quote.getId();

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
            orderRepository.removeEmptyQuotes();
            
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
        orderRepository.incrementInstrumentUpdateCounter(id);
        
        boolean significantCashGcChange = false;
        boolean significantRegGcChange = false;


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

        processedUpdateCounter.incrementAndGet();

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
                if (orderRepository.untrackInstrument(Id)) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("processMarketUpdate: Instrument {} is not eligible, removed from tracking", Id);
                    }
                    // Cancel any existing orders for this instrument
                    cancelOrdersForInstrument(Id);
                }
                return;
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
                boolean isMarketOpen = orderRepository.isTermCodeActive(termCode);
                if (!isMarketOpen) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("processMarketUpdate: {} market closed, skipping update", termCode);
                    }
                    return;
                }
            }
            // Get existing quote status if any
            ActiveQuote existingQuote = orderRepository.getQuote(Id);
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
            
            // Validate pricing decision and handle early returns
            if (!decision.hasBid && !decision.hasAsk) {
                LOGGER.debug("No valid prices available for instrument {}. Skipping market update.", Id);
                return;
            }

            // Apply pricing decision
            if (existingQuote == null) {
                // Create new quotes if we have valid prices
                if (decision.hasBid || decision.hasAsk) {
                    // Use OrderRepository to get or create quote
                    existingQuote = orderRepository.getQuote(Id);
                    
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
                    boolean updateBid = (!hasActiveBid || ((currentBidPrice != decision.bidPrice) && (existingQuote.getBidAge() > 1500)));
                    LOGGER.info("processMarketUpdate: Current bid price for {}: {}, update required: {}", 
                        Id, currentBidPrice, updateBid);
                    if (updateBid) {
                        MarketOrder existingBidOrder = existingQuote.getBidOrder();
                        if (existingBidOrder != null) {
                            LOGGER.info("Cancelling existing bid order for {}: {}", Id, existingBidOrder);
                            cancelOrder(existingBidOrder, Id);
                            orderRepository.untrackInstrument(Id);
                        } else {
                            LOGGER.info("No existing bid order to cancel for {}", Id);
                        }
                        //placing new order
                        LOGGER.info("Placing new bid order for {}: price={}, source={}, bidSize={}", 
                            Id, decision.bidPrice, decision.bidSource, best.getBidSize());
                        if (best.getBidSize()>0) {
                            placeOrder(Id, nativeInstrument, "Buy", decision.bidPrice, decision.bidSource);
                        }
                        orderRepository.trackInstrument(Id);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Updated bid for {}: price={}, source={}, isGcBased={}", 
                                Id, decision.bidPrice, decision.bidSource, decision.isGcBasedBid);
                        }
                    }
                } else if (hasActiveBid && !decision.hasBid) {
                    // Cancel bid if we have no valid pricing reference
                    LOGGER.info("Cancelling existing bid order for {} - no valid pricing reference", Id);
                    MarketOrder existingBidOrder = existingQuote.getBidOrder();
                    if (existingBidOrder != null) {
                        cancelOrder(existingBidOrder, Id);
                        orderRepository.untrackInstrument(Id);
                    }
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Cancelled bid for {} - no valid pricing reference", Id);
                    }
                }
                
                if (decision.hasAsk) {
                    // Check if we need to update ask
                    double currentAskPrice = existingQuote.getAskPrice();
                    boolean updateAsk = (!hasActiveAsk || ((currentAskPrice != decision.askPrice) && (existingQuote.getAskAge() > 1500)));
                    LOGGER.info("processMarketUpdate: Current ask price for {}: {}, update required: {}", 
                        Id, currentAskPrice, updateAsk);    
                    if (updateAsk) {
                        LOGGER.info("Cancelling existing ask order for {}: {}", Id, existingQuote.getAskOrder());
                        MarketOrder existingAskOrder = existingQuote.getAskOrder();
                        if (existingAskOrder != null) {
                            cancelOrder(existingAskOrder, Id);
                            orderRepository.untrackInstrument(Id);
                        } else {
                            LOGGER.info("No existing ask order to cancel for {}", Id);
                        }
                        LOGGER.info("Placing new ask order for {}: price={}, source={}, askSize={}", 
                            Id, decision.askPrice, decision.askSource, best.getAskSize());
                        if (best.getAskSize()>0) {
                            placeOrder(Id, nativeInstrument, "Sell", decision.askPrice, decision.askSource);
                        }
                        // if (updatedAsk != null) {
                        orderRepository.trackInstrument(Id);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Updated ask for {}: price={}, source={}, isGcBased={}", 
                                Id, decision.askPrice, decision.askSource, decision.isGcBasedAsk);
                        }
                        // }
                    }
                } else if (hasActiveAsk && !decision.hasAsk) {
                    // Cancel ask if we have no valid price
                    MarketOrder existingAskOrder = existingQuote.getAskOrder();
                    if (existingAskOrder != null) {
                        cancelOrder(existingAskOrder, Id);
                        orderRepository.untrackInstrument(Id);
                    } else {
                        LOGGER.info("No existing ask order to cancel for {}", Id);
                    }
                }
            }
            orderRepository.incrementInstrumentUpdateCounter(Id);
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
            LOGGER.info("  Tracked instruments: {}", orderRepository.getTrackedInstruments().size());
            LOGGER.info("  Active quotes: {}", orderRepository.getAllActiveQuotes().size());
            LOGGER.info("  Bond eligibility listener status:");
            LOGGER.info("    Eligible bonds: {}", bondEligibilityListener.getEligibleBonds().size());
            LOGGER.info("    Bond to instrument mappings: {}", bondEligibilityListener.bondToInstrumentMaps.size());
            
            // Log top 5 instruments with most updates
            LOGGER.info("  Top instruments by update count:");
            Map<String, Integer> topCounters = orderRepository.getInstrumentUpdateCounts(5);
            for (Map.Entry<String, Integer> entry : topCounters.entrySet()) {
                LOGGER.info("    {}: {} updates", entry.getKey(), entry.getValue());
            }
            
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
        status.put("trackedInstruments", orderRepository.getTrackedInstruments().size());
        status.put("activeQuotes", orderRepository.getAllActiveQuotes().size());
        status.put("eligibleBonds", bondEligibilityListener.getEligibleBonds().size());
        
        // Count active orders
        int activeBids = 0;
        int activeAsks = 0;
        for (ActiveQuote quote : orderRepository.getAllActiveQuotes()) {
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

    // Update mapOrderIdToReqId to use OrderRepository:
    @Override
    public void mapOrderIdToReqId(String orderId, int reqId) {
        orderRepository.mapOrderIdToReqId(orderId, reqId);
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
                if (orderRepository.isTermCodeActive("C")){
                    makeMarketsForEligibleBonds("C");
                }

                if (orderRepository.isTermCodeActive("REG")){
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
                orderRepository.trackInstrument(Id);
                String termCode = (String) bondData.get("termCode");
                // Try to create initial markets for this bond
                tryCreateOrUpdateMarkets(Id);
            } else {
                // Bond became ineligible, remove from tracked set and cancel orders
                cancelOrdersForInstrument(Id);
                // Remove from active quotes
                orderRepository.removeQuote(Id);

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
                    marketsSkipped++; // Count skipped markets
                    continue; // Skip to next bond instead of returning
                }
                // Check if we're already tracking this instrument
                if (Id == null || Id.isEmpty()) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("makeMarketsForEligibleBonds: Empty bond ID, skipping");
                    }
                    marketsSkipped++;
                    continue;
                }
                if (!orderRepository.isInstrumentTracked(Id)) {
                    // New eligible bond, not yet tracking
                    LOGGER.debug("makeMarketsForEligibleBonds: New eligible bond detected, Id={}", Id);
                    int result = tryCreateOrUpdateMarkets(Id);
                    if (result > 0) {
                        marketsCreated++;
                    } else {
                        marketsSkipped++;
                    }
                } else {
                    // Already tracking this instrument
                    ActiveQuote existingQuote = orderRepository.getQuote(Id);
                    if (existingQuote != null) {
                        LOGGER.debug("makeMarketsForEligibleBonds: Found existing quote for Id={}", Id);
                    }
                    if(existingQuote.isBidActive()) {
                        // Existing quote is still active, skip update
                        LOGGER.debug("makeMarketsForEligibleBonds: Existing quote for Id={} is still active, skipping update", Id);
                        marketsSkipped++;
                        continue;
                    } else {
                        int result = tryCreateOrUpdateMarkets(Id);
                        if (result > 0) {
                            marketsCreated++;
                        } else {
                            marketsSkipped++;
                        }
                    }
                    marketsUpdated++;
                }
            }
            // Clean up any instruments that are no longer eligible
            Set<String> instrumentsToRemove = new HashSet<>();
            for (String Id : orderRepository.getTrackedInstruments()) {
                if (!eligibleBonds.contains(Id)) {
                    instrumentsToRemove.add(Id);
                }
            }
            
            int marketsRemoved = 0;
            for (String Id : instrumentsToRemove) {
                cancelOrdersForInstrument(Id);
                orderRepository.removeQuote(Id);
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
                orderRepository.untrackInstrument(Id);
                return -1;
            }
            
            // Get the native instrument ID
            String nativeInstrument = null;
            if (depthListener != null) {
                nativeInstrument = depthListener.getInstrumentFieldBySourceString(
                    Id, config.getMarketSource(), false);
                LOGGER.debug("tryCreateOrUpdateMarkets: Native instrument for Id={} is {}", Id, nativeInstrument);
            }
            
            if (nativeInstrument == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("No native instrument ID found for {}", Id);
                }
                orderRepository.untrackInstrument(Id);
                return -1;
            }
            
            // Get the appropriate GC reference data
            GCBest gcBest = "C".equals(termCode) ? GCBestManager.getInstance().getCashGCBest() : GCBestManager.getInstance().getRegGCBest();

            // Use unified pricing model (with no market data available)
            PricingDecision decision = calculateUnifiedPrices(Id, termCode, null, gcBest);

            // Check if existing quotes need validation
            ActiveQuote existingQuote = orderRepository.findQuote(Id);
            if (existingQuote == null) {
                // Create new quotes if we have valid prices
                if (decision.hasBid || decision.hasAsk) {
                    existingQuote = new ActiveQuote(Id);
                    // activeQuotes.put(Id, existingQuote);
                    orderRepository.getQuote(Id);
                    
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Creating initial markets for bond: {}", Id);
                    }
                    if (decision.hasBid) {
                        placeOrder(Id, nativeInstrument, "Buy", decision.bidPrice, decision.bidSource);
                        orderRepository.trackInstrument(Id);
                    }
                } else {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("No valid pricing available for new bond: {}", Id);
                        orderRepository.untrackInstrument(Id);
                        return -1;
                    }
                }
            } else {
                // Only update sides that need attention
                if (!existingQuote.isBidActive() && decision.hasBid) {
                    placeOrder(Id, nativeInstrument, "Buy", decision.bidPrice, decision.bidSource);
                    orderRepository.trackInstrument(Id);
                }
                
                if (!existingQuote.isAskActive() && decision.hasAsk) {
                    placeOrder(Id, nativeInstrument, "Sell", decision.askPrice, decision.askSource);
                    orderRepository.trackInstrument(Id);
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
            for (ActiveQuote quote : orderRepository.getAllActiveQuotes()) {
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
            orderRepository.removeAllMappingsForReqId(reqId);
            
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
    private void cancelOrdersForInstrument(String Id) {
        try {
            ActiveQuote quote = orderRepository.findQuote(Id);
            if (quote != null) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Cancelling orders for ineligible instrument: {}", Id);
                }

                MarketOrder bidOrder = quote.getBidOrder();
                MarketOrder askOrder = quote.getAskOrder();
                
                if (bidOrder != null) {
                    cancelOrder(bidOrder, Id);
                }
                
                if (askOrder != null) {
                    cancelOrder(askOrder, Id);
                }
                
                orderRepository.untrackInstrument(Id);

                // Remove from tracking
                orderRepository.removeQuote(Id);
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error cancelling orders for {}: {}", Id, e.getMessage(), e);
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
        // summary.put("activeQuotes", activeQuotes.size());
        summary.put("activeQuotes", orderRepository.getAllActiveQuotes().size());
        summary.put("trackedInstruments", orderRepository.getTrackedInstruments().size());

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
            orderRepository.clearAllQuotes();
            for (String Id : new HashSet<>(orderRepository.getTrackedInstruments())) {
                orderRepository.untrackInstrument(Id);
            };
            orderRepository.clearAllOrderMappings();;
            orderRepository.clearInstrumentUpdateCounters();
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

    /**
     * Implements the asynchronous shutdown process for the market maker component.
     * This is called as part of the first phase of the platform's two-phase shutdown.
     * 
     * @param phase The shutdown phase (1 for initial preparation, 2 for final cleanup)
     * @return true if ready to stop, false if more processing is needed
     */
    public boolean asyncShutdown(int phase) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("MarketMaker async shutdown phase {}", phase);
        }
        
        try {
            if (phase == 1) {
                // First phase: disable trading but don't cancel orders yet
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Async shutdown phase 1: Preparing for shutdown");
                }
                
                // Disable market making to prevent new orders
                this.enabled = false;
                
                // Stop the scheduler to prevent any new tasks
                if (scheduler != null && !scheduler.isShutdown()) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Shutting down market maker scheduler");
                    }
                    
                    List<Runnable> pendingTasks = scheduler.shutdownNow();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Cancelled {} pending scheduler tasks", pendingTasks.size());
                    }
                }
                
                // Signal that we need another phase
                return false;
                
            } else if (phase == 2) {
                // Second phase: Cancel all orders and clean up
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Async shutdown phase 2: Cancelling orders and cleaning up");
                }
                
                // Check if MKV is still available for cancelling orders
                boolean mkvAvailable = false;
                try {
                    Mkv mkv = Mkv.getInstance();
                    if (mkv != null) {
                        MkvPublishManager pm = mkv.getPublishManager();
                        mkvAvailable = (pm != null);
                    }
                } catch (Exception e) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("MKV API is already stopped, cannot get instance");
                    }
                }
                
                if (mkvAvailable) {
                    // Cancel all orders with a short timeout
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("MKV still available, cancelling all active quotes");
                    }
                    
                    boolean success = cancelAllOrders(5000);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Order cancellation complete: success={}", success);
                    }
                }
                
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Async shutdown phase 2 complete");
                }
                
                // Signal that we're done
                return true;
            } else {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Unknown async shutdown phase: {}", phase);
                }
                return true; // Default to ready to stop for unknown phases
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Error during market maker async shutdown phase {}: {}", phase, e.getMessage(), e);
            }
            return true; // Return ready to stop if we encounter an error
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
     * @param price The order price
     * @param referenceSource The source of the reference price
     */
    private void placeOrder(String Id, String nativeInstrument, String verb, 
                        double price, String referenceSource) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("placeOrder: Id={}, Native={}, Side={}, Price={}, Source={}", 
                Id, nativeInstrument, verb, price, referenceSource);
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
                LOGGER.info("Placing {} order on {}: {} ({}), price={}, reference={}", 
                    verb, config.getMarketSource(), nativeInstrument, Id, price, referenceSource);
            }

            String instrumentId = bondEligibilityListener.getInstrumentIdForBond(Id, 
                nativeInstrument.contains("_C_") ? "C" : "REG");
            double size = config.getMinSize(); // Default to minimum size
            if (depthListener != null && instrumentId != null) {
                double venueMinimum = orderRepository.getCachedVenueMinimum(instrumentId, config.getMarketSource(), config.getMinSize());

                if (venueMinimum > 0) {
                    // Also check hedge venue minimum if auto-hedge enabled
                    if (config.isAutoHedge() && referenceSource != null && isTargetVenue(referenceSource)) {
                        double hedgeMinimum = orderRepository.getCachedVenueMinimum(instrumentId, referenceSource, config.getMinSize());
                        // Use the larger of the two minimums
                        size = Math.max(size, Math.max(venueMinimum, hedgeMinimum));
                    } else {
                        // Just ensure we meet the trading venue minimum
                        size = Math.max(size, venueMinimum);
                    }
                    
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Adjusted order size for {}: venue minimum={}, final size={}", 
                            Id, venueMinimum, size);
                    }
                } else {
                    size = venueMinimum;
                }
            }

            //check if venue is active
            if (!orderRepository.isVenueActive(config.getMarketSource())) {
                LOGGER.warn("Venue {} is not active for trader {}", config.getMarketSource(), fenicsTrader);
                return;
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

                if (order.getErrStr() != null && !order.getErrStr().isEmpty()) {
                    if (LOGGER.isErrorEnabled()) {
                        LOGGER.error("Error placing order: {}", order.getErrStr());
                    }
                    return; // Exit early if there was an error
                }

                // Update the appropriate quote
                ActiveQuote quote = orderRepository.getQuote(Id);

                if ("Buy".equals(verb)) {
                // Update the ActiveQuote with bid information
                    quote.setBidOrder(order, referenceSource, price);

                    LOGGER.info("Order saved in quote with the following details:  instrId={}, price={}, verb={}, reqId={}, source={}",
                        order.getInstrId(), order.getPrice(), order.getVerb(), order.getMyReqId(), order.getMarketSource());

                    // Set source flags explicitly
                    quote.setGcBasedBid(referenceSource != null && referenceSource.startsWith("GC_"));
                    quote.setMarketBasedBid(referenceSource != null && 
                                        !referenceSource.startsWith("GC_") && 
                                        !referenceSource.equals("DEFAULT"));
                    
                    // Explicitly set active status
                    quote.setBidActive(true);
                    
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Bid quote updated for {}: price={}, source={}, reqId={}, isGcBased={}, isMarketBased={}", 
                            Id, price, referenceSource, order.getMyReqId(),
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
                            Id, price, referenceSource, order.getMyReqId(),
                            quote.isGcBasedAsk(), quote.isMarketBasedAsk());
                    }
                }

                orderRepository.trackInstrument(Id);

                // Now also track this instrument
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("placeOrder: Order placed successfully");
                }
            } else {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Failed to place {} order for {}", verb, Id);
                }
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("placeOrder: Failed to place order");
                }
            }
        } catch (Exception e) {
            LOGGER.error("placeOrder: Error placing order for {}", Id, e);
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
                orderRepository.clearAllQuotes();
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
            return;
        }
        
        try {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Cancelling order: Id={}, orderId={}, reqId={}, marketSource={}, price={}, verb={}, instrId={}", 
                    Id, order.getOrderId(), order.getMyReqId(), order.getMarketSource(), order.getPrice(), order.getVerb(), order.getInstrId());
            }

            String marketSource = order.getMarketSource();
            String traderId = orderManager.getTraderForVenue(marketSource);
            String orderId = order.getOrderId();

            // Send the cancel request
            LOGGER.info("Sending cancel request for order: Id={}, orderId={}, reqId={}", 
                Id, orderId, order.getMyReqId());
            // Null checks prior to sending cancel request
            if (marketSource == null || traderId == null || orderId == null) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Cannot cancel order: missing marketSource, traderId, or orderId for Id={}", Id);
                }
                return; // Cannot proceed without valid identifiers
            }

            MarketOrder cancelOrder = order.orderCancel(marketSource, traderId, orderId, orderManager);

            // Update the ActiveQuote even for already dead orders
            if (order.isDead()) {
                ActiveQuote quote = orderRepository.findQuote(Id);
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
            }

            if (cancelOrder != null) {
                // Update the ActiveQuote
                ActiveQuote quote = orderRepository.findQuote(Id);
                if (quote == null) {
                    LOGGER.warn("No active quote found for Id={}", Id);
                }
                if (quote != null) {
                    // Determine if it's a bid or ask order
                    // First check if bid and ask orders exist before logging their details
                    MarketOrder bidOrder = quote.getBidOrder();
                    MarketOrder askOrder = quote.getAskOrder();

                    // Safely log information with null checks
                    if (bidOrder != null && askOrder != null) {
                        LOGGER.info("Order cancelled successfully: Id={}, orderId={}, reqId={}, bidReqId={}, bidOrderId={}, askReqId={}, askOrderId={}", 
                            Id, orderId, order.getMyReqId(), bidOrder.getMyReqId(), bidOrder.getOrderId(), askOrder.getMyReqId(), askOrder.getOrderId());
                    } else if (bidOrder != null) {
                        LOGGER.info("Order cancelled successfully: Id={}, orderId={}, reqId={}, bidReqId={}, bidOrderId={}, askOrder=null", 
                            Id, orderId, order.getMyReqId(), bidOrder.getMyReqId(), bidOrder.getOrderId());
                    } else if (askOrder != null) {
                        LOGGER.info("Order cancelled successfully: Id={}, orderId={}, reqId={}, bidOrder=null, askReqId={}, askOrderId={}", 
                            Id, orderId, order.getMyReqId(), askOrder.getMyReqId(), askOrder.getOrderId());
                    } else {
                        LOGGER.info("Order cancelled successfully: Id={}, orderId={}, reqId={}, both bid and ask orders are null", 
                            Id, orderId, order.getMyReqId());
                    }
                                        
                    if (quote.getBidOrder() != null && quote.getBidOrder().getMyReqId() == order.getMyReqId()) {
                        quote.setBidOrder(null, null, 0);
                        quote.setBidActive(false);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Cancelled bid order for {}: reqId={}", Id, order.getMyReqId());
                        }
                    }
                    
                    if (quote.getAskOrder() != null && quote.getAskOrder().getMyReqId() == order.getMyReqId()) {
                        quote.setAskOrder(null, null, 0);
                        quote.setAskActive(false);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Cancelled ask order for {}: reqId={}", Id, order.getMyReqId());
                        }
                    }
                }
                // Track the cancel request
                removeOrder(order.getMyReqId());
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
        double cachedValue = orderRepository.getCachedVenueMinimum(instrumentId, venue, -1);
        if (cachedValue < 0) {
            cachedValue = depthListener.getMinimumQuantityBySource(instrumentId, venue, config.getMinSize());
            orderRepository.cacheVenueMinimum(instrumentId, venue, cachedValue);
        }
        return cachedValue;
    }
    
}