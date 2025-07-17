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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import org.json.JSONObject;

/**
 * Supported venues:
 * - FENICS_USREPO
 * - BTEC_REPO_US
 * - DEALERWEB_REPO
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
    private static final String MARKET_DATA_CHANNEL = "market_data";
    private static final String ORDER_COMMAND_CHANNEL = "order_commands";
    private static final String ORDER_RESPONSE_CHANNEL = "order_responses";
    
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
     * Enhanced order manager implementation with price caching
     */
    private static class EnhancedOrderManager implements IOrderManager {
        // 缓存最佳价格，key 为 instrument ID
        private final ConcurrentHashMap<String, Best> bestPriceCache = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Long> priceUpdateTime = new ConcurrentHashMap<>();
        
        @Override
        public void orderDead(MarketOrder order) {
            LOGGER.info("Order terminated: {}", order.getOrderId());
        }
        
        @Override
        public MarketOrder addOrder(String MarketSource, String TraderId, String instrId, 
                String verb, double qty, double price, String type, String tif) {
            LOGGER.debug("Add order called - not implemented in enhanced manager");
            return null;
        }
        
        @Override
        public void best(Best best, double cash_gc, double reg_gc, GCBest gcBestCash, GCBest gcBestREG) {
            // 缓存最佳价格
            if (best != null && best.getId() != null) {
                String instrumentId = best.getId();
                bestPriceCache.put(instrumentId, best);
                priceUpdateTime.put(instrumentId, System.currentTimeMillis());
                
                // 记录价格更新（减少日志噪音，只在价格变化时记录）
                Best oldBest = bestPriceCache.get(instrumentId);
                if (oldBest == null || 
                    oldBest.getBid() != best.getBid() || 
                    oldBest.getAsk() != best.getAsk()) {
                    LOGGER.debug("Price update for {}: Bid={} @ {}, Ask={} @ {}", 
                        instrumentId, best.getBid(), best.getBidSize(), 
                        best.getAsk(), best.getAskSize());
                }
            }
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
        
        /**
         * 获取缓存的最佳价格
         */
        public Best getBestPrice(String instrumentId) {
            // 直接查找
            Best best = bestPriceCache.get(instrumentId);
            if (best != null) {
                return best;
            }
            
            // 尝试模糊匹配
            for (Map.Entry<String, Best> entry : bestPriceCache.entrySet()) {
                if (entry.getKey().contains(instrumentId) || instrumentId.contains(entry.getKey())) {
                    return entry.getValue();
                }
            }
            
            return null;
        }
        
        /**
         * 获取所有包含指定 CUSIP 的价格
         */
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
        
        /**
         * 获取价格更新时间
         */
        public long getPriceUpdateTime(String instrumentId) {
            Long time = priceUpdateTime.get(instrumentId);
            return time != null ? time : 0L;
        }
        
        /**
         * 价格是否过期（超过30秒）
         */
        public boolean isPriceStale(String instrumentId) {
            long updateTime = getPriceUpdateTime(instrumentId);
            if (updateTime == 0) return true;

            return (System.currentTimeMillis() - updateTime) > 100000; // 100秒
        }
    }
    
    /**
     * Constructor
     * 
     * @param reqId Request ID for this instance
     */
    public MultiVenueOrderCreator(int reqId) {
        this.myReqId = reqId;
        this.creationTimestamp = System.currentTimeMillis();
        LOGGER.debug("MultiVenueOrderCreator initialized - ReqId: {}", reqId);
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
     * 自动选择最佳价格的场所 - 基于老板的逻辑
     * 
     * @param cusip CUSIP标识符
     * @param side 订单方向 (Buy/Sell)
     * @return 最佳场所配置，如果没有找到则返回null
     */
    private static VenueConfig selectBestVenueForPrice(String cusip, String side) {
        LOGGER.info("Analyzing venues for best {} price for {}", side, cusip);
        
        VenueConfig bestVenue = null;
        double bestPrice = 0.0;
        String bestPriceSource = "";
        Map<String, VenueAnalysis> venueAnalysis = new HashMap<>();
        
        // 分析每个可用场所
        for (String venueName : VENUE_PRIORITY) {
            VenueConfig venue = VENUE_CONFIGS.get(venueName);
            Boolean isActive = venueStatus.get(venueName);
            
            VenueAnalysis analysis = analyzeVenuePrice(cusip, side, venue, isActive);
            venueAnalysis.put(venueName, analysis);
            
            // 选择最佳价格 - 只考虑活跃且有效价格的场所
            if (Boolean.TRUE.equals(isActive) && analysis.hasValidPrice && !analysis.isStale) {
                if (bestVenue == null || isPriceBetter(analysis.targetPrice, bestPrice, side)) {
                    bestVenue = venue;
                    bestPrice = analysis.targetPrice;
                    bestPriceSource = analysis.priceSource;
                    LOGGER.info("New best venue: {} with {} price: {} (from {})", 
                        venueName, side, bestPrice, bestPriceSource);
                }
            }
        }
        
        // 输出分析结果摘要
        LOGGER.info("=== VENUE SELECTION SUMMARY ===");
        for (Map.Entry<String, VenueAnalysis> entry : venueAnalysis.entrySet()) {
            VenueAnalysis analysis = entry.getValue();
            String status = analysis.isActive ? 
                (analysis.hasValidPrice && !analysis.isStale ? 
                    String.format("Price: %.2f (Spread: %.4f)", analysis.targetPrice, analysis.spread) : 
                    "No valid price") :
                "Inactive";
            LOGGER.info("  {} | {} | {}", entry.getKey(), 
                analysis.isActive && analysis.hasValidPrice && !analysis.isStale ? "✓" : "✗", status);
        }
        
        if (bestVenue != null) {
            LOGGER.info("Selected Best Venue: {} | {} Price: {} | Source: {}", 
                bestVenue.marketSource, side, bestPrice, bestPriceSource);
        } else {
            LOGGER.warn("No venue found with valid pricing for {} {}", side, cusip);
            // 回退到当前活跃场所
            bestVenue = selectBestVenue();
            if (bestVenue != null) {
                LOGGER.info("Falling back to current best venue: {}", bestVenue.marketSource);
            }
        }
        LOGGER.info("=== END VENUE SELECTION ===");
        
        return bestVenue;
    }
    
    /**
     * 场所价格分析结果
     */
    private static class VenueAnalysis {
        final String venueName;
        final boolean hasValidPrice;
        final double targetPrice;
        final double bidPrice;
        final double askPrice;
        final double spread;
        final String priceSource;
        final String instrumentKey;
        final boolean isStale;
        final boolean isActive;
        
        VenueAnalysis(String venueName, boolean hasValidPrice, double targetPrice, 
                      double bidPrice, double askPrice, String priceSource, 
                      String instrumentKey, boolean isStale, boolean isActive) {
            this.venueName = venueName;
            this.hasValidPrice = hasValidPrice;
            this.targetPrice = targetPrice;
            this.bidPrice = bidPrice;
            this.askPrice = askPrice;
            this.spread = (askPrice > 0 && bidPrice > 0) ? (askPrice - bidPrice) : 0.0;
            this.priceSource = priceSource;
            this.instrumentKey = instrumentKey;
            this.isStale = isStale;
            this.isActive = isActive;
        }
    }
    
    /**
     * 分析特定场所的价格情况
     * 
     * @param cusip CUSIP标识符
     * @param side 订单方向
     * @param venue 场所配置
     * @param isActive 场所是否活跃
     * @return 场所分析结果
     */
    private static VenueAnalysis analyzeVenuePrice(String cusip, String side, VenueConfig venue, Boolean isActive) {
        boolean venueActive = Boolean.TRUE.equals(isActive);
        
        if (!venueActive) {
            return new VenueAnalysis(venue.marketSource, false, 0.0, 0.0, 0.0, 
                "Venue inactive", "", false, false);
        }
        
        try {
            // 查找该场所的instrument映射
            String nativeId = findInstrumentMapping(cusip, venue.marketSource);
            if (nativeId == null) {
                return new VenueAnalysis(venue.marketSource, false, 0.0, 0.0, 0.0, 
                    "No mapping", "", false, true);
            }
            
            // 查找价格数据
            Best bestPrice = null;
            String usedKey = null;
            
            // 1. 尝试nativeId
            bestPrice = orderManager.getBestPrice(nativeId);
            if (bestPrice != null) {
                usedKey = nativeId;
            }
            
            // 2. 尝试CUSIP + 后缀
            if (bestPrice == null) {
                String[] suffixes = {"_C_Fixed", "_REG_Fixed"};
                for (String suffix : suffixes) {
                    String keyToTry = cusip + suffix;
                    bestPrice = orderManager.getBestPrice(keyToTry);
                    if (bestPrice != null) {
                        usedKey = keyToTry;
                        break;
                    }
                }
            }
            
            if (bestPrice == null) {
                return new VenueAnalysis(venue.marketSource, false, 0.0, 0.0, 0.0, 
                    "No price data", usedKey, false, true);
            }
            
            // 检查价格是否过期
            boolean isStale = orderManager.isPriceStale(usedKey);
            
            // 获取价格来源
            String priceSource = getPriceSource(bestPrice, side);
            
            // 计算目标价格 - 基于老板的策略
            double targetPrice = 0.0;
            boolean hasValidPrice = false;
            
            if ("Buy".equalsIgnoreCase(side)) {
                if (bestPrice.getAsk() > 0) {
                    targetPrice = bestPrice.getAsk() - 0.01; // 买单：略低于ask
                    hasValidPrice = true;
                } else if (bestPrice.getBid() > 0) {
                    // 如果没有ask，使用bid作为基准
                    targetPrice = bestPrice.getBid();
                    hasValidPrice = true;
                }
            } else { // Sell
                if (bestPrice.getBid() > 0) {
                    targetPrice = bestPrice.getBid() + 0.01; // 卖单：略高于bid
                    hasValidPrice = true;
                } else if (bestPrice.getAsk() > 0) {
                    // 如果没有bid，使用ask作为基准
                    targetPrice = bestPrice.getAsk();
                    hasValidPrice = true;
                }
            }
            
            return new VenueAnalysis(venue.marketSource, hasValidPrice, 
                targetPrice, bestPrice.getBid(), bestPrice.getAsk(), 
                priceSource, usedKey, isStale, true);
            
        } catch (Exception e) {
            LOGGER.error("Error analyzing venue {}: ", venue.marketSource, e);
            return new VenueAnalysis(venue.marketSource, false, 0.0, 0.0, 0.0, 
                "Error", "", false, true);
        }
    }
    
    /**
     * 获取价格来源信息
     * 
     * @param bestPrice Best价格对象
     * @param side 订单方向
     * @return 价格来源字符串
     */
    private static String getPriceSource(Best bestPrice, String side) {
        try {
            Class<?> bestClass = bestPrice.getClass();
            
            if ("Buy".equalsIgnoreCase(side)) {
                // 买单关注Ask来源
                java.lang.reflect.Method getAskSrc = bestClass.getMethod("getAskSrc");
                String askSource = (String) getAskSrc.invoke(bestPrice);
                return askSource != null && !askSource.isEmpty() ? askSource : "Unknown";
            } else {
                // 卖单关注Bid来源
                java.lang.reflect.Method getBidSrc = bestClass.getMethod("getBidSrc");
                String bidSource = (String) getBidSrc.invoke(bestPrice);
                return bidSource != null && !bidSource.isEmpty() ? bidSource : "Unknown";
            }
        } catch (Exception e) {
            return "Unknown";
        }
    }
    
    /**
     * 判断价格是否更优 - 基于老板的策略
     * 
     * @param newPrice 新价格
     * @param currentBest 当前最佳价格
     * @param side 订单方向
     * @return true如果新价格更优
     */
    private static boolean isPriceBetter(double newPrice, double currentBest, String side) {
        if (currentBest == 0.0) return true; // 第一个有效价格
        
        if ("Buy".equalsIgnoreCase(side)) {
            // 买单：价格越低越好（节省成本）
            return newPrice < currentBest;
        } else {
            // 卖单：价格越高越好（增加收益）
            return newPrice > currentBest;
        }
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
            loggerContext.getLogger("MkvConnection").setLevel(Level.WARN);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.DepthListener").setLevel(Level.WARN);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.Instrument").setLevel(Level.WARN);
            
            // Set as Best
            loggerContext.getLogger("Best").setLevel(Level.WARN);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.Best").setLevel(Level.WARN);
            loggerContext.getLogger("GCBest").setLevel(Level.WARN);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.GCBest").setLevel(Level.WARN);
            
            // Set GCBestManager to INFO for concise updates
            loggerContext.getLogger("GCBestManager").setLevel(Level.INFO);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.GCBestManager").setLevel(Level.INFO);
            
            // Suppress OrderRepository shutdown statistics
            loggerContext.getLogger("OrderRepository").setLevel(Level.WARN);
            loggerContext.getLogger("com.iontrading.automatedMarketMaking.OrderRepository").setLevel(Level.WARN);
            
            // Keep main application at INFO level
            loggerContext.getLogger("MultiVenueOrderCreator").setLevel(Level.INFO);
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
        LOGGER.info("");
        
        VenueConfig selectedVenue = null;
        int updateCounter = 0;
        
        try {
            // Initialize and start MKV platform
            MultiVenueOrderCreator mainInstance = new MultiVenueOrderCreator(0);
            
            // Close and clean
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutdown signal received:");
                if (redisMessageBridge != null) {
                    try {
                        redisMessageBridge.stop();
                        LOGGER.info("Redis Bridge Stopped");
                    } catch (Exception e) {
                        LOGGER.error("Error stopping Redis Bridge: ", e);
                    }
                }

                // Stop MKV platform
                try {
                    Mkv.stop();
                    LOGGER.info("MKV Platform stopped");
                } catch (Exception e) {
                    LOGGER.error("Error stopping MKV Platform: ", e);
                }
                
                LOGGER.info("Shutdown complete");
            }));
            
            // Initialize Redis message bridge
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
                
                // Initialize EnhancedOrderManager
                orderManager = new EnhancedOrderManager();
                depthListener = new DepthListener(orderManager);
                
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
                            System.out.println("\nWaiting for market data...");
                            System.out.print("Loading: ");
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
            
            LOGGER.info("Redis bridge ready - waiting for order commands...");
            
            try {
                while (true) {
                    Thread.sleep(30000); // 改为30秒检查一次
                    updateCounter++;
                    
                    // 监控 Redis 连接（但不输出测试成功的日志）
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
                    
                    // 每10分钟输出一次价格缓存状态（仅在有新价格时）
                    if (orderManager != null && updateCounter % 20 == 0) { 
                        Map<String, Best> cachedPrices = orderManager.getAllPricesForCusip(TEST_INSTRUMENTS[0]);
                        if (!cachedPrices.isEmpty()) {
                            // 检查是否有非过期的价格
                            boolean hasCurrentPrices = false;
                            for (Map.Entry<String, Best> entry : cachedPrices.entrySet()) {
                                if (!orderManager.isPriceStale(entry.getKey())) {
                                    hasCurrentPrices = true;
                                    break;
                                }
                            }
                            
                            if (hasCurrentPrices) {
                                LOGGER.info("Active prices available for {}: {} entries", TEST_INSTRUMENTS[0], cachedPrices.size());
                                for (Map.Entry<String, Best> entry : cachedPrices.entrySet()) {
                                    Best best = entry.getValue();
                                    boolean isStale = orderManager.isPriceStale(entry.getKey());
                                    if (!isStale && (best.getBid() > 0 || best.getAsk() > 0)) {
                                        LOGGER.info("  {} - Bid: {}, Ask: {} [FRESH]",
                                            entry.getKey(), best.getBid(), best.getAsk());
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.info("Application interrupted, shutting down...");
                Thread.currentThread().interrupt();
            }
            
        } catch (Exception e) {
            LOGGER.error("Application error: ", e);
            
            // Clean up resources
            if (redisMessageBridge != null) {
                try {
                    redisMessageBridge.stop();
                    LOGGER.info("Redis message bridge stopped");
                } catch (Exception ex) {
                    LOGGER.error("Failed to stop Redis message bridge: ", ex);
                }
            }
            
            Mkv.stop();
            LOGGER.info("Application terminated due to error");
            System.exit(1);
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
                                    ((MkvPattern) mkvObject).subscribe(LOGIN_FIELDS, MultiVenueOrderCreator.this);
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
     * 根据缓存的市场数据计算动态价格
     */
    private static double calculateDynamicPriceFromCache(String nativeId, String cusip, 
            String side, Double requestedPrice) {
        LOGGER.debug("Calculating dynamic price for {} {} (native: {})", side, cusip, nativeId);
        
        try {
            if (orderManager == null) {
                LOGGER.warn("OrderManager not available, using default price");
                return DEFAULT_ORDER_PRICE;
            }
            
            Best bestPrice = null;
            String usedKey = null;
            
            // 1. 首先尝试 nativeId（这个是从 instrument mapping 来的正确 ID）
            if (nativeId != null && !nativeId.isEmpty()) {
                bestPrice = orderManager.getBestPrice(nativeId);
                if (bestPrice != null) {
                    usedKey = nativeId;
                    LOGGER.info("Found price with nativeId: {}", nativeId);
                }
            }
            
            // 2. 如果没找到，尝试添加后缀
            if (bestPrice == null) {
                String[] suffixes = {"_C_Fixed", "_REG_Fixed"};
                for (String suffix : suffixes) {
                    String keyToTry = cusip + suffix;
                    bestPrice = orderManager.getBestPrice(keyToTry);
                    if (bestPrice != null) {
                        usedKey = keyToTry;
                        LOGGER.info("Found price with suffix: {}", keyToTry);
                        break;
                    }
                }
            }
            
            // 3. 如果还没找到，搜索缓存中包含该 CUSIP 的所有键
            if (bestPrice == null) {
                Map<String, Best> allPrices = orderManager.getAllPricesForCusip("");
                LOGGER.info("Searching {} cached keys for matches containing '{}'", allPrices.size(), cusip);
                
                for (String key : allPrices.keySet()) {
                    if (key.contains(cusip)) {
                        LOGGER.info("Found cached key containing '{}': {}", cusip, key);
                        if (bestPrice == null) {
                            bestPrice = allPrices.get(key);
                            usedKey = key;
                            LOGGER.info("Using first match: {}", key);
                        }
                    }
                }
            }
            
            if (bestPrice == null) {
                LOGGER.warn("No market data found for CUSIP: {} - using default price", cusip);
                return DEFAULT_ORDER_PRICE;
            }
            
            // 简化版的市场数据显示
            String bidSource = getPriceSourceReflection(bestPrice, "getBidSrc");
            String askSource = getPriceSourceReflection(bestPrice, "getAskSrc");
            
            // 简洁的市场数据输出
            LOGGER.info("Market Data | {} | Bid: {} @ {} ({}) | Ask: {} @ {} ({})", 
                usedKey,
                bestPrice.getBid(), bestPrice.getBidSize(), bidSource,
                bestPrice.getAsk(), bestPrice.getAskSize(), askSource);
            
            // 检查价格是否过期
            boolean isStale = orderManager.isPriceStale(usedKey);
            if (isStale) {
                LOGGER.warn("Price data is STALE (>100s old) - use with caution");
            }
            
            // 计算价格逻辑...
            double calculatedPrice;
            if ("Buy".equalsIgnoreCase(side)) {
                double askPrice = bestPrice.getAsk();
                if (askPrice > 0) {
                    calculatedPrice = askPrice - 0.01;
                    LOGGER.info("Buy strategy: Ask {} - 0.01 = {}", askPrice, calculatedPrice);
                } else {
                    calculatedPrice = DEFAULT_ORDER_PRICE;
                    LOGGER.info("No valid Ask price, using default: {}", calculatedPrice);
                }
            } else { // Sell
                double bidPrice = bestPrice.getBid();
                if (bidPrice > 0) {
                    calculatedPrice = bidPrice + 0.01;
                    LOGGER.info("Sell strategy: Bid {} + 0.01 = {}", bidPrice, calculatedPrice);
                } else {
                    calculatedPrice = DEFAULT_ORDER_PRICE;
                    LOGGER.info("No valid Bid price, using default: {}", calculatedPrice);
                }
            }
            
            // 确保价格精度为 2 位小数
            calculatedPrice = Math.round(calculatedPrice * 100.0) / 100.0;
            
            LOGGER.info("Final {} price for {}: {} (Source: {})", 
                side, cusip, calculatedPrice, usedKey);
            
            return calculatedPrice;
            
        } catch (Exception e) {
            LOGGER.error("Error calculating dynamic price from cache: ", e);
            return DEFAULT_ORDER_PRICE;
        }
    }
    
    /**
     * 辅助方法：使用反射获取价格来源
     */
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
    
    /**
     * Create an order
     * 
     * @param instrumentId Instrument identifier
     * @param originalCusip Original CUSIP
     * @return MultiVenueOrderCreator instance or null if failed
     */
    public static MultiVenueOrderCreator createOrder(String instrumentId, String originalCusip) {
        reqId++;
        
        LOGGER.info("");
        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("Venue: {}", activeVenueConfig.marketSource);
        LOGGER.info("Trader: {}", activeVenueConfig.traderId);
        LOGGER.info("Instrument: {}", instrumentId);
        LOGGER.info("Quantity: {}", ORDER_QUANTITY);
        LOGGER.info("Price: {}", String.format("%.4f", dynamicOrderPrice));
        LOGGER.info("--------------------------------------------------------");
        
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
     * @return MultiVenueOrderCreator instance or null if failed
     */
    private static MultiVenueOrderCreator createOrderWithFunction(MkvFunction fn, 
            String instrumentId, String originalCusip) {
        try {
            String freeText = MarketDef.getFreeText(String.valueOf(reqId), APPLICATION_ID);
            
            MultiVenueOrderCreator order = new MultiVenueOrderCreator(reqId);
            
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
                    
                    // 发布订单成功响应到Redis
                    publishOrderResponseToRedis(
                        extractedOrderId != null ? extractedOrderId : "PENDING_" + myReqId,
                        "SUBMITTED", 
                        "Order submitted successfully", 
                        details.venue
                    );
                }
            } else {
                OrderDetails details = orderTracking.get(String.valueOf(myReqId));
                if (details != null) {
                    details.status = "FAILED";
                    details.errorMsg = result;
                    
                    // 发布订单失败响应到Redis
                    publishOrderResponseToRedis(
                        "FAILED_" + myReqId,
                        "FAILED", 
                        result, 
                        details.venue
                    );
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
            LOGGER.info("{} {}", name, registered ? "registered" : "unregistered");
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
    private static final String REDIS_CHANNEL = "order_updates";
    
    /**
     * Initialize Redis connection pool
     */
    private static void initRedis() {
        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(128);
            poolConfig.setMaxIdle(128);
            poolConfig.setMinIdle(16);
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(true);
            poolConfig.setTestWhileIdle(true);
            poolConfig.setBlockWhenExhausted(true);
            poolConfig.setMaxWaitMillis(10000);

            jedisPool = new JedisPool(poolConfig, "cacheuat", 6379, 10000, null);
            LOGGER.info("Redis connection pool initialized");

            // Start message subscription thread
            new Thread(MultiVenueOrderCreator::subscribeToOrderUpdates).start();
            
        } catch (Exception e) {
            LOGGER.error("Error initializing Redis: ", e);
        }
    }
    
    /**
     * Subscribe to order update messages
     */
    private static void subscribeToOrderUpdates() {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    handleOrderUpdateMessage(message);
                }
            }, REDIS_CHANNEL);
            
        } catch (Exception e) {
            LOGGER.error("Error in Redis subscription: ", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
    
    /**
     * Handle order update messages
     *
     * @param message Message content
     */
    private static void handleOrderUpdateMessage(String message) {
        LOGGER.info("Received order update message: {}", message);
        
        try {
            // Parse JSON message
            JSONObject jsonMessage = new JSONObject(message);
            String orderId = jsonMessage.getString("orderId");
            String status = jsonMessage.getString("status");
            String errorMsg = jsonMessage.has("errorMsg") ? jsonMessage.getString("errorMsg") : "";

            // Update order status
            updateOrderStatus(orderId, status);

            // Log detailed information
            LOGGER.info("Order update - ID: {} | Status: {} | Error: {}", orderId, status, errorMsg);
            
        } catch (Exception e) {
            LOGGER.error("Error handling order update message: ", e);
        }
    }
    
    /**
     * Initialize Redis message bridge
     */
    private static void initializeRedisBridge() {
        try {
            redisMessageBridge = new RedisMessageBridge(REDIS_HOST, REDIS_PORT);

            // Test Redis connection
            if (redisMessageBridge.testConnection()) {
                redisMessageBridge.start();

                // Start heartbeat service
                Thread heartbeatThread = new Thread(() -> {
                    while (true) {
                        try {
                            Thread.sleep(30000); // 30秒心跳
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
    
    /**
     * Publish market data to Redis (if available)
     */
    private static void publishMarketDataToRedis(String instrument, double bidPrice, double askPrice) {
        if (redisMessageBridge != null) {
            try {
                redisMessageBridge.publishMarketData(instrument, bidPrice, askPrice, 
                    100.0, 100.0, System.currentTimeMillis());
            } catch (Exception e) {
                LOGGER.debug("Failed to publish market data to Redis: ", e);
            }
        }
    }
    
    /**
     * Publish order response to Redis (if available)
     */
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
    
    /**
     * Create order from Redis message - 支持灵活的价格和场所选择
     */
    public static boolean createOrderFromRedis(String cusip, String side, double quantity, double price, String venue) {
        if (depthListener == null) {
            LOGGER.error("DepthListener not initialized, cannot create order from Redis");
            return false;
        }
        
        try {
            VenueConfig targetVenue;
            boolean useUserPrice = (price > 0); // 用户是否指定了价格
            boolean useAutoVenue = (venue == null || venue.equals("AUTO") || venue.equals("BEST_PRICE"));
            
            // ========== 场所选择逻辑 ==========
            if (useAutoVenue) {
                LOGGER.info("Auto-selecting best venue for {} {} {}", side, quantity, cusip);
                targetVenue = selectBestVenueForPrice(cusip, side);
                
                if (targetVenue == null) {
                    LOGGER.error("No suitable venue found for {} {}", side, cusip);
                    // 回退到当前活跃场所
                    targetVenue = activeVenueConfig;
                    if (targetVenue == null) {
                        LOGGER.error("No active venue available");
                        return false;
                    }
                    LOGGER.warn("Falling back to current active venue: {}", targetVenue.marketSource);
                }
            } else if (VENUE_CONFIGS.containsKey(venue)) {
                // 使用用户指定的场所
                targetVenue = VENUE_CONFIGS.get(venue);
                LOGGER.info("Using user-specified venue: {}", venue);
            } else {
                LOGGER.error("Unknown venue specified: {}", venue);
                return false;
            }
            
            // 临时更新activeVenueConfig
            VenueConfig originalVenue = activeVenueConfig;
            activeVenueConfig = targetVenue;
            
            try {
                // 查找instrument映射
                String nativeId = findInstrumentMapping(cusip, targetVenue.marketSource);
                if (nativeId == null) {
                    LOGGER.error("No instrument mapping found for CUSIP: {} on venue: {}", 
                        cusip, targetVenue.marketSource);
                    return false;
                }
                
                // ========== 价格选择逻辑 ==========
                double finalPrice;
                
                if (useUserPrice) {
                    // 用户指定了价格，直接使用
                    finalPrice = price;
                    LOGGER.info("Using user-specified price: {}", finalPrice);
                } else {
                    // 用户没有指定价格，使用动态市场价格
                    LOGGER.info("User did not specify price, calculating dynamic price from market data");
                    finalPrice = calculateDynamicPriceFromCache(nativeId, cusip, side, null);
                    
                    // 如果无法获取市场价格，使用默认价格
                    if (finalPrice == DEFAULT_ORDER_PRICE) {
                        LOGGER.warn("Could not determine market price, using default: {}", DEFAULT_ORDER_PRICE);
                    }
                }
                
                // 价格决策总结
                String priceStrategy = useUserPrice ? 
                    String.format("User-specified: %.2f", price) : 
                    "Market-based (AUTO)";
                String venueStrategy = useAutoVenue ? 
                    "Auto-selected" : 
                    "User-specified";
                    
                LOGGER.info("Order Strategy Summary:");
                LOGGER.info("  Price: {} -> Final: {}", priceStrategy, finalPrice);
                LOGGER.info("  Venue: {} -> Final: {}", venueStrategy, targetVenue.marketSource);
                
                LOGGER.info("Creating Redis order: {} {} {} @ {} -> instrument: {} on {}", 
                    side, quantity, cusip, finalPrice, nativeId, targetVenue.marketSource);
                
                // 创建订单
                MultiVenueOrderCreator orderCreator = createParameterizedOrder(
                    nativeId, cusip, side, quantity, finalPrice);
                
                return orderCreator != null;
                
            } finally {
                // 恢复原来的activeVenueConfig
                activeVenueConfig = originalVenue;
            }
            
        } catch (Exception e) {
            LOGGER.error("Error creating order from Redis: ", e);
            return false;
        }
    }
    
    /**
     * Search for instrument mapping based on CUSIP
     * 
     * @param cusip CUSIP identifier
     * @param venue Target venue (optional)
     * @return Native instrument ID or null if not found
     */
    private static String findInstrumentMapping(String cusip, String venue) {
        try {
            // Use current active venue or specified venue
            VenueConfig targetVenue = activeVenueConfig;
            if (venue != null && !venue.equals("AUTO") && VENUE_CONFIGS.containsKey(venue)) {
                targetVenue = VENUE_CONFIGS.get(venue);
            }
            
            if (targetVenue == null) {
                LOGGER.error("No target venue available for mapping");
                return null;
            }

            // Try mapping with suffix
            String[] suffixes = {"_C_Fixed", "_REG_Fixed"};
            for (String suffix : suffixes) {
                String testId = cusip + suffix;
                String result = depthListener.getInstrumentFieldBySourceString(
                    testId, targetVenue.marketSource, false);
                
                if (result != null) {
                    LOGGER.info("Found mapping with suffix: {} -> {} on {}", testId, result, targetVenue.marketSource);
                    return result;
                }
            }
            
            LOGGER.warn("No mapping found for CUSIP: {} on venue: {}", cusip, targetVenue.marketSource);
            return null;
            
        } catch (Exception e) {
            LOGGER.error("Error finding instrument mapping: ", e);
            return null;
        }
    }
    
    /**
     * Create parameterized order
     * 
     * @param instrumentId Instrument identifier
     * @param originalCusip Original CUSIP
     * @param side Order side
     * @param quantity Order quantity
     * @param price Order price
     * @return MultiVenueOrderCreator instance or null if failed
     */
    private static MultiVenueOrderCreator createParameterizedOrder(String instrumentId, String originalCusip, 
            String side, double quantity, double price) {
        reqId++;
        
        // 确保价格有效
        if (price <= 0) {
            LOGGER.warn("Invalid price {} received, calculating dynamic price", price);
            price = calculateDynamicPriceFromCache(instrumentId, originalCusip, side, null);
        }
        
        // 最终价格验证
        if (price <= 0) {
            LOGGER.error("Final price is still invalid: {}, using default", price);
            price = DEFAULT_ORDER_PRICE;
        }
        
        // 确保价格精度
        price = Math.round(price * 100.0) / 100.0;
        
        LOGGER.info("");
        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("Venue: {}", activeVenueConfig.marketSource);
        LOGGER.info("Trader: {}", activeVenueConfig.traderId);
        LOGGER.info("Instrument: {}", instrumentId);
        LOGGER.info("Side: {}", side);
        LOGGER.info("Quantity: {}", quantity);
        LOGGER.info("Price: {}", String.format("%.2f", price));
        LOGGER.info("--------------------------------------------------------");
        
        MkvPublishManager pm = Mkv.getInstance().getPublishManager();
        String functionName = activeVenueConfig.getOrderFunction();
        
        try {
            MkvFunction fn = pm.getMkvFunction(functionName);
            
            if (fn != null) {
                return createOrderWithParameterizedFunction(fn, instrumentId, originalCusip, side, quantity, price);
            } else {
                LOGGER.error("Order function not found: {}", functionName);
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Error creating parameterized order: ", e);
            return null;
        }
    }
    
    /**
     * Create order using parameterized function
     * 
     * @param fn MkvFunction for order creation
     * @param instrumentId Instrument identifier
     * @param originalCusip Original CUSIP
     * @param side Order side
     * @param quantity Order quantity
     * @param price Order price
     * @return MultiVenueOrderCreator instance or null if failed
     */
    private static MultiVenueOrderCreator createOrderWithParameterizedFunction(MkvFunction fn, 
            String instrumentId, String originalCusip, String side, double quantity, double price) {
        try {
            String freeText = MarketDef.getFreeText(String.valueOf(reqId), APPLICATION_ID);
            
            MultiVenueOrderCreator order = new MultiVenueOrderCreator(reqId);
            
            OrderDetails details = new OrderDetails("PENDING_" + reqId, reqId, 
                originalCusip, activeVenueConfig.marketSource);
            orderTracking.put(String.valueOf(reqId), details);
            
            MkvSupply args = MkvSupplyFactory.create(new Object[] {
                activeVenueConfig.traderId,        // Trader ID
                instrumentId,                       // Instrument
                side,                              // Verb (使用参数)
                Double.valueOf(price),             // Price (使用参数)
                Double.valueOf(quantity),          // QtyShown (使用参数)
                Double.valueOf(quantity),          // QtyTot (使用参数)
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
            LOGGER.debug("Parameterized order function called on {} with trader {}", 
                activeVenueConfig.marketSource, activeVenueConfig.traderId);
            
            return order;
            
        } catch (Exception e) {
            LOGGER.error("Error creating parameterized order: ", e);
            return null;
        }
    }
}