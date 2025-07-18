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
    
    // Active orders monitoring - track CUSIPs that have active orders
    private static final Set<String> activeOrderCusips = new HashSet<>();
    
    // Static log time tracking for avoiding duplicate price messages
    private static final Map<String, Long> staticLogTimeTracker = new ConcurrentHashMap<>();
    
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
        final String cusip;
        
        OrderDetails(String orderId, int reqId, String instrumentId, String venue, String cusip) {
            this.orderId = orderId;
            this.reqId = reqId;
            this.instrumentId = instrumentId;
            this.venue = venue;
            this.cusip = cusip;
            this.timestamp = System.currentTimeMillis();
        }
        
        // 保持向后兼容性的构造函数
        OrderDetails(String orderId, int reqId, String instrumentId, String venue) {
            this(orderId, reqId, instrumentId, venue, instrumentId);
        }
    }
    
    /**
     * 统一定价模型 - 核心定价逻辑
     */
    public static class UnifiedPricingModel {
        
        // 最小价差配置 (2bp = 0.02)
        private static final double MIN_SPREAD = 0.02;
        private static final double SPREAD_ADJUSTMENT = 0.01;
        
        // 价格结果类
        public static class PricingResult {
            public final double bidPrice;      // 买价
            public final double askPrice;      // 卖价
            public final boolean hasBid;       // 是否有有效买价
            public final boolean hasAsk;       // 是否有有效卖价
            public final String bidSource;     // 买价来源
            public final String askSource;     // 卖价来源
            public final String priceQuality;  // 价格质量评级
            
            public PricingResult(double bidPrice, double askPrice, boolean hasBid, boolean hasAsk,
                               String bidSource, String askSource, String priceQuality) {
                this.bidPrice = bidPrice;
                this.askPrice = askPrice;
                this.hasBid = hasBid;
                this.hasAsk = hasAsk;
                this.bidSource = bidSource;
                this.askSource = askSource;
                this.priceQuality = priceQuality;
            }
        }
        
        /**
         * 计算统一价格 - 核心定价方法
         * 
         * @param marketBest 市场Best价格
         * @param gcBestCash GC现金价格
         * @param gcBestREG GC常规价格
         * @param targetVenues 目标交易场所列表
         * @param instrumentId 工具ID用于检查活跃订单
         * @return 定价结果
         */
        public static PricingResult calculateUnifiedPrices(Best marketBest, GCBest gcBestCash, 
                                                          GCBest gcBestREG, Set<String> targetVenues, String instrumentId) {
            // 改为 DEBUG 级别
            LOGGER.debug("=== 统一定价计算开始 ===");
            
            // 1. 数据质量评估
            DataQuality marketQuality = evaluateMarketData(marketBest, targetVenues);
            DataQuality gcQuality = evaluateGCData(gcBestCash, gcBestREG);
            
            LOGGER.debug("市场数据质量: {} | GC数据质量: {}", marketQuality.level, gcQuality.level);
            
            // 2. 买价计算
            BidCalculation bidCalc = calculateBidPrice(marketQuality, gcQuality);
            
            // 3. 卖价计算 (永远不使用GC数据)
            AskCalculation askCalc = calculateAskPrice(marketQuality);
            
            // 4. 价格倒挂保护
            PricingResult finalResult = applyInversionProtection(bidCalc, askCalc);
            
            // 只在有重要变化时输出日志，并且只在有活跃订单时输出INFO级别
            if (finalResult.priceQuality.equals("ADJUSTED")) {
                // 检查是否有活跃订单需要监控
                boolean hasActiveOrder = hasActiveOrderForInstrument(instrumentId);
                
                if (hasActiveOrder) {
                    // 避免重复输出相同的价格倒挂信息
                    String priceKey = String.format("%s_%.2f_%.2f", instrumentId, 
                        finalResult.bidPrice, finalResult.askPrice);
                    
                    long currentTime = System.currentTimeMillis();
                    Long lastLogTime = staticLogTimeTracker.get(priceKey);
                    
                    // 只有在第一次或超过30秒才输出INFO级别日志
                    if (lastLogTime == null || (currentTime - lastLogTime) > 30000) {
                        LOGGER.info("价格倒挂已修正 - 买价: {} ({}) | 卖价: {} ({})", 
                            finalResult.hasBid ? String.format("%.2f", finalResult.bidPrice) : "N/A",
                            finalResult.bidSource,
                            finalResult.hasAsk ? String.format("%.2f", finalResult.askPrice) : "N/A", 
                            finalResult.askSource);
                        staticLogTimeTracker.put(priceKey, currentTime);
                    } else {
                        LOGGER.debug("价格倒挂已修正 - 买价: {} ({}) | 卖价: {} ({}) (重复抑制)", 
                            finalResult.hasBid ? String.format("%.2f", finalResult.bidPrice) : "N/A",
                            finalResult.bidSource,
                            finalResult.hasAsk ? String.format("%.2f", finalResult.askPrice) : "N/A", 
                            finalResult.askSource);
                    }
                } else {
                    LOGGER.debug("价格倒挂已修正 - 买价: {} ({}) | 卖价: {} ({}) (无活跃订单)", 
                        finalResult.hasBid ? String.format("%.2f", finalResult.bidPrice) : "N/A",
                        finalResult.bidSource,
                        finalResult.hasAsk ? String.format("%.2f", finalResult.askPrice) : "N/A", 
                        finalResult.askSource);
                }
            } else {
                LOGGER.debug("最终定价 - 买价: {} ({}) | 卖价: {} ({})", 
                    finalResult.hasBid ? String.format("%.2f", finalResult.bidPrice) : "N/A",
                    finalResult.bidSource,
                    finalResult.hasAsk ? String.format("%.2f", finalResult.askPrice) : "N/A", 
                    finalResult.askSource);
            }
            
            LOGGER.debug("=== 统一定价计算完成 ===");
            
            return finalResult;
        }
        
        /**
         * 检查指定工具是否有活跃订单（静态方法）
         */
        private static boolean hasActiveOrderForInstrument(String instrumentId) {
            // 检查是否有活跃的订单追踪
            for (OrderDetails order : orderTracking.values()) {
                if ("PENDING".equals(order.status) || "SUBMITTED".equals(order.status) || 
                    "FILLED".equals(order.status) || "PARTIALLY_FILLED".equals(order.status)) {
                    // 检查订单是否与此工具相关
                    if (instrumentId.contains(order.cusip) || order.cusip.contains(instrumentId)) {
                        return true;
                    }
                }
            }
            
            // 检查 activeOrderCusips 集合
            for (String cusip : activeOrderCusips) {
                if (instrumentId.contains(cusip) || cusip.contains(instrumentId)) {
                    return true;
                }
            }
            
            return false;
        }
        
        /**
         * 数据质量评估结果
         */
        private static class DataQuality {
            final String level;  // HIGH, MEDIUM, LOW, NONE
            final double bidPrice;
            final double askPrice;
            final String bidSource;
            final String askSource;
            final boolean hasBid;
            final boolean hasAsk;
            
            DataQuality(String level, double bidPrice, double askPrice, 
                       String bidSource, String askSource, boolean hasBid, boolean hasAsk) {
                this.level = level;
                this.bidPrice = bidPrice;
                this.askPrice = askPrice;
                this.bidSource = bidSource;
                this.askSource = askSource;
                this.hasBid = hasBid;
                this.hasAsk = hasAsk;
            }
        }
        
        /**
         * 评估市场数据质量
         */
        private static DataQuality evaluateMarketData(Best marketBest, Set<String> targetVenues) {
            if (marketBest == null) {
                return new DataQuality("NONE", 0, 0, "", "", false, false);
            }
            
            boolean hasBid = marketBest.getBid() > 0;
            boolean hasAsk = marketBest.getAsk() > 0;
            String bidSource = getPriceSourceReflection(marketBest, "getBidSrc");
            String askSource = getPriceSourceReflection(marketBest, "getAskSrc");
            
            // 检查来源是否在目标场所列表中
            boolean bidFromTarget = targetVenues.contains(bidSource);
            boolean askFromTarget = targetVenues.contains(askSource);
            
            String level;
            if (hasBid && hasAsk && bidFromTarget && askFromTarget) {
                level = "HIGH";
            } else if ((hasBid && bidFromTarget) || (hasAsk && askFromTarget)) {
                level = "MEDIUM";
            } else if (hasBid || hasAsk) {
                level = "LOW";
            } else {
                level = "NONE";
            }
            
            return new DataQuality(level, marketBest.getBid(), marketBest.getAsk(),
                                  bidSource, askSource, hasBid && bidFromTarget, hasAsk && askFromTarget);
        }
        
        /**
         * 评估GC数据质量
         */
        private static DataQuality evaluateGCData(GCBest gcBestCash, GCBest gcBestREG) {
            // 优先使用现金GC，其次使用REG
            GCBest gcToUse = null;
            String gcType = "";
            
            if (gcBestCash != null && (gcBestCash.getBid() > 0 || gcBestCash.getAsk() > 0)) {
                gcToUse = gcBestCash;
                gcType = "CASH";
            } else if (gcBestREG != null && (gcBestREG.getBid() > 0 || gcBestREG.getAsk() > 0)) {
                gcToUse = gcBestREG;
                gcType = "REG";
            }
            
            if (gcToUse == null) {
                return new DataQuality("NONE", 0, 0, "", "", false, false);
            }
            
            boolean hasBid = gcToUse.getBid() > 0;
            boolean hasAsk = gcToUse.getAsk() > 0;
            
            String level;
            if (hasBid && hasAsk) {
                level = "HIGH";
            } else if (hasBid || hasAsk) {
                level = "MEDIUM";
            } else {
                level = "NONE";
            }
            
            return new DataQuality(level, gcToUse.getBid(), gcToUse.getAsk(),
                                  "GC_" + gcType, "GC_" + gcType, hasBid, hasAsk);
        }
        
        /**
         * 买价计算结果
         */
        private static class BidCalculation {
            final double price;
            final String source;
            final boolean valid;
            
            BidCalculation(double price, String source, boolean valid) {
                this.price = price;
                this.source = source;
                this.valid = valid;
            }
        }
        
        /**
         * 计算买价
         */
        private static BidCalculation calculateBidPrice(DataQuality market, DataQuality gc) {
            // 买价策略：可以使用市场数据和GC数据
            
            if (market.hasBid && gc.hasBid) {
                // 两者都有买价，选择较低的（保守策略）
                if (market.bidPrice <= gc.bidPrice) {
                    double finalBid = market.bidPrice + SPREAD_ADJUSTMENT;
                    return new BidCalculation(finalBid, market.bidSource + " (优于GC)", true);
                } else {
                    double finalBid = gc.bidPrice + SPREAD_ADJUSTMENT;
                    return new BidCalculation(finalBid, gc.bidSource + " (优于市场)", true);
                }
            } else if (market.hasBid) {
                // 只有市场买价
                double finalBid = market.bidPrice + SPREAD_ADJUSTMENT;
                return new BidCalculation(finalBid, market.bidSource, true);
            } else if (gc.hasBid) {
                // 只有GC买价
                double finalBid = gc.bidPrice + SPREAD_ADJUSTMENT;
                return new BidCalculation(finalBid, gc.bidSource, true);
            } else {
                // 无买价数据
                return new BidCalculation(0, "无数据", false);
            }
        }
        
        /**
         * 卖价计算结果
         */
        private static class AskCalculation {
            final double price;
            final String source;
            final boolean valid;
            
            AskCalculation(double price, String source, boolean valid) {
                this.price = price;
                this.source = source;
                this.valid = valid;
            }
        }
        
        /**
         * 计算卖价
         */
        private static AskCalculation calculateAskPrice(DataQuality market) {
            // 卖价策略：永远不使用GC数据
            
            if (market.hasAsk) {
                double finalAsk = market.askPrice - SPREAD_ADJUSTMENT;
                return new AskCalculation(finalAsk, market.askSource, true);
            } else {
                // 无卖价数据
                return new AskCalculation(0, "无数据", false);
            }
        }
        
        /**
         * 应用价格倒挂保护
         */
        private static PricingResult applyInversionProtection(BidCalculation bid, AskCalculation ask) {
            // 如果买卖价都有效，检查是否倒挂
            if (bid.valid && ask.valid && bid.price >= ask.price) {
                LOGGER.debug("检测到价格倒挂 - 买价: {} >= 卖价: {}", bid.price, ask.price);
                
                // 计算中间价
                double midPrice = (bid.price + ask.price) / 2.0;
                
                // 使用最小价差重新计算
                double newBid = midPrice - (MIN_SPREAD / 2.0);
                double newAsk = midPrice + (MIN_SPREAD / 2.0);
                
                // 确保价格精度
                newBid = Math.round(newBid * 100.0) / 100.0;
                newAsk = Math.round(newAsk * 100.0) / 100.0;
                
                LOGGER.debug("价格倒挂修正 - 新买价: {} | 新卖价: {}", newBid, newAsk);
                
                return new PricingResult(newBid, newAsk, true, true,
                                       bid.source + " (已修正)", ask.source + " (已修正)",
                                       "ADJUSTED");
            }
            
            // 正常情况，确保价格精度
            double finalBid = bid.valid ? Math.round(bid.price * 100.0) / 100.0 : 0;
            double finalAsk = ask.valid ? Math.round(ask.price * 100.0) / 100.0 : 0;
            
            String quality = (bid.valid && ask.valid) ? "NORMAL" : "PARTIAL";
            
            return new PricingResult(finalBid, finalAsk, bid.valid, ask.valid,
                                   bid.source, ask.source, quality);
        }
    }
    
    /**
     * Enhanced order manager implementation with price caching
     */
    private static class EnhancedOrderManager implements IOrderManager {
        // 缓存最佳价格，key 为 instrument ID
        private final ConcurrentHashMap<String, Best> bestPriceCache = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Long> priceUpdateTime = new ConcurrentHashMap<>();
        
        // 缓存GC价格数据
        private final ConcurrentHashMap<String, GCBest> gcCashCache = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, GCBest> gcRegCache = new ConcurrentHashMap<>();
        
        // 添加价格变化追踪
        private final ConcurrentHashMap<String, UnifiedPricingModel.PricingResult> lastPricingResults = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Long> lastLogTime = new ConcurrentHashMap<>();
        private static final long LOG_INTERVAL = 10000; // 10秒内相同价格只记录一次
        
        // 目标交易场所集合
        private static final Set<String> TARGET_VENUES = new HashSet<>(Arrays.asList(
            "FENICS_USREPO", "BTEC_REPO_US", "DEALERWEB_REPO"
        ));
        
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
                
                // 缓存GC数据
                if (gcBestCash != null) {
                    gcCashCache.put(instrumentId, gcBestCash);
                }
                if (gcBestREG != null) {
                    gcRegCache.put(instrumentId, gcBestREG);
                }
                
                // 使用统一定价模型计算价格
                UnifiedPricingModel.PricingResult pricing = 
                    UnifiedPricingModel.calculateUnifiedPrices(best, gcBestCash, gcBestREG, TARGET_VENUES, instrumentId);
                
                // 检查是否需要记录日志
                boolean shouldLog = false;
                UnifiedPricingModel.PricingResult lastPricing = lastPricingResults.get(instrumentId);
                Long lastLog = lastLogTime.get(instrumentId);
                long currentTime = System.currentTimeMillis();
                
                if (lastPricing == null || 
                    !isSamePricing(pricing, lastPricing) || 
                    (lastLog == null || currentTime - lastLog > LOG_INTERVAL)) {
                    shouldLog = true;
                    lastPricingResults.put(instrumentId, pricing);
                    lastLogTime.put(instrumentId, currentTime);
                }
                
                // 只在价格有变化或间隔超时时记录，并且只有在有活跃订单时才输出
                if (shouldLog && !"NONE".equals(pricing.priceQuality)) {
                    // 检查是否有活跃订单需要监控这个工具
                    boolean hasActiveOrder = hasActiveOrderForInstrument(instrumentId);
                    
                    if (hasActiveOrder) {
                        LOGGER.info("价格更新 [{}] - 买: {} @ {} | 卖: {} @ {} | 质量: {}", 
                            instrumentId,
                            pricing.hasBid ? String.format("%.2f", pricing.bidPrice) : "N/A", 
                            pricing.bidSource,
                            pricing.hasAsk ? String.format("%.2f", pricing.askPrice) : "N/A", 
                            pricing.askSource,
                            pricing.priceQuality);
                    } else {
                        // 如果没有活跃订单，使用DEBUG级别记录
                        LOGGER.debug("价格更新 [{}] - 买: {} @ {} | 卖: {} @ {} | 质量: {} (无活跃订单)", 
                            instrumentId,
                            pricing.hasBid ? String.format("%.2f", pricing.bidPrice) : "N/A", 
                            pricing.bidSource,
                            pricing.hasAsk ? String.format("%.2f", pricing.askPrice) : "N/A", 
                            pricing.askSource,
                            pricing.priceQuality);
                    }
                }
            }
        }
        
        /**
         * 检查指定工具是否有活跃订单
         */
        private boolean hasActiveOrderForInstrument(String instrumentId) {
            // 检查是否有活跃的订单追踪
            for (OrderDetails order : orderTracking.values()) {
                if ("PENDING".equals(order.status) || "SUBMITTED".equals(order.status) || 
                    "FILLED".equals(order.status) || "PARTIALLY_FILLED".equals(order.status)) {
                    // 检查订单是否与此工具相关
                    if (instrumentId.contains(order.cusip) || order.cusip.contains(instrumentId)) {
                        return true;
                    }
                }
            }
            
            // 检查 activeOrderCusips 集合
            for (String cusip : activeOrderCusips) {
                if (instrumentId.contains(cusip) || cusip.contains(instrumentId)) {
                    return true;
                }
            }
            
            return false;
        }
        
        /**
         * 比较两个定价结果是否相同
         */
        private boolean isSamePricing(UnifiedPricingModel.PricingResult p1, 
                                      UnifiedPricingModel.PricingResult p2) {
            return p1.hasBid == p2.hasBid &&
                   p1.hasAsk == p2.hasAsk &&
                   Math.abs(p1.bidPrice - p2.bidPrice) < 0.001 &&
                   Math.abs(p1.askPrice - p2.askPrice) < 0.001 &&
                   p1.priceQuality.equals(p2.priceQuality);
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
         * 获取统一定价结果
         */
        public UnifiedPricingModel.PricingResult getUnifiedPrice(String instrumentId) {
            Best best = getBestPrice(instrumentId);
            if (best == null) {
                return new UnifiedPricingModel.PricingResult(0, 0, false, false, "", "", "NONE");
            }
            
            // 获取对应的GC数据
            GCBest gcCash = gcCashCache.get(instrumentId);
            GCBest gcReg = gcRegCache.get(instrumentId);
            
            return UnifiedPricingModel.calculateUnifiedPrices(best, gcCash, gcReg, TARGET_VENUES, instrumentId);
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
         * 价格是否过期（超过100秒）
         */
        public boolean isPriceStale(String instrumentId) {
            long updateTime = getPriceUpdateTime(instrumentId);
            if (updateTime == 0) return true;

            return (System.currentTimeMillis() - updateTime) > 100000; // 100秒
        }
        
        /**
         * 获取所有缓存的定价结果
         */
        public Map<String, UnifiedPricingModel.PricingResult> getAllCachedPricingResults() {
            return new HashMap<>(lastPricingResults);
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
     * 选择任何活跃场所
     */
    private static VenueConfig selectBestVenue() {
        for (Map.Entry<String, VenueConfig> entry : VENUE_CONFIGS.entrySet()) {
            String venueName = entry.getKey();
            VenueConfig venue = entry.getValue();
            Boolean isActive = venueStatus.get(venueName);
            
            if (Boolean.TRUE.equals(isActive)) {
                return venue;
            }
        }
        
        // 如果没有活跃场所，返回第一个可用场所
        return VENUE_CONFIGS.values().iterator().next();
    }
    
    /**
     * 基于价格选择最佳场所
     */
    private static VenueConfig selectBestVenueForPrice(String cusip, String side) {
        VenueConfig bestVenue = null;
        double bestPrice = 0.0;
        String bestPriceSource = "";
        
        // 分析所有场所
        for (Map.Entry<String, VenueConfig> entry : VENUE_CONFIGS.entrySet()) {
            String venueName = entry.getKey();
            VenueConfig venue = entry.getValue();
            Boolean isActive = venueStatus.get(venueName);
            
            VenueAnalysis analysis = analyzeVenuePrice(cusip, side, venue, isActive);
            
            // 选择最佳价格 - 只考虑活跃且有效价格的场所
            if (Boolean.TRUE.equals(isActive) && analysis.hasValidPrice && !analysis.isStale) {
                if (bestVenue == null || isPriceBetter(analysis.targetPrice, bestPrice, side)) {
                    bestVenue = venue;
                    bestPrice = analysis.targetPrice;
                    bestPriceSource = analysis.priceSource;
                }
            }
        }
        
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
            
            // 使用统一定价模型获取价格
            UnifiedPricingModel.PricingResult pricing = orderManager.getUnifiedPrice(nativeId);
            
            if (pricing.priceQuality.equals("NONE")) {
                return new VenueAnalysis(venue.marketSource, false, 0.0, 0.0, 0.0, 
                    "No price data", nativeId, false, true);
            }
            
            // 检查价格是否过期
            boolean isStale = orderManager.isPriceStale(nativeId);
            
            // 计算目标价格
            double targetPrice = 0.0;
            String priceSource = "";
            boolean hasValidPrice = false;
            
            if ("Buy".equalsIgnoreCase(side)) {
                if (pricing.hasAsk) {
                    targetPrice = pricing.askPrice;
                    priceSource = pricing.askSource;
                    hasValidPrice = true;
                } else if (pricing.hasBid) {
                    targetPrice = pricing.bidPrice + 0.02;
                    priceSource = pricing.bidSource + " (买+2bp)";
                    hasValidPrice = true;
                }
            } else { // Sell
                if (pricing.hasBid) {
                    targetPrice = pricing.bidPrice;
                    priceSource = pricing.bidSource;
                    hasValidPrice = true;
                } else if (pricing.hasAsk) {
                    targetPrice = pricing.askPrice - 0.02;
                    priceSource = pricing.askSource + " (卖-2bp)";
                    hasValidPrice = true;
                }
            }
            
            return new VenueAnalysis(venue.marketSource, hasValidPrice, 
                targetPrice, pricing.bidPrice, pricing.askPrice, 
                priceSource, nativeId, isStale, true);
            
        } catch (Exception e) {
            LOGGER.error("Error analyzing venue {}: ", venue.marketSource, e);
            return new VenueAnalysis(venue.marketSource, false, 0.0, 0.0, 0.0, 
                "Error", "", false, true);
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
     * 使用统一定价模型计算动态价格
     */
    private static double calculateDynamicPriceWithUnifiedModel(String nativeId, String cusip, 
            String side, Double requestedPrice) {
        LOGGER.info("使用统一定价模型计算 {} {} 价格", side, cusip);
        
        try {
            if (orderManager == null) {
                LOGGER.warn("OrderManager not available, using default price");
                return DEFAULT_ORDER_PRICE;
            }
            
            // 获取统一定价结果
            UnifiedPricingModel.PricingResult pricing = orderManager.getUnifiedPrice(nativeId);
            
            if (pricing.priceQuality.equals("NONE")) {
                // 尝试使用CUSIP查找
                Map<String, Best> allPrices = orderManager.getAllPricesForCusip(cusip);
                for (String key : allPrices.keySet()) {
                    pricing = orderManager.getUnifiedPrice(key);
                    if (!pricing.priceQuality.equals("NONE")) {
                        LOGGER.info("使用备选键 {} 的统一定价", key);
                        break;
                    }
                }
            }
            
            // 根据买卖方向选择价格
            double finalPrice;
            String priceInfo;
            
            if ("Buy".equalsIgnoreCase(side)) {
                if (pricing.hasAsk) {
                    // 买单使用卖价（我们要买，看对方的卖价）
                    finalPrice = pricing.askPrice;
                    priceInfo = String.format("使用统一卖价: %.2f (来源: %s)", 
                        finalPrice, pricing.askSource);
                } else if (pricing.hasBid) {
                    // 如果没有卖价，使用买价作为参考
                    finalPrice = pricing.bidPrice + 0.02; // 加2bp作为买价
                    priceInfo = String.format("无卖价，使用买价+2bp: %.2f (基于: %s)", 
                        finalPrice, pricing.bidSource);
                } else {
                    finalPrice = DEFAULT_ORDER_PRICE;
                    priceInfo = "无市场价格，使用默认值";
                }
            } else { // Sell
                if (pricing.hasBid) {
                    // 卖单使用买价（我们要卖，看对方的买价）
                    finalPrice = pricing.bidPrice;
                    priceInfo = String.format("使用统一买价: %.2f (来源: %s)", 
                        finalPrice, pricing.bidSource);
                } else if (pricing.hasAsk) {
                    // 如果没有买价，使用卖价作为参考
                    finalPrice = pricing.askPrice - 0.02; // 减2bp作为卖价
                    priceInfo = String.format("无买价，使用卖价-2bp: %.2f (基于: %s)", 
                        finalPrice, pricing.askSource);
                } else {
                    finalPrice = DEFAULT_ORDER_PRICE;
                    priceInfo = "无市场价格，使用默认值";
                }
            }
            
            LOGGER.info("{} | 价格质量: {}", priceInfo, pricing.priceQuality);
            
            return finalPrice;
            
        } catch (Exception e) {
            LOGGER.error("统一定价计算出错: ", e);
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
                
                // 如果订单完成或取消，从活跃订单列表中移除CUSIP
                if ("FILLED".equals(status) || "CANCELLED".equals(status) || "REJECTED".equals(status)) {
                    activeOrderCusips.remove(details.cusip);
                    LOGGER.info("Removed CUSIP {} from active order monitoring (Status: {})", details.cusip, status);
                } else if ("SUBMITTED".equals(status)) {
                    // 订单已提交，设置1分钟后自动清理（避免无限日志）
                    scheduleOrderCleanup(details, 60000); // 1分钟 = 60秒
                }
                break;
            }
        }
    }
    
    /**
     * Schedule automatic order cleanup after timeout
     * 
     * @param orderDetails Order details to cleanup
     * @param timeoutMs Timeout in milliseconds
     */
    private static void scheduleOrderCleanup(OrderDetails orderDetails, long timeoutMs) {
        Thread cleanupThread = new Thread(() -> {
            try {
                Thread.sleep(timeoutMs);
                
                // 检查订单是否仍为 SUBMITTED 状态
                if ("SUBMITTED".equals(orderDetails.status)) {
                    LOGGER.info("Auto-cleanup: Order {} still in SUBMITTED state after {} minutes, removing from active monitoring", 
                        orderDetails.orderId, timeoutMs / 60000);
                    
                    // 更新状态为 ASSUMED_FILLED 并从活跃列表中移除
                    orderDetails.status = "ASSUMED_FILLED";
                    activeOrderCusips.remove(orderDetails.cusip);
                    
                    LOGGER.info("Removed CUSIP {} from active order monitoring (Auto-cleanup)", orderDetails.cusip);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        cleanupThread.setDaemon(true);
        cleanupThread.start();
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
            
            // 添加对统一定价模型的日志配置
            loggerContext.getLogger("UnifiedPricingModel").setLevel(Level.INFO);
            loggerContext.getLogger(UnifiedPricingModel.class).setLevel(Level.INFO);
            
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
            
            // Only display venue status summary when there are active orders
            // This reduces startup noise as venue status is checked dynamically during order placement
            
            // 不在启动时选择默认场所，只在真正需要时选择
            if (selectedVenue != null) {
                // 只有用户明确指定了场所才设置
                activeVenueConfig = selectedVenue;
                Boolean isActive = venueStatus.get(activeVenueConfig.marketSource);
                if (!Boolean.TRUE.equals(isActive)) {
                    LOGGER.warn("WARNING: Selected venue {} is INACTIVE", activeVenueConfig.marketSource);
                    LOGGER.warn("Orders may fail with error 101. Consider selecting an active venue.");
                }
                LOGGER.debug("User-specified venue: {} with Trader: {}", 
                    activeVenueConfig.marketSource, activeVenueConfig.traderId);
            } else {
                // 系统启动时不选择默认场所
                activeVenueConfig = null;
                LOGGER.debug("No default venue selected - will auto-select when needed");
            }
            
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
                    
                    // 每10分钟输出一次市场状态汇总
                    if (orderManager != null && updateCounter % 20 == 0) { 
                        Map<String, UnifiedPricingModel.PricingResult> currentPrices = new HashMap<>();
                        Map<String, Best> cachedPrices = orderManager.getAllPricesForCusip(TEST_INSTRUMENTS[0]);
                        
                        int validPriceCount = 0;
                        int stalePriceCount = 0;
                        int adjustedPriceCount = 0;
                        
                        for (Map.Entry<String, Best> entry : cachedPrices.entrySet()) {
                            if (!orderManager.isPriceStale(entry.getKey())) {
                                UnifiedPricingModel.PricingResult pricing = orderManager.getUnifiedPrice(entry.getKey());
                                if (!"NONE".equals(pricing.priceQuality)) {
                                    validPriceCount++;
                                    if ("ADJUSTED".equals(pricing.priceQuality)) {
                                        adjustedPriceCount++;
                                    }
                                    currentPrices.put(entry.getKey(), pricing);
                                }
                            } else {
                                stalePriceCount++;
                            }
                        }
                        
                        if (validPriceCount > 0) {
                            LOGGER.info("=== 市场状态汇总 ===");
                            LOGGER.info("活跃价格: {} | 过期价格: {} | 倒挂修正: {}", 
                                validPriceCount, stalePriceCount, adjustedPriceCount);
                            
                            // 只显示前3个有效价格作为示例
                            int count = 0;
                            for (Map.Entry<String, UnifiedPricingModel.PricingResult> entry : currentPrices.entrySet()) {
                                if (count++ >= 3) break;
                                UnifiedPricingModel.PricingResult p = entry.getValue();
                                LOGGER.info("  {} - 买: {} | 卖: {} | 质量: {}", 
                                    entry.getKey(),
                                    p.hasBid ? String.format("%.2f", p.bidPrice) : "N/A",
                                    p.hasAsk ? String.format("%.2f", p.askPrice) : "N/A",
                                    p.priceQuality);
                            }
                            LOGGER.info("===================");
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
     * Create an order
     * 
     * @param instrumentId Instrument identifier
     * @param originalCusip Original CUSIP
     * @return MultiVenueOrderCreator instance or null if failed
     */
    public static MultiVenueOrderCreator createOrder(String instrumentId, String originalCusip) {
        reqId++;
        
        // 如果没有活跃场所，选择一个
        if (activeVenueConfig == null) {
            activeVenueConfig = selectBestVenue();
            if (activeVenueConfig == null) {
                LOGGER.error("No active venue available for order creation");
                return null;
            }
        }
        
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
               originalCusip, activeVenueConfig.marketSource, originalCusip);
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
    * Create order from Redis message - 支持灵活的价格和场所选择
    */
   public static boolean createOrderFromRedis(String cusip, String side, double quantity, double price, String venue) {
       if (depthListener == null) {
           LOGGER.error("DepthListener not initialized, cannot create order from Redis");
           return false;
       }
       
       // 添加CUSIP到活跃订单列表
       activeOrderCusips.add(cusip);
       LOGGER.debug("Added CUSIP {} to active order monitoring", cusip);
       
       try {
           VenueConfig targetVenue;
           boolean useUserPrice = (price > 0); // 用户是否指定了价格
           boolean useAutoVenue = (venue == null || venue.equals("AUTO") || venue.equals("BEST_PRICE"));
           
           // 场所选择逻辑
           if (useAutoVenue) {
               // 使用基于价格的选择
               targetVenue = selectBestVenueForPrice(cusip, side);
               
               if (targetVenue == null) {
                   // 回退到任何活跃场所
                   targetVenue = selectBestVenue();
                   if (targetVenue == null) {
                       LOGGER.error("No active venue available");
                       return false;
                   }
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
                   // 使用统一定价模型
                   LOGGER.info("Using unified pricing model to calculate price");
                   finalPrice = calculateDynamicPriceWithUnifiedModel(nativeId, cusip, side, null);
                   
                   if (finalPrice == DEFAULT_ORDER_PRICE) {
                       LOGGER.warn("Unified pricing failed, using default price: {}", DEFAULT_ORDER_PRICE);
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
                   LOGGER.info("Found mapping: {} -> {} on {}", testId, result, targetVenue.marketSource);
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
       
       // 确保有活跃场所
       if (activeVenueConfig == null) {
           LOGGER.error("No active venue configured for parameterized order creation");
           return null;
       }
       
       // 确保价格有效
       if (price <= 0) {
           LOGGER.warn("Invalid price {} received, calculating dynamic price", price);
           price = calculateDynamicPriceWithUnifiedModel(instrumentId, originalCusip, side, null);
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
               originalCusip, activeVenueConfig.marketSource, originalCusip);
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
}