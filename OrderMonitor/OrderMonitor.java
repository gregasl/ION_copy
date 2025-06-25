// 订单监控系统 - 独立程序，监听算法心跳并在失效时清理订单
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.helper.MkvSupplyBuilder;
import com.iontrading.mkv.qos.MkvQoS;
import com.iontrading.mkv.exceptions.MkvException;

/**
 * 独立的订单监控程序
 * 监听FENICS_Market_Maker算法心跳，检测到异常时清理未完成订单
 * 
 * 与FENICS_Market_Maker系统集成：
 * 1. 监听FENICS_Market_Maker的心跳信号 (HEARTBEAT:ION:MARKETMAKERUAT)
 * 2. 监控FENICS_Market_Maker的订单状态 (ION:ION_ORDER_DATA_UAT:USD.CM_ORDER.VMO_REPO_US.*)
 * 3. 在算法失效时自动清理相关订单
 * 4. 支持UAT环境配置
 * 
 * 使用真实的交易配置：
 * - 真实的trader ID: TEST2, asldevtrd1, frosasl1
 * - 真实的broker: BTEC_REPO_US, DEALERWEB_REPO, FENICS_USREPO
 * - 真实的CUSIP格式: 9位数字字母组合
 */
public class OrderMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderMonitor.class);
    private static final String REDIS_HOST = "cacheuat";
    private static final int REDIS_PORT = 6379;
    private static final String HEARTBEAT_CHANNEL = "HEARTBEAT:ION:MARKETMAKERUAT";
    private static final String ORDER_CHANNEL = "ION:ION_ORDER_DATA_UAT:USD.CM_ORDER.VMO_REPO_US.";
    private static final long HEARTBEAT_INTERVAL = 30000;  // 30秒
    private static final int MAX_MISSED_HEARTBEATS = 10;   // 错过10次触发清理 (5分钟)
    
    // 真实的交易配置 - 从FENICS_Market_Maker复制
    private static final Map<String, String> VENUE_TO_TRADER_MAP = new HashMap<>();
    static {
        VENUE_TO_TRADER_MAP.put("BTEC_REPO_US", "TEST2");
        VENUE_TO_TRADER_MAP.put("DEALERWEB_REPO", "asldevtrd1");
        VENUE_TO_TRADER_MAP.put("FENICS_USREPO", "frosasl1");
    }
    
    // 真实的CUSIP示例 - 美国国债CUSIP格式
    private static final String[] REAL_CUSIP_EXAMPLES = {
        "912810TM9",  // 2年期国债
        "912810TV8",  // 3年期国债
        "912810TW6",  // 5年期国债
        "912810TX4",  // 7年期国债
        "912810TY2",  // 10年期国债
        "912810TZ9",  // 20年期国债
        "912810UA3",  // 30年期国债
        "91282CJQ7",  // TIPS 5年期
        "91282CJR5",  // TIPS 10年期
        "91282CJS3"   // TIPS 30年期
    };
    
    // 订单ID计数器 - 用于生成唯一订单ID
    private static final AtomicLong orderIdCounter = new AtomicLong(1);
    
    // 心跳跟踪：算法ID -> 最后心跳时间
    private final Map<String, Long> algoLastHeartbeat = new ConcurrentHashMap<>();
    
    // 活跃订单：订单ID -> 订单信息
    private final Map<String, OrderInfo> activeOrders = new ConcurrentHashMap<>();
    
    // 市场源 -> 订单列表的映射（用于快速查找）
    private final Map<String, Map<String, Set<String>>> sourceToAlgoToOrders = new ConcurrentHashMap<>();
    
    // 订单统计：算法名 -> 新增订单计数（用于批量日志）
    private final Map<String, Integer> newOrderCounts = new ConcurrentHashMap<>();
    private long lastOrderLogTime = 0;
    private static final long ORDER_LOG_INTERVAL = 10000; // 10秒输出一次统计
    
    private JedisPool jedisPool;
    private volatile boolean running = false;
    private Thread heartbeatChecker;
    private Thread redisListener;
    
    /**
     * 订单信息类
     */
    static class OrderInfo {
        final String OrigId;
        final String Trader;
        final String CompNameOrigin;
        final String OrigSrc;
        final String InstrumentId;  // 添加CUSIP字段
        
        OrderInfo(String OrigId, String Trader, String CompNameOrigin, String OrigSrc, String InstrumentId) {
            this.OrigId = OrigId;
            this.Trader = Trader;
            this.CompNameOrigin = CompNameOrigin;
            this.OrigSrc = OrigSrc;
            this.InstrumentId = InstrumentId;
        }
        
        @Override
        public String toString() {
            return String.format("Order[OrigId=%s, Trader=%s, CompNameOrigin=%s, OrigSrc=%s, CUSIP=%s]", 
                OrigId, Trader, CompNameOrigin, OrigSrc, InstrumentId);
        }
    }
    
    /**
     * 生成真实的订单ID
     * 根据FENICS_Market_Maker中的规定，订单ID应该符合ION系统的标准格式
     * 订单记录ID格式: USD.CM_ORDER.VMO_REPO_US.{orderId}
     * 
     * 不同broker的订单ID格式：
     * FENICS_USREPO: {BROKER}_{TRADER}_{SEQUENCE}
     * BTEC_REPO_US: {BROKER}_MKV_SP25_{CUSIP}_C_Par_0_{TIMESTAMP}
     * DEALERWEB_REPO: {BROKER}_{TRADER}_{SEQUENCE}
     */
    private String generateRealOrderId(String broker, String trader, String cusip) {
        long timestamp = System.currentTimeMillis();
        long sequence = orderIdCounter.getAndIncrement();
        
        if ("FENICS_USREPO".equals(broker)) {
            // FENICS_USREPO格式: FENICS_USREPO_frosasl1_123456
            return String.format("%s_%s_%d", broker, trader, sequence);
        } else if ("BTEC_REPO_US".equals(broker)) {
            // BTEC_REPO_US格式: BTEC_REPO_US_MKV_SP25_912810TM9_C_Par_0_1750678565672
            return String.format("%s_MKV_SP25_%s_C_Par_0_%d", broker, cusip, timestamp);
        } else if ("DEALERWEB_REPO".equals(broker)) {
            // DEALERWEB_REPO格式: DEALERWEB_REPO_asldevtrd1_123456
            return String.format("%s_%s_%d", broker, trader, sequence);
        } else {
            // 默认格式
            return String.format("%s_%s_%d", broker, trader, sequence);
        }
    }
    
    /**
     * 获取标准格式的CUSIP
     * 根据broker使用不同的格式
     */
    private String getStandardCusip(String broker, String cusip) {
        if ("BTEC_REPO_US".equals(broker)) {
            return cusip + "_C_Par_0";  // BTEC格式
        } else {
            return cusip + "_C_Fixed";  // FENICS和DEALERWEB格式
        }
    }
    
    /**
     * 生成订单记录ID
     * 根据FENICS_Market_Maker中的规定：USD.CM_ORDER.VMO_REPO_US.{orderId}
     */
    private String generateOrderRecordId(String orderId) {
        return "USD.CM_ORDER.VMO_REPO_US." + orderId;
    }
    
    /**
     * 启动监控器
     */
    public void start() {
        LOGGER.info("启动订单监控器...");
        LOGGER.info("心跳间隔: {}秒, 最大错过次数: {}, 超时时间: {}分钟", 
            HEARTBEAT_INTERVAL/1000, MAX_MISSED_HEARTBEATS, (MAX_MISSED_HEARTBEATS * HEARTBEAT_INTERVAL) / 60000);
        LOGGER.info("支持的交易商: {}", VENUE_TO_TRADER_MAP);
        
        running = true;
        initRedis();
        startHeartbeatChecker();
        startRedisListener();
        LOGGER.info("订单监控器启动完成");
    }
    
    /**
     * 停止监控器
     */
    public void stop() {
        LOGGER.info("正在停止订单监控器...");
        running = false;
        if (heartbeatChecker != null) {
            heartbeatChecker.interrupt();
            try {
                heartbeatChecker.join(5000);
            } catch (InterruptedException e) {
                LOGGER.warn("等待心跳检查线程结束被中断");
            }
        }
        if (redisListener != null) {
            redisListener.interrupt();
            try {
                redisListener.join(5000);
            } catch (InterruptedException e) {
                LOGGER.warn("等待Redis监听线程结束被中断");
            }
        }
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
        LOGGER.info("订单监控器已停止");
    }
    
    /**
     * 初始化Redis连接池
     */
    private void initRedis() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(3);      // 减少连接数，因为是独立程序
        config.setMaxIdle(2);
        config.setTestOnBorrow(true);
        jedisPool = new JedisPool(config, REDIS_HOST, REDIS_PORT);
        try (Jedis jedis = jedisPool.getResource()) {
            String pong = jedis.ping();
            LOGGER.info("Redis连接成功: {}", pong);
        } catch (Exception e) {
            LOGGER.error("Redis连接失败", e);
            throw new RuntimeException("无法连接到Redis", e);
        }
    }
    
    /**
     * 启动心跳检查线程
     * 定期检查算法心跳，发现超时则清理订单
     */
    private void startHeartbeatChecker() {
        heartbeatChecker = new Thread(() -> {
            LOGGER.info("心跳检查线程启动");
            
            while (running) {
                try {
                    // 每30秒检查一次
                    Thread.sleep(HEARTBEAT_INTERVAL);
                    checkAndCleanup();
                } catch (InterruptedException e) {
                    LOGGER.info("心跳检查线程被中断");
                    break;
                } catch (Exception e) {
                    LOGGER.error("心跳检查异常", e);
                }
            }
            
            LOGGER.info("心跳检查线程结束");
        }, "heartbeat-checker");
        
        heartbeatChecker.setDaemon(false);
        heartbeatChecker.start();
    }
    
    /**
     * 启动Redis监听线程
     * 订阅心跳和订单频道
     */
    private void startRedisListener() {
        redisListener = new Thread(() -> {
            LOGGER.info("Redis监听线程启动");
            
            JedisPubSub pubSub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    try {
                        if (HEARTBEAT_CHANNEL.equals(channel)) {
                            processHeartbeat(message);
                        } else if (channel.startsWith(ORDER_CHANNEL)) {
                            processOrder(message);
                        }
                    } catch (Exception e) {
                        LOGGER.error("处理消息失败: channel={}, error={}", channel, e.getMessage());
                    }
                }
                
                @Override
                public void onPMessage(String pattern, String channel, String message) {
                    onMessage(channel, message);
                }
                
                @Override
                public void onSubscribe(String channel, int subscribedChannels) {
                    LOGGER.info("订阅频道: {}", channel);
                }
                
                @Override
                public void onPSubscribe(String pattern, int subscribedChannels) {
                    LOGGER.info("订阅模式: {}", pattern);
                }
            };
            
            // 持续监听，断开后重连
            while (running) {
                try (Jedis jedis = jedisPool.getResource()) {
                    LOGGER.info("连接Redis并订阅频道...");
                    // 使用模式订阅来监听所有订单消息
                    jedis.psubscribe(pubSub, HEARTBEAT_CHANNEL, ORDER_CHANNEL + "*");
                } catch (Exception e) {
                    LOGGER.error("Redis订阅异常: {}", e.getMessage());
                    if (running) {
                        LOGGER.info("5秒后重新连接...");
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException ie) {
                            break;
                        }
                    }
                }
            }
            
            LOGGER.info("Redis监听线程结束");
        }, "redis-listener");
        
        redisListener.setDaemon(false);
        redisListener.start();
    }
    
    /**
     * 处理心跳消息
     */
    private void processHeartbeat(String message) {
        try {
            // 解析心跳消息 - 修正为与FENICS_Market_Maker一致的格式
            String application = extractField(message, "application");
            String hostname = extractField(message, "hostname");
            String state = extractField(message, "state");
            
            if (application != null) {
                // 使用application作为心跳键，与订单清理逻辑保持一致
                // 注意：FENICS_Market_Maker使用"OrderManagementUAT"作为应用名称
                algoLastHeartbeat.put(application, System.currentTimeMillis());
                LOGGER.debug("收到应用 {} 的心跳，主机: {}，状态: {}", application, hostname, state);
            }
        } catch (Exception e) {
            LOGGER.error("处理心跳消息失败: {}", e.getMessage());
        }
    }
    
    /**
     * 处理订单消息
     * 从CM_ORDER消息中提取必要信息
     */
    private void processOrder(String message) {
        try {
            // 只处理JSON格式的消息
            if (!message.startsWith("{")) {
                return;
            }
            
            // 提取必要字段
            String OrigId = extractField(message, "OrigId");
            String CompNameOrigin = extractField(message, "CompNameOrigin");
            String Trader = extractField(message, "Trader");
            String OrigSrc = extractField(message, "OrigSrc");
            String Active = extractField(message, "Active");
            String ActiveStr = extractField(message, "ActiveStr");
            String InstrumentId = extractField(message, "InstrumentId");  // 添加CUSIP字段
            
            // 判断订单是否活跃
            boolean isActive = "1".equals(Active) || "Yes".equalsIgnoreCase(ActiveStr);
            
            if (isActive && OrigId != null) {
                // 添加活跃订单
                OrderInfo order = new OrderInfo(OrigId, Trader, CompNameOrigin, OrigSrc, InstrumentId);
                activeOrders.put(OrigId, order);
                
                // 更新市场源->算法->订单映射
                sourceToAlgoToOrders
                    .computeIfAbsent(OrigSrc, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(CompNameOrigin, k -> ConcurrentHashMap.newKeySet())
                    .add(OrigId);
                

                // 统计新增订单数量
                newOrderCounts.merge(CompNameOrigin, 1, Integer::sum);
                
                // 定期输出订单统计（而不是每个订单都输出）
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastOrderLogTime > ORDER_LOG_INTERVAL) {
                    outputOrderStatistics();
                    lastOrderLogTime = currentTime;
                }
                
                // 订单活动也算作心跳
                algoLastHeartbeat.put(CompNameOrigin, System.currentTimeMillis());
            } else if (!isActive && OrigId != null) {
                // 移除非活跃订单（静默处理，不输出日志）
                OrderInfo removed = activeOrders.remove(OrigId);
                if (removed != null) {
                    // 从市场源->算法->订单映射中移除
                    Map<String, Set<String>> algoToOrders = sourceToAlgoToOrders.get(removed.OrigSrc);
                    if (algoToOrders != null) {
                        Set<String> orders = algoToOrders.get(removed.CompNameOrigin);
                        if (orders != null) {
                            orders.remove(OrigId);
                        }
                    }
                    // 只在debug级别输出订单完成信息
                    LOGGER.debug("订单已完成: {} [{}] CUSIP: {}", OrigId, CompNameOrigin, removed.InstrumentId);
                }
            }
            
        } catch (Exception e) {
            LOGGER.error("处理订单消息失败: {}", e.getMessage());
        }
    }
    
    /**
     * 输出订单统计信息
     */
    private void outputOrderStatistics() {
        if (newOrderCounts.isEmpty()) {
            return;
        }
        
        StringBuilder sb = new StringBuilder("新增活跃订单统计: ");
        for (Map.Entry<String, Integer> entry : newOrderCounts.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
        }
        LOGGER.info(sb.toString());
        
        // 清空统计
        newOrderCounts.clear();
    }
    
    /**
     * 检查心跳并清理超时市场源的订单
     */
    private void checkAndCleanup() {
        long currentTime = System.currentTimeMillis();
        long timeout = MAX_MISSED_HEARTBEATS * HEARTBEAT_INTERVAL;
        
        boolean hasTimeout = false;
        int activeSources = 0;
        
        // 检查每个市场源的心跳状态
        for (Map.Entry<String, Long> entry : algoLastHeartbeat.entrySet()) {
            String marketSource = entry.getKey();
            Long lastHeartbeat = entry.getValue();
            
            if (lastHeartbeat == null) {
                // 还没收到过心跳，跳过
                LOGGER.debug("市场源 {} 尚未收到心跳", marketSource);
                continue;
            }
            
            long timeSinceLastHeartbeat = currentTime - lastHeartbeat;
            
            if (timeSinceLastHeartbeat > timeout) {
                // 超时，需要清理
                hasTimeout = true;
                LOGGER.error("市场源 {} 心跳超时！最后心跳: {}秒前", 
                    marketSource, timeSinceLastHeartbeat / 1000);
                
                // 清理该市场源的所有订单
                cleanupMarketSourceOrders(marketSource);
                
                // 移除心跳记录，避免重复清理
                algoLastHeartbeat.remove(marketSource);
            } else {
                // 正常
                activeSources++;
                LOGGER.debug("市场源 {} 正常，最后心跳: {}秒前", 
                    marketSource, timeSinceLastHeartbeat / 1000);
            }
        }
        
        // 只在有超时或状态变化时输出详细信息
        if (hasTimeout) {
            LOGGER.warn("=== 心跳检查完成 - 发现超时市场源 ===");
        } else if (activeSources > 0 || activeOrders.size() > 0) {
            // 只在有活跃市场源或订单时才输出统计信息，但频率降低
            LOGGER.debug("心跳检查 - 活跃市场源: {}, 活跃订单: {}", activeSources, activeOrders.size());
        }
    }
    
    /**
     * 清理特定市场源的所有订单
     */
    private void cleanupMarketSourceOrders(String marketSource) {
        LOGGER.warn(">>>>>>> 开始清理市场源 {} 的所有订单 <<<<<<<", marketSource);
        
        int successCount = 0;
        int failCount = 0;
        
        // 直接遍历并取消订单
        for (OrderInfo order : activeOrders.values()) {
            if (marketSource.equals(order.CompNameOrigin)) {
                try {
                    LOGGER.warn("正在取消订单: {}", order);
                    cancelOrder(order);
                    activeOrders.remove(order.OrigId);
                    successCount++;
                } catch (Exception e) {
                    LOGGER.error("取消订单失败: {}, 错误: {}", order.OrigId, e.getMessage());
                    failCount++;
                }
            }
        }
        
        if (successCount == 0 && failCount == 0) {
            LOGGER.warn("市场源 {} 没有活跃订单", marketSource);
        } else {
            LOGGER.warn("清理完成 - 成功: {}, 失败: {}", successCount, failCount);
        }
    }
    
    /**
     * 取消订单
     * 根据对话，这里应该使用简化的取消逻辑
     */
    private void cancelOrder(OrderInfo order) {
        LOGGER.warn("【取消订单】{}", order);
        
        // 根据对话，这里应该实现简单的取消逻辑
        // 由于是独立程序，不依赖ION MKV，使用Redis发布取消命令
        publishCancelCommand(order);
    }
    
    /**
     * 通过Redis发布取消命令
     * 其他组件可以监听这个命令并执行实际的取消操作
     */
    private void publishCancelCommand(OrderInfo order) {
        try (Jedis jedis = jedisPool.getResource()) {
            // 构造取消命令消息
            Map<String, Object> cancelData = new HashMap<>();
            cancelData.put("action", "CANCEL");
            cancelData.put("orderId", order.OrigId);
            cancelData.put("origId", order.OrigId);
            cancelData.put("trader", order.Trader);
            cancelData.put("algoSource", order.OrigSrc);
            cancelData.put("instrumentId", order.InstrumentId);  // 添加CUSIP
            cancelData.put("timestamp", System.currentTimeMillis());
            cancelData.put("reason", "HEARTBEAT_TIMEOUT");
            
            // 转换为JSON格式
            String cancelMsg = mapToJson(cancelData);
            
            // 发布到取消命令频道
            String cancelChannel = "ORDER:CANCEL:COMMAND:" + order.OrigSrc;
            jedis.publish(cancelChannel, cancelMsg);
            
            LOGGER.info("已发布取消命令到频道 {}: {}", cancelChannel, cancelMsg);
        } catch (Exception e) {
            LOGGER.error("发布取消命令失败", e);
            throw new RuntimeException("无法发布取消命令", e);
        }
    }
    
    /**
     * 从JSON字符串中提取字段值
     * 简单的JSON解析，避免引入额外依赖
     */
    private String extractField(String json, String field) {
        try {
            String pattern = "\"" + field + "\":";
            int start = json.indexOf(pattern);
            if (start == -1) return null;
            
            start += pattern.length();
            
            // 跳过空格
            while (start < json.length() && json.charAt(start) == ' ') {
                start++;
            }
            
            if (json.charAt(start) == '"') {
                // 字符串值
                start++;
                int end = json.indexOf('"', start);
                return end > start ? json.substring(start, end) : null;
            } else {
                // 数字或布尔值
                int end = start;
                while (end < json.length() && 
                       json.charAt(end) != ',' && 
                       json.charAt(end) != '}' &&
                       json.charAt(end) != ' ') {
                    end++;
                }
                return json.substring(start, end).trim();
            }
        } catch (Exception e) {
            LOGGER.debug("提取字段 {} 失败: {}", field, e.getMessage());
            return null;
        }
    }
    
    /**
     * Map转JSON（支持不同类型）
     */
    private String mapToJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":");
            
            Object value = entry.getValue();
            if (value instanceof String) {
                sb.append("\"").append(value).append("\"");
            } else {
                sb.append(value);
            }
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * 测试ION连接 - 发布市场数据
     * 使用真实的交易配置
     */
    public void testIONConnection() {
        LOGGER.info("=== 开始ION连接测试 ===");
        
        // 首先测试每个broker的订单ID格式
        LOGGER.info("=== 验证订单ID格式 ===");
        String testCusip = "912810TM9";
        
        for (String broker : VENUE_TO_TRADER_MAP.keySet()) {
            String trader = VENUE_TO_TRADER_MAP.get(broker);
            
            String orderId = generateRealOrderId(broker, trader, testCusip);
            String orderRecordId = generateOrderRecordId(orderId);
            String standardCusip = getStandardCusip(broker, testCusip);
            
            LOGGER.info("Broker: {}", broker);
            LOGGER.info("  Trader: {}", trader);
            LOGGER.info("  CUSIP: {}", testCusip);
            LOGGER.info("  订单ID: {}", orderId);
            LOGGER.info("  订单记录ID: {}", orderRecordId);
            LOGGER.info("  标准CUSIP: {}", standardCusip);
            LOGGER.info("");
        }
        
        Mkv mkv = null;
        try {
            LOGGER.info("开始初始化ION...");
            
            // 初始化ION
            MkvQoS qos = new MkvQoS();
            String[] args = new String[]{"-c", "mkv.jinit"};
            qos.setArgs(args);
            mkv = Mkv.start(qos);
            if (mkv == null) {
                throw new RuntimeException("ION初始化失败");
            }
            LOGGER.info("ION初始化成功");

            // 1. 发布市场数据类型 (PRICE)
            String priceTypeName = "PRICE";
            String[] priceFieldNames = {"ID", "ASK", "BID", "QTY"};
            MkvFieldType[] priceFieldTypes = {
                MkvFieldType.STR, 
                MkvFieldType.REAL,
                MkvFieldType.REAL, 
                MkvFieldType.REAL
            };
            
            MkvType priceType = new MkvType(priceTypeName, priceFieldNames, priceFieldTypes);
            priceType.publish();
            LOGGER.info("PRICE类型发布成功");

            // 2. 发布深度数据类型 (CM_DEPTH)
            String depthTypeName = "CM_DEPTH";
            String[] depthFieldNames = {
                "Bid0", "Ask0", "BidQty0", "AskQty0",
                "Bid1", "Ask1", "BidQty1", "AskQty1",
                "Bid2", "Ask2", "BidQty2", "AskQty2"
            };
            MkvFieldType[] depthFieldTypes = new MkvFieldType[12];
            Arrays.fill(depthFieldTypes, MkvFieldType.REAL);
            
            MkvType depthType = new MkvType(depthTypeName, depthFieldNames, depthFieldTypes);
            depthType.publish();
            LOGGER.info("CM_DEPTH类型发布成功");

            // 3. 同时保留CM_ORDER类型（用于测试订单监控）
            String orderTypeName = "CM_ORDER";
            String[] orderFieldNames = {"OrigId", "CompNameOrigin", "Trader", "OrigSrc", "Active", "ActiveStr", "InstrumentId"};
            MkvFieldType[] orderFieldTypes = {
                MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.STR, 
                MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.STR
            };
            
            MkvType orderType = new MkvType(orderTypeName, orderFieldNames, orderFieldTypes);
            orderType.publish();
            LOGGER.info("CM_ORDER类型发布成功");

            // 4. 创建并发布价格记录 - 使用真实CUSIP
            String realCusip = REAL_CUSIP_EXAMPLES[0]; // 使用第一个真实CUSIP
            String priceRecordId = "USD.PRICE.FENICS_USREPO." + realCusip + "_C_Fixed";
            MkvRecord priceRecord = new MkvRecord(priceRecordId, priceTypeName);
            priceRecord.publish();
            LOGGER.info("价格记录发布成功: {}", priceRecordId);

            // 5. 创建并发布深度记录 - 使用真实CUSIP
            String depthRecordId = "USD.CM_DEPTH.FENICS_USREPO." + realCusip + "_C_Fixed";
            MkvRecord depthRecord = new MkvRecord(depthRecordId, depthTypeName);
            depthRecord.publish();
            LOGGER.info("深度记录发布成功: {}", depthRecordId);

            // 6. 创建测试订单记录 - 使用真实CUSIP和trader
            String trader = VENUE_TO_TRADER_MAP.get("FENICS_USREPO");
            String broker = "FENICS_USREPO";
            String orderId = generateRealOrderId(broker, trader, realCusip);
            String orderRecordId = generateOrderRecordId(orderId);
            MkvRecord orderRecord = new MkvRecord(orderRecordId, orderTypeName);
            orderRecord.publish();
            LOGGER.info("订单记录发布成功: {}", orderRecordId);

            // 7. 发布初始测试订单 - 使用真实配置
            publishTestOrder(orderRecord, true, realCusip);

            // 8. 发布测试心跳到Redis（模拟算法心跳）
            publishTestHeartbeat("FENICS_USREPO");

            // 9. 循环更新市场数据和心跳
            LOGGER.info("开始市场数据和心跳循环...");
            for (int i = 0; i < 5; i++) {
                // 更新价格数据
                updatePriceData(priceRecord, i);
                
                // 更新深度数据
                updateDepthData(depthRecord, i);
                
                // 发布心跳
                publishTestHeartbeat("FENICS_USREPO");
                
                LOGGER.info("第 {} 轮更新完成", i + 1);
                Thread.sleep(10000); // 每10秒更新一次
            }

            // 10. 模拟心跳停止（触发清理）
            LOGGER.warn("=== 停止发送心跳，等待触发清理 ===");
            LOGGER.warn("=== 订单将在5分钟后自动清理 ===");
            Thread.sleep(300000); // 等待5分钟，超过10次心跳间隔

            // 11. 恢复心跳
            LOGGER.info("=== 恢复心跳发送 ===");
            publishTestHeartbeat("FENICS_USREPO");
            
            // 12. 继续更新数据
            for (int i = 5; i < 8; i++) {
                updatePriceData(priceRecord, i);
                updateDepthData(depthRecord, i);
                publishTestHeartbeat("FENICS_USREPO");
                Thread.sleep(10000);
            }

            // 13. 撤销测试订单
            publishTestOrder(orderRecord, false, realCusip);
            
            LOGGER.info("测试完成！");
            
        } catch (Exception e) {
            LOGGER.error("测试失败: {}", e.getMessage(), e);
            throw new RuntimeException("测试失败", e);
        } finally {
            if (mkv != null) {
                Mkv.stop();
                LOGGER.info("ION已停止");
            }
        }
    }

    /**
     * 发布测试订单 - 使用真实配置
     */
    private void publishTestOrder(MkvRecord record, boolean isActive, String cusip) {
        try {
            // 使用真实的trader和broker配置
            String trader = VENUE_TO_TRADER_MAP.get("FENICS_USREPO");
            String broker = "FENICS_USREPO";
            
            // 生成真实的订单ID
            String orderId = generateRealOrderId(broker, trader, cusip);
            
            int[] fields = {0, 1, 2, 3, 4, 5, 6};
            Object[] values = {
                orderId,            // OrigId (使用真实订单ID)
                broker,            // CompNameOrigin (使用真实broker)
                trader,            // Trader (使用真实trader)
                broker,            // OrigSrc (使用真实broker)
                isActive ? "1" : "0",  // Active
                isActive ? "Yes" : "No", // ActiveStr
                getStandardCusip(broker, cusip) // InstrumentId (使用标准CUSIP格式)
            };
            
            record.supply(fields, values);
            LOGGER.info("测试订单已发布: OrderId={}, Active={}, Trader={}, Broker={}, CUSIP={}", 
                orderId, isActive, trader, broker, cusip);

            // 同时发布到Redis
            try (Jedis jedis = jedisPool.getResource()) {
                Map<String, Object> orderData = new HashMap<>();
                orderData.put("OrigId", orderId);
                orderData.put("CompNameOrigin", broker);
                orderData.put("Trader", trader);
                orderData.put("OrigSrc", broker);
                orderData.put("Active", isActive ? "1" : "0");
                orderData.put("ActiveStr", isActive ? "Yes" : "No");
                orderData.put("InstrumentId", getStandardCusip(broker, cusip));
                
                String orderJson = mapToJson(orderData);
                jedis.publish(ORDER_CHANNEL + orderId, orderJson);
                LOGGER.info("Redis订单消息已发布: {}", orderJson);
            }
            
        } catch (MkvException e) {
            LOGGER.error("发布测试订单失败", e);
        }
    }

    /**
     * 更新价格数据
     */
    private void updatePriceData(MkvRecord record, int iteration) {
        try {
            double basePrice = 100.0;
            double spread = 0.5;
            double qty = 1000000.0;
            
            // 添加一些随机波动
            double randomFactor = 1 + (Math.random() - 0.5) * 0.02; // ±1%
            double bid = (basePrice - spread) * randomFactor;
            double ask = (basePrice + spread) * randomFactor;
            
            int[] fields = {0, 1, 2, 3}; // ID, ASK, BID, QTY
            Object[] values = {
                "TEST_INSTRUMENT_001",
                ask,
                bid,
                qty
            };
            
            record.supply(fields, values);
            LOGGER.info("价格更新 #{}: BID={:.4f}, ASK={:.4f}, QTY={}", 
                iteration, bid, ask, qty);
                
        } catch (MkvException e) {
            LOGGER.error("更新价格数据失败", e);
        }
    }

    /**
     * 更新深度数据
     */
    private void updateDepthData(MkvRecord record, int iteration) {
        try {
            double basePrice = 100.0;
            double[] bids = new double[3];
            double[] asks = new double[3];
            double[] bidQtys = new double[3];
            double[] askQtys = new double[3];
            
            // 生成3档深度数据
            for (int i = 0; i < 3; i++) {
                double randomFactor = 1 + (Math.random() - 0.5) * 0.02;
                bids[i] = (basePrice - 0.5 - i * 0.1) * randomFactor;
                asks[i] = (basePrice + 0.5 + i * 0.1) * randomFactor;
                bidQtys[i] = 1000000.0 * (3 - i); // 数量递减
                askQtys[i] = 1000000.0 * (3 - i);
            }
            
            int[] fields = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
            Object[] values = {
                bids[0], asks[0], bidQtys[0], askQtys[0],
                bids[1], asks[1], bidQtys[1], askQtys[1],
                bids[2], asks[2], bidQtys[2], askQtys[2]
            };
            
            record.supply(fields, values);
            LOGGER.info("深度更新 #{}: Bid0={:.4f}/Ask0={:.4f}, Bid1={:.4f}/Ask1={:.4f}", 
                iteration, bids[0], asks[0], bids[1], asks[1]);
                
        } catch (MkvException e) {
            LOGGER.error("更新深度数据失败", e);
        }
    }

    /**
     * 发布测试心跳 - 修正为与FENICS_Market_Maker一致的格式
     */
    private void publishTestHeartbeat(String marketSource) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, Object> heartbeatData = new HashMap<>();
            heartbeatData.put("hostname", System.getenv("COMPUTERNAME"));
            heartbeatData.put("application", "OrderManagementUAT");  // 修正为与FENICS_Market_Maker完全一致
            heartbeatData.put("state", "RUNNING");
            heartbeatData.put("continuousTrading", true);
            heartbeatData.put("activeOrders", 1);
            heartbeatData.put("marketMaker", "RUNNING");
            heartbeatData.put("instruments", 1);
            heartbeatData.put("cached orders", 1);
            heartbeatData.put("latest best prices", 1);
            
            String heartbeatJson = mapToJson(heartbeatData);
            jedis.publish(HEARTBEAT_CHANNEL, heartbeatJson);
            LOGGER.info("心跳已发布: {}", heartbeatJson);
        } catch (Exception e) {
            LOGGER.error("发布心跳失败", e);
        }
    }
}