// package com.iontrading.automatedMarketMaking.wrapper;

// import com.iontrading.automatedMarketMaking.*;
// import com.iontrading.mkv.Mkv;
// import com.iontrading.mkv.MkvComponent;
// import com.iontrading.mkv.enums.MkvPlatformEvent;
// import com.iontrading.mkv.enums.MkvShutdownMode;
// import com.iontrading.mkv.events.MkvPlatformListener;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import redis.clients.jedis.JedisPool;
// import redis.clients.jedis.JedisPoolConfig;
// import redis.clients.jedis.Jedis;

// import java.util.concurrent.CountDownLatch;
// import java.util.concurrent.atomic.AtomicBoolean;

// /**
//  * Standalone ION Wrapper Service
//  * This service runs independently and provides Python access to ION operations
//  */
// public class IONWrapperService implements MkvPlatformListener, IOrderManager {
    
//     private static final Logger LOGGER = LoggerFactory.getLogger(IONWrapperService.class);
    
//     // Configuration
//     private static final String REDIS_HOST = System.getProperty("redis.host", "cacheuat");
//     private static final int REDIS_PORT = Integer.parseInt(System.getProperty("redis.port", "6379"));
//     private static final String COMPONENT_NAME = "ION_PYTHON_WRAPPER";
    
//     // Core components
//     private JedisPool jedisPool;
//     private IONCommandProcessor commandProcessor;
//     private Thread commandProcessorThread;
//     private final AtomicBoolean running = new AtomicBoolean(false);
//     private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    
//     // Application ID for order tracking
//     private final String applicationId = "PythonWrapper_" + System.currentTimeMillis();
    
//     /**
//      * Main entry point
//      */
//     public static void main(String[] args) {
//         LOGGER.info("Starting ION Wrapper Service...");
        
//         try {
//             IONWrapperService service = new IONWrapperService();
//             service.start();
            
//             // Add shutdown hook
//             Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                 LOGGER.info("Shutdown hook triggered");
//                 service.stop();
//             }));
            
//             // Wait for shutdown
//             service.awaitShutdown();
            
//         } catch (Exception e) {
//             LOGGER.error("Fatal error in ION Wrapper Service", e);
//             System.exit(1);
//         }
//     }
    
//     /**
//      * Start the wrapper service
//      */
//     public void start() throws Exception {
//         LOGGER.info("Initializing ION Wrapper Service");
        
//         // Initialize Redis connection pool
//         initializeRedis();
        
//         // Initialize MKV API
//         initializeMkv();
        
//         // Start command processor
//         startCommandProcessor();
        
//         // Publish startup status
//         publishStatus("STARTED", "ION Wrapper Service is ready");
        
//         LOGGER.info("ION Wrapper Service started successfully");
//     }
    
//     /**
//      * Stop the wrapper service
//      */
//     public void stop() {
//         if (!running.compareAndSet(true, false)) {
//             LOGGER.info("Service already stopped");
//             return;
//         }
        
//         LOGGER.info("Stopping ION Wrapper Service");
        
//         try {
//             // Publish shutdown status
//             publishStatus("STOPPING", "ION Wrapper Service is shutting down");
            
//             // Stop command processor
//             stopCommandProcessor();
            
//             // Stop MKV
//             stopMkv();
            
//             // Close Redis pool
//             if (jedisPool != null) {
//                 jedisPool.close();
//             }
            
//             LOGGER.info("ION Wrapper Service stopped successfully");
            
//         } catch (Exception e) {
//             LOGGER.error("Error during shutdown", e);
//         } finally {
//             shutdownLatch.countDown();
//         }
//     }
    
//     /**
//      * Wait for service shutdown
//      */
//     public void awaitShutdown() throws InterruptedException {
//         shutdownLatch.await();
//     }
    
//     /**
//      * Initialize Redis connection pool
//      */
//     private void initializeRedis() {
//         LOGGER.info("Initializing Redis connection to {}:{}", REDIS_HOST, REDIS_PORT);
        
//         JedisPoolConfig poolConfig = new JedisPoolConfig();
//         poolConfig.setMaxTotal(10);
//         poolConfig.setMaxIdle(5);
//         poolConfig.setMinIdle(1);
//         poolConfig.setTestOnBorrow(true);
//         poolConfig.setTestOnReturn(true);
//         poolConfig.setTestWhileIdle(true);
        
//         jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT);
        
//         // Test connection
//         try (Jedis jedis = jedisPool.getResource()) {
//             String pong = jedis.ping();
//             if (!"PONG".equals(pong)) {
//                 throw new RuntimeException("Redis ping failed");
//             }
//             LOGGER.info("Redis connection established");
//         }
//     }
    
//     /**
//      * Initialize MKV API
//      */
//     private void initializeMkv() throws Exception {
//         LOGGER.info("Initializing MKV API");
        
//         // Register platform listener for clean shutdown
//         Mkv.getInstance().getPlatform().addPlatformListener(this);
        
//         // Start MKV with minimal configuration
//         String[] mkvArgs = {
//             "-c", COMPONENT_NAME,
//             "-l", "3"  // Log level
//         };
        
//         Mkv.start(true, mkvArgs);
        
//         LOGGER.info("MKV API initialized");
//     }
    
//     /**
//      * Start the command processor
//      */
//     private void startCommandProcessor() {
//         LOGGER.info("Starting command processor");
        
//         // Create command processor with this service as order manager
//         commandProcessor = new IONCommandProcessor(null, jedisPool) {
//             @Override
//             public String getApplicationId() {
//                 return applicationId;
//             }
            
//             // Override to use this service's order management methods
//             @Override
//             public void orderDead(MarketOrder order) {
//                 IONWrapperService.this.orderDead(order);
//             }
            
//             @Override
//             public void mapOrderIdToReqId(String orderId, int reqId) {
//                 IONWrapperService.this.mapOrderIdToReqId(orderId, reqId);
//             }
//         };
        
//         // Start command processor thread
//         commandProcessorThread = new Thread(() -> {
//             Jedis subscriberJedis = null;
//             try {
//                 subscriberJedis = jedisPool.getResource();
//                 LOGGER.info("Command processor listening on channels");
                
//                 // Subscribe to command channels
//                 subscriberJedis.subscribe(commandProcessor, 
//                     "ION:COMMANDS", 
//                     "ION:COMMANDS:PRIORITY"
//                 );
                
//             } catch (Exception e) {
//                 LOGGER.error("Error in command processor", e);
//             } finally {
//                 if (subscriberJedis != null) {
//                     jedisPool.returnResource(subscriberJedis);
//                 }
//             }
//         }, "ION-Command-Processor");
        
//         commandProcessorThread.setDaemon(false);
//         commandProcessorThread.start();
//         running.set(true);
        
//         LOGGER.info("Command processor started");
//     }
    
//     /**
//      * Stop the command processor
//      */
//     private void stopCommandProcessor() {
//         LOGGER.info("Stopping command processor");
        
//         if (commandProcessor != null) {
//             commandProcessor.shutdown();
//         }
        
//         if (commandProcessorThread != null && commandProcessorThread.isAlive()) {
//             commandProcessorThread.interrupt();
//             try {
//                 commandProcessorThread.join(5000);
//             } catch (InterruptedException e) {
//                 LOGGER.warn("Interrupted while waiting for command processor to stop");
//             }
//         }
//     }
    
//     /**
//      * Stop MKV API
//      */
//     private void stopMkv() {
//         try {
//             LOGGER.info("Stopping MKV API");
//             Mkv.stop();
//         } catch (Exception e) {
//             LOGGER.error("Error stopping MKV", e);
//         }
//     }
    
//     /**
//      * Publish service status to Redis
//      */
//     private void publishStatus(String status, String message) {
//         try (Jedis jedis = jedisPool.getResource()) {
//             String statusJson = String.format(
//                 "{\"service\":\"ION_WRAPPER\",\"status\":\"%s\",\"message\":\"%s\",\"timestamp\":%d}",
//                 status, message, System.currentTimeMillis()
//             );
//             jedis.publish("ION:SERVICE:STATUS", statusJson);
//         } catch (Exception e) {
//             LOGGER.error("Error publishing status", e);
//         }
//     }
    
//     // IOrderManager implementation
//     @Override
//     public void orderDead(MarketOrder order) {
//         LOGGER.warn("Order dead: reqId={}, error={}", order.getMyReqId(), order.getErrStr());
//         // Command processor will handle the response
//     }
    
//     @Override
//     public void mapOrderIdToReqId(String orderId, int reqId) {
//         LOGGER.info("Order created: orderId={}, reqId={}", orderId, reqId);
//         // Command processor will handle the response
//     }
    
//     @Override
//     public String getApplicationId() {
//         return applicationId;
//     }
    
//     // MkvPlatformListener implementation
//     @Override
//     public void onPlatformEvent(MkvPlatformEvent event) {
//         LOGGER.info("Platform event: {}", event);
        
//         switch (event) {
//             case STOP_code:
//                 LOGGER.info("Received STOP event from platform");
//                 stop();
//                 break;
//             case IDLE_REGISTER_code:
//             case REGISTER_code:
//                 LOGGER.info("MKV registration complete");
//                 break;
//         }
//     }
    
//     @Override
//     public void onShutdown(MkvComponent component, MkvShutdownMode mode) {
//         LOGGER.info("MKV shutdown initiated: component={}, mode={}", component, mode);
        
//         if (mode == MkvShutdownMode.ABORT || mode == MkvShutdownMode.FAST) {
//             // Fast shutdown
//             stop();
//         } else {
//             // Graceful shutdown
//             new Thread(() -> {
//                 try {
//                     Thread.sleep(1000); // Allow time for cleanup
//                     stop();
//                 } catch (InterruptedException e) {
//                     stop();
//                 }
//             }).start();
//         }
//     }
// }