// package com.iontrading.automatedMarketMaking.wrapper;

// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.iontrading.automatedMarketMaking.*;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import redis.clients.jedis.Jedis;
// import redis.clients.jedis.JedisPubSub;

// import java.util.*;
// import java.util.concurrent.*;

// /**
//  * ION Command Processor - Handles commands from Python clients via Redis
//  * This class extends the existing Redis control mechanism to support
//  * comprehensive ION operations.
//  */
// public class IONCommandProcessor extends JedisPubSub implements IOrderManager {
    
//     private static final Logger LOGGER = LoggerFactory.getLogger(IONCommandProcessor.class);
//     private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
//     // Redis channels
//     private static final String COMMAND_CHANNEL = "ION:COMMANDS";
//     private static final String PRIORITY_COMMAND_CHANNEL = "ION:COMMANDS:PRIORITY";
//     private static final String RESPONSE_CHANNEL = "ION:RESPONSES";
//     private static final String MARKET_DATA_CHANNEL = "ION:MARKET:DEPTH";
    
//     // Component references
//     private final OrderManagement orderManagement;
//     private final redis.clients.jedis.JedisPool jedisPool;
    
//     // Command tracking
//     private final Map<String, CommandContext> pendingCommands = new ConcurrentHashMap<>();
//     private final Map<Integer, String> reqIdToCommandId = new ConcurrentHashMap<>();
    
//     // Executor for async operations
//     private final ExecutorService commandExecutor = Executors.newFixedThreadPool(10);
    
//     public IONCommandProcessor(OrderManagement orderManagement, redis.clients.jedis.JedisPool jedisPool) {
//         this.orderManagement = orderManagement;
//         this.jedisPool = jedisPool;
//     }
    
//     @Override
//     public void onMessage(String channel, String message) {
//         LOGGER.info("Received command on channel {}: {}", channel, message);
        
//         try {
//             Map<String, Object> command = OBJECT_MAPPER.readValue(message, Map.class);
//             String commandId = (String) command.get("command_id");
//             String commandType = (String) command.get("command_type");
//             Map<String, Object> parameters = (Map<String, Object>) command.get("parameters");
            
//             // Store command context for tracking
//             CommandContext context = new CommandContext(commandId, commandType, parameters);
//             pendingCommands.put(commandId, context);
            
//             // Process command asynchronously
//             commandExecutor.submit(() -> processCommand(context));
            
//         } catch (Exception e) {
//             LOGGER.error("Error processing command: {}", e.getMessage(), e);
//             sendErrorResponse(null, "PARSE_ERROR", e.getMessage());
//         }
//     }
    
//     private void processCommand(CommandContext context) {
//         try {
//             switch (context.commandType) {
//                 case "CREATE_ORDER":
//                     handleCreateOrder(context);
//                     break;
//                 case "UPDATE_ORDER":
//                     handleUpdateOrder(context);
//                     break;
//                 case "CANCEL_ORDER":
//                     handleCancelOrder(context);
//                     break;
//                 case "SUBSCRIBE_DEPTH":
//                     handleSubscribeDepth(context);
//                     break;
//                 case "SUBSCRIBE_TRADE":
//                     handleSubscribeTrade(context);
//                     break;
//                 case "GET_INSTRUMENT_INFO":
//                     handleGetInstrumentInfo(context);
//                     break;
//                 case "MM_START":
//                     handleMarketMakerStart(context);
//                     break;
//                 case "MM_STOP":
//                     handleMarketMakerStop(context);
//                     break;
//                 case "MM_CONFIG":
//                     handleMarketMakerConfig(context);
//                     break;
//                 default:
//                     sendErrorResponse(context.commandId, "UNKNOWN_COMMAND", 
//                         "Unknown command type: " + context.commandType);
//             }
//         } catch (Exception e) {
//             LOGGER.error("Error executing command {}: {}", context.commandType, e.getMessage(), e);
//             sendErrorResponse(context.commandId, "EXECUTION_ERROR", e.getMessage());
//         }
//     }
    
//     private void handleCreateOrder(CommandContext context) {
//         try {
//             // Extract parameters
//             String marketSource = (String) context.parameters.get("market_source");
//             String traderId = (String) context.parameters.get("trader_id");
//             String instrumentId = (String) context.parameters.get("instrument_id");
//             String verb = (String) context.parameters.get("verb");
//             double quantity = ((Number) context.parameters.get("quantity")).doubleValue();
//             double price = ((Number) context.parameters.get("price")).doubleValue();
//             String orderType = (String) context.parameters.getOrDefault("order_type", "Limit");
//             String timeInForce = (String) context.parameters.getOrDefault("time_in_force", "Day");
            
//             LOGGER.info("Creating order: {} {} {} @ {} on {}", 
//                 verb, quantity, instrumentId, price, marketSource);
            
//             // Create order using existing MarketOrder class
//             MarketOrder order = MarketOrder.orderCreate(
//                 marketSource, traderId, instrumentId, verb, 
//                 quantity, price, orderType, timeInForce, this
//             );
            
//             if (order != null) {
//                 // Map request ID to command ID for tracking
//                 reqIdToCommandId.put(order.getMyReqId(), context.commandId);
                
//                 // Response will be sent when onResult is called
//                 LOGGER.info("Order creation initiated with reqId: {}", order.getMyReqId());
//             } else {
//                 sendErrorResponse(context.commandId, "ORDER_CREATION_FAILED", 
//                     "Failed to create order - check gateway connection");
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("Error in handleCreateOrder: {}", e.getMessage(), e);
//             sendErrorResponse(context.commandId, "VALIDATION_ERROR", e.getMessage());
//         }
//     }
    
//     private void handleUpdateOrder(CommandContext context) {
//         try {
//             String marketSource = (String) context.parameters.get("market_source");
//             String traderId = (String) context.parameters.get("trader_id");
//             String orderId = (String) context.parameters.get("order_id");
//             String instrumentId = (String) context.parameters.get("instrument_id");
//             String verb = (String) context.parameters.get("verb");
//             double quantity = ((Number) context.parameters.get("quantity")).doubleValue();
//             double price = ((Number) context.parameters.get("price")).doubleValue();
            
//             LOGGER.info("Updating order: {} to {} @ {}", orderId, quantity, price);
            
//             MarketOrder order = MarketOrder.orderUpdate(
//                 marketSource, traderId, orderId, instrumentId, verb,
//                 quantity, price, this
//             );
            
//             if (order != null) {
//                 reqIdToCommandId.put(order.getMyReqId(), context.commandId);
//             } else {
//                 sendErrorResponse(context.commandId, "ORDER_UPDATE_FAILED", 
//                     "Failed to update order");
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("Error in handleUpdateOrder: {}", e.getMessage(), e);
//             sendErrorResponse(context.commandId, "VALIDATION_ERROR", e.getMessage());
//         }
//     }
    
//     private void handleCancelOrder(CommandContext context) {
//         try {
//             String marketSource = (String) context.parameters.get("market_source");
//             String traderId = (String) context.parameters.get("trader_id");
//             String orderId = (String) context.parameters.get("order_id");
            
//             LOGGER.info("Cancelling order: {} on {}", orderId, marketSource);
            
//             MarketOrder order = MarketOrder.orderCancel(
//                 marketSource, traderId, orderId, this
//             );
            
//             if (order != null) {
//                 reqIdToCommandId.put(order.getMyReqId(), context.commandId);
//             } else {
//                 sendErrorResponse(context.commandId, "ORDER_CANCEL_FAILED", 
//                     "Failed to cancel order");
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("Error in handleCancelOrder: {}", e.getMessage(), e);
//             sendErrorResponse(context.commandId, "VALIDATION_ERROR", e.getMessage());
//         }
//     }
    
//     private void handleSubscribeDepth(CommandContext context) {
//         try {
//             // This would typically trigger depth subscription
//             // For now, send acknowledgment
//             Map<String, Object> response = new HashMap<>();
//             response.put("subscription_id", UUID.randomUUID().toString());
//             response.put("channel", MARKET_DATA_CHANNEL);
            
//             sendSuccessResponse(context.commandId, response);
            
//         } catch (Exception e) {
//             LOGGER.error("Error in handleSubscribeDepth: {}", e.getMessage(), e);
//             sendErrorResponse(context.commandId, "SUBSCRIPTION_ERROR", e.getMessage());
//         }
//     }
    
//     private void handleGetInstrumentInfo(CommandContext context) {
//         try {
//             String instrumentId = (String) context.parameters.get("instrument_id");
//             String source = (String) context.parameters.get("source");
//             Boolean isAON = (Boolean) context.parameters.getOrDefault("is_aon", false);
            
//             // Get native instrument ID from DepthListener
//             String nativeId = OrderManagement.getNativeInstrumentId(instrumentId, source, isAON);
            
//             Map<String, Object> response = new HashMap<>();
//             response.put("instrument_id", instrumentId);
//             response.put("source", source);
//             response.put("native_id", nativeId);
            
//             sendSuccessResponse(context.commandId, response);
            
//         } catch (Exception e) {
//             LOGGER.error("Error in handleGetInstrumentInfo: {}", e.getMessage(), e);
//             sendErrorResponse(context.commandId, "LOOKUP_ERROR", e.getMessage());
//         }
//     }
    
//     private void handleMarketMakerStart(CommandContext context) {
//         boolean success = orderManagement.startMarketMaking();
        
//         Map<String, Object> response = new HashMap<>();
//         response.put("status", success ? "RUNNING" : "FAILED");
        
//         if (success) {
//             sendSuccessResponse(context.commandId, response);
//         } else {
//             sendErrorResponse(context.commandId, "MM_START_FAILED", 
//                 "Failed to start market maker");
//         }
//     }
    
//     private void handleMarketMakerStop(CommandContext context) {
//         boolean success = orderManagement.stopMarketMaking();
        
//         Map<String, Object> response = new HashMap<>();
//         response.put("status", success ? "STOPPED" : "FAILED");
        
//         if (success) {
//             sendSuccessResponse(context.commandId, response);
//         } else {
//             sendErrorResponse(context.commandId, "MM_STOP_FAILED", 
//                 "Failed to stop market maker");
//         }
//     }
    
//     private void handleMarketMakerConfig(CommandContext context) {
//         // Implementation would update market maker configuration
//         // For now, acknowledge the command
//         sendSuccessResponse(context.commandId, context.parameters);
//     }
    
//     private void handleSubscribeTrade(CommandContext context) {
//         // Implementation would set up trade subscription
//         Map<String, Object> response = new HashMap<>();
//         response.put("subscription_id", UUID.randomUUID().toString());
//         response.put("status", "ACTIVE");
        
//         sendSuccessResponse(context.commandId, response);
//     }
    
//     // IOrderManager implementation
//     @Override
//     public void orderDead(MarketOrder order) {
//         String commandId = reqIdToCommandId.remove(order.getMyReqId());
//         if (commandId != null) {
//             sendErrorResponse(commandId, "ORDER_DEAD", 
//                 "Order failed: " + order.getErrStr());
//         }
//     }
    
//     @Override
//     public void mapOrderIdToReqId(String orderId, int reqId) {
//         // Store the mapping
//         orderManagement.getOrderRepository().mapOrderIdToReqId(orderId, reqId);
        
//         // Send success response
//         String commandId = reqIdToCommandId.remove(reqId);
//         if (commandId != null) {
//             Map<String, Object> data = new HashMap<>();
//             data.put("order_id", orderId);
//             data.put("request_id", reqId);
            
//             sendSuccessResponse(commandId, data);
//         }
//     }
    
//     @Override
//     public String getApplicationId() {
//         return "PythonWrapper";
//     }
    
//     // Response helpers
//     private void sendSuccessResponse(String commandId, Map<String, Object> data) {
//         Map<String, Object> response = new HashMap<>();
//         response.put("command_id", commandId);
//         response.put("status", "SUCCESS");
//         response.put("timestamp", System.currentTimeMillis());
//         response.put("data", data);
//         response.put("error", null);
        
//         publishResponse(response);
//     }
    
//     private void sendErrorResponse(String commandId, String errorCode, String errorMessage) {
//         Map<String, Object> error = new HashMap<>();
//         error.put("code", errorCode);
//         error.put("message", errorMessage);
        
//         Map<String, Object> response = new HashMap<>();
//         response.put("command_id", commandId);
//         response.put("status", "ERROR");
//         response.put("timestamp", System.currentTimeMillis());
//         response.put("data", null);
//         response.put("error", error);
        
//         publishResponse(response);
//     }
    
//     private void publishResponse(Map<String, Object> response) {
//         Jedis jedis = null;
//         try {
//             jedis = jedisPool.getResource();
//             String jsonResponse = OBJECT_MAPPER.writeValueAsString(response);
//             jedis.publish(RESPONSE_CHANNEL, jsonResponse);
            
//             // Also publish to client-specific channel if specified
//             String commandId = (String) response.get("command_id");
//             if (commandId != null && commandId.contains(":")) {
//                 String clientId = commandId.split(":")[0];
//                 jedis.publish(RESPONSE_CHANNEL + ":" + clientId, jsonResponse);
//             }
            
//         } catch (Exception e) {
//             LOGGER.error("Error publishing response: {}", e.getMessage(), e);
//         } finally {
//             if (jedis != null) {
//                 jedisPool.returnResource(jedis);
//             }
//         }
//     }
    
//     // Command context helper class
//     private static class CommandContext {
//         final String commandId;
//         final String commandType;
//         final Map<String, Object> parameters;
//         final long timestamp;
        
//         CommandContext(String commandId, String commandType, Map<String, Object> parameters) {
//             this.commandId = commandId;
//             this.commandType = commandType;
//             this.parameters = parameters;
//             this.timestamp = System.currentTimeMillis();
//         }
//     }
    
//     // Cleanup resources
//     public void shutdown() {
//         commandExecutor.shutdown();
//         try {
//             if (!commandExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
//                 commandExecutor.shutdownNow();
//             }
//         } catch (InterruptedException e) {
//             commandExecutor.shutdownNow();
//         }
//     }
// }