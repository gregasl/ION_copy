package com.iontrading.redisOrder;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvLog;

/**
 * Simple Redis queue consumer that processes JSON order messages
 * and creates, updates, or cancels market orders.
 * 
 * This class provides a lightweight, focused implementation for processing
 * orders from a Redis queue without complex abstractions or dependencies.
 * 
 * JSON message format examples:
 * 
 * Create order:
 * {
 *   "operation": "create",
 *   "traderId": "TRADER1",
 *   "instrId": "EURUSD",
 *   "side": "Buy",
 *   "quantity": "1000000",
 *   "price": "1.0875",
 *   "type": "Limit",
 *   "tif": "Day"
 * }
 * 
 * Update order:
 * {
 *   "operation": "update",
 *   "traderId": "TRADER1",
 *   "orderId": "12345",
 *   "instrId": "EURUSD",
 *   "side": "Buy",
 *   "quantity": "2000000",
 *   "price": "1.0880",
 *   "tif": "Day"
 * }
 * 
 * Cancel order:
 * {
 *   "operation": "cancel",
 *   "traderId": "TRADER1",
 *   "orderId": "12345"
 * }
 */
public class OrderQueueConsumer {
    
    private static MkvLog log = Mkv.getInstance().getLogManager().getLogFile("REDIS_ORDER");
    private static IONLogger logger = new IONLogger(log, 2, "OrderQueueConsumer");

    private final JedisPool jedisPool;
    private final AtomicBoolean running;
    private final String queueName;
    private final int pollTimeoutSeconds;
    
    /**
     * Creates a new order queue consumer
     * 
     * @param jedisPool The Redis connection pool
     * @param logger The logger for output
     * @param running Flag to control the consumer thread
     * @param queueName The Redis queue name to consume from
     * @param pollTimeoutSeconds Timeout for polling the queue
     */
    public OrderQueueConsumer(
            JedisPool jedisPool, 
            AtomicBoolean running,
            String queueName,
            int pollTimeoutSeconds) {

        this.jedisPool = jedisPool;
        this.running = running;
        this.queueName = queueName;
        this.pollTimeoutSeconds = pollTimeoutSeconds;
    }
    
    public class OrderProcessingResult {
        private final boolean success;
        private final String errorMessage;
        private final MarketOrder order;

        public OrderProcessingResult(boolean success, String errorMessage, MarketOrder order) {
            this.success = success;
            this.errorMessage = errorMessage;
            this.order = order;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public MarketOrder getOrder() {
            return order;
        }
    }

    /**
     * Starts consuming from the Redis queue in a separate thread
     */
    public void start() {
        Thread consumerThread = new Thread(() -> {
            logger.info("Starting order queue consumer thread");
            
            while (running.get()) {
                try (Jedis jedis = jedisPool.getResource()) {
                    // BLPOP blocks until message is available or timeout occurs
                    List<String> result = jedis.blpop(pollTimeoutSeconds, queueName);
                    
                    if (result != null && result.size() == 2) {
                        String message = result.get(1);
                        processOrderMessage(message);
                    }
                } catch (Exception e) {
                    logger.error("Error in order consumer: " + e.getMessage());
                    // Short pause to avoid tight loop on persistent errors
                    try { Thread.sleep(1000); } catch (InterruptedException ie) { 
                        Thread.currentThread().interrupt();
                        break; 
                    }
                }
            }
            
            logger.info("Order queue consumer thread stopped");
        });
        
        consumerThread.setName("Order-Queue-Consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
        logger.info("Order queue consumer started for queue: " + queueName);
    }
    
    /**
     * Process an order message from the queue
     * 
     * @param message The JSON message
     */
    private void processOrderMessage(String message) {
        try {
            logger.info("Processing order message: " + message);
            
            if (!message.startsWith("{")) {
                logger.error("Invalid message format, expected JSON: " + message);
                return;
            }
            
            // Extract operation type
            String operation = extractJsonValue(message, "operation");
            if (operation == null) {
                logger.error("Missing operation field in message");
                return;
            }
            
            // Extract common fields
            String traderId = extractJsonValue(message, "traderId");
            
            // Process based on operation type
            switch (operation.toLowerCase()) {
                case "create":
                    handleOrderCreate(message, traderId);
                    break;
                    
                case "update":
                    handleOrderUpdate(message, traderId);
                    break;
                    
                case "cancel":
                    handleOrderCancel(message, traderId);
                    break;
                    
                default:
                    logger.error("Unknown operation type: " + operation);
            }
            
        } catch (Exception e) {
            logger.error("Error processing message: " + e.getMessage());
        }
    }
    
    /**
     * Handle order create operation
     */
    private OrderProcessingResult handleOrderCreate(String message, String traderId) {
        try {
            // Extract required fields
            String instrId = extractJsonValue(message, "instrId");
            String side = extractJsonValue(message, "side");
            String type = extractJsonValue(message, "type", "Limit");
            String tif = extractJsonValue(message, "tif", "Day");
            
            // Parse numeric values
            double quantity = Double.parseDouble(extractJsonValue(message, "quantity", "0"));
            double price = Double.parseDouble(extractJsonValue(message, "price", "0"));
            
            // Validate fields
            if (traderId == null || instrId == null || side == null ||
                quantity <= 0 || price <= 0) {
                logger.error("Missing or invalid required fields for order create");
                return new OrderProcessingResult(false, "Failed to create order", null);
            }
            
            // Create the order
            logger.info(String.format("Creating order: %s %s %s @ %.2f x %.2f", 
                side, instrId, price, quantity));
                
            MarketOrder order = MarketOrder.orderCreate(
                traderId, instrId, side, 
                quantity, price, type, tif
            );
            
            if (order != null) {
                logger.info("Order created successfully with ID: " + order.getMyReqId());
                return new OrderProcessingResult(true, null, order);
            } else {
                logger.error("Failed to create order");
                return new OrderProcessingResult(false, "Failed to create order", null);
            }
            
        } catch (Exception e) {
            logger.error("Error creating order: " + e.getMessage());
            return new OrderProcessingResult(false, "Error creating order: " + e.getMessage(), null);
        }
    }
    
    /**
     * Handle order update operation
     */
    private OrderProcessingResult handleOrderUpdate(String message, String traderId) {
        try {
            // Extract required fields
            String orderId = extractJsonValue(message, "orderId");
            String instrId = extractJsonValue(message, "instrId");
            String side = extractJsonValue(message, "side");
            String tif = extractJsonValue(message, "tif", "Day");

            // Parse numeric values
            double quantity = Double.parseDouble(extractJsonValue(message, "quantity", "0"));
            double price = Double.parseDouble(extractJsonValue(message, "price", "0"));
            
            // Validate fields
            if (traderId == null || orderId == null || instrId == null || side == null || quantity <= 0 || price <= 0) {
                logger.error("Missing or invalid required fields for order update");
                return new OrderProcessingResult(false, "Missing or invalid required fields for order update", null);
            }
            
            // Update the order
            logger.info(String.format("Updating order: %s ID:%s @ %.2f x %.2f",
                traderId, orderId, price, quantity));

            MarketOrder order = MarketOrder.orderUpdate(
                traderId, orderId, instrId, side,
                quantity, price, tif
            );
            
            if (order != null) {
                logger.info("Order updated successfully with request ID: " + order.getMyReqId());
                return new OrderProcessingResult(true, null, order);
            } else {
                logger.error("Failed to update order");
                return new OrderProcessingResult(false, "Failed to update order", null);
            }
            
        } catch (Exception e) {
            logger.error("Error updating order: " + e.getMessage());
            return new OrderProcessingResult(false, "Error updating order: " + e.getMessage(), null);
        }
    }
    
    /**
     * Handle order cancel operation
     */
    private OrderProcessingResult handleOrderCancel(String message, String traderId) {
        try {
            // Extract required fields
            String orderId = extractJsonValue(message, "orderId");
            
            // Validate fields
            if (traderId == null || orderId == null) {
                logger.error("Missing required fields for order cancel");
                return new OrderProcessingResult(false, "Missing required fields for order cancel", null);
            }
            
            // Cancel the order
            logger.info("Cancelling order: " + orderId);
                
            MarketOrder order = MarketOrder.orderCancel(
                traderId, orderId
            );
            
            if (order != null) {
                logger.info("Order cancellation requested successfully with ID: " + order.getMyReqId());
                return new OrderProcessingResult(true, null, order);
            } else {
                logger.error("Failed to cancel order");
                return new OrderProcessingResult(false, "Failed to cancel order", null);
            }
            
        } catch (Exception e) {
            logger.error("Error cancelling order: " + e.getMessage());
            return new OrderProcessingResult(false, "Error cancelling order: " + e.getMessage(), null);
        }
    }
    
    /**
     * Simple JSON field extractor with default value
     * 
     * @param json The JSON string
     * @param fieldName The field name to extract
     * @param defaultValue The default value to return if field not found
     * @return The field value or defaultValue if not found
     */
    private String extractJsonValue(String json, String fieldName, String defaultValue) {
        try {
            // Escape special regex characters in field name
            String escapedFieldName = Pattern.quote(fieldName);
            String pattern = "\"" + escapedFieldName + "\"\\s*:\\s*";
            
            Pattern p = Pattern.compile(pattern);
            Matcher m = p.matcher(json);
            
            if (!m.find()) {
                return defaultValue;
            }
            
            int start = m.end();
            
            if (start >= json.length()) {
                return defaultValue;
            }
            
            char firstChar = json.charAt(start);
            
            if (firstChar == '"') {
                // Handle quoted strings with proper escape sequence handling
                StringBuilder result = new StringBuilder();
                int pos = start + 1;
                
                while (pos < json.length()) {
                    char c = json.charAt(pos);
                    if (c == '"') {
                        // Check if it's escaped
                        int backslashCount = 0;
                        int checkPos = pos - 1;
                        while (checkPos >= start + 1 && json.charAt(checkPos) == '\\') {
                            backslashCount++;
                            checkPos--;
                        }
                        
                        if (backslashCount % 2 == 0) {
                            // Even number of backslashes means the quote is not escaped
                            return result.toString();
                        }
                    }
                    result.append(c);
                    pos++;
                }
                
                return defaultValue; // Unclosed string
            } else {
                // Handle unquoted values
                int end = start;
                int braceCount = 0;
                int bracketCount = 0;
                
                while (end < json.length()) {
                    char c = json.charAt(end);
                    if (c == '{') braceCount++;
                    else if (c == '}') braceCount--;
                    else if (c == '[') bracketCount++;
                    else if (c == ']') bracketCount--;
                    else if ((c == ',' || c == '}' || c == ']') && braceCount == 0 && bracketCount == 0) {
                        break;
                    }
                    end++;
                }
                
                String value = json.substring(start, end).trim();
                return "null".equals(value) ? defaultValue : value;
            }
        } catch (Exception e) {
            logger.error("Error parsing JSON field '" + fieldName + "': " + e.getMessage());
            return defaultValue;
        }
    }
    
    /**
     * Simple JSON field extractor
     * 
     * @param json The JSON string
     * @param fieldName The field name to extract
     * @return The field value or null if not found
     */
    private String extractJsonValue(String json, String fieldName) {
        return extractJsonValue(json, fieldName, null);
    }
    
    /**
     * Static constants for order types, time in force, and sides
     * Included here to reduce the number of files needed
     */
    public static final class MarketConstants {
        
        // Order types
        public static final String LIMIT = "Limit";
        public static final String MARKET = "Market";
        
        // Time in force values
        public static final String DAY = "Day";
        public static final String IOC = "IOC"; // Immediate or Cancel
        public static final String FOK = "FOK"; // Fill or Kill
        
        // Order sides/verbs
        public static final String BUY = "Buy";
        public static final String SELL = "Sell";
    }
}
