package com.iontrading.automatedMarketMaking;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvLog;

import java.util.function.Function;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Enhanced ActiveQuote serves as the unified order tracking system,
 * centralizing quote management and order lifecycle tracking
 * across OrderManagement and MarketMaker components.
 */
public class ActiveQuote {
    private static MkvLog log = Mkv.getInstance().getLogManager().getLogFile("MarketMaker");
    private static IONLogger logger = new IONLogger(log, Mkv.getInstance().getProperties().getIntProperty("DEBUG"), "ActiveQuote");

    // Thread safety mechanism
    private final Object lock = new Object();
    
    // Basic instrument information
    private final String id;
    private final String cusip;
    private final String termCode;  // "C" or "REG"
    
    // Order tracking - bid side
    private MarketOrder bidOrder;
    private String bidReferenceSource;
    private double bidPrice;
    private String bidVenue;
    private String bidTraderId;
    private String bidNativeInstrumentId;
    private String bidOrderId;
    private int bidReqId;
    private int bidUpdateFailureCount = 0;
    private long bidBackoffUntilTime = 0;
    private long bidCreationTime = 0;
    private boolean isBidActive = false;
    private boolean isGcBasedBid = false;
    private boolean isMarketBasedBid = false;
    
    // Order tracking - ask side
    private MarketOrder askOrder;
    private String askReferenceSource;
    private double askPrice;
    private String askVenue;
    private String askTraderId;
    private String askNativeInstrumentId;
    private String askOrderId;
    private int askReqId;
    private int askUpdateFailureCount = 0;
    private long askBackoffUntilTime = 0;
    private long askCreationTime = 0;
    private boolean isAskActive = false;
    private boolean isGcBasedAsk = false;
    private boolean isMarketBasedAsk = false;
    
    // Order history tracking with LRU cache (LinkedHashMap with accessOrder=true)
    private final Map<String, OrderHistoryEntry> orderHistory = 
        Collections.synchronizedMap(new LinkedHashMap<String, OrderHistoryEntry>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, OrderHistoryEntry> eldest) {
                return size() > 20; // Keep last 20 order history entries per instrument
            }
        });
    
    // Metrics for monitoring
    private final AtomicInteger totalOrdersPlaced = new AtomicInteger(0);
    private final AtomicInteger totalOrdersUpdated = new AtomicInteger(0);
    private final AtomicInteger totalOrdersCancelled = new AtomicInteger(0);
    private final AtomicInteger totalDuplicatesDetected = new AtomicInteger(0);
    
    /**
     * Represents a historical order entry for tracking purposes
     */
    public static class OrderHistoryEntry {
        private final String orderId;
        private final String side;
        private final long creationTime;
        private long completionTime;
        private String status;  // CREATED, UPDATED, CANCELLED, FILLED, EXPIRED
        private double price;
        private double quantity;
        private double filledQuantity;
        
        public OrderHistoryEntry(String orderId, String side, double price, double quantity) {
            this.orderId = orderId;
            this.side = side;
            this.price = price;
            this.quantity = quantity;
            this.creationTime = System.currentTimeMillis();
            this.status = "CREATED";
            this.filledQuantity = 0.0;
        }
        
        public void updateStatus(String newStatus) {
            this.status = newStatus;
            if (newStatus.equals("CANCELLED") || newStatus.equals("FILLED") || newStatus.equals("EXPIRED")) {
                this.completionTime = System.currentTimeMillis();
            }
        }
        
        public void updatePrice(double newPrice) {
            this.price = newPrice;
        }
        
        public void updateFilledQuantity(double filledQty) {
            this.filledQuantity = filledQty;
        }
        
        public String getOrderId() { return orderId; }
        public String getSide() { return side; }
        public long getCreationTime() { return creationTime; }
        public long getCompletionTime() { return completionTime; }
        public String getStatus() { return status; }
        public double getPrice() { return price; }
        public double getQuantity() { return quantity; }
        public double getFilledQuantity() { return filledQuantity; }
        public long getLifetimeMs() {
            if (completionTime > 0) {
                return completionTime - creationTime;
            }
            return System.currentTimeMillis() - creationTime;
        }
    }

    /**
     * Creates a new ActiveQuote for an instrument
     * 
     * @param id The instrument identifier (e.g., "91282CAT8_C_Fixed")
     */
    public ActiveQuote(String id) {
        this.id = id;
        
        // Extract CUSIP and term code
        if (id.contains("_")) {
            String[] parts = id.split("_");
            this.cusip = parts[0];
            this.termCode = parts.length > 1 ? parts[1] : "";
        } else {
            this.cusip = id;
            this.termCode = "";
        }
    }
    
    /**
     * @return The instrument ID this quote is for
     */
    public String getId() { 
        return id;
    }
    
    /**
     * @return The CUSIP for this instrument
     */
    public String getCusip() {
        return cusip;
    }
    
    /**
     * @return The term code for this instrument (C or REG)
     */
    public String getTermCode() {
        return termCode;
    }
   
    /**
     * @return The current bid order or null if none exists
     */
    public MarketOrder getBidOrder() { 
        return bidOrder;
    }
    
    /**
     * @return The current ask order or null if none exists
     */
    public MarketOrder getAskOrder() {
        return askOrder;
    }
    
    /**
     * @return The bid order ID or null if no active bid
     */
    public String getBidOrderId() {
        return bidOrderId;
    }
    
    /**
     * @return The ask order ID or null if no active ask
     */
    public String getAskOrderId() {
        return askOrderId;
    }
    
    /**
     * @return The bid request ID or 0 if no active bid
     */
    public int getBidReqId() {
        return bidReqId;
    }
    
    /**
     * @return The ask request ID or 0 if no active ask
     */
    public int getAskReqId() {
        return askReqId;
    }
    
    /**
     * Updates the bid order and its associated metadata
     * 
     * @param order The new bid order
     * @param referenceSource The data source used for pricing
     * @param price The price of the order
     */
    public void setBidOrder(MarketOrder order, String referenceSource, double price) {
        synchronized(lock) {
            // Add previous order to history if it exists and is different
            if (bidOrder != null && (order == null || order.getMyReqId() != bidOrder.getMyReqId())) {
                addToOrderHistory(bidOrder, "Buy", "REPLACED", bidPrice);
            }
            
            this.bidOrder = order;
            this.bidReferenceSource = referenceSource;
            this.bidPrice = price;
            this.isGcBasedBid = referenceSource != null && referenceSource.startsWith("GC_");
            this.isMarketBasedBid = referenceSource != null && !isGcBasedBid && 
                                !referenceSource.equals("DEFAULT");
            this.bidCreationTime = System.currentTimeMillis();
            
            if (order != null) {
                this.bidOrderId = order.getOrderId();
                this.bidReqId = order.getMyReqId();
                
                // Add new order to history
                addToOrderHistory(order, "Buy", "CREATED", price);
                
                // Update metrics
                totalOrdersPlaced.incrementAndGet();
            } else {
                this.bidOrderId = null;
                this.bidReqId = 0;
            }
        }
    }
    
    /**
     * Updates the ask order and its associated metadata
     * 
     * @param order The new ask order
     * @param referenceSource The data source used for pricing
     * @param price The price of the order
     */
    public void setAskOrder(MarketOrder order, String referenceSource, double price) {
        synchronized(lock) {
            // Add previous order to history if it exists and is different
            if (askOrder != null && (order == null || order.getMyReqId() != askOrder.getMyReqId())) {
                addToOrderHistory(askOrder, "Sell", "REPLACED", askPrice);
            }
            
            this.askOrder = order;
            this.askReferenceSource = referenceSource;
            this.askPrice = price;
            this.isGcBasedAsk = referenceSource != null && referenceSource.startsWith("GC_");
            this.isMarketBasedAsk = referenceSource != null && !isGcBasedAsk && 
                                !referenceSource.equals("DEFAULT");
            this.askCreationTime = System.currentTimeMillis();
            
            if (order != null) {
                this.askOrderId = order.getOrderId();
                this.askReqId = order.getMyReqId();
                
                // Add new order to history
                addToOrderHistory(order, "Sell", "CREATED", price);
                
                // Update metrics
                totalOrdersPlaced.incrementAndGet();
            } else {
                this.askOrderId = null;
                this.askReqId = 0;
            }
        }
    }
    
    /**
     * Set bid activity status
     * 
     * @param isActive Whether the bid is now active
     */
    public void setBidActive(boolean isActive) {
        synchronized(lock) {
            boolean statusChanged = this.isBidActive != isActive;
            this.isBidActive = isActive;
            
            if (statusChanged && bidOrder != null) {
                bidOrder.setActive(isActive);
                
                // Update order history
                String status = isActive ? "ACTIVE" : "INACTIVE";
                updateOrderHistory(bidOrder.getOrderId(), status);
                
                if (!isActive) {
                    logger.info("Bid for "+id+" deactivated by status change");
                }
            }
        }
    }
    
    /**
     * Set ask activity status
     * 
     * @param isActive Whether the ask is now active
     */
    public void setAskActive(boolean isActive) {
        synchronized(lock) {
            boolean statusChanged = this.isAskActive != isActive;
            this.isAskActive = isActive;
            
            if (statusChanged && askOrder != null) {
                askOrder.setActive(isActive);
                
                // Update order history
                String status = isActive ? "ACTIVE" : "INACTIVE";
                updateOrderHistory(askOrder.getOrderId(), status);
                
                if (!isActive) {
                    logger.info("Ask for "+id+" deactivated by status change");
                }
            }
        }
    }
    
    /**
     * Update the bid venue information
     */
    public void setBidVenue(String venue, String traderId, String nativeId) {
        synchronized(lock) {
            this.bidVenue = venue;
            this.bidTraderId = traderId;
            this.bidNativeInstrumentId = nativeId;
        }
    }
    
    /**
     * Update the ask venue information
     */
    public void setAskVenue(String venue, String traderId, String nativeId) {
        synchronized(lock) {
            this.askVenue = venue;
            this.askTraderId = traderId;
            this.askNativeInstrumentId = nativeId;
        }
    }
    
    /**
     * Record that an order was cancelled
     */
    public void recordCancellation(String orderId) {
        synchronized(lock) {
            updateOrderHistory(orderId, "CANCELLED");
            totalOrdersCancelled.incrementAndGet();
        }
    }
    
    /**
     * Record order fill information
     */
    public void recordFill(String orderId, double filledQty) {
        synchronized(lock) {
            OrderHistoryEntry entry = orderHistory.get(orderId);
            if (entry != null) {
                entry.updateFilledQuantity(filledQty);
                if (Math.abs(filledQty - entry.getQuantity()) < 0.01) {
                    entry.updateStatus("FILLED");
                } else {
                    entry.updateStatus("PARTIAL_FILL");
                }
            }
        }
    }
    
    /**
     * Update order history with a new status
     */
    private void updateOrderHistory(String orderId, String status) {
        if (orderId == null) return;
        
        OrderHistoryEntry entry = orderHistory.get(orderId);
        if (entry != null) {
            entry.updateStatus(status);
        }
    }
    
    /**
     * Add an order to the history tracking
     */
    private void addToOrderHistory(MarketOrder order, String side, String status, double price) {
        if (order == null || order.getOrderId() == null) return;
        
        OrderHistoryEntry entry = new OrderHistoryEntry(
            order.getOrderId(),
            side,
            price,
            0.0 // We don't have quantity in MarketOrder directly
        );
        entry.updateStatus(status);
        orderHistory.put(order.getOrderId(), entry);
    }
    
    public void validateOrderReferences() {
        synchronized(lock) {
            // Validate bid order reference
            if (bidOrder != null && !bidOrderId.equals(bidOrder.getOrderId())) {
                logger.warn("Inconsistent bid order reference for "+id+": "+bidOrderId+" vs "+bidOrder.getOrderId());
                bidOrderId = bidOrder.getOrderId(); // Fix inconsistency
            }
            
            // Validate ask order reference
            if (askOrder != null && !askOrderId.equals(askOrder.getOrderId())) {
                logger.warn("Inconsistent ask order reference for " + id + ": " + askOrderId + " vs " + askOrder.getOrderId());
                askOrderId = askOrder.getOrderId(); // Fix inconsistency
            }
        }
    }

    /**
     * Check for duplicate orders and return the one that should be cancelled
     * 
     * @param side The order side ("Buy" or "Sell")
     * @param orderId The new order ID
     * @param time The new order timestamp
     * @return The order ID that should be cancelled (null if no duplicate)
     */
    public String checkForDuplicate(String side, String orderId, int time) {
        synchronized(lock) {
            if (orderId == null) return null;
            
            if ("Buy".equals(side)) {
                if (bidOrderId != null && !bidOrderId.equals(orderId) && isBidActive) {
                    totalDuplicatesDetected.incrementAndGet();
                    
                    // Compare creation times to determine which to keep
                    if (bidCreationTime > 0 && time > 0) {
                        if (time > bidCreationTime) {
                            // New order is newer, cancel existing one
                            return bidOrderId;
                        } else {
                            // Existing order is newer, cancel new one
                            return orderId;
                        }
                    }
                }
            } else if ("Sell".equals(side)) {
                if (askOrderId != null && !askOrderId.equals(orderId) && isAskActive) {
                    totalDuplicatesDetected.incrementAndGet();
                    
                    // Compare creation times to determine which to keep
                    if (askCreationTime > 0 && time > 0) {
                        if (time > askCreationTime) {
                            // New order is newer, cancel existing one
                            return askOrderId;
                        } else {
                            // Existing order is newer, cancel new one
                            return orderId;
                        }
                    }
                }
            }
            
            return null;  // No duplicate found
        }
    }
    
    /**
     * Sets whether the bid is GC-based
     * 
     * @param isGcBased Whether the bid is based on GC data
     */
    public void setGcBasedBid(boolean isGcBased) {
        this.isGcBasedBid = isGcBased;
    }

    /**
     * Sets whether the ask is GC-based
     * 
     * @param isGcBased Whether the ask is based on GC data
     */
    public void setGcBasedAsk(boolean isGcBased) {
        this.isGcBasedAsk = isGcBased;
    }

    /**
     * Sets whether the bid is market-based
     * 
     * @param isMarketBased Whether the bid is based on market data
     */
    public void setMarketBasedBid(boolean isMarketBased) {
        this.isMarketBasedBid = isMarketBased;
    }

    /**
     * Sets whether the ask is market-based
     * 
     * @param isMarketBased Whether the ask is based on market data
     */
    public void setMarketBasedAsk(boolean isMarketBased) {
        this.isMarketBasedAsk = isMarketBased;
    }

    /**
     * @return The reference source for bid pricing
     */
    public String getBidReferenceSource() { 
        return bidReferenceSource; 
    }
    
    /**
     * @return The reference source for ask pricing
     */
    public String getAskReferenceSource() { 
        return askReferenceSource; 
    }
    
    /**
     * @return The current bid price
     */
    public double getBidPrice() { 
        return bidPrice; 
    }
    
    /**
     * @return The current ask price
     */
    public double getAskPrice() { 
        return askPrice; 
    }
    
    /**
     * @return The venue for the bid order
     */
    public String getBidVenue() {
        return bidVenue;
    }
    
    /**
     * @return The venue for the ask order
     */
    public String getAskVenue() {
        return askVenue;
    }
    
    /**
     * @return The trader ID for the bid order
     */
    public String getBidTraderId() {
        return bidTraderId;
    }
    
    /**
     * @return The trader ID for the ask order
     */
    public String getAskTraderId() {
        return askTraderId;
    }
    
    /**
     * @return The native instrument ID for the bid
     */
    public String getBidNativeInstrumentId() {
        return bidNativeInstrumentId;
    }
    
    /**
     * @return The native instrument ID for the ask
     */
    public String getAskNativeInstrumentId() {
        return askNativeInstrumentId;
    }
    
    /**
     * @return Whether the bid price is based on GC data
     */
    public boolean isGcBasedBid() { 
        return isGcBasedBid; 
    }
    
    /**
     * @return Whether the ask price is based on GC data
     */
    public boolean isGcBasedAsk() { 
        return isGcBasedAsk; 
    }
    
    /**
     * @return Whether the bid price is based on market data
     */
    public boolean isMarketBasedBid() { 
        return isMarketBasedBid; 
    }
    
    /**
     * @return Whether the ask price is based on market data
     */
    public boolean isMarketBasedAsk() { 
        return isMarketBasedAsk; 
    }
    
    /**
     * @return The number of consecutive bid update failures
     */
    public int getBidUpdateFailureCount() { 
        return bidUpdateFailureCount; 
    }
    
    /**
     * @return The number of consecutive ask update failures
     */
    public int getAskUpdateFailureCount() { 
        return askUpdateFailureCount; 
    }
    
    /**
     * @return The timestamp until which bid updates are backed off
     */
    public long getBidBackoffUntilTime() { 
        return bidBackoffUntilTime; 
    }
    
    /**
     * @return The timestamp until which ask updates are backed off
     */
    public long getAskBackoffUntilTime() { 
        return askBackoffUntilTime; 
    }
    
    /**
     * @return Whether the bid is currently active
     */
    public boolean isBidActive() {
        return isBidActive && bidOrder != null && !bidOrder.isDead();
    }
    
    /**
     * @return Whether the ask is currently active
     */
    public boolean isAskActive() {
        return isAskActive && askOrder != null && !askOrder.isDead();
    }
    
    /**
     * @return The age of the bid order in milliseconds, or 0 if none
     */
    public long getBidAge() {
        return bidCreationTime > 0 ? System.currentTimeMillis() - bidCreationTime : 0;
    }
    
    /**
     * @return The age of the ask order in milliseconds, or 0 if none
     */
    public long getAskAge() {
        return askCreationTime > 0 ? System.currentTimeMillis() - askCreationTime : 0;
    }
    
    /**
     * Increment the bid failure counter and calculate backoff time
     */
    public void incrementBidFailure() {
        bidUpdateFailureCount++;
        // Calculate exponential backoff time (in milliseconds)
        long backoffMs = Math.min(300_000, (long)(1000 * Math.pow(2, bidUpdateFailureCount)));
        bidBackoffUntilTime = System.currentTimeMillis() + backoffMs;

        logger.info("Bid failure count for " + id + " incremented to " + bidUpdateFailureCount + ", backing off for " + backoffMs + "ms");
    }
    
    /**
     * Increment the ask failure counter and calculate backoff time
     */
    public void incrementAskFailure() {
        askUpdateFailureCount++;
        // Calculate exponential backoff time (in milliseconds)
        long backoffMs = Math.min(300_000, (long)(1000 * Math.pow(2, askUpdateFailureCount)));
        askBackoffUntilTime = System.currentTimeMillis() + backoffMs;

        logger.info("Ask failure count for " + id + " incremented to " + askUpdateFailureCount + ", backing off for " + backoffMs + "ms");
    }
    
    /**
     * Reset the bid failure counter and backoff time
     */
    public void resetBidFailure() {
        bidUpdateFailureCount = 0;
        bidBackoffUntilTime = 0;
        
        logger.info("Bid failure count for " + id + " reset");
    }
    
    /**
     * Reset the ask failure counter and backoff time
     */
    public void resetAskFailure() {
        askUpdateFailureCount = 0;
        askBackoffUntilTime = 0;
        
        logger.info("Ask failure count for " + id + " reset");
    }
    
    /**
     * @return Whether the bid is currently in backoff
     */
    public boolean isBidInBackoff() {
        return System.currentTimeMillis() < bidBackoffUntilTime;
    }
    
    /**
     * @return Whether the ask is currently in backoff
     */
    public boolean isAskInBackoff() {
        return System.currentTimeMillis() < askBackoffUntilTime;
    }
    
    /**
     * Gets a copy of the order history
     */
    public List<OrderHistoryEntry> getOrderHistory() {
        synchronized(lock) {
            return new ArrayList<>(orderHistory.values());
        }
    }
    
    /**
     * Gets metrics about order activity for this instrument
     */
    public Map<String, Integer> getMetrics() {
        Map<String, Integer> metrics = new HashMap<>();
        metrics.put("totalOrdersPlaced", totalOrdersPlaced.get());
        metrics.put("totalOrdersUpdated", totalOrdersUpdated.get());
        metrics.put("totalOrdersCancelled", totalOrdersCancelled.get());
        metrics.put("totalDuplicatesDetected", totalDuplicatesDetected.get());
        return metrics;
    }

    /**
     * @return String representation of ActiveQuote for debugging
     */
    @Override
    public String toString() {
        return "ActiveQuote[instrument=" + id + 
            ", bidActive=" + isBidActive() + 
            ", askActive=" + isAskActive() + 
            ", bidPrice=" + bidPrice + 
            ", askPrice=" + askPrice + 
            ", bidSource=" + bidReferenceSource + 
            ", askSource=" + askReferenceSource + "]";
    }
    
    /**
     * Creates a status summary map for monitoring and reporting
     * 
     * @return A map containing the current quote status
     */
    public Map<String, Object> getStatusSummary() {
        Map<String, Object> status = new HashMap<>();
        status.put("id", id);
        status.put("cusip", cusip);
        status.put("termCode", termCode);
        status.put("bidActive", isBidActive());
        status.put("askActive", isAskActive());
        status.put("bidPrice", bidPrice);
        status.put("askPrice", askPrice);
        status.put("bidSource", bidReferenceSource);
        status.put("askSource", askReferenceSource);
        status.put("bidGcBased", isGcBasedBid);
        status.put("askGcBased", isGcBasedAsk);
        status.put("bidOrderId", bidOrderId);
        status.put("askOrderId", askOrderId);
        status.put("bidReqId", bidReqId);
        status.put("askReqId", askReqId);
        status.put("bidAge", getBidAge());
        status.put("askAge", getAskAge());
        status.put("bidBackoffRemaining", Math.max(0, bidBackoffUntilTime - System.currentTimeMillis()));
        status.put("askBackoffRemaining", Math.max(0, askBackoffUntilTime - System.currentTimeMillis()));
        status.put("historyCount", orderHistory.size());
        status.put("metrics", getMetrics());
        return status;
    }

    /**
     * Performs an atomic operation on the quote
     * @param operation The function to apply to the quote
     * @return The result of the operation
     */
    public <T> T atomicOperation(Function<ActiveQuote, T> operation) {
        synchronized(lock) {
            return operation.apply(this);
        }
    }
    
    /**
     * Creates or finds an order by ID
     * @param orderId The order ID to find
     * @param side Order side (Buy/Sell)
     * @return true if order belongs to this quote, false otherwise
     */
    public boolean containsOrder(String orderId, String side) {
        if (orderId == null) return false;
        
        synchronized(lock) {
            if ("Buy".equals(side)) {
                return orderId.equals(bidOrderId);
            } else if ("Sell".equals(side)) {
                return orderId.equals(askOrderId);
            }
            
            // Check both sides if side not specified
            return orderId.equals(bidOrderId) || orderId.equals(askOrderId);
        }
    }
    
    /**
     * Gets the side (Buy/Sell) for an order ID
     * @param orderId The order ID
     * @return "Buy", "Sell", or null if not found
     */
    public String getSideForOrderId(String orderId) {
        if (orderId == null) return null;
        
        synchronized(lock) {
            if (orderId.equals(bidOrderId)) {
                return "Buy";
            } else if (orderId.equals(askOrderId)) {
                return "Sell";
            }
            return null;
        }
    }
    
    /**
     * Update order status based on side
     * 
     * @param side The order side ("Buy" or "Sell")
     * @param isActive Whether the order is active
     */
    public void setOrderActive(String side, boolean isActive) {
        if ("Buy".equals(side)) {
            setBidActive(isActive);
        } else if ("Sell".equals(side)) {
            setAskActive(isActive);
        }
    }

    /**
     * Get order active status based on side
     * 
     * @param side The order side ("Buy" or "Sell")
     * @return Whether the order is active
     */
    public boolean isOrderActive(String side) {
        if ("Buy".equals(side)) {
            return isBidActive();
        } else if ("Sell".equals(side)) {
            return isAskActive();
        }
        return false;
    }

    /**
     * Get the order ID for a specific side
     * 
     * @param side The order side ("Buy" or "Sell")
     * @return The order ID or null if not found
     */
    public String getOrderId(String side) {
        if ("Buy".equals(side)) {
            return getBidOrderId();
        } else if ("Sell".equals(side)) {
            return getAskOrderId();
        }
        return null;
    }
}