package com.iontrading.automatedMarketMaking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.DateTimeException;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * OrderRepository serves as the centralized store for all orders and quotes.
 * It provides thread-safe access to order information across both OrderManagement
 * and MarketMaker components.
 */
public class OrderRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderRepository.class);
    
    // Singleton instance
    private static volatile OrderRepository instance;
    
    // Thread safety
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    // Primary data stores
    private final Map<String, ActiveQuote> quotesByInstrument = new ConcurrentHashMap<>();
    private final Map<Integer, MarketOrder> ordersByReqId = new ConcurrentHashMap<>();
    private final Map<String, Integer> orderIdToReqIdMap = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> cusipToInstrumentMap = new ConcurrentHashMap<>();
    private final Map<String, Boolean> orderActivityStatusMap = new ConcurrentHashMap<>();

    private final Map<String, Best> latestBestByInstrument = new ConcurrentHashMap<>();
    private final Map<String, Long> lastOrderUpdateTime = new ConcurrentHashMap<>();
    private final Set<String> trackedInstruments = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> instrumentUpdateCounters = new ConcurrentHashMap<>();
    private final Map<String, Double> venueMinimumCache = new ConcurrentHashMap<>();
    private final Map<String, Boolean> venueActive = new ConcurrentHashMap<>();
    private final Map<String, Boolean> orderStatus = new ConcurrentHashMap<>();

    private final Map<String, Long> lastTradeTimeByInstrument = 
    Collections.synchronizedMap(new LinkedHashMap<String, Long>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > 10000;
        }
    });

    private final Map<String, Boolean> termCodeActiveStatus = new ConcurrentHashMap<>();

    // Tracking for duplicate detection
    private final Map<String, String> orderTracking = new ConcurrentHashMap<>(); // orderId -> timestamp
    
    private final Map<String, String> instrumentToBondMap = new ConcurrentHashMap<>();

    // Private constructor to enforce singleton pattern
    private OrderRepository() {
        // Register a shutdown hook to log statistics on exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("OrderRepository shutdown - Final statistics:");
            LOGGER.info("  Total quotes managed: {}", quotesByInstrument.size());
            LOGGER.info("  Total orders tracked: {}", ordersByReqId.size());
            LOGGER.info("  Total order ID mappings: {}", orderIdToReqIdMap.size());
            LOGGER.info("  Total CUSIP to instrument mappings: {}", cusipToInstrumentMap.size());
        }));
    }
    
    /**
     * Get the singleton instance
     */
    public static OrderRepository getInstance() {
        if (instance == null) {
            synchronized(OrderRepository.class) {
                if (instance == null) {
                    instance = new OrderRepository();
                }
            }
        }
        return instance;
    }
    
    /**
     * Get a quote by instrument ID, creating one if it doesn't exist
     */
    public ActiveQuote getQuote(String instrumentId) {
        if (instrumentId == null) return null;
        
        // Fast path - check if it already exists
        ActiveQuote quote = quotesByInstrument.get(instrumentId);
        if (quote != null) return quote;
        
        // Slow path - create a new quote
        lock.writeLock().lock();
        try {
            // Double-check after acquiring lock
            quote = quotesByInstrument.get(instrumentId);
            if (quote == null) {
                quote = new ActiveQuote(instrumentId);
                quotesByInstrument.put(instrumentId, quote);
                
                // Update CUSIP to instrument mapping
                String cusip = getCusipFromInstrumentId(instrumentId);
                if (cusip != null) {
                    cusipToInstrumentMap.computeIfAbsent(cusip, k -> new HashSet<>()).add(instrumentId);
                }
                
                LOGGER.debug("Created new ActiveQuote for {}", instrumentId);
            }
            return quote;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Add a venue's active status
     * 
     * @param venue The venue identifier
     * @param isActive True if the venue is active, false otherwise
     */

    public void addVenueActive(String venue, boolean isActive) {
        if (venue == null || venue.isEmpty()) return;
        
        lock.writeLock().lock();
        try {
            venueActive.put(venue, isActive);
            LOGGER.debug("Set venue {} active status to {}", venue, isActive);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the active status of a venue
     * 
     * @param venue The venue identifier
     * @return True if the venue is active, false otherwise
     */
    public boolean isVenueActive(String venue) {
        if (venue == null || venue.isEmpty()) return false;

        lock.readLock().lock();
        try {
            return venueActive.getOrDefault(venue, false);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get an existing quote without creating a new one
     */
    public ActiveQuote findQuote(String instrumentId) {
        return quotesByInstrument.get(instrumentId);
    }
    
    /**
     * Store a market order in the repository
     */
    public void storeOrder(MarketOrder order) {
        if (order == null) return;
        
        lock.writeLock().lock();
        try {
            // Store by request ID
            ordersByReqId.put(order.getMyReqId(), order);
            
            // If order has an order ID, map it to request ID
            String orderId = order.getOrderId();
            if (orderId != null && !orderId.isEmpty()) {
                orderIdToReqIdMap.put(orderId, order.getMyReqId());
            }
            
            LOGGER.debug("Stored order with reqId={}, orderId={}", order.getMyReqId(), orderId);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Remove an order by request ID
     */
    public MarketOrder removeOrder(int reqId) {
        lock.writeLock().lock();
        try {
            MarketOrder order = ordersByReqId.remove(reqId);
            if (order != null && order.getOrderId() != null) {
                orderIdToReqIdMap.remove(order.getOrderId());
                
                LOGGER.debug("Removed order with reqId={}, orderId={}", reqId, order.getOrderId());
            }
            return order;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get an order by request ID
     */
    public MarketOrder getOrderByReqId(int reqId) {
        return ordersByReqId.get(reqId);
    }
    
    /**
     * Get an order by order ID
     */
    public MarketOrder getOrderByOrderId(String orderId) {
        if (orderId == null || orderId.isEmpty()) return null;
        
        Integer reqId = orderIdToReqIdMap.get(orderId);
        if (reqId != null) {
            return ordersByReqId.get(reqId);
        }
        
        // Order ID not found in the map
        return null;
    }
    
    /**
     * Map an order ID to a request ID
     */
    public void mapOrderIdToReqId(String orderId, int reqId) {
        if (orderId == null || orderId.isEmpty()) return;
        
        lock.writeLock().lock();
        try {
            orderIdToReqIdMap.put(orderId, reqId);
            
            // Update the order object if we have it
            MarketOrder order = ordersByReqId.get(reqId);
            if (order != null) {
                order.setOrderId(orderId);
            }
            
            LOGGER.debug("Mapped orderId={} to reqId={}", orderId, reqId);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get the ActiveQuote for an order by its request ID
     */
    public ActiveQuote getQuoteForOrder(int reqId) {
        MarketOrder order = getOrderByReqId(reqId);
        if (order == null || order.getInstrId() == null) return null;
        
        return findQuote(order.getInstrId());
    }
    
    /**
     * Get the ActiveQuote for an order by its order ID
     */
    public ActiveQuote getQuoteForOrderId(String orderId) {
        MarketOrder order = getOrderByOrderId(orderId);
        if (order == null || order.getInstrId() == null) return null;
        
        return findQuote(order.getInstrId());
    }
    
    /**
     * Check for a duplicate order and return the one that should be cancelled
     * 
     * @param instrumentId The instrument ID
     * @param side The order side ("Buy" or "Sell")
     * @param orderId The new order ID
     * @param timestamp The order timestamp
     * @return String[] with [orderIdToCancel, traderId, venue] or null if no duplicate
     */
    public String[] checkForDuplicate(String instrumentId, String side, String orderId, int timestamp) {
        if (instrumentId == null || side == null || orderId == null) return null;
        
        // Get the quote for this instrument
        ActiveQuote quote = getQuote(instrumentId);
        
        // Check for duplicate using the quote
        String duplicateId = quote.checkForDuplicate(side, orderId, timestamp);
        
        if (duplicateId != null) {
            // We found a duplicate to cancel
            String cancelSide = "Buy".equals(side) ? "Buy" : "Sell";
            String traderId = "Buy".equals(cancelSide) ? quote.getBidTraderId() : quote.getAskTraderId();
            String venue = "Buy".equals(cancelSide) ? quote.getBidVenue() : quote.getAskVenue();
            
            return new String[] { duplicateId, traderId, venue };
        }
        
        return null;
    }
    
    /**
     * Get all quotes for a specific CUSIP
     */
    public List<ActiveQuote> getQuotesForCusip(String cusip) {
        if (cusip == null) return Collections.emptyList();
        
        List<ActiveQuote> quotes = new ArrayList<>();
        
        Set<String> instrumentIds = cusipToInstrumentMap.get(cusip);
        if (instrumentIds != null) {
            for (String instrumentId : instrumentIds) {
                ActiveQuote quote = findQuote(instrumentId);
                if (quote != null) {
                    quotes.add(quote);
                }
            }
        }
        
        return quotes;
    }
    
    /**
     * Register an order as dead/cancelled
     */
    public void markOrderDead(MarketOrder order) {
        if (order == null) return;
        
        int reqId = order.getMyReqId();
        String orderId = order.getOrderId();
        String instrumentId = order.getInstrId();
        
        if (instrumentId != null) {
            ActiveQuote quote = findQuote(instrumentId);
            if (quote != null) {
                String side = null;
                
                // Determine which side this order is on
                if (orderId != null) {
                    side = quote.getSideForOrderId(orderId);
                }
                
                if (side != null) {
                    if ("Buy".equals(side)) {
                        if (quote.getBidReqId() == reqId) {
                            quote.recordCancellation(orderId);
                            quote.setBidActive(false);
                            quote.setBidOrder(null, null, 0);
                        }
                    } else if ("Sell".equals(side)) {
                        if (quote.getAskReqId() == reqId) {
                            quote.recordCancellation(orderId);
                            quote.setAskActive(false);
                            quote.setAskOrder(null, null, 0);
                        }
                    }
                }
            }
        }
        
        // Remove the order from our tracking
        removeOrder(reqId);
    }
    
    /**
     * Remove a quote from the repository
     */
    public void removeQuote(String instrumentId) {
        if (instrumentId == null) return;
        
        lock.writeLock().lock();
        try {
            ActiveQuote removed = quotesByInstrument.remove(instrumentId);
            if (removed != null) {
                String cusip = getCusipFromInstrumentId(instrumentId);
                if (cusip != null) {
                    Set<String> instruments = cusipToInstrumentMap.get(cusip);
                    if (instruments != null) {
                        instruments.remove(instrumentId);
                        if (instruments.isEmpty()) {
                            cusipToInstrumentMap.remove(cusip);
                        }
                    }
                }
                LOGGER.debug("Removed quote for instrument: {}", instrumentId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes all ActiveQuotes that have no bid and no ask orders
     * @return The number of empty quotes removed
     */
    public int removeEmptyQuotes() {
        int removed = 0;
        lock.writeLock().lock();
        try {
            List<String> idsToRemove = new ArrayList<>();
            
            // First identify quotes to remove
            for (Map.Entry<String, ActiveQuote> entry : quotesByInstrument.entrySet()) {
                String instrumentId = entry.getKey();
                ActiveQuote quote = entry.getValue();
                
                if (quote.getBidOrder() == null && quote.getAskOrder() == null) {
                    idsToRemove.add(instrumentId);
                    LOGGER.info("Removing empty quote for {}", instrumentId);
                }
            }
            
            // Then remove them
            for (String id : idsToRemove) {
                removeQuote(id);
                removed++;
            }
            
            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove all order ID mappings for a specific request ID
     */
    public void removeAllMappingsForReqId(int reqId) {
        lock.writeLock().lock();
        try {
            orderIdToReqIdMap.values().removeIf(id -> id == reqId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Update order status in the corresponding quote
     */
    public void updateOrderStatus(String instrumentId, String side, boolean isActive) {
        if (instrumentId == null || side == null) return;
        
        ActiveQuote quote = findQuote(instrumentId);
        if (quote != null) {
            if ("Buy".equals(side)) {
                quote.setBidActive(isActive);
            } else if ("Sell".equals(side)) {
                quote.setAskActive(isActive);
            }
        }
        
        LOGGER.debug("Updated order status: instrument={}, side={}, active={}", 
            instrumentId, side, isActive);
    }
    
    /**
     * Gets the activity status for a combination of side and instrument ID
     * 
     * @param side The order side ("Buy" or "Sell")
     * @param instrumentId The instrument ID
     * @return The active status or false if not found
     */
    public boolean getOrderActivityStatus(String side, String instrumentId) {
        if (side == null || instrumentId == null) {
            return false;
        }
        
        String activityKey = side + ":" + instrumentId;
        return orderActivityStatusMap.getOrDefault(activityKey, false);
    }

/**
 * Gets a copy of the order activity status map
 * This is primarily for backward compatibility with code that accessed
 * the old orderActiveStatusMap directly
 * 
 * @return An unmodifiable view of the order activity status map
 */
public Map<String, Boolean> getOrderActivityStatusMap() {
    return Collections.unmodifiableMap(orderActivityStatusMap);
}
    
    /**
     * Get all active quotes
     */
    public List<ActiveQuote> getAllActiveQuotes() {
        List<ActiveQuote> activeQuotes = new ArrayList<>();
        for (ActiveQuote quote : quotesByInstrument.values()) {
            if (quote.isBidActive() || quote.isAskActive()) {
                activeQuotes.add(quote);
            }
        }
        return activeQuotes;
    }
    
public void setOrderStatus(String orderId, boolean isActive) {
        if (orderId == null || orderId.isEmpty()) return;
        
        lock.writeLock().lock();
        try {
            orderStatus.put(orderId, isActive);
            LOGGER.debug("Set order status for {} to {}", orderId, isActive);
        } finally {
            lock.writeLock().unlock();
        }
    }

public boolean getOrderStatus(String orderId) {
        if (orderId == null || orderId.isEmpty()) return false;

        lock.readLock().lock();
        try {
            return orderStatus.getOrDefault(orderId, false);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all orders currently in the repository
     */
    public Collection<MarketOrder> getAllOrders() {
        return new ArrayList<>(ordersByReqId.values());
    }

    /**
     * Cancel all orders for a specific term type
     */
    public void cancelOrdersByTerm(String termCode, Consumer<MarketOrder> canceller) {
        if (termCode == null || canceller == null) return;
        
        for (ActiveQuote quote : quotesByInstrument.values()) {
            if (quote.getTermCode().equals(termCode)) {
                // Cancel bid if active
                if (quote.isBidActive() && quote.getBidOrder() != null) {
                    canceller.accept(quote.getBidOrder());
                }
                
                // Cancel ask if active
                if (quote.isAskActive() && quote.getAskOrder() != null) {
                    canceller.accept(quote.getAskOrder());
                }
            }
        }
    }
    
    /**
     * Clear all quotes from the repository
     */
    public void clearAllQuotes() {
        lock.writeLock().lock();
        try {
            quotesByInstrument.clear();
            cusipToInstrumentMap.clear();
            LOGGER.info("Cleared all quotes from repository");
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clearAllOrderMappings() {
        lock.writeLock().lock();
        try {
            orderIdToReqIdMap.clear();
            LOGGER.info("Cleared all order ID mappings");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Extract CUSIP from an instrument ID
     */
    private String getCusipFromInstrumentId(String instrumentId) {
        if (instrumentId == null) return null;
        
        int underscorePos = instrumentId.indexOf('_');
        if (underscorePos > 0) {
            return instrumentId.substring(0, underscorePos);
        }
        
        return instrumentId;
    }
    
    public void standardizeTimestamps(String orderId, int localTimestamp) {
        long epochMillis = convertLocalTimeToMillis(localTimestamp);
        MarketOrder order = getOrderByOrderId(orderId);
        if (order != null) {
            order.setStandardizedTimestamp(epochMillis);
        }
    }

    /**
     * Converts a local timestamp in HHMMSSmmm format to milliseconds since epoch
     * for comparison with System.currentTimeMillis()
     * 
     * @param localTimestamp Timestamp in HHMMSSmmm format (e.g., 142456145 for 14:24:56.145)
     * @return The equivalent milliseconds since epoch for today's date
     */
    public long convertLocalTimeToMillis(int localTimestamp) {
        try {
            // Extract time components
            int hours = localTimestamp / 10000000;
            int minutes = (localTimestamp / 100000) % 100;
            int seconds = (localTimestamp / 1000) % 100;
            int millis = localTimestamp % 1000;
            
            // Validate time components to prevent invalid time creation
            if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59 ||
                seconds < 0 || seconds > 59 || millis < 0 || millis > 999) {
                LOGGER.warn("Invalid time components in timestamp {}: {}:{}:{}.{}", 
                    localTimestamp, hours, minutes, seconds, millis);
                throw new IllegalArgumentException("Invalid time components in timestamp");
            }
            
            // Create a LocalTime object
            LocalTime localTime = LocalTime.of(hours, minutes, seconds, millis * 1000000);
            
            // Combine with today's date and convert to milliseconds
            // Using system default timezone as specified in the requirements
            return localTime.atDate(java.time.LocalDate.now())
                        .atZone(java.time.ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
        } catch (Exception e) {
            LOGGER.error("Error converting timestamp {} to milliseconds: {}", 
                localTimestamp, e.getMessage(), e);
            throw new IllegalArgumentException("Invalid timestamp format: " + localTimestamp, e);
        }
    }

    /**
     * Get repository statistics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalQuotes", quotesByInstrument.size());
        stats.put("activeQuotes", getAllActiveQuotes().size());
        stats.put("totalOrders", ordersByReqId.size());
        stats.put("totalOrderIdMappings", orderIdToReqIdMap.size());
        stats.put("totalCusips", cusipToInstrumentMap.size());
        return stats;
    }
    
    /**
     * Sets a standardized timestamp on an order object following ISO 8601 format
     * 
     * @param order The order object to update
     * @param timestamp The timestamp to standardize (from HHMMSSmmm format or epoch millis)
     * @param timeZone Optional timezone identifier (defaults to system timezone if null)
     * @return The updated order object
     * @throws IllegalArgumentException if the order is null or timestamp format is invalid
     */
    public MarketOrder setStandardizedTimestamp(MarketOrder order, Object timestamp, String timeZone) {
        if (order == null) {
            LOGGER.error("Cannot set timestamp on null order");
            throw new IllegalArgumentException("Order cannot be null");
        }
        
        try {
            // Get timezone to use (default to system timezone if none provided)
            ZoneId zoneId = timeZone != null ? ZoneId.of(timeZone) : ZoneId.systemDefault();
            
            // Convert timestamp to epoch milliseconds based on input type
            long epochMillis;
            
            if (timestamp instanceof Integer) {
                // Handle HHMMSSmmm format
                epochMillis = convertLocalTimeToMillis((Integer)timestamp);
            } else if (timestamp instanceof Long) {
                // Already in epoch milliseconds
                epochMillis = (Long)timestamp;
            } else if (timestamp instanceof String) {
                try {
                    // Try parsing as integer (HHMMSSmmm format)
                    epochMillis = convertLocalTimeToMillis(Integer.parseInt((String)timestamp));
                } catch (NumberFormatException e) {
                    // Not a number, try parsing as ISO date
                    Instant instant = Instant.parse((String)timestamp);
                    epochMillis = instant.toEpochMilli();
                }
            } else {
                throw new IllegalArgumentException("Unsupported timestamp format: " + timestamp);
            }
            
            // Create ISO 8601 timestamp (YYYY-MM-DDTHH:mm:ss.sssZ)
            String iso8601Timestamp = Instant.ofEpochMilli(epochMillis)
                .atZone(zoneId)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
            
            // Update order with standardized timestamp
            order.setStandardizedTimestamp(epochMillis);
            order.setStandardizedTimestampStr(iso8601Timestamp);
            
            LOGGER.debug("Updated order {} with standardized timestamp: {}", 
                order.getOrderId(), iso8601Timestamp);
            
            // Store the standardized timestamp in the order mapping for future use
            if (order.getOrderId() != null) {
                lock.writeLock().lock();
                try {
                    // We might want to add a separate mapping for timestamps
                    // This could be useful for querying orders by time range
                    // orderTimestampMap.put(order.getOrderId(), epochMillis);
                } finally {
                    lock.writeLock().unlock();
                }
            }
            
            return order;
        } catch (DateTimeException | IllegalArgumentException e) {
            LOGGER.error("Error standardizing timestamp {} for order {}: {}", 
                timestamp, order.getOrderId(), e.getMessage(), e);
            throw new IllegalArgumentException("Invalid timestamp format: " + timestamp, e);
        }
    }

    /**
     * Sets a standardized timestamp on an order object using system timezone
     * 
     * @param order The order object to update
     * @param timestamp The timestamp to standardize
     * @return The updated order object
     */
    public MarketOrder setStandardizedTimestamp(MarketOrder order, Object timestamp) {
        return setStandardizedTimestamp(order, timestamp, null);
    }

    /**
     * Sets a standardized timestamp on an order identified by its order ID
     * 
     * @param orderId The ID of the order to update
     * @param timestamp The timestamp to standardize
     * @param timeZone Optional timezone identifier
     * @return The updated order object or null if order not found
     */
    public MarketOrder setStandardizedTimestamp(String orderId, Object timestamp, String timeZone) {
        MarketOrder order = getOrderByOrderId(orderId);
        if (order != null) {
            return setStandardizedTimestamp(order, timestamp, timeZone);
        } else {
            LOGGER.warn("Cannot set timestamp - order not found with ID: {}", orderId);
            return null;
        }
    }

    /**
     * Store the latest best price for an instrument
     */
    public void storeBestPrice(String instrumentId, Best best) {
        if (instrumentId != null && best != null) {
            latestBestByInstrument.put(instrumentId, best);
        }
    }

    /**
     * Get the latest best price for an instrument
     */
    public Best getLatestBest(String instrumentId) {
        return latestBestByInstrument.get(instrumentId);
    }

    /**
     * Add an instrument to the tracked set
     */
    public void trackInstrument(String instrumentId) {
        if (instrumentId != null) {
            trackedInstruments.add(instrumentId);
        }
    }

    /**
     * Check if an instrument is being tracked
     */
    public boolean isInstrumentTracked(String instrumentId) {
        return trackedInstruments.contains(instrumentId);
    }

    /**
     * Remove an instrument from tracking
     */
    public boolean untrackInstrument(String instrumentId) {
        return trackedInstruments.remove(instrumentId);
    }

    /**
     * Get all tracked instruments
     */
    public Set<String> getTrackedInstruments() {
        return Collections.unmodifiableSet(trackedInstruments);
    }

    /**
     * Get the count of tracked best prices
     * @return The number of instruments with latest best prices
     */
    public int getLatestBestCount() {
        return latestBestByInstrument.size();
    }

    /**
     * Record last update time for an instrument and side
     */
    public void recordLastOrderUpdateTime(String instrumentId, String side, long timestamp) {
        if (instrumentId != null && side != null) {
            lastOrderUpdateTime.put(instrumentId + ":" + side, timestamp);
        }
    }

    /**
     * Get the last update time for an instrument and side
     */
    public long getLastOrderUpdateTime(String instrumentId, String side) {
        if (instrumentId == null || side == null) {
            return 0;
        }
        Long time = lastOrderUpdateTime.get(instrumentId + ":" + side);
        return (time != null) ? time : 0;
    }

    public void recordLastTradeTime(String instrumentId, long timestamp) {
        lastTradeTimeByInstrument.put(instrumentId, timestamp);
    }

    public long getLastTradeTime(String instrumentId, long defaultValue) {
        return lastTradeTimeByInstrument.getOrDefault(instrumentId, defaultValue);
    }

    public void incrementInstrumentUpdateCounter(String instrumentId) {
        instrumentUpdateCounters.computeIfAbsent(instrumentId, k -> new AtomicInteger(0)).incrementAndGet();
    }

    public Map<String, Integer> getInstrumentUpdateCounts(int limit) {
        // Create a snapshot for top N instruments
        return instrumentUpdateCounters.entrySet().stream()
            .sorted((a, b) -> Integer.compare(b.getValue().get(), a.getValue().get()))
            .limit(limit)
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().get(),
                (v1, v2) -> v1,
                LinkedHashMap::new
            ));
    }

    public void cacheVenueMinimum(String instrumentId, String venue, double minimumSize) {
        String cacheKey = instrumentId + ":" + venue;
        venueMinimumCache.put(cacheKey, minimumSize);
    }

    public double getCachedVenueMinimum(String instrumentId, String venue, double defaultValue) {
        String cacheKey = instrumentId + ":" + venue;
        return venueMinimumCache.getOrDefault(cacheKey, defaultValue);
    }

    public void mapInstrumentToBond(String instrumentId, String bondId) {
        instrumentToBondMap.put(instrumentId, bondId);
    }

    public String getBondForInstrument(String instrumentId) {
        return instrumentToBondMap.get(instrumentId);
    }

    public void setTermCodeStatus(String termCode, boolean isActive) {
        termCodeActiveStatus.put(termCode, isActive);
    }

    public boolean isTermCodeActive(String termCode) {
        return termCodeActiveStatus.getOrDefault(termCode, false);
    }

    /**
     * Clears all instrument update counters
     */
    public void clearInstrumentUpdateCounters() {
        instrumentUpdateCounters.clear();
    }

    /**
     * Detects if there are multiple active orders on the same side of the market
     * for a specific instrument.
     * 
     * @param instrumentId The instrument ID to check
     * @param side Order side to check ("Buy" or "Sell")
     * @return List of active order IDs on the specified side, empty if none found
     */
    public List<String> getActiveOrdersForSide(String instrumentId, String side) {
        if (instrumentId == null || side == null) {
            return Collections.emptyList();
        }
        
        List<String> activeOrders = new ArrayList<>();
        
        lock.readLock().lock();
        try {
            // First check the ActiveQuote for this instrument
            ActiveQuote quote = quotesByInstrument.get(instrumentId);
            if (quote != null) {
                // Add the order from the quote if it's active
                if ("Buy".equals(side) && quote.isBidActive()) {
                    MarketOrder bidOrder = quote.getBidOrder();
                    if (bidOrder != null && bidOrder.getOrderId() != null) {
                        activeOrders.add(bidOrder.getOrderId());
                    }
                } else if ("Sell".equals(side) && quote.isAskActive()) {
                    MarketOrder askOrder = quote.getAskOrder();
                    if (askOrder != null && askOrder.getOrderId() != null) {
                        activeOrders.add(askOrder.getOrderId());
                    }
                }
            }
            
            // Then check all orders to find any that match the instrument and side
            // but aren't in the quote (which would indicate multiple orders)
            for (MarketOrder order : ordersByReqId.values()) {
                if (order != null && 
                    instrumentId.equals(order.getInstrId()) && 
                    side.equals(order.getVerb())) {
                
                    String orderId = order.getOrderId();
                
                    // Only include active orders that aren't already in our list
                    if (orderId != null && !activeOrders.contains(orderId) && 
                        orderStatus.getOrDefault(orderId, false)) {
                        activeOrders.add(orderId);
                    }
                }
            }
        
            return activeOrders;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if there are multiple active orders for a given instrument side.
     * This is a convenience method that uses getActiveOrdersForSide internally.
     * 
     * @param instrumentId The instrument ID to check
     * @param side Order side to check ("Buy" or "Sell")
     * @return true if multiple active orders exist, false otherwise
     */
    public boolean hasMultipleActiveOrdersForSide(String instrumentId, String side) {
        return getActiveOrdersForSide(instrumentId, side).size() > 1;
    }

    /**
     * Detects any instruments with multiple active orders on the same side.
     * 
     * @return Map where keys are instrument IDs and values are maps of sides to lists of order IDs
     */
    public Map<String, Map<String, List<String>>> detectMultipleActiveOrders() {
        Map<String, Map<String, List<String>>> result = new HashMap<>();
    
        lock.readLock().lock();
        try {
            // Check all tracked instruments
            for (String instrumentId : trackedInstruments) {
                List<String> buyOrders = getActiveOrdersForSide(instrumentId, "Buy");
                List<String> sellOrders = getActiveOrdersForSide(instrumentId, "Sell");
            
                if (buyOrders.size() > 1 || sellOrders.size() > 1) {
                    Map<String, List<String>> sidesToOrders = new HashMap<>();
                
                    if (buyOrders.size() > 1) {
                        sidesToOrders.put("Buy", buyOrders);
                    }
                
                    if (sellOrders.size() > 1) {
                        sidesToOrders.put("Sell", sellOrders);
                    }
                
                    if (!sidesToOrders.isEmpty()) {
                        result.put(instrumentId, sidesToOrders);
                    }
                }
            }
        
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }
}