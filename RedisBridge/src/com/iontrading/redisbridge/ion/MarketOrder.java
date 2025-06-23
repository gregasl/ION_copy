package com.iontrading.redisbridge.ion;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a market order.
 * This class contains information about a market order.
 */
public class MarketOrder {
    
    // Buy verb
    public static final int BUY = 1;
    
    // Sell verb
    public static final int SELL = 2;
    
    // Order ID
    private String orderId;
    
    // Client order ID
    private String clientOrderId;
    
    // Trader ID
    private String traderId;
    
    // Instrument ID
    private String instrumentId;
    
    // Verb (buy or sell)
    private int verb;
    
    // Price
    private double price;
    
    // Quantity
    private double quantity;
    
    // Filled quantity
    private double filledQuantity;
    
    // Order type
    private String orderType;
    
    // Time in force
    private String timeInForce;
    
    // Status
    private String status;
    
    // Creation time
    private long creationTime;
    
    // Update time
    private long updateTime;
    
    // Additional properties
    private final Map<String, Object> properties;
    
    /**
     * Creates a new MarketOrder.
     */
    public MarketOrder() {
        this.properties = new HashMap<>();
        this.verb = BUY;
        this.price = 0;
        this.quantity = 0;
        this.filledQuantity = 0;
        this.orderType = MarketDef.ORDER_TYPE_MARKET;
        this.timeInForce = MarketDef.TIME_IN_FORCE_DAY;
        this.status = MarketDef.ORDER_STATUS_NEW;
        this.creationTime = System.currentTimeMillis();
        this.updateTime = this.creationTime;
    }
    
    /**
     * Creates a new MarketOrder with the specified order ID.
     * 
     * @param orderId The order ID
     */
    public MarketOrder(String orderId) {
        this();
        this.orderId = orderId;
    }
    
    /**
     * Creates a new MarketOrder with the specified order ID and client order ID.
     * 
     * @param orderId The order ID
     * @param clientOrderId The client order ID
     */
    public MarketOrder(String orderId, String clientOrderId) {
        this(orderId);
        this.clientOrderId = clientOrderId;
    }
    
    /**
     * Gets the order ID.
     * 
     * @return The order ID
     */
    public String getOrderId() {
        return orderId;
    }
    
    /**
     * Sets the order ID.
     * 
     * @param orderId The order ID
     */
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }
    
    /**
     * Gets the client order ID.
     * 
     * @return The client order ID
     */
    public String getClientOrderId() {
        return clientOrderId;
    }
    
    /**
     * Sets the client order ID.
     * 
     * @param clientOrderId The client order ID
     */
    public void setClientOrderId(String clientOrderId) {
        this.clientOrderId = clientOrderId;
    }
    
    /**
     * Gets the trader ID.
     * 
     * @return The trader ID
     */
    public String getTraderId() {
        return traderId;
    }
    
    /**
     * Sets the trader ID.
     * 
     * @param traderId The trader ID
     */
    public void setTraderId(String traderId) {
        this.traderId = traderId;
    }
    
    /**
     * Gets the instrument ID.
     * 
     * @return The instrument ID
     */
    public String getInstrumentId() {
        return instrumentId;
    }
    
    /**
     * Sets the instrument ID.
     * 
     * @param instrumentId The instrument ID
     */
    public void setInstrumentId(String instrumentId) {
        this.instrumentId = instrumentId;
    }
    
    /**
     * Gets the verb.
     * 
     * @return The verb
     */
    public int getVerb() {
        return verb;
    }
    
    /**
     * Sets the verb.
     * 
     * @param verb The verb
     */
    public void setVerb(int verb) {
        this.verb = verb;
    }
    
    /**
     * Gets the price.
     * 
     * @return The price
     */
    public double getPrice() {
        return price;
    }
    
    /**
     * Sets the price.
     * 
     * @param price The price
     */
    public void setPrice(double price) {
        this.price = price;
    }
    
    /**
     * Gets the quantity.
     * 
     * @return The quantity
     */
    public double getQuantity() {
        return quantity;
    }
    
    /**
     * Sets the quantity.
     * 
     * @param quantity The quantity
     */
    public void setQuantity(double quantity) {
        this.quantity = quantity;
    }
    
    /**
     * Gets the filled quantity.
     * 
     * @return The filled quantity
     */
    public double getFilledQuantity() {
        return filledQuantity;
    }
    
    /**
     * Sets the filled quantity.
     * 
     * @param filledQuantity The filled quantity
     */
    public void setFilledQuantity(double filledQuantity) {
        this.filledQuantity = filledQuantity;
    }
    
    /**
     * Gets the order type.
     * 
     * @return The order type
     */
    public String getOrderType() {
        return orderType;
    }
    
    /**
     * Sets the order type.
     * 
     * @param orderType The order type
     */
    public void setOrderType(String orderType) {
        this.orderType = orderType;
    }
    
    /**
     * Gets the time in force.
     * 
     * @return The time in force
     */
    public String getTimeInForce() {
        return timeInForce;
    }
    
    /**
     * Sets the time in force.
     * 
     * @param timeInForce The time in force
     */
    public void setTimeInForce(String timeInForce) {
        this.timeInForce = timeInForce;
    }
    
    /**
     * Gets the status.
     * 
     * @return The status
     */
    public String getStatus() {
        return status;
    }
    
    /**
     * Sets the status.
     * 
     * @param status The status
     */
    public void setStatus(String status) {
        this.status = status;
    }
    
    /**
     * Gets the creation time.
     * 
     * @return The creation time
     */
    public long getCreationTime() {
        return creationTime;
    }
    
    /**
     * Sets the creation time.
     * 
     * @param creationTime The creation time
     */
    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }
    
    /**
     * Gets the update time.
     * 
     * @return The update time
     */
    public long getUpdateTime() {
        return updateTime;
    }
    
    /**
     * Sets the update time.
     * 
     * @param updateTime The update time
     */
    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }
    
    /**
     * Gets a property.
     * 
     * @param key The property key
     * @return The property value, or null if not found
     */
    public Object getProperty(String key) {
        return properties.get(key);
    }
    
    /**
     * Sets a property.
     * 
     * @param key The property key
     * @param value The property value
     */
    public void setProperty(String key, Object value) {
        properties.put(key, value);
    }
    
    /**
     * Gets all properties.
     * 
     * @return The properties
     */
    public Map<String, Object> getProperties() {
        return new HashMap<>(properties);
    }
    
    /**
     * Checks if the order is active.
     * 
     * @return true if the order is active, false otherwise
     */
    public boolean isActive() {
        return status.equals(MarketDef.ORDER_STATUS_NEW) || 
               status.equals(MarketDef.ORDER_STATUS_PARTIALLY_FILLED);
    }
    
    /**
     * Checks if the order is filled.
     * 
     * @return true if the order is filled, false otherwise
     */
    public boolean isFilled() {
        return status.equals(MarketDef.ORDER_STATUS_FILLED);
    }
    
    /**
     * Checks if the order is canceled.
     * 
     * @return true if the order is canceled, false otherwise
     */
    public boolean isCanceled() {
        return status.equals(MarketDef.ORDER_STATUS_CANCELED);
    }
    
    /**
     * Checks if the order is rejected.
     * 
     * @return true if the order is rejected, false otherwise
     */
    public boolean isRejected() {
        return status.equals(MarketDef.ORDER_STATUS_REJECTED);
    }
    
    /**
     * Checks if the order is expired.
     * 
     * @return true if the order is expired, false otherwise
     */
    public boolean isExpired() {
        return status.equals(MarketDef.ORDER_STATUS_EXPIRED);
    }
    
    /**
     * Checks if the order is a buy order.
     * 
     * @return true if the order is a buy order, false otherwise
     */
    public boolean isBuy() {
        return verb == BUY;
    }
    
    /**
     * Checks if the order is a sell order.
     * 
     * @return true if the order is a sell order, false otherwise
     */
    public boolean isSell() {
        return verb == SELL;
    }
    
    /**
     * Checks if the order is a market order.
     * 
     * @return true if the order is a market order, false otherwise
     */
    public boolean isMarketOrder() {
        return orderType.equals(MarketDef.ORDER_TYPE_MARKET);
    }
    
    /**
     * Checks if the order is a limit order.
     * 
     * @return true if the order is a limit order, false otherwise
     */
    public boolean isLimitOrder() {
        return orderType.equals(MarketDef.ORDER_TYPE_LIMIT);
    }
    
    /**
     * Gets the remaining quantity.
     * 
     * @return The remaining quantity
     */
    public double getRemainingQuantity() {
        return quantity - filledQuantity;
    }
    
    /**
     * Gets the fill percentage.
     * 
     * @return The fill percentage
     */
    public double getFillPercentage() {
        if (quantity <= 0) {
            return 0;
        }
        
        return (filledQuantity / quantity) * 100;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MarketOrder{");
        sb.append("orderId='").append(orderId).append('\'');
        sb.append(", clientOrderId='").append(clientOrderId).append('\'');
        sb.append(", traderId='").append(traderId).append('\'');
        sb.append(", instrumentId='").append(instrumentId).append('\'');
        sb.append(", verb=").append(verb);
        sb.append(", price=").append(price);
        sb.append(", quantity=").append(quantity);
        sb.append(", filledQuantity=").append(filledQuantity);
        sb.append(", orderType='").append(orderType).append('\'');
        sb.append(", timeInForce='").append(timeInForce).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", creationTime=").append(creationTime);
        sb.append(", updateTime=").append(updateTime);
        sb.append('}');
        return sb.toString();
    }
}
