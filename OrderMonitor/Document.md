# VMO Order Execution System - Complete Guide


## Overview
The VMO (Virtual Market Overlay) system provides unified order routing across multiple trading venues for US Repo markets. This document outlines the technical implementation for order placement and execution.


## 1. Order Flow Process
### 1.1 CUSIP to Execution Flow
```
Input: "912828ZF0" (CUSIP) @ Price 4.50
    ↓
VMO Search: "UST_10Y_Fixed" (VMO ID)
    ↓
VMO checks 10 levels of market data:
    Level 0: Ask=4.50 @ DEALERWEB    Bid=4.50 @ BTEC
    Level 1: Ask=4.49 @ BTEC         Bid=4.51 @ DEALERWEB  
    Level 2: Ask=4.48 @ FENICS       Bid=4.52 @ FENICS
    ... continues to Level 9
    ↓
VMO selects optimal venue
    ↓
Maps to venue-specific ID:
┌─────────────────────┬─────────────────┐
│ DEALERWEB_REPO     │ "DW_UST_10Y"    │
│ BTEC_REPO_US       │ "BTEC_10YR_UST" │
│ FENICS_USREPO      │ "FEN_UST10Y"    │
└─────────────────────┴─────────────────┘
```
### 1.2 Technical Architecture
```
Send Order → MkvFunction → ION Gateway → Execution Venue
      ↑                                          ↓
      ←────── Order Confirmation (onResult) ←────
```


## 2. Price Execution Scenarios
### Scenario: Input Price 4.50
**Market Conditions:**
```
Best Ask: 4.50 @ DEALERWEB
Best Bid: 4.49 @ BTEC
```
#### Buy Order @ 4.50
**Process:**
1. VMO sees DEALERWEB has ask price 4.50 (exact match)
2. VMO routes to DEALERWEB
3. Order may immediately execute @ 4.50
**Result:** Execution price = 4.50 (if liquidity available)
#### Sell Order @ 4.50
**Process:**
1. VMO sees highest bid is 4.49 @ BTEC (below your price)
2. VMO routes order to appropriate venue
3. Sell order rests on order book @ 4.50
4. Waits for buyer at 4.50
**Result:** No immediate execution, order rests on book

### Complex Scenarios
#### A. Market Depth Impact
```
Level 0: Ask=4.50 @ DEALERWEB (Size: 50)
Level 1: Ask=4.51 @ BTEC (Size: 200)
Your Order: Buy 100 @ 4.50
```
**Results:**
- First 50 execute at DEALERWEB @ 4.50
- Remaining 50:
  - Rest on book @ 4.50 (Day order)
  - Get cancelled (IOC order)
#### B. Price Improvement
```
Your Order: Buy 100 @ 4.50
Market Ask: 4.48 @ FENICS
```
- VMO routes to FENICS
- May execute at 4.48 (better than limit)
#### C. Time-in-Force Behavior
```java
// Day Order - Rests on book
Buy 100 @ 4.50, "Day"
// IOC Order - Immediate or Cancel
Buy 100 @ 4.50, "IOC"  
// FOK Order - Fill or Kill
Buy 100 @ 4.50, "FOK"
```


## 3. Order Implementation
### 3.1 Order Functions
#### VMO Level Order (Recommended)
```java
// Function: VMO_REPO_US_VCMIOrderAdd181
orderCreate(
    "VMO_REPO_US",     // VMO routing
    "TRADER_ID",       // Trader ID
    "UST_10Y_Fixed",   // Instrument
    "Buy",             // Side
    100,               // Quantity
    4.50,              // Price
    "Limit",           // Order type
    "Day"              // Time in force
);
```
#### Direct Venue Orders
```java
// Alternative functions for direct venue access:
// FENICS_USREPO_VCMIOrderAdd181
// DEALERWEB_REPO_VCMIOrderAdd181
// BTEC_REPO_US_VCMIOrderAdd181
```

### 3.2 Order Parameters
#### Required Parameters
| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| MarketSource | String | VMO or specific venue | "VMO_REPO_US" |
| TraderId | String | Trading ID | "TRADER123" |
| InstrumentId | String | Security identifier | "UST_10Y_Fixed" |
| Verb | String | Buy/Sell direction | "Buy" |
| Price | Double | Order price | 4.50 |
| Quantity | Double | Order amount | 100 |
| Type | String | Order type | "Limit" |
| TimeInForce | String | Duration | "Day" |
#### Optional Parameters
- IsSoft: 0
- Attribute: 0  
- CustomerInfo: ""
- FreeText: "APP_" + reqId
- StopCond: 0
- StopId: ""
- StopPrice: 0.0

### 3.3 Time-in-Force Options
| TIF | Description | Use Case |
|-----|-------------|----------|
| Day | Valid for trading day | Standard resting orders |
| IOC | Immediate or Cancel | Aggressive execution |
| FOK | Fill or Kill | All-or-none execution |
| FAS | Fill and Store | Partial fills allowed |


## 4. Add Order
### Passive Order (Post Liquidity)
```java
orderCreate(
    "VMO_REPO_US",
    traderId,
    "UST_10Y_Fixed",
    "Buy",
    100,
    4.48,              // Below market
    "Limit",
    "Day"              // Rests in order book
);
```
### Aggressive Order (Take Liquidity)
```java
orderCreate(
    "VMO_REPO_US",
    traderId,
    "UST_10Y_Fixed", 
    "Buy",
    100,
    4.52,              // Above market
    "Limit",
    "IOC"              // Immediate execution
);
```

#### Basic Order Function
```java
public MarketOrder placeVMOOrder(
    String instrument,
    String side,
    double quantity,
    double price,
    String orderType,
    String timeInForce
) {
    // Get VMO function
    MkvPublishManager pm = Mkv.getInstance().getPublishManager();
    MkvFunction fn = pm.getMkvFunction("VMO_REPO_US_VCMIOrderAdd181");
    
    // Create parameters
    MkvSupply args = MkvSupplyFactory.create(new Object[] {
        TRADER_ID,                    // Trader ID
        instrument,                   // Instrument
        side,                        // Buy/Sell
        Double.valueOf(price),       // Price
        Double.valueOf(quantity),    // Shown quantity
        Double.valueOf(quantity),    // Total quantity
        orderType,                   // Order type
        timeInForce,                 // TIF
        0, 0, "", "APP_" + reqId++, 0, "", 0.0  // Defaults
    });
    
    // Execute order
    MarketOrder order = new MarketOrder(...);
    fn.call(args, order);
    
    return order;
}
```

#### Result Handling
```java
public class OrderHandler implements MkvFunctionCallListener {
    
    @Override
    public void onResult(MkvFunctionCallEvent event, MkvSupply result) {
        String resultStr = result.getString(0);
        // Format: "0:OK -Result {-Id {ORDER_ID} ...}"
        LOGGER.info("Order created: {}", resultStr);
    }
    
    @Override
    public void onError(MkvFunctionCallEvent event, byte errCode, String errStr) {
        LOGGER.error("Order failed: {} - {}", errCode, errStr);
    }
}
```


## 5. Cancel Order
#### Cancel Method
```java
public static MarketOrder orderCancel(
    String marketSource,    // VMO or specific venue
    String traderId,        // Trader ID
    String orderId,         // Order ID to cancel (NOT reqId)
    IOrderManager orderManager
)
```
#### Required Parameters
```java
MkvSupply args = MkvSupplyFactory.create(new Object[] {
    traderId,                              // Trader ID
    orderId,                               // Order ID
    MarketDef.getFreeText(reqId, appId)   // Tracking info
});
```
#### Cancel Examples
**Single Order Cancel:**
```java
// Step 1: Create order
MarketOrder order = orderCreate(...);
// Step 2: Get order ID from callback
String orderId = order.getOrderId();  // e.g., "5697620189428842553_20250603"
// Step 3: Cancel order
MarketOrder cancelOrder = MarketOrder.orderCancel(
    "VMO_REPO_US",
    "TRADER123",
    orderId,
    this
);
```
**Batch Cancel All Orders:**
```java
public void cancelAllOrders() {
    Collection<MarketOrder> allOrders = orderRepository.getAllOrders();
    for (MarketOrder order : allOrders) {
        if ("Cancel".equals(order.getVerb())) continue;
        String orderId = order.getOrderId();
        if (orderId != null && !orderId.isEmpty()) {
            MarketOrder.orderCancel(
                order.getMarketSource(),
                traderId,
                orderId,
                this
            );
        }
    }
}
```

## 6. Key Points
- **Price Control**: VMO does NOT modify your order price
- **Routing Logic**: VMO determines optimal venue based on current market conditions
- **Execution**: Actual fills occur at the selected venue with venue-specific IDs
- **Monitoring**: Track orders via subscription to ORDER pattern