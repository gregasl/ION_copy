/*
 * MarketOrder
 *
 * MarketOrder accomplishes two tasks: putting order on the market (through a 
 * static method that return a MarketOrder instance) and to implement the logic
 * for getting back from the gateway the response to the request for adding an
 * order and to listen for the updates of the order.
 * The onSupply method implements also the logic that decides if an order can
 * generate trades or is "dead".
 *
 * ION Trading U.K. Limited supplies this software code is for testing purposes
 * only. The scope of this software code is exclusively limited to the
 * demonstration of the structure of an application using the ION(tm) Common
 * Market and ION Trading U.K. Limited does not guarantee the correct behavior
 * of any deployed application using this software code.
 * This software code has not been thoroughly tested under all conditions.
 * ION, therefore, cannot guarantee or imply reliability, serviceability, or
 * function of this software.
 * Any use of this software outside of this defined scope is the sole
 * responsibility of the user.
 *
 * ION Trading ltd (2005)
 */

package com.iontrading.redisOrder;

import java.util.concurrent.atomic.AtomicInteger;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvLog;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionCallListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyFactory;

/**
 * The MarketOrder class represents an order in the market and handles:
 * 1. Order creation via gateway functions
 * 2. Order lifecycle management (monitoring updates)
 * 3. Communication with the OrderManager when the order's state changes
 * 
 * Implements both MkvFunctionCallListener (to receive the response to order creation)
 * and MkvRecordListener (to receive ongoing updates about the order's state)
 */
public class MarketOrder implements MkvFunctionCallListener, MkvRecordListener {
  
  /**
   * Timestamp when the order was created (in milliseconds since epoch)
   */
  private final long creationTimestamp;

  // Add logger for debugging
    private static MkvLog log = Mkv.getInstance().getLogManager().getLogFile("REDIS_ORDER");
    private static IONLogger logger = new IONLogger(log, 2, "MarketOrder");  
  /**
   * An id that should be unique during the life of the component.
   * This is a simple counter implementation - in a production system,
   * the generation of unique identifiers would likely be more sophisticated.
   */
  private static final AtomicInteger reqId = new AtomicInteger(0);

  private long standardizedTimestamp; // Epoch milliseconds
  private String standardizedTimestampStr; // ISO 8601 format

  /**
   * Creates a new market order by calling the VCMIOrderAdd181 function on the gateway.
   * This is the main entry point for creating orders and registering a listener for updates.
   * 
   * @param instrId The instrument identifier
   * @param verb The order direction ("Buy" or "Sell")
   * @param qty The quantity of the order
   * @param price The price of the order
   * @param type The order type (e.g., "Limit", "Market")
   * @param tif Time in force (e.g., "Day", "IOC", "FOK", "FAS", "FAK")
   * @return A new MarketOrder object or null if creation failed
   * Syntax	<source>_VCMIOrderAdd181 (<User>, <InstrumentId>, <Verb>, <Price>, <QtyShown>, <QtyTot>, <Type>, <TimeInForce>, <IsSoft>, <Attribute>, <CustomerInfo>, <FreeText>, <StopCond>, <StopId>, <StopPrice>, ... )
   */

  public static MarketOrder orderCreate(String TraderId, String instrId, String verb,
      double qty, double price, String type, String tif) {
    
    // Increment the request ID to ensure uniqueness
    reqId.incrementAndGet();
    
    // Log the order creation attempt
    logger.info("Creating VMO order request for "+TraderId+" "+verb+" "+instrId+" @ "+qty+" "+price);

    // Get the publish manager to access functions
    MkvPublishManager pm = Mkv.getInstance().getPublishManager();
    
    // Get the order add function from the ION gateway hard coding for VMO only
    MkvFunction fn = pm.getMkvFunction("VMO_VCMIOrderAdd181");
    
    if (fn == null) {
      logger.error("Failed to get VCMIOrderAdd181 function from gateway");
      return null;
    }
    
    try {
      // The free text will contain the application ID to identify orders from this component
      String freeText = "ORDER_ID FROM APP";
      
      // Create a new MarketOrder object that will track this order
      MarketOrder order = new MarketOrder(reqId.get(), instrId, verb, qty, price, tif);

      // Create the function arguments for the order creation
      // This is specific to the VCMIOrderAdd181 function interface
      MkvSupply args = MkvSupplyFactory.create(new Object[] {
        TraderId,                                 // User                  
        instrId,                                        // InstrumentId          
        verb,                                           // Verb (Buy/Sell)                 
        Double.valueOf(price),                          // Price                 
        Double.valueOf(qty),                                // QtyShown (visible quantity)             
        Double.valueOf(qty),                                // QtyTot (total quantity)               
        type,                                           // Type (Limit, Market, etc.)                 
        tif,                                            // TimeInForce (DAY, IOC, FOK, etc.)          
        Integer.valueOf(0),                                 // IsSoft                
        Integer.valueOf(0),                                 // Attribute             
        "",                                                  // CustomerInfo          
        "" + reqId + " " + freeText,                       // FreeText (contains reqId and appId)             
        Integer.valueOf(0),                                 // StopCond              
        "",                                             // StopId                
        Double.valueOf(0)                                   // StopPrice             
      });

      logger.info("Creating order with args: {}" + args);

      // Call the function with this MarketOrder as the listener for the response
      fn.call(args, order);
      
      return order;
    } catch (MkvException e) {
      logger.error("Error creating order: {} " + e.getMessage());
    }
    return null;
  }


  /**
   * Updates an existing order in the market
   *
   * @param TraderId The trader ID for the order
   * @param instrId The instrument ID for the order
   * @param verb The order direction ("Buy" or "Sell")
   * @param qty The quantity of the order
   * @param price The price of the order
   * @param type The order type (e.g., "Limit", "Market")
   * @param tif Time in force (e.g., "Day", "IOC", "FOK", "FAS", "FAK")
   * @return A new MarketOrder object or null if creation failed
   * Syntax	<source>_VCMIOrderRwt181 (<User>, <OrderId>, <Verb>, <Price>, <QtyShown>, <QtyTot>, <IsSoft>, <Attribute>, <StopCond>, <StopId>, <StopPrice>, ... )
   */

  public static MarketOrder orderUpdate(String TraderId, String orderId, String instrId, String verb,
      double qty, double price, String tif) {

    // Increment the request ID to ensure uniqueness
    reqId.incrementAndGet();

    // Log the order update attempt
    logger.info("Creating order update request #" + reqId + " for " + verb + " " + instrId + " @ " + qty + ", " + price);


    // Get the publish manager to access functions
    MkvPublishManager pm = Mkv.getInstance().getPublishManager();
    
    // Get the order add function from the ION gateway, hardcoding VMO repo
    MkvFunction fn = pm.getMkvFunction("VMO_VCMIOrderRwt181");
    
    if (fn == null) {
      logger.info("Failed to get VCMIOrderRwt181 function from gateway");
      return null;
    }
    
    try {
      // The free text will contain the application ID to identify orders from this component
      String freeText = "ORDER_ID FROM APP";

      // Create a new MarketOrder object that will track this order
      MarketOrder order = new MarketOrder(reqId.get(), instrId, verb, qty, price, tif);

      // Create the function arguments for the order creation
      // This is specific to the VCMIOrderRwt181 function interface
      MkvSupply args = MkvSupplyFactory.create(new Object[] {
        TraderId,                                 // User                  
        orderId,                                        // OrderId
        verb,                                           // Verb (Buy/Sell)                 
        Double.valueOf(price),                          // Price                 
        Double.valueOf(qty),                            // QtyShown (visible quantity)             
        Double.valueOf(qty),                            // QtyTot (total quantity)               
        Integer.valueOf(0),                             // IsSoft                
        Integer.valueOf(0),                             // Attribute             
        Integer.valueOf(0),                                 // StopCond              
        "",                                             // StopId                
        Double.valueOf(0)                               // StopPrice             
      });

      logger.info("Updating order with args: " + args);

      // Call the function with this MarketOrder as the listener for the response
      fn.call(args, order);
      
      return order;
    } catch (MkvException e) {
      logger.error("Error creating order: " + e.getMessage());
    }
    return null;
  }

  /**
   * Cancels an existing order in the market
   * 
   * @param marketSource The source market (e.g., "DEALERWEB_REPO")
   * @param traderId The trader ID for the order
   * @param orderId The ID of the order to cancel
   * @param orderManager The order manager that will be notified of order events
   * @return A MarketOrder object representing the cancel request or null if cancellation failed
   */
  public static MarketOrder orderCancel(String traderId, String orderId) {
      
      // Increment the request ID to ensure uniqueness for tracking this cancel request
      reqId.incrementAndGet();

      // Log the order cancellation attempt
      logger.info("Cancelling order: reqId=" + reqId + ", orderId=" + orderId + ", trader=" + traderId);

      // Get the publish manager to access functions
      MkvPublishManager pm = Mkv.getInstance().getPublishManager();
      
      // Get the order delete function from the ION gateway
      MkvFunction fn = pm.getMkvFunction("VMO_VCMIOrderDel");
      
      if (fn == null) {
          logger.error("Failed to get VCMIOrderDel function from gateway for VMO");
          return null;
      }
      
      try {
          // The free text will contain the application ID to identify cancel requests from this component
          String freeText = "REDIS_ORDER CANCEL REQUEST FROM APP";
          
          // Create a new MarketOrder object that will track this cancel request
          // Using special values for a cancel request
          MarketOrder order = new MarketOrder(reqId.get(), orderId, "Cancel", 0, 0, "CANCEL");

          // Create the function arguments for the order cancellation
          // This is specific to the VCMIOrderDel function interface
          MkvSupply args = MkvSupplyFactory.create(new Object[] {
              traderId,    // User                  
              orderId,     // OrderID
              "" + reqId, freeText  // FreeText with request ID and app ID
          });

          logger.info("Cancelling order with args: " + args);

          
          // Call the function with this MarketOrder as the listener for the response
          fn.call(args, order);
          
          return order;
      } catch (MkvException e) {
          logger.error("Error cancelling order: " + e.getMessage() + " " + e);
      }
      return null;
  }

  /**
   * The instrument ID for this order
   */
  private final String instrId;

  /**
   * The order ID assigned by the market after creation
   */
  private String orderId;

  /**
   * The order verb (Buy/Sell)
   */
  private final String verb;

  /**
   * The quantity filled for this order
   */
  private double qtyFilled;

  private boolean orderDead = false;

  /**
   * The price for this order
   */
  private double price;

  /**
   * The request ID assigned during creation
   */
  private final int myReqId;

  /**
   * Error code if order creation failed
   */
  private byte errCode = (byte) 0;

  /**
   * Error description if order creation failed
   */
  private String errStr = "";

  /** 
   * Creates a new instance of MarketOrder
   * Private constructor - instances should be created via orderCreate() or orderCancel()
   */
  private MarketOrder(int _reqId, String instrId, String verb, double qty,
      double price, String tif) {
      this.myReqId = _reqId;
      this.verb = verb;
      this.instrId = instrId;
      this.creationTimestamp = System.currentTimeMillis();

      if ("Cancel".equals(verb)) {
          logger.info("Cancel request created: reqId=" + _reqId + ", instrId=" + instrId);
      } else if ("UPDATE".equals(tif)) {
          logger.info("Update request created: reqId=" + _reqId + ", instrId=" + instrId);
      } else {
          logger.info("MarketOrder created: reqId=" + _reqId + ", instrId=" + instrId + ", verb=" + verb);
      }
  }

  /**
   * Called when the function call to create an order fails.
   * The order manager is notified that the order is "dead".
   */
  public void onError(MkvFunctionCallEvent mkvFunctionCallEvent,
      byte errCode, String errStr) {
    logger.error("Error creating order: reqId=" + myReqId + ", errCode=" + errCode + ", errStr=" + errStr);
    
    // Add more detailed logging
    if (mkvFunctionCallEvent != null) {
      logger.error("Order creation failed: reqId=" + myReqId + ", errCode=" + errCode + ", errStr=" + errStr);
    }

    // Save the error information
    this.errCode = errCode;
    this.errStr = errStr;
}

public void onResult(MkvFunctionCallEvent mkvFunctionCallEvent, MkvSupply mkvSupply) {
    try {
        if (mkvSupply == null) {
            logger.warn("onResult received null supply for reqId=" + myReqId);
            return;
        }
        
        // Get the full string representation of the supply
        String resultString = mkvSupply.getString(mkvSupply.firstIndex());
        logger.debug("Raw order result for reqId=" + myReqId + ": " + resultString);

        // Parse the string to extract the order ID
        // The format is typically: "0:OK -Result {-Id {5697620189428842553_20250603} -OrderTmpId {...} }"
        String orderId = null;
        int idIndex = resultString.indexOf("-Id {");
        
        if (idIndex >= 0) {
            // Extract the string between "-Id {" and the next "}"
            int startIndex = idIndex + 5; // Length of "-Id {"
            int endIndex = resultString.indexOf("}", startIndex);
            
            if (endIndex > startIndex) {
                orderId = resultString.substring(startIndex, endIndex).trim();
                
                // Set the order ID in this object
                setOrderId(orderId);
                
                // Log the successful order creation
                logger.info("Order successfully created: reqId=" + myReqId + ", orderId=" + orderId);
                
            } else {
                logger.warn("Could not parse order ID - invalid format: reqId=" + myReqId);
            }
        } else {
            logger.warn("Could not find -Id in result string: reqId=" + myReqId + ", resultString=" + resultString);
        }
    } catch (Exception e) {
        logger.error("Error processing order creation result: reqId=" + myReqId + ", error=" + e.getMessage() + ", exception=" + e);
    }
  }


  /**
   * Gets the timestamp when this order was created.
   * 
   * @return The creation timestamp in milliseconds since epoch
   */
  public long getCreationTimestamp() {
      return creationTimestamp;
  }

  /**
   * @return The instrument ID for this order
   */
  public String getInstrId() {
    return instrId;
  }
  
  /**
 * @return The quantity filled for this order
 */
public double getQtyFilled() {
    return qtyFilled;
}

  /**
 * @return The quantity filled for this order
 */
public void setQtyFilled(double qtyFilled) {
    this.qtyFilled = qtyFilled;
}

/**
 * Get the request ID for this order
 * 
 * @return The request ID
 */
public int getMyReqId() {
    return myReqId;
}

/**
 * @return The price for this order
 */
public double getPrice() {
    return price;
}

/**
 * @return True if the order is dead (cancelled, filled, rejected, etc.)
 */
public boolean isDead() {
    return orderDead;
}

/**
 * @return The verb (Buy/Sell) for this order
 */
public String getVerb() {
    return verb;
}

/**
 * @return The error code if order creation failed
 */
  public byte getErrCode() {
    return errCode;
  }

  /**
   * @return The error description if order creation failed
   */
  public String getErrStr() {
    return errStr;
  }

  /**
   * @return The order ID assigned by the market
   */
  public String getOrderId() {
    return orderId;
  }

  /**
   * Sets the order ID assigned by the market
   * This is now accessible to support cancel operations
   */
  public void setOrderId(String oid) {
    logger.info("Setting order ID: reqId=" + myReqId + ", orderId=" + oid);
    orderId = oid;
  }

  public static long getOrderTtlMs() {
        // Fixed value of 5 minutes (300,000 ms) for order TTL
        return 300000;
    }
  
  // /**
  //  * Checks if this order has been in the market for longer than the time-to-live
  //  * and should be automatically cancelled.
  //  * 
  //  * @return true if the order is expired and should be cancelled, false otherwise
  //  */
  // public boolean isExpired() {
  //     // Don't apply expiration to cancel requests
  //     if ("Cancel".equals(verb)) {
  //         return false;
  //     }
      
  //     long currentTime = System.currentTimeMillis();
  //     long orderAge = currentTime - creationTimestamp;
      
  //     long currentTtl = getOrderTtlMs();
      
  //     // Check if the order age exceeds the TTL
  //     boolean expired = orderAge > currentTtl;

  //     if (expired && orderId == null) {
  //         // If the order is expired but has no ID, it means it was never successfully created
  //         logger.info("Order never created: reqId=" + myReqId + ", age=" + (orderAge / 1000) + " seconds");

  //         if (orderCallback != null) {
  //           orderCallback.removeOrder(myReqId);
  //         }
  //     } else if (expired && orderId != null) {
  //         // If the order is expired, remove it
  //         logger.info("Order is expired and will be cancelled: reqId=" + myReqId + ", orderId=" + orderId);
          
  //         if (orderCallback != null) {
  //           orderCallback.orderDead(this);
  //         }
  //     }

  //     if (expired) {
  //         logger.info("Order expired: reqId=" + myReqId + ", orderId=" + orderId + ", age=" + (orderAge / 1000) + " seconds");
  //     }

  //     return expired;
  // }

  /**
   * Gets the age of this order in milliseconds.
   * 
   * @return the age of the order in milliseconds
   */
  public long getAgeMillis() {
      return System.currentTimeMillis() - creationTimestamp;
  }
  
  public long getStandardizedTimestamp() {
      return standardizedTimestamp;
  }

  public void setStandardizedTimestamp(long timestamp) {
      this.standardizedTimestamp = timestamp;
  }

  public String getStandardizedTimestampStr() {
      return standardizedTimestampStr;
  }

  public void setStandardizedTimestampStr(String timestampStr) {
      this.standardizedTimestampStr = timestampStr;
  }

  /**
   * Sets the active status of this order based on MKV updates
   * 
   * @param active Whether the order is active
   */
  public void setActive(boolean active) {
      this.orderDead = !active;
      logger.debug("Order " + (orderId != null ? orderId : myReqId) + " active status set to " + active);
    }

  /**
   * Gets the active status of this order
   * 
   * @return true if the order is active, false otherwise
   */
  public boolean isActive() {
      return !orderDead;
  }

  /**
   * Not interested in partial updates for the order
   */
  public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
      boolean param) {
    // Not interested in partial updates
    logger.info("Received partial update for order: reqId=" + myReqId);
  }

  /**
   * Process full updates for the order record.
   * This is where we track the order's lifecycle and detect when it's "dead".
   */
  public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
      boolean param) {
    try {
      
      // Get and store the order ID from the market
      setOrderId(mkvRecord.getValue("OrigId").toString());

      logger.info("Order ID updated: reqId=" + myReqId + ", orderId=" + getOrderId());

      // If the order is closed, notify the order manager
    } catch (Exception e) {
      logger.error("Error processing order update: " + e.getMessage());
    }
  }
}