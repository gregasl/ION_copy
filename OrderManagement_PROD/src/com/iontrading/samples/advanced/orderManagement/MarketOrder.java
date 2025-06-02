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

package com.iontrading.samples.advanced.orderManagement;

import java.util.logging.Logger;
import java.util.logging.Level;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvFunction;
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
 * 3. Communication with the IOrderManager when the order's state changes
 * 
 * Implements both MkvFunctionCallListener (to receive the response to order creation)
 * and MkvRecordListener (to receive ongoing updates about the order's state)
 */
public class MarketOrder implements MkvFunctionCallListener, MkvRecordListener {
  
  private static String marketSource;
	
  /**
   * Timestamp when the order was created (in milliseconds since epoch)
   */
  private final long creationTimestamp;

  // Add logger for debugging
  private static final Logger LOGGER = Logger.getLogger(MarketOrder.class.getName());
  
  /**
   * An id that should be unique during the life of the component.
   * This is a simple counter implementation - in a production system,
   * the generation of unique identifiers would likely be more sophisticated.
   */
  private static int reqId = 0;

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
   * @param orderManager The order manager that will be notified of order events
   * @return A new MarketOrder object or null if creation failed
   */
  		  
  public static MarketOrder orderCreate(String MarketSource, String TraderId, String instrId, String verb,
      double qty, double price, String type, String tif,
      IOrderManager orderManager) {
    
    // Increment the request ID to ensure uniqueness
    reqId++;
    
    // Log the order creation attempt
    if (LOGGER.isLoggable(Level.INFO)) {
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Creating order request #" + reqId + " for " + verb + " " + instrId + " " + qty + " @ " + price);
    }

    // Get the publish manager to access functions
    MkvPublishManager pm = Mkv.getInstance().getPublishManager();
    
    marketSource = MarketSource;
    
    // Get the order add function from the ION gateway
    MkvFunction fn = pm.getMkvFunction(MarketSource + "_VCMIOrderAdd181");
    
    if (fn == null) {
      ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Failed to get VCMIOrderAdd181 function from gateway");
      return null;
    }
    
    try {
      // The free text will contain the application ID to identify orders from this component
      String freeText = orderManager.getApplicationId();
      
      // Create a new MarketOrder object that will track this order
      MarketOrder order = new MarketOrder(reqId, instrId, verb, qty, price, tif, orderManager);
     
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
        "",                                             // CustomerInfo          
        MarketDef.getFreeText("" + reqId, freeText),    // FreeText (contains reqId and appId)             
        Integer.valueOf(0),                                 // StopCond              
        "",                                             // StopId                
        Double.valueOf(0)                                   // StopPrice             
      });
        
      if (LOGGER.isLoggable(Level.INFO)) {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Creating order with args: " + args);
      }
      // Call the function with this MarketOrder as the listener for the response
      fn.call(args, order);
      
      return order;
    } catch (MkvException e) {
      ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error creating order: " + e.getMessage());
      e.printStackTrace();
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
  public static MarketOrder orderCancel(String marketSource, String traderId, String orderId,
          IOrderManager orderManager) {
      
      // Increment the request ID to ensure uniqueness for tracking this cancel request
      reqId++;
      
      // Log the order cancellation attempt
      if (LOGGER.isLoggable(Level.INFO)) {
          ApplicationLogging.logAsync(LOGGER, Level.INFO, "Cancelling order: reqId=" + reqId + 
                  ", orderId=" + orderId + ", trader=" + traderId);
      }

      // Get the publish manager to access functions
      MkvPublishManager pm = Mkv.getInstance().getPublishManager();
      
      // Get the order delete function from the ION gateway
      MkvFunction fn = pm.getMkvFunction(marketSource + "_VCMIOrderDel");
      
      if (fn == null) {
          ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Failed to get VCMIOrderDel function from gateway for " + marketSource);
          return null;
      }
      
      try {
          // The free text will contain the application ID to identify cancel requests from this component
          String freeText = orderManager.getApplicationId();
          
          // Create a new MarketOrder object that will track this cancel request
          // Using special values for a cancel request
          MarketOrder order = new MarketOrder(reqId, orderId, "Cancel", 0, 0, "CANCEL", orderManager);
          order.setOrderId(orderId);
          
          // Create the function arguments for the order cancellation
          // This is specific to the VCMIOrderDel function interface
          MkvSupply args = MkvSupplyFactory.create(new Object[] {
              traderId,    // User                  
              orderId,     // OrderID
              MarketDef.getFreeText("" + reqId, freeText)  // FreeText with request ID and app ID
          });
              
          if (LOGGER.isLoggable(Level.INFO)) {
              ApplicationLogging.logAsync(LOGGER, Level.INFO, "Cancelling order with args: " + args);
          }
          
          // Call the function with this MarketOrder as the listener for the response
          fn.call(args, order);
          
          // Log to machine-readable format
          ApplicationLogging.logOrderUpdate(
              "CANCEL_SENT", 
              order.getMyReqId(),
              orderId,
              "Cancel request sent"
          );
          
          return order;
      } catch (MkvException e) {
          ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error cancelling order: " + e.getMessage(), e);
      }
      return null;
  }
  
  /**
   * Order manager that will be notified of order lifecycle events
   */
  private final IOrderManager orderCallback;

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
      double price, String tif, IOrderManager callback) {
      this.myReqId = _reqId;
      this.verb = verb;
      this.orderCallback = callback;
      this.instrId = instrId;
      this.creationTimestamp = System.currentTimeMillis();
      if (LOGGER.isLoggable(Level.INFO)) {
          if ("Cancel".equals(verb)) {
              ApplicationLogging.logAsync(LOGGER, Level.INFO, "Cancel request created: reqId=" + _reqId + ", instrId=" + instrId);
          } else {
              ApplicationLogging.logAsync(LOGGER, Level.INFO, "MarketOrder created: reqId=" + _reqId + ", instrId=" + instrId + ", verb=" + verb);
          }
      }
  }

  /**
   * Called when the function call to create an order fails.
   * The order manager is notified that the order is "dead".
   */
  public void onError(MkvFunctionCallEvent mkvFunctionCallEvent,
      byte errCode, String errStr) {
    ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error creating order: reqId=" + myReqId + ", errCode=" + errCode + ", errStr=" + errStr);
    System.out.println("_VCMIOrderAdd181 Failure {" + myReqId + "} {"
        + errCode + "} {" + errStr + "}");
    
    // Add more detailed logging
    if (mkvFunctionCallEvent != null) {
    ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Order creation failed: reqId=" + myReqId + 
                  ", errCode=" + errCode + ", errStr=" + errStr);
    }
    
    // Save the error information
    this.errCode = errCode;
    this.errStr = errStr;
    
    // Notify the order manager that this order is dead
    orderCallback.orderDead(this);
  }

  /**
   * Called when the function call to create an order succeeds.
   * The order is now in the market and we'll start receiving updates.
   */
  /**
   * Called when the function call to create an order succeeds.
   * The order is now in the market and we'll start receiving updates.
   */
  public void onResult(MkvFunctionCallEvent mkvFunctionCallEvent, MkvSupply mkvSupply) {
	    try {
	        // Check if we have a response
	        if (mkvSupply != null) {
	            // Get the first field index
	            int firstIndex = mkvSupply.firstIndex();

	            if (firstIndex >= 0 && mkvSupply.isSet(firstIndex)) {
	                // The first field should be the order ID
	                String responseOrderId = mkvSupply.getString(firstIndex);

	                if (responseOrderId != null && !responseOrderId.isEmpty()) {
	                    // Extract just the ID portion using regex
	                    String extractedId = extractIdFromResponse(responseOrderId);
	                    
	                    // Set the extracted order ID
	                    setOrderId(extractedId);

	                    ApplicationLogging.logAsync(LOGGER, Level.INFO,
	                        "Order creation succeeded: reqId=" + myReqId +
	                        ", orderId=" + extractedId);

	                    // Log to machine-readable format if needed
	                    ApplicationLogging.logOrderUpdate(
	                        "ORDER_ID_ASSIGNED",
	                        myReqId,
	                        extractedId,
	                        "Order ID assigned by market"
	                    );
	                } else {
	                    ApplicationLogging.logAsync(LOGGER, Level.WARNING,
	                        "Order creation succeeded but empty order ID received: reqId=" + myReqId);
	                }
	            } else {
	                ApplicationLogging.logAsync(LOGGER, Level.WARNING,
	                    "Order creation succeeded but no order ID field in response: reqId=" + myReqId);
	            }
	        } else {
	            ApplicationLogging.logAsync(LOGGER, Level.WARNING,
	                "Order creation succeeded but received null response: reqId=" + myReqId);
	        }
	    } catch (Exception e) {
	        ApplicationLogging.logAsync(LOGGER, Level.SEVERE,
	            "Error extracting order ID from response: reqId=" + myReqId +
	            ", error=" + e.getMessage(), e);
	    }
	}

	/**
	 * Extracts the ID portion from the response string
	 * Example input: "0:OK -Result {-Id {4214177636_20250515}  -OrderTmpId {MKV_USP_91282CMV0_C_Par_0_1747306800044}  }"
	 * Example output: "4214177636_20250515"
	 */
	private String extractIdFromResponse(String responseString) {
	    // Look for pattern: -Id {XXXX}
	    int idStart = responseString.indexOf("-Id {");
	    if (idStart >= 0) {
	        idStart += 5; // Move past "-Id {"
	        int idEnd = responseString.indexOf("}", idStart);
	        if (idEnd > idStart) {
	            return responseString.substring(idStart, idEnd).trim();
	        }
	    }
	    
	    // If we couldn't extract the ID, return the original string
	    return responseString;
	}

  /**
   * @return The instrument ID for this order
   */
  public String getInstrId() {
    return instrId;
  }

  /**
   * @return The marketSource for this order
   */
  public String getMarketSource() {
    return marketSource;
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
   * @return The request ID for this order
   */
  public int getMyReqId() {
    return myReqId;
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
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Setting order ID: reqId=" + myReqId + ", orderId=" + oid);
      orderId = oid;
  }
  
  public static long getOrderTtlMs() {
	    return OrderManagement.getOrderTtlForCurrentTime();
	}
  
  /**
   * Checks if this order has been in the market for longer than the time-to-live
   * and should be automatically cancelled.
   * 
   * @return true if the order is expired and should be cancelled, false otherwise
   */
  public boolean isExpired() {
      // Don't apply expiration to cancel requests
      if ("Cancel".equals(verb)) {
          return false;
      }
      
      long currentTime = System.currentTimeMillis();
      long orderAge = currentTime - creationTimestamp;
      
      long currentTtl = getOrderTtlMs();
      
      // Check if the order age exceeds the TTL
      boolean expired = orderAge > currentTtl;
      
      if (expired && LOGGER.isLoggable(Level.INFO)) {
          ApplicationLogging.logAsync(LOGGER, Level.INFO, 
              "Order expired: reqId=" + myReqId + 
              ", orderId=" + orderId + 
              ", age=" + (orderAge / 1000) + " seconds");
      }
      
      return expired;
  }

  /**
   * Gets the age of this order in milliseconds.
   * 
   * @return the age of the order in milliseconds
   */
  public long getAgeMillis() {
      return System.currentTimeMillis() - creationTimestamp;
  }
  
  /**
   * Not interested in partial updates for the order
   */
  public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
      boolean param) {
    // Not interested in partial updates
    ApplicationLogging.logAsync(LOGGER, Level.INFO, "Received partial update for order: reqId=" + myReqId);
  }

  /**
   * Process full updates for the order record.
   * This is where we track the order's lifecycle and detect when it's "dead".
   */
  public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
      boolean param) {
    try {
      // Check if the order is still active
      int active = mkvRecord.getValue("Active").getInt();
      boolean closed = (active == 0);
      
      // Get and store the order ID from the market
      setOrderId(mkvRecord.getValue("Id").toString());
      
      ApplicationLogging.logAsync(LOGGER, Level.INFO, "Received full update for order: reqId=" + myReqId + 
                  ", orderId=" + getOrderId() + ", active=" + active);

      // If the order is closed, notify the order manager
      if (closed && orderCallback != null) {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Order is now dead: reqId=" + myReqId + 
                      ", orderId=" + getOrderId());
        orderCallback.orderDead(this);
      }
    } catch (Exception e) {
      ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error processing order update: " + e.getMessage());
      e.printStackTrace();
    }
  }
}