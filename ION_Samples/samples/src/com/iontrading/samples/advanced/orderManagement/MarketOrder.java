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

public class MarketOrder implements MkvFunctionCallListener, MkvRecordListener {
  
  /**
	 * An id that should unique during the life of the component. In real time
	 * life the generation of unique identifiers is likely far more
	 * sophisticated.
	 */
	private static int reqId = 0;

	/**
	 * Call the VCMIOrderAdd181 function on the gateway and register a listener
	 * for the function call result/error. The method returns a MarketOrder
	 * object that implements both the listeners for the function call and for
	 * the order record updates.
	 */
	public static MarketOrder orderCreate(String instrId, String verb,
			double qty, double price, String type, String tif,
			IOrderManager orderManager) {
    
		reqId++;
		
		MkvPublishManager pm = Mkv.getInstance().getPublishManager();
		MkvFunction fn = pm.getMkvFunction(MarketDef.CM_SOURCE + "_VCMIOrderAdd181");
		
		if (fn==null) {
			return null;
		}
		
		try {
			String freeText = orderManager.getApplicationId();
			MarketOrder order = new MarketOrder(reqId, instrId, verb, qty, price, tif, orderManager);
			MkvSupply args = MkvSupplyFactory.create(new Object[] {
		      MarketDef.USER,                                 //User                  
		      instrId,                                        //InstrumentId          
		      verb,                                           //Verb                  
		      new Double(price),                              //Price                 
		      new Double(qty),                                //QtyShown              
		      new Double(qty),                                //QtyTot                
		      type,                                           //Type                  
		      tif,                                            //TimeInForce           
		      new Integer(0),                                 //IsSoft                
		      new Integer(0),                                 //Attribute             
		      "CUST_ORDER_MAN",                               //CustomerInfo          
		      MarketDef.getFreeText("" + reqId, freeText),    //FreeText              
		      new Integer(0),                                 //StopCond              
		      "",                                             //StopId                
		      new Double(0)                                   //StopPrice             
			});
		
			System.out.println("Adding order {" + reqId + "} " 
					+ "instrId {" + instrId + "} " 
					+ "verb {" + verb + "} " 
					+ "qty {" + qty + "} " 
					+ "price {" + price + "}");
		    
		    fn.call(args, order);
		    
		    return order;
		} catch (MkvException e) {
		    e.printStackTrace();
		}
		return null;
	}

  /**
	 * Order manager object. It's used to notify an order is "dead"
	 */
	private final IOrderManager orderCallback;

	private final String instrId;

	private String orderId;

	private final String verb;

	private final int myReqId;

	private byte errCode = (byte) 0;

	private String errStr = "";

	/** Creates a new instance of MarketOrder */
	private MarketOrder(int _reqId, String instrId, String verb, double qty,
			double price, String tif, IOrderManager callback) {
		this.myReqId = _reqId;
		this.verb = verb;
		this.orderCallback = callback;
		this.instrId = instrId;
	}

	/**
	 * The function call failed. The order manager is notified that the order is
	 * "dead".
	 */
	public void onError(MkvFunctionCallEvent mkvFunctionCallEvent,
			byte errCode, String errStr) {
		System.out.println("VCMIOrderAdd181 Failure {" + myReqId + "} {"
				+ errCode + "} {" + errStr + "}");
		orderCallback.orderDead(this);
	}

	public void onResult(MkvFunctionCallEvent mkvFunctionCallEvent,
			MkvSupply mkvSupply) {
		System.out.println("VCMIOrderAdd181 OK result {" + myReqId + "}");
	}

	public String getInstrId() {
		return instrId;
	}

	public String getVerb() {
		return verb;
	}

	public byte getErrCode() {
		return errCode;
	}

	public String getErrStr() {
		return errStr;
	}

	public int getMyReqId() {
		return myReqId;
	}

	public String getOrderId() {
		return orderId;
	}

	private void setOrderId(String oid) {
		orderId = oid;
	}

	public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
			boolean param) {
		// not interested
	}

	public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
			boolean param) {
		try {
			int active = mkvRecord.getValue("Active").getInt();
			boolean closed = (active == 0);
			setOrderId(mkvRecord.getValue("Id").toString());

			System.out.println("Got order update Id {" + getOrderId() + "} "
					+ "ReqId {" + getMyReqId() + "} " 
					+ "Verb {" + getVerb() + "} "
					+ "Price {" + mkvRecord.getValue("Price") + "} "
					+ "QtyHit {" + mkvRecord.getValue("QtyHit") + "} "
					+ "QtyTot {" + mkvRecord.getValue("QtyTot") + "} "
					+ "Active {" + active + "} " 
					+ "Closed {" + closed + "}");

			if (closed && orderCallback != null) {
				orderCallback.orderDead(this);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
