/*
 * OrderManagement
 *
 * OrderManagement object starts the component, register the relevant listeners
 * then subscribes to the depths of the configured instruments and to the orders
 * chain.
 * The set up of the subscription is done in onPublishIdle in order to not perform
 * expensive calculations in the onPublish method.
 *
 * Requirements:
 * A CM compliant market gateway must be set up and the constants in the MarketDef
 * class have to be adjusted to use gateways publications.
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvPersistentRecordSet;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSubscribeManager;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvChainAction;
import com.iontrading.mkv.events.MkvChainListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

public class OrderManagement implements MkvPublishListener, MkvRecordListener,
		MkvChainListener, IOrderManager {

	public static void main(String[] args) {
		OrderManagement om = new OrderManagement();
		MkvQoS qos = new MkvQoS();
		qos.setArgs(args);
		qos.setPublishListeners(new MkvPublishListener[] { om });
		try {
			// starts the API
			Mkv.start(qos);

			// set up permanent subscription to the depths and to
			// the order chain
			om.subscribeToDepths();
		} catch (MkvException e) {
			e.printStackTrace();
		}
	}

	private final Map orders = new HashMap();

	private final String applicationId;

	private boolean FASorderSent = false;

	/** Creates a new instance of OrderManagement */
	public OrderManagement() {
		applicationId = Integer
				.toHexString((int) System.currentTimeMillis() % 1000);
	}

	public String getApplicationId() {
		return applicationId;
	}

	public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
		// do nothing
	}

	/**
	 * This event is fired at the end of a publications download. The component
	 * performs all the actions needed to deals with depths and orders.
	 */
	public void onPublishIdle(String component, boolean start) {
		MkvPublishManager man = Mkv.getInstance().getPublishManager();
		MkvChain chain = man.getMkvChain(MarketDef.getName("CM_ORDER",
				MarketDef.USER));
		if (chain != null && !chain.isSubscribed(this)) {
			try {
				chain.subscribe(this);
			} catch (Exception e) {
				// handle the exception
				e.printStackTrace();
			}
		}
		sendFASOrder();
	}

	/**
	 * Send a FAS order to the market
	 */
	private void sendFASOrder() {
		if (MarketDef.SEND_FILL_AND_STORE && !FASorderSent) {
			addOrder(MarketDef.INSTR_IDS[0], "Sell", 50.0, 100.0, "Limit",
					"FAS");
		}
	}

	/**
	 * Subscribe to all the depth records the component is interested in.
	 */
	public void subscribeToDepths() {
		MkvSubscribeManager mkvSubscribe = Mkv.getInstance()
				.getSubscribeManager();
		for (int i = 0; i < MarketDef.INSTR_IDS.length; i++) {
			DepthListener depthListener = new DepthListener(
					MarketDef.INSTR_IDS[i], this);
			MkvPersistentRecordSet rset = mkvSubscribe.persistentSubscribe(
					depthListener, MarketDef.DEPTH_FIELDS);
			rset.add(MarketDef.getName("CM_DEPTH", MarketDef.INSTR_IDS[i]));
		}
	}

	/**
	 * Not interested in this event because our component is a pure subscriber
	 * and is not supposed receiving request for subscriptions.
	 */
	public void onSubscribe(MkvObject mkvObject) {
	}

	/**
	 * Cache a MarketOrder record using the request id as key.
	 */
	private void addOrder(MarketOrder order) {
		orders.put(new Integer(order.getMyReqId()), order);
	}

	/**
	 * Remove a MarketOrder object from the cache by key.
	 */
	public void removeOrder(int reqId) {
		orders.remove(new Integer(reqId));
	}

	/**
	 * Retrieve a MarketOrder object from the cache by key.
	 */
	public MarketOrder getOrder(int reqId) {
		return (MarketOrder) orders.get(new Integer(reqId));
	}

	/**
	 * The component doesn't need to listen to partial updates
	 */
	public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
			boolean isSnap) {
		// don't care
	}

	/**
	 * Get the updates for the CM_ORDER records. If the received update is
	 * related to an order that has a cached MarketOrder object counterpart the
	 * supply event is forwarded.
	 */
	public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
			boolean isSnap) {
		String userData = mkvRecord.getValue("UserData").getString();
		String freeText = mkvRecord.getValue("FreeText").getString();
		if ("".equals(userData) || !freeText.equals(getApplicationId())) {
			// the order is not one the orders put by the component.
			return;
		} else {
			try {
				int reqId = Integer.parseInt(userData);
				MarketOrder order = getOrder(reqId);
				if (order != null) {
					order.onFullUpdate(mkvRecord, mkvSupply, isSnap);
				}
			} catch (Exception e) {
				// handle the exception
			}
		}
	}

	/**
	 * Notify an order is no longer able to trade. At this point the MarketOrder
	 * is removed from the cache.
	 */
	public void orderDead(MarketOrder order) {
		System.out.println("Order {" + order.getOrderId() + "} is dead.");
		removeOrder(order.getMyReqId());
		// sendFASOrder();
	}

	/**
	 * Add an order for the given instrument, quantity and so on. If the call
	 * for adding the order succeed a new marketOrder object is stored in the
	 * internal cache.
	 */
	public MarketOrder addOrder(String instrId, String verb, double qty,
			double price, String type, String tif) {

		MarketOrder order = MarketOrder.orderCreate(instrId, verb, qty, price,
				type, tif, this);
		if (order != null) {
			addOrder(order);
			FASorderSent = true;
		}

		return order;
	}

	/**
	 * Notify an update of the best. On the base of the configuration in
	 * MarketDef the component can decide to aggress the market
	 */
	public void best(Best best) {

		if (MarketDef.AGGRESS) {
			if (best.getAsk() <= 100 && best.getAsk() > 0) {
				addOrder(best.getInstrumentId(), "Buy", best.getAskSize(), best
						.getAsk(), "Limit", "FAK");
			}
		}
	}

	public void onSupply(MkvChain chain, String record, int pos,
			MkvChainAction action) {
		switch (action.intValue()) {
		case MkvChainAction.INSERT_code:
		case MkvChainAction.APPEND_code:
			subscribeToRecord(record);
			break;
		case MkvChainAction.SET_code:
			for (Iterator iter = chain.iterator(); iter.hasNext();) {
				String recName = (String) iter.next();
				subscribeToRecord(recName);
			}
			break;
		case MkvChainAction.DELETE_code:
			// Do nothing. Order chain is ordered, so probably this DELETE
			// event will be followed by an INSERT to chainge the position
			// of the record within the chain.
			break;
		}
	}

	private void subscribeToRecord(String record) {
		try {
			MkvRecord rec = Mkv.getInstance().getPublishManager().getMkvRecord(
					record);
			rec.subscribe(MarketDef.ORDER_FIELDS, this);
		} catch (Exception e) {
			// do something
		}
	}
}
