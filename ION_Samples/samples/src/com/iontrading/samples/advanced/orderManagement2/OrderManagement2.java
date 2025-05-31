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

package com.iontrading.samples.advanced.orderManagement2;

import java.util.HashMap;
import java.util.Map;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;
import com.iontrading.samples.advanced.orderManagement.MarketDef;
import com.iontrading.samples.advanced.orderManagement.MarketOrder;
import com.iontrading.samples.advanced.orderManagement.OrderManagement;

public class OrderManagement2 extends OrderManagement {

	public static void main(String[] args) {
		OrderManagement om = new OrderManagement2();
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

	private Map outstandingOrdersXInstrument = new HashMap();

	/** Creates a new instance of OrderManagement */
	public OrderManagement2() {

	}

	public void orderDead(MarketOrder order) {
		super.orderDead(order);
		outstandingOrdersXInstrument.remove(order.getInstrId());
	}

	public MarketOrder addOrder(String instrId, String verb, double qty,
			double price, String type, String tif) {
		MarketOrder order = super
				.addOrder(instrId, verb, qty, price, type, tif);

		if (order != null) {
			outstandingOrdersXInstrument.put(instrId, order);
		}

		return order;
	}

	/**
	 * Notify an update of the best. On the base of the configuration in
	 * MarketDef the component can decide to aggress the market
	 */
	public void best(String instrId, double ask, double askSize, double bid,
			double bidSize) {

		if (MarketDef.AGGRESS) {
			if (outstandingOrdersXInstrument.containsKey(instrId)) {
				return;
			} else {
				if (ask <= 100 && ask > 0) {
					addOrder(instrId, "Buy", 100, 99, "Limit", "FAS");
				}
			}
		}
	}
}
