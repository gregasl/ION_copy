/*
 * IOrderManager
 * 
 * The order manager is supposed to be notified when an order is dead, when a best
 * update is received and allow putting order on the market.
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

public interface IOrderManager {

	/**
	 * Notify an order is "dead" (cannot generate further trades).
	 */
	public void orderDead(MarketOrder order);

	/**
	 * Put an order on the market
	 */
	public MarketOrder addOrder(String instrId, String verb, double qty,
			double price, String type, String tif);

	/**
	 * Notify a best update
	 */
	public void best(Best best);

	/**
	 * Return an identifier supposed to be unique among the clients that can put
	 * order on the market. It's used to ditinguish orders sent by the component
	 * from orders sent by other component.
	 */
	public String getApplicationId();
}
