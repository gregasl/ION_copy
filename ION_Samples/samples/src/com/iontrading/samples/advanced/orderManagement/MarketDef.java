/*
 * MarketDef
 *
 * Global definitions for the component.
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

public class MarketDef {

	/**
	 * Set to true if the component is supposed to aggress best prices.
	 */
	public static boolean AGGRESS = true;

	/**
	 * Set to true to place a FAS order at startup
	 */
	public static boolean SEND_FILL_AND_STORE = false;

	/**
	 * The owner of the orders
	 */
	public static String USER = "ion";

	/**
	 * The set of instruments the component is interested in.
	 */
	public static String[] INSTR_IDS = new String[] { "EBM_DE0001023810",
			"EBM_DE0001134971", "EBM_DE0001135002", "EBM_DE0001135028",
			"EBM_DE0001135036" };

	/**
	 * The source of the gateway the component should place orders.
	 */
	public static String CM_SOURCE = "MOCK_MTS";

	/**
	 * Helper to ensure names will be correctly formatted following the ION
	 * convention.
	 */
	public static String getName(String instrument, String id) {
		return "EUR." + instrument + "." + CM_SOURCE + "." + id;
	}

	/**
	 * The standard way to pass the user data and the free text to the
	 * VCMIOrderAdd is to pack them in a single string using the ascii character
	 * 0x01 as separator.
	 */
	public static String getFreeText(String userData, String freeText) {
		String p = "\1";
		return p + userData + p + freeText;
	}

	/**
	 * Standard CM_ORDER fields to be subscribed to.
	 */
	public static String[] ORDER_FIELDS = { "Id", "QtyHit", "QtyTot", "Active",
			"Status", "Price", "FreeText", "UserData" };

	/**
	 * Standard CM_DEPTH fields to be subscribed to.
	 */
	public static String[] DEPTH_FIELDS = { "Ask0", "Bid0", "AskSize0",
			"BidSize0" };
}
