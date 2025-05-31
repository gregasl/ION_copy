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

package com.iontrading.samples.advanced.stp;

public class MarketDef {

	/**
	 * The source of the gateway the component should place orders.
	 */
	public static String CM_SOURCE = "MOCK";

	/**
	 * Helper to ensure names will be correctly formatted following the ION
	 * convention.
	 */
	public static String getName(String instrument, String id) {
		return "EUR." + instrument + "." + CM_SOURCE + "." + id;
	}

	/**
	 * Helper to ensure names will be correctly formatted following the ION
	 * convention.
	 */
	public static String getTypeName(String typeName) {
		return CM_SOURCE + "_" + typeName;
	}

}
