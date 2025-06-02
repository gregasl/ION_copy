/*
 * MarketDef
 *
 * Global definitions for the component.
 *
 */

package com.iontrading.samples.advanced.orderManagement;

/**
 * MarketDef contains all global configuration settings for the order management system.
 * This includes connectivity settings, instrument lists, user information, and fields
 * to subscribe to in the market data.
 */
public class MarketDef {

	/**
	 * Constructs the combined free text field for order submissions.
	 * The standard way to pass user data and free text to VCMIOrderAdd
	 * is to pack them in a single string using ASCII 0x01 as separator.
	 * 
	 * @param userData User-defined data (typically the request ID)
	 * @param freeText Free text information (typically the application ID)
	 * @return Properly formatted combined string with separators
	 */
	public static String getFreeText(String userData, String freeText) {
		String p = "\1";  // ASCII 0x01 separator
		return p + userData + p + freeText;
	}

	/**
	 * The standard order fields to subscribe to for CM_ORDER records.
	 * These fields provide essential information about order state and execution.
	 */
	public static String[] ORDER_FIELDS = { "Id", "QtyHit", "QtyTot", "Active",
			"Status", "Price", "FreeText", "UserData" };

	/**
	 * The standard depth fields to subscribe to for CM_DEPTH records.
	 * These provide the best bid/ask prices and sizes.
	 */
	public static String[] DEPTH_FIELDS = { "Id", "Ask0", "Ask1", "Ask2", "Ask3", "Ask4", "Ask5", "Ask6", "Ask7", "Ask8", "Ask9",
    		"AskSrc0", "AskSrc1", "AskSrc2", "AskSrc3", "AskSrc4", "AskSrc5", "AskSrc6", "AskSrc7", "AskSrc8", "AskSrc9", 
    		"AskAttribute0", "Bid0", "Bid1", "Bid2", "Bid3", "Bid4", "Bid5", "Bid6", "Bid7", "Bid8", "Bid9",
    		"BidSrc0", "BidSrc1", "BidSrc2", "BidSrc3", "BidSrc4", "BidSrc5", "BidSrc6", "BidSrc7", "BidSrc8", "BidSrc9", 
    		"BidAttribute0", "AskSize0", "BidSize0", "BidSize0_Min", "AskSize0_Min", "TrdValueLast" };
	
}