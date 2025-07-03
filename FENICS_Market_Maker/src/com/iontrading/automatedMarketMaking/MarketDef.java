/*
 * MarketDef
 *
 * Global definitions for the component.
 *
 */

package com.iontrading.automatedMarketMaking;

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
	public static String[] ORDER_FIELDS = { "Active", "ActiveStr", "CompNameOrigin", "Id", "InstrumentId", "IntQtyGoal", "OrderNo", "OrigId", "OrigInstrId",
                        "OrigSrc", "Price", "QtyFill", "QtyHit", "QtyStatus",  "QtyStatusStr", "QtyTot", "Time", "TimeCreation","TimeStamp",
			"TradingStatus", "TradingStatusStr", "Verb", "VerbStr" };

        /**
         * The standard login fields to subscribe to for CM_LOGIN records.
         * These fields provide essential information about user login state.
         */
        public static String[] LOGIN_FIELDS = { "Id", "Src0", "Src1", "Src2", "Src3", "Src4", "Src5", "Src6", "Src7", "TStatusStr", "TStatusStr0", "TStatusStr1", 
                        "TStatusStr2", "TStatusStr3", "TStatusStr4", "TStatusStr5", "TStatusStr6", "TStatusStr7"};

	/**
	 * The standard depth fields to subscribe to for CM_DEPTH records.
	 * These provide the best bid/ask prices and sizes.
	 */
	public static String[] DEPTH_FIELDS = { "Id", "Ask0", "Ask1", "Ask2", "Ask3", "Ask4", "Ask5", "Ask6", "Ask7", "Ask8", "Ask9",
    		"AskSrc0", "AskSrc1", "AskSrc2", "AskSrc3", "AskSrc4", "AskSrc5", "AskSrc6", "AskSrc7", "AskSrc8", "AskSrc9", 
    		"Bid0", "Bid1", "Bid2", "Bid3", "Bid4", "Bid5", "Bid6", "Bid7", "Bid8", "Bid9",
    		"BidSrc0", "BidSrc1", "BidSrc2", "BidSrc3", "BidSrc4", "BidSrc5", "BidSrc6", "BidSrc7", "BidSrc8", "BidSrc9", 
    		"AskSize0", "BidSize0", "AskSize1", "BidSize1", "AskSize2", "BidSize2", "AskSize3", "BidSize3",
                "AskSize4", "BidSize4", "AskSize5", "BidSize5", "AskSize6", "BidSize6", "AskSize7", "BidSize7",
                "AskSize8", "BidSize8", "AskSize9", "BidSize9",
                "BidSize0_Min", "AskSize0_Min", "BidSize1_Min", "AskSize1_Min", "BidSize2_Min", "AskSize2_Min", "BidSize3_Min", "AskSize3_Min",
                "BidSize4_Min", "AskSize4_Min", "BidSize5_Min", "AskSize5_Min", "BidSize6_Min", "AskSize6_Min", "BidSize7_Min", "AskSize7_Min",
                "BidSize8_Min", "AskSize8_Min", "BidSize9_Min", "AskSize9_Min",
                "TrdValueLast", "Ask0Status", "Bid0Status", 
                "Ask1Status", "Bid1Status", "Ask2Status", "Bid2Status", "Ask3Status", "Bid3Status", "Ask4Status", "Bid4Status", 
                "Ask5Status", "Bid5Status", "Ask6Status", "Bid6Status", "Ask7Status", "Bid7Status", "Ask8Status", "Bid8Status", "Ask9Status", "Bid9Status", 
                "AskAttribute0", "BidAttribute0", "AskAttribute1", "BidAttribute1", "AskAttribute2", "BidAttribute2", "AskAttribute3", "BidAttribute3",
                "AskAttribute4", "BidAttribute4", "AskAttribute5", "BidAttribute5", "AskAttribute6", "BidAttribute6", "AskAttribute7", "BidAttribute7",
                "AskAttribute8", "BidAttribute8", "AskAttribute9", "BidAttribute9"
        };


    /**
     * The standard instrument fields to subscribe to for CM_INSTRUMENT records.
     * These provide mapping information for instrument IDs across different markets.
     */
    public static String[] INSTRUMENT_FIELDS = { 
            "Id", "IsAon",
            "Id0", "Src0", "Attribute0", "QtyMin0", "Id1", "Src1", "Attribute1", "QtyMin1",
            "Id2", "Src2", "Attribute2", "QtyMin2", "Id3", "Src3", "Attribute3", "QtyMin3",
            "Id4", "Src4", "Attribute4", "QtyMin4", "Id5", "Src5", "Attribute5", "QtyMin5",
            "Id6", "Src6", "Attribute6", "QtyMin6", "Id7", "Src7", "Attribute7", "QtyMin7",
            "Id8", "Src8", "Attribute8", "QtyMin8", "Id9", "Src9", "Attribute9", "QtyMin9",
            "Id10", "Src10", "Attribute10", "QtyMin10", "Id11", "Src11", "Attribute11", "QtyMin11",
            "Id12", "Src12", "Attribute12", "QtyMin12", "Id13", "Src13", "Attribute13", "QtyMin13",
            "Id14", "Src14", "Attribute14", "QtyMin14", "Id15", "Src15", "Attribute15", "QtyMin15"
    };
    
    /**
     * Pattern names for market data subscriptions
     */
    public static final String ORDER_PATTERN = "USD.CM_ORDER.VMO_REPO_US.";
    public static final String INSTRUMENT_PATTERN = "USD.CM_INSTRUMENT.VMO_REPO_US.";
    public static final String DEPTH_PATTERN = "USD.CM_DEPTH.VMO_REPO_US.";
    public static final String LOGIN_PATTERN = "USD.CM_LOGIN.VMO_REPO_US.";

    /**
     * Special instrument IDs for GC levels
     */
    public static final String GC_TU10_CASH = "USD.CM_DEPTH.VMO_REPO_US.GC_TU10_C_Fixed";
    public static final String GC_TU10_REG = "USD.CM_DEPTH.VMO_REPO_US.GC_TU10_REG_Fixed";
    
    /**
     * Venue identifiers
     */
    public static final String FENICS_USREPO = "FENICS_USREPO";
    public static final String DEALERWEB_REPO = "DEALERWEB_REPO";
    public static final String BTEC_REPO_US = "BTEC_REPO_US";
    public static final String BGC = "BGC";
}