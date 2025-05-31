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

package com.iontrading.samples.advanced.cne;


public class MarketDef {

    /**
     * The owner of the orders
     */
    public static String USER = "ion";

    /**
     * The source of the gateway the component should place orders.
     */
    public static String CM_SOURCE = "MOCK";

    /**
     * Heartbeat frequency for RFQ locking
     */
    public static final int HEARTBEAT_FREQ = 500;

    /**
     * Price update frequency
     */
    public static final int PRICER_FREQ = 2000;

    /**
     * default "on the wire" time
     */
    public static final int ON_THE_WIRE_TIME = 15;

    /**
     * Helper to ensure names will be correctly formatted following the ION
     * convention.
     */
    public static String getName(String instrument, String id) {
        return "EUR." + instrument + "." + CM_SOURCE + "." + id;
    }

    /**
     * The standard way to pass the user data and the free text to the VCMIOrderAdd
     * is to pack them in a single string using the ascii character 0x01 as separator.
     */
    public static String getFreeText(String userData, String freeText) {
        String p = "\1";
        return p + userData + p + freeText;
    }

    /**
     * Standard CM_ORDER fields to be subscribed to.
     */
    public static final String[] RFQ_FIELDS = {
        "Id",

        "CNEReserved",
        "CNELockOwner",
        "TraderLockOwner",

        "CustFirm",
        "RFQNLegs",
        "RFQTypeStr",
        "DFDDealerActions",

        "InstrumentId",
        "MaturityDate",
        "RFQDateSettl",
        "RFQDaysSettl",
        "RFQVerb",
        "RFQCustValue",
        "RFQQty",
        "StatusStr",

        "RFQClosed",

        "ANTraderList"
    };
}
