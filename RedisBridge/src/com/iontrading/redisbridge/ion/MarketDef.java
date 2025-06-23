package com.iontrading.redisbridge.ion;

/**
 * Defines market constants.
 * This class contains constants for market definitions.
 */
public class MarketDef {
    
    // Price flags
    public static final int PRICE_TRADEABLE = 1;
    public static final int PRICE_INDICATIVE = 2;
    public static final int PRICE_MARKET_MAKER = 4;
    public static final int PRICE_EXCHANGE = 8;
    public static final int PRICE_FIRM = 16;
    public static final int PRICE_EXECUTABLE = 32;
    
    // Order status
    public static final String ORDER_STATUS_NEW = "NEW";
    public static final String ORDER_STATUS_PARTIALLY_FILLED = "PARTIALLY_FILLED";
    public static final String ORDER_STATUS_FILLED = "FILLED";
    public static final String ORDER_STATUS_CANCELED = "CANCELED";
    public static final String ORDER_STATUS_REJECTED = "REJECTED";
    public static final String ORDER_STATUS_EXPIRED = "EXPIRED";
    
    // Order types
    public static final String ORDER_TYPE_MARKET = "MARKET";
    public static final String ORDER_TYPE_LIMIT = "LIMIT";
    public static final String ORDER_TYPE_STOP = "STOP";
    public static final String ORDER_TYPE_STOP_LIMIT = "STOP_LIMIT";
    
    // Time in force
    public static final String TIME_IN_FORCE_DAY = "DAY";
    public static final String TIME_IN_FORCE_GTC = "GTC";
    public static final String TIME_IN_FORCE_IOC = "IOC";
    public static final String TIME_IN_FORCE_FOK = "FOK";
    
    // Instrument types
    public static final String INSTRUMENT_TYPE_EQUITY = "EQUITY";
    public static final String INSTRUMENT_TYPE_FUTURE = "FUTURE";
    public static final String INSTRUMENT_TYPE_OPTION = "OPTION";
    public static final String INSTRUMENT_TYPE_BOND = "BOND";
    public static final String INSTRUMENT_TYPE_FX = "FX";
    
    // Venues
    public static final String VENUE_EXCHANGE = "EXCHANGE";
    public static final String VENUE_OTC = "OTC";
    public static final String VENUE_DARK_POOL = "DARK_POOL";
    
    // Currencies
    public static final String CURRENCY_USD = "USD";
    public static final String CURRENCY_EUR = "EUR";
    public static final String CURRENCY_GBP = "GBP";
    public static final String CURRENCY_JPY = "JPY";
    
    // Error codes
    public static final int ERROR_INVALID_REQUEST = 1000;
    public static final int ERROR_INVALID_INSTRUMENT = 1001;
    public static final int ERROR_INVALID_ORDER = 1002;
    public static final int ERROR_INVALID_TRADER = 1003;
    public static final int ERROR_SYSTEM_DISABLED = 1004;
    public static final int ERROR_INTERNAL_ERROR = 1005;
    
    /**
     * Private constructor to prevent instantiation.
     */
    private MarketDef() {
        // Do nothing
    }
}
