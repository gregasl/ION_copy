package com.iontrading.automatedMarketMaking;

import java.util.Map;

/**
 * Interface for listening to bond eligibility changes
 */
public interface EligibilityChangeListener {
    /**
     * Called when a bond's eligibility for market making changes
     * 
     * @param cusip The bond identifier (CUSIP)
     * @param isEligible Whether the bond is now eligible
     * @param bondData Additional bond data
     */
    void onEligibilityChange(String cusip, boolean isEligible, Map<String, Object> bondData);
}