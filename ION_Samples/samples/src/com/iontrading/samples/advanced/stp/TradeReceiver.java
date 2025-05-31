/*
 * TradeChainSubscriber
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

import java.util.HashSet;
import java.util.Set;

public class TradeReceiver {

	/**
	 * This set contains the trades that can't be processed for any reason.
	 */
	private final Set unsentTrades = new HashSet();;

	public TradeReceiver() {
	}

	/**
	 * Called by external agents whenever a trade is date or a trade is updated.
	 * 
	 * @param trade
	 *            The new trade of the trade just updated.
	 */
	public void notifyTrade(Trade trade) {
		if (!sendTrade(trade)) {
			unsentTrades.add(trade);
			System.out.println("Cannot send trade : " + trade);
		} else {
			System.out.println("Sent trade : " + trade);
		}
	}

	/**
	 * Implement the logic to process a trade. Can be a new trade performed or
	 * an update of an existsing trade.
	 * 
	 * @param trade
	 *            The Trade object to be processed.
	 * @return true if the trade is correctly processed, false otherwise.
	 */
	private boolean sendTrade(Trade trade) {
		// implement the logic to send a trade
		return true;
	}
}
