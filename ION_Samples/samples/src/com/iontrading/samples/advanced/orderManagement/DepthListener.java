/*
 * DepthListener
 *
 * The depth class listen for the best changes for a single instrument and notifies
 * the order manager the new prices.
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

import java.util.Properties;

import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSubscribeProxy;

public class DepthListener implements MkvRecordListener {

	private static MkvSubscribeProxy proxy;

	private static void initProxy(MkvRecord rec) throws MkvException {
		if (proxy == null) {
			Properties p = new Properties();
			p.setProperty("Ask0", "ask");
			p.setProperty("AskSize0", "askSize");
			p.setProperty("Bid0", "bid");
			p.setProperty("BidSize0", "bidSize");
			System.out.println("Creating Proxy for DepthListener");
			proxy = new MkvSubscribeProxy(Best.class, p);
		}
	}

	private final Best best;

	private final IOrderManager orderManager;

	public DepthListener(String instrId, IOrderManager manager) {
		this.best = new Best(instrId);
		this.orderManager = manager;
	}

	public String getInstrumentId() {
		return best.getInstrumentId();
	}

	/**
	 * The object is not supposed to listen for partial updates.
	 */
	public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
			boolean isSnapshot) {
		// do nothing
	}

	/**
	 * Listen for bests and notify the order manager.
	 */
	public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
			boolean isSnapshot) {
		try {
			initProxy(mkvRecord);
			proxy.update(mkvRecord, mkvSupply, best);
			System.out.println("DEPTH: Instrument {" + best.getInstrumentId()
					+ "} " + "Ask {" + best.getAsk() + " " + best.getAskSize()
					+ "} " + "Bid {" + best.getBid() + " " + best.getBidSize()
					+ "}");
			orderManager.best(best);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}