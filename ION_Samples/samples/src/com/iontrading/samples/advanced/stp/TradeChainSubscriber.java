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

import java.util.HashMap;
import java.util.Map;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSubscribeManager;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvChainAction;
import com.iontrading.mkv.events.MkvChainListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

public class TradeChainSubscriber implements
	MkvRecordListener, MkvChainListener {

	/**
	 * Cache the trades. The key is record id and the value is the Trade object.
	 */
	private final Map tradeCache = new HashMap();

	/**
	 * Contains the logic of the STP application.
	 */
	private final TradeReceiver tradeReceiver = new TradeReceiver();

    public TradeChainSubscriber() {

    }

    /**
     * Gets a trade object from the cache.
     * If the trade does not exists in the cache, i.e. the component's just started or the trade is new,
     * a new instance of Trade is created.
     * @param id
     * 			The id of the underlying ION record
     * @return
     * 			The instance of Trade with the given id.
     */
    public Trade getTrade(String id) {
    	Trade t = (Trade)tradeCache.get(id);
    	if (t==null) {
    		t = new Trade();
    		tradeCache.put(id, t);
    	}
    	return t;
    }

    /**
     * Start the component and register the initial listeners.
     * @param args
     * 			Arguments got from command line (enriched if is the case).
     */
    public void start(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        try {
            // Start the engine and get back the instance of Mkv (unique during the
            // life of a component).
            Mkv.start(qos);
            subscribeToChain();
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new TradeChainSubscriber().start(args);
    }

    /**
     * Subscribe to the TRADE chain.
     * Supposed to be called after the initialization of the engine, it use the
     * "permanent subscription" mechanism to subscribe to the chain.
     */
    private void subscribeToChain() {
        MkvSubscribeManager subman = Mkv.getInstance().getSubscribeManager();
        subman.persistentSubscribe(
        		MarketDef.getName("CM_TRADE", "TRADE"),
        		this, this, Trade.FIELDS);
    }

    public void onSupply(MkvChain mkvChain, String recordName, int position,
            MkvChainAction mkvChainAction) {
    }

	public void onPartialUpdate(MkvRecord record, MkvSupply update, boolean isSnapshot) {
	}

	public void onFullUpdate(MkvRecord record, MkvSupply update, boolean isSnapshot) {
        try {
        	Trade t = getTrade(record.getName());
        	t.update(record, update);
           	tradeReceiver.notifyTrade(t);
        } catch (Exception e) {
            // handle the exception
            e.printStackTrace();
        }

	}
}