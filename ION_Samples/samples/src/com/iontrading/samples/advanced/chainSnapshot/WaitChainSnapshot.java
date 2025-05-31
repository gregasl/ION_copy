/*
 * WaitChainSnapshot
 *
 * ION Trading U.K. Limited supplies this software code is for testing purposes
 * only. The scope of this software code is exclusively limited to the
 * demonstration of the structure of an application using the ION(tm) Common
 * Market and ION Trading U.K. Limited does not guarantee the correct behavior
 * of any deployed application using this software code. This software code has
 * not been thoroughly tested under all conditions. ION, therefore, cannot
 * guarantee or imply reliability, serviceability, or function of this software.
 * Any use of this software outside of this defined scope is the sole
 * responsibility of the user.
 *
 * ION Trading ltd (2005)
 */

package com.iontrading.samples.advanced.chainSnapshot;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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

public class WaitChainSnapshot implements MkvChainListener, MkvRecordListener {
    
    private static final String FIELDS[] = { "ID", "BID", "QTY" };
    private static final String SOURCE = "MYPUB";
    private static final String CHAIN_NAME = "EUR.PRICE." + SOURCE + ".QUOTES";

    private int numset = 0;
    
    private final Set pending = Collections.synchronizedSet(new HashSet());
    
    
    public WaitChainSnapshot(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        try {
            // Start the engine and get back the instance of Mkv (unique during the
            // life of a component).
            Mkv mkv = Mkv.start(qos);
            // place the permanent subscription
            subscribeToChain();
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }
    
    private void subscribeToChain() {
        MkvSubscribeManager subman = Mkv.getInstance().getSubscribeManager();
        subman.persistentSubscribe(CHAIN_NAME, this, this, FIELDS);
    }
    
    public void onSupply(MkvChain chain, String record, int pos,
            MkvChainAction action) {
        switch (action.intValue()) {
            case MkvChainAction.DELETE_code:
                pending.remove(record);
                break;
            case MkvChainAction.INSERT_code:
            case MkvChainAction.APPEND_code:
                pending.add(record);
                break;
            case MkvChainAction.RESET_code:
                pending.clear();
                break;
            case MkvChainAction.SET_code:
            	synchronized (pending) {
	                if (numset++ == 0) {
	                    pending.clear();
	                }
	                pending.addAll(chain);
            	}
                break;
                
            case MkvChainAction.IDLE_code:
                numset = 0;
                break;
        }
    }
    
    public void onPartialUpdate(MkvRecord record, MkvSupply supply,
            boolean isSnapshot) {
    }
    
    public void onFullUpdate(MkvRecord record, MkvSupply supply,
            boolean isSnapshot) {
        
    	synchronized(pending) {
	        if (pending.isEmpty()) {
	        	// Don't react to subsequent updates
	            return;
	        } else {
	            pending.remove(record.getName());
	            if (!pending.isEmpty()) {
	                return;
	            }
	        }
    	}
        
        // Here we can assume to have a complete snapshot of chain records
    }
    
    public static void main(String[] args) {
        new WaitChainSnapshot(args);
    }
}