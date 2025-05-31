/*
 * CNELogic
 *
 * This class contains the "intelligence" of the CNE. All the relevant events 
 * are notified by the other objects and CNELogig is the one responsible for 
 * taking decision and command actions.
 *
 * Requirements:
 * A CM4D2C compliant market gateway must be set up and the constants in the 
 * MarketDef class have to be adjusted to use gateways publications.
 * The component has been tested against MOCK_D2C.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.iontrading.mkv.MkvRecord;

public class CNELogic {
    
    /** Creates a new instance of CNELogic */
    public CNELogic() {
        HeartBeat hb = new HeartBeat();
        hb.setDaemon(true);
        hb.start();
        
        Pricer p = new Pricer();
        p.setDaemon(true);
        p.start();
    }
    
    private Set locked = Collections.synchronizedSet(new HashSet());
    
    /**
     * Notify a trader has locked manually the rfq.
     * The logic try to restore the autoreply and autoupdate to true.
     * It's supposed to fail because the gateway should not allow the CNE to
     * get in control again.
     * @param rfq   The rfq just locked by a trader.
     */
    public void traderLockedRFQ(R4Q rfq) {
        if (rfq.getTraderLockOwner().equals("")) {
            System.out.println("-> LOGIC: Trader unlocked RFQ {" + rfq.getId() + "}");
        } else {
            System.out.println("-> LOGIC: Trader {" + rfq.getTraderLockOwner() + "} " +
                    "locked RFQ {" + rfq.getId() + "}");
        }
    }
    
    /**
     * Notify an rfq has been locked by the cne application.
     * Send an initial price and set the autoreply/autoupdate to true.
     * @param rfq   The just locked rfq.
     */
    public void rfqLocked(R4Q rfq) {
        finalizeBenchmark(rfq);
        //rfq.changePrice(2.5);
        //rfq.setAutoReply(true, isAutoupdatable(rfq), 2.5);

        locked.add(rfq);
 
//        if (rfq.getANTraderList().indexOf("/andrea/")==-1) {
//            System.out.println("-> LOGIC: RFQ {" + rfq.getId() + "} \n" +
//                    "          Trader {andrea} isn't in the trader list.");
//            rfq.setTraderList("andrea");
//        }
    }
    
    protected boolean isAutoupdatable(R4Q rfq) {
        return rfq.getRFQQty()<=5.0;
    }
    
    /**
     * Check if an rfq should be locked or released.
     * If the rfq is lockable (see rfq.isCNEReserved()) it tries to lock it.
     * If the rfq is closed it notify the rfq is closed.
     * If the rfq is in Accepted-Subject status it releases it.
     * @param rfq   The rfq to be checked.
     */
    public void check(R4Q rfq) {
        if (rfq.isCNEReserved() && !rfq.isClosed()) {
            rfq.lock();
            rfq.reply(isAutoupdatable(rfq), 2.5);
        } else if (rfq.isClosed()) {
            rfqClosed(rfq);
        } else if (rfq.isAcceptedSubject() && locked.contains(rfq)) {
            rfq.unLock(42, "releasing unmanageable RFQ");
            rfqClosed(rfq);
        } else if (rfq.isSubject()) {
        	// do some check
            rfq.reject();
        }
    }
    
    /**
     * Notify the rfq is closed.
     * @param rfq   The rfq just closed.
     */
    public void rfqClosed(R4Q rfq) {
        if (locked.remove(rfq)) {
            System.out.println("-> LOGIC: Released RFQ {" + rfq.getId() + "}");
        }
    }
    
    private class HeartBeat extends Thread {
        
        public void run() {
            while (true) {
                Set s = null; 
                synchronized (locked) {
                    s = new HashSet(locked);
                }
                for (Iterator iter = s.iterator(); iter.hasNext(); ) {
                    R4Q rfq = (R4Q)iter.next();
                    rfq.heartBeat(false);
                }
                try {
                    this.sleep(MarketDef.HEARTBEAT_FREQ);
                } catch (Exception e) {}
            }
        }
    }
    
    private class Pricer extends Thread {
        
        public void run() {
            while (true) {
                Set s = null;
                synchronized (locked) {
                    s = new HashSet(locked);
                }
                for (Iterator iter = s.iterator(); iter.hasNext(); ) {
                    R4Q rfq = (R4Q)iter.next();
                    double price = 2.5 +  0.1 * Math.sin(System.currentTimeMillis());
                    rfq.changePrice(price);
                }
                try {
                    this.sleep(MarketDef.PRICER_FREQ);
                } catch (Exception e) {}
            }
        }
    }
    
    private Map benchmarks = new HashMap();
    
    public void initBenchmark(String recordName) {
        benchmarks.put(recordName, new Long(System.nanoTime()));
    }
    
    public void finalizeBenchmark(R4Q rfq) {
    	MkvRecord rec = rfq.getMkvRecord();
    	if (rec == null)
    		return;
        Long l = (Long)benchmarks.remove(rec.getName());
        if (l!=null) {
            System.out.println("-> LOGIC: Lock time RFQ {" + rfq.getId() + "} ms {" +
                    ((System.nanoTime() - l.longValue())/1000000) + "}");
        }
    }
}
