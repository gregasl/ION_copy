/*
 * Subscriber
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

package com.iontrading.samples.simpleSubscriber2;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSubscribeProxy;
import com.iontrading.mkv.qos.MkvQoS;

public class Subscriber {
    
    private static final String SOURCE = "VMO_REPO_US";

    private static final String RECORD_PREFIX = "USD.CM_DEPTH." + SOURCE + ".";

    private static final String RECORD_ID = "*_C_Fixed";

    private static final String RECORD_NAME = RECORD_PREFIX + RECORD_ID;
    
    private MyPrice price = new MyPrice();

    public static void main(String[] args) {
        Subscriber sub = new Subscriber(args);
    }
    
    /** Creates a new instance of Subscriber */
    public Subscriber(String[] args) {
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        qos.setPublishListeners(new MkvPublishListener[] {new PublishListener()});
        try {
            Mkv mkv = Mkv.start(qos);
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }
    
    private class DataListener implements MkvRecordListener {
        private MkvSubscribeProxy proxy;

        DataListener() {
        	proxy = new MkvSubscribeProxy(MyPrice.class);
        }
    
        public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
                boolean isSnapshot) {}
        
        public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, 
                boolean isSnapshot) {
        	try {
                proxy.update(mkvRecord, mkvSupply, price);
                String out = "Supply {ID {" + price.getID() + "} " + " ASK {"
                        + price.getASK() + "} " + " BID {" + price.getBID()
                        + "} " + " QTY {" + price.getQTY() + "}}";
                System.out.println(out);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
    
    private class PublishListener implements MkvPublishListener {
    	
    	void subscribeRecord(MkvRecord rec) {
            try {
                MkvRecordListener listener = new DataListener();
                rec.subscribe(new String[] {"BID", "ASK"}, listener);
            } catch (Exception e) {
                e.printStackTrace();
            }
    	}
    	
    	public void onPublish(MkvObject mkvObject, boolean start, boolean dwl) {
        	if (start && !dwl) {
	            if (mkvObject.getMkvObjectType().equals(MkvObjectType.RECORD)) {
	                if (mkvObject.getName().equals(RECORD_NAME)) {
	                    System.out.println("Intercepted " + (start?"":"un") +
	                            "publication : " + mkvObject.getName() + " Dwl {" + dwl + "}");
	                    subscribeRecord(((MkvRecord)mkvObject));
	                }
	            }
        	}
        }
        
        public void onPublishIdle(String component, boolean start) {
        	if (start) {
        		MkvRecord rec = Mkv.getInstance().getPublishManager().getMkvRecord(RECORD_NAME);
        		if (rec!=null) {
        			subscribeRecord(rec);
        		}
        	}
        }

        public void onSubscribe(MkvObject mkvObject) {
            // not interested in this event because our component is a pure subscriber
            // and is not supposed receiving request for subscriptions.
        }
        
    }
}
