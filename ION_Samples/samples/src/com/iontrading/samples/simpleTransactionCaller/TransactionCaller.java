/**
 * TransactionCaller
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

package com.iontrading.samples.simpleTransactionCaller;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.MkvTimer;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.events.MkvTransactionCallEvent;
import com.iontrading.mkv.events.MkvTransactionCallListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyBuilder;
import com.iontrading.mkv.qos.MkvQoS;

public class TransactionCaller {
    
    private static final String SOURCE = "MYPUB";
    private static final String RECORD_PREFIX = "EUR.PRICE." + SOURCE + ".";
    private static final String RECORD_ID = "FGBLH3";
    private static final String RECORD_NAME = RECORD_PREFIX + RECORD_ID;
    
    /** Creates a new instance of Subscriber */
    public TransactionCaller(String[] args) {
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        qos.setPublishListeners(new MkvPublishListener[] {new PublishListener()});
        try {
            Mkv mkv = Mkv.start(qos);
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        TransactionCaller sub = new TransactionCaller(args);
    }
    
    private class DataListener implements MkvRecordListener {
        public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, 
                boolean isSnapshot) {
            onFullUpdate(mkvRecord, mkvSupply, isSnapshot);
        }
        
        public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, 
                boolean isSnapshot) {
            try {
                String out = "Record " + mkvRecord.getName() + " : ";
                MkvType type = mkvRecord.getMkvType();
                int cursor = mkvSupply.firstIndex();
                while (cursor!=-1) {
                    out += type.getFieldName(cursor) +
                            " {" + mkvSupply.getObject(cursor) + "} ";
                    cursor = mkvSupply.nextIndex(cursor);
                }
                System.out.println(out);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    private class MyTimer extends MkvTimer {
        
        public MyTimer(MkvRecord record) {
            super("TransactionTimer", 500, record);
        }
        
        public boolean call(long time, Object userData) {
            MkvRecord record = (MkvRecord)userData;
            double d = 5 + (Math.floor(Math.sin(time)*100))/100.0;
            System.out.println("Calling for transaction on " + record.getName() +
                    " for QTY=" + d);
            MkvSupplyBuilder builder = new MkvSupplyBuilder(record);
            builder.setField("QTY", new Double(d));
            try {
                record.transaction(builder.getSupply(), new MyTransactionListener());
                return true;
            } catch (Exception e) {
                // handle exception
                e.printStackTrace();
                return false;
            }
        }
    }
    
    private class MyTransactionListener implements MkvTransactionCallListener {
        public void onResult(MkvTransactionCallEvent event, byte resCode, 
                String resStr) {
            System.out.println("Transaction res:" + resCode);
        }
    }
    
    private class PublishListener implements MkvPublishListener {
        public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
            if (mkvObject.getMkvObjectType().equals(MkvObjectType.RECORD)) {
                if (mkvObject.getName().equals(RECORD_NAME)) {
                    System.out.println("Intercepted " + (pub_unpub?"":"un") +
                            "publication : " + mkvObject.getName() + " Dwl {" + dwl + "}");
                    try {
                        ((MkvRecord)mkvObject).subscribe(new DataListener());
                        
                        // start the updater
                        new MyTimer((MkvRecord)mkvObject).start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        
        public void onPublishIdle(String component, boolean start) {
        }
        
        public void onSubscribe(MkvObject mkvObject) {
            // not interested in this event because our component is a pure subscriber
            // and is not supposed receiving request for subscriptions.
        }
        
    }
}
