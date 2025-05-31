/*
 * PersistentSubscription
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

package com.iontrading.samples.persistentSubscription;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;
import com.iontrading.mkv.MkvPersistentRecordSet;
import com.iontrading.mkv.MkvLifeCycleListener;
import com.iontrading.mkv.enums.MkvPersistentLifeCycleStatus;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;

/**
 *
 * @author lveraldi
 */
public class PersistentSubscription {

    /** Creates a new instance of PersistentSubscription */
    public PersistentSubscription(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        try {
            // Start the engine and get back the instance of Mkv (unique during the
            // life of a component).
            Mkv mkv = Mkv.start(qos);

            // Persistent subscriptions allows to subscribe to records regardless of the data being available
            // at the time the subscription is carried on; data will eventually arrive once made available;
            // declarative subscriptions implement transparent resumption of data flow
            // after temporary unavailability of data.

            // Persistent subscriptions are convenient because they do not require the application
            // to implement event driven behavior, thus abstracting the application business logic
            // from the technical details entailed by a distributed system.

            // The PersistentSubscription implements declarative subscription of records;
            // in addition to the supply handler, the constructor takes an optional lifecycle listener,
            // which is notified of events relating to the lifecycle of the subscription/record.

            // Persistent subscription behave the same as regular subscriptions with respect to adding/removing fields
            // to an already existent subscription, except it is not possible to remove individual fields
            // if the subscription has been started with an empty field list.

            // Persistent subscriptions leverage the Recordset object when available.

            MkvPersistentRecordSet set = mkv.getInstance().getSubscribeManager().persistentSubscribe(new RecordListener(), new LifeCycleListener());
            set.add("EUR.CM_DEPTH.BTEC.XY001");
            set.add("EUR.CM_DEPTH.BTEC.XY002");
            set.subscribe(new String[] { "ASK", "BID" });
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new PersistentSubscription(args);
    }

	class LifeCycleListener implements MkvLifeCycleListener {
	    // This method is notified for every change of the persistent subscription lifecycle
	    // it is associated to.
	    public void onLifeCycle(String recordName, MkvPersistentLifeCycleStatus status) {
	        System.out.println("Subscription status change: RecordName {" + recordName + "} NewStatus {" + status + "}");
        }
    }

    class RecordListener implements MkvRecordListener {
        // The subscriber can manage the incoming supplies for PersistentSubscription
        // exactly we same way it would do for normal subscriptions
        public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {}

        // The subscriber can manage the incoming supplies for PersistentSubscription
        // exactly we same way it would do for normal subscriptions
        public void onFullUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
            try {
                String out = "Record " + record.getName() + " : ";
                int cursor = supply.firstIndex();

                while (cursor!=-1) {
                    out +=  record.getMkvType().getFieldName(cursor) + " {" + supply.getObject(cursor) + "} ";
                    cursor = supply.nextIndex(cursor);
                }

                System.out.println((isSnapshot?"Snp ":"Upd ") + out);
            } catch (Exception e) {
                e.printStackTrace();
            }
		}
    }
}
