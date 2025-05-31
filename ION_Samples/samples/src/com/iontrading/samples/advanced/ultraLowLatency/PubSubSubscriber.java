/*
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
 * ION Trading ltd (2006-2007)
 */

package com.iontrading.samples.advanced.ultraLowLatency;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.MkvLifeCycleListener;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.enums.MkvChainAction;
import com.iontrading.mkv.enums.MkvPersistentLifeCycleStatus;
import com.iontrading.mkv.events.MkvChainListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

/**
 * This sample shows how to leverage the Ultra Low Latency message passing on same-host connections. 
 * The application code is a standard Subscriber code, 
 * since the usage of Ultra Low Latency message passing is completely transparent to the application.
 * 
 * Publisher and Subscriber needs to be running on the same-host and connected with one another. 
 *
 * To enable the Ultra Low Latency data transfer among components running on the same host 
 * both the Publisher and Subscriber component shall be configured with the following setting:
 *
 *   mkv.ultra_low_latency = 1
 *   
 * A shared memory driver process needs to be running on the same host: 
 * see the "Java API Ultra Low Latency Driver" package associated with Java API product 
 * on the ION Tracker Website, for further information. 
 * 
 * Latency performance can be inspected using the ION Performance Meter tool, 
 * focusing on the E2E Latencies table.
 * 
 * @author lveraldi
 *
 */
public class PubSubSubscriber {

    public static void main(String[] args) {
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        qos.setPublishListeners(new MkvPublishListener[] { new PublishListener() });

        try {
            /* Register the component onto the ION Platform */
            Mkv.start(qos);

            /* Start business level activity */
            new PubSubSubscriber();
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    static class PublishListener implements MkvPublishListener {
        String chainName = null;
        boolean subcribed = false;

        public void onPublish(MkvObject object, boolean start, boolean dwl) {
            /* Capture the chain name */
            if(object.getMkvObjectType() == MkvObjectType.CHAIN && object.getName().startsWith("EUR.QUOTE."))
                chainName = (start) ? object.getName() : null;
        }

        public void onPublishIdle(String component, boolean start) {
            if(chainName != null && !subcribed) {
                /* Subscribe the chain persistently */
                Mkv.getInstance().getSubscribeManager().persistentSubscribe(chainName,
                        new MkvChainListener() {
                            public void onSupply(MkvChain chain, String record, int pos, MkvChainAction action) {}
                        },
                        new MkvRecordListener() {
                            public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {}
                            public void onFullUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
                                System.out.println("E2EDelay msecs: " + (Mkv.currentTimeMillis() - record.getLastMUTStamp()));
                            }
                        },
                        new MkvLifeCycleListener() {
                            public void onLifeCycle(String recordName, MkvPersistentLifeCycleStatus status) {}
                        });

                subcribed = true;
            }
        }

        public void onSubscribe(MkvObject obj) {}
    }
}