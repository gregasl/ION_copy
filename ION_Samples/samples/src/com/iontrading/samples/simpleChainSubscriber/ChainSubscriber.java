/*
 * ChainSubscriber
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

package com.iontrading.samples.simpleChainSubscriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.redis.client.config.RedisConfig;
import redis.clients.jedis.Jedis;
import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvChainAction;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.events.MkvChainListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvConnectionException;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.exceptions.MkvObjectNotAvailableException;
import com.iontrading.mkv.qos.MkvQoS;

public class ChainSubscriber implements MkvPlatformListener, MkvPublishListener, MkvChainListener,
        MkvRecordListener {
    
    private String[] _fields = { "ID", "BID", "QTY" };
    private static final String SOURCE = "MYPUB";
    private static final String CHAIN_NAME = "EUR.PRICE." + SOURCE + ".QUOTES";
    private static final Logger logger = LoggerFactory.getLogger(ChainSubscriber.class);
    private static final String PUBLISH_PREFIX = "ION_TMP:";
    
    private RedisPublisher publisher;

    public ChainSubscriber(String[] args) {
        // Create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        qos.setPlatformListeners(new MkvPlatformListener[] {this});
        
        RedisConfig config = new RedisConfig();
        
        try {
            // Start the engine and get back the instance of Mkv (unique during the
            // life of a component).
            Jedis pubJedis = config.getJedisClient();
            publisher = new RedisPublisher(pubJedis, PUBLISH_PREFIX);
            Mkv mkv = Mkv.start(qos);
            
            final RedisPublisher finalPublisher = publisher;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {    
                logger.info("Shutting down application...");
                if (finalPublisher != null) {
                    finalPublisher.shutdown();
                }
            }));
        } catch (MkvException e) {
            e.printStackTrace();
            if (publisher != null) {
                publisher.shutdown();
            }
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        new ChainSubscriber(args);
    }

    @Override
    public void onPublish(MkvObject mkvObject, boolean start, boolean download) {
        if (start) {
            if (!download) {
                // A new object has been published runtime
                if (mkvObject.getMkvObjectType().equals(MkvObjectType.CHAIN) &&
                        mkvObject.getName().equals(CHAIN_NAME)) {
                    try {
                        ((MkvChain) mkvObject).subscribe(this);
                        // The chain has been subscribed to
                    } catch (MkvObjectNotAvailableException e) {
                        e.printStackTrace();
                    } catch (MkvConnectionException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            // Handle the un-publish.
        }
    }

    @Override
    public void onPublishIdle(String component, boolean start) {
        MkvChain c = Mkv.getInstance().getPublishManager().getMkvChain(CHAIN_NAME);
        if (c != null) {
            try {
                c.subscribe(this);
            } catch (MkvObjectNotAvailableException e) {
                e.printStackTrace();
            } catch (MkvConnectionException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void onSubscribe(MkvObject mkvObject) {
        // Not interested in this event.
    }

    @Override
    public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {}

    @Override
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
        try {
            String out = "Record " + mkvRecord.getName() + " : ";
            int cursor = mkvSupply.firstIndex();
            while (cursor != -1) {
                out += mkvRecord.getMkvType().getFieldName(cursor) +
                        " {" + mkvSupply.getObject(cursor) + "} ";
                cursor = mkvSupply.nextIndex(cursor);
            }
            String message = (isSnapshot ? "Snp " : "Upd ") + out;
            System.out.println(message);
            if (publisher != null) {
                publisher.publishMessageAsync(CHAIN_NAME, message);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onSupply(MkvChain mkvChain, String recordName, int position,
                         MkvChainAction mkvChainAction) {
        try {
            switch (mkvChainAction.intValue()) {
                case MkvChainAction.IDLE_code:
                    System.out.println("Chain IDLE");
                    for (int i = 0; i < mkvChain.size(); i++) {
                        String _recordName = (String) mkvChain.get(i);
                        subscribeToRecord(_recordName);
                    }
                    break;
                case MkvChainAction.RESET_code:
                    System.out.println("Chain RESET");
                    for (int i = 0; i < mkvChain.size(); i++) {
                        String _recordName = (String) mkvChain.get(i);
                        unsubscribeToRecord(_recordName);
                    }
                    break;
                case MkvChainAction.INSERT_code:
                case MkvChainAction.APPEND_code:
                    System.out.println("Chain APPEND : " + recordName);
                    subscribeToRecord(recordName);
                    break;
                case MkvChainAction.DELETE_code:
                    System.out.println("Chain DELETE : " + recordName);
                    unsubscribeToRecord(recordName);
                    break;
            }
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    private synchronized void subscribeToRecord(String recName) throws MkvException {
        MkvRecord rec =
                Mkv.getInstance().getPublishManager().getMkvRecord(recName);
        if (rec != null && !rec.isSubscribed(this, _fields)) {
            System.out.println("Subscribing " + recName);
            rec.subscribe(this);
        }
    }

    private void unsubscribeToRecord(String recName) throws MkvException {
        MkvRecord rec =
                Mkv.getInstance().getPublishManager().getMkvRecord(recName);
        if (rec != null) {
            System.out.println("Unsubscribing " + recName);
            rec.unsubscribe(this);
        }
    }

    @Override
    public void onComponent(MkvComponent comp, boolean start) {}

    @Override
    public void onConnect(String comp, boolean start) {}

    @Override
    public void onMain(MkvPlatformEvent event) {
        if (event.equals(MkvPlatformEvent.START)) {
            Mkv.getInstance().getPublishManager().addPublishListener(this);
        }
    }
}
