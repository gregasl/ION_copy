/*
 * ChainSubscriber
 *
 * Subscribe to a chain using the permanent subscription
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

package com.iontrading.samples.simplePermChainSubscriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvPersistentChain;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSubscribeManager;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvChainAction;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvChainListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvConnectionException;

import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.exceptions.MkvObjectNotAvailableException;
import com.iontrading.mkv.helper.MkvSupplyUtils;
import com.iontrading.mkv.qos.MkvQoS;

import com.iontrading.samples.simplePermChainSubscriber.config.RedisConfig;

public class ChainSubscriber implements MkvChainListener, MkvRecordListener {

    private String _fields[] = { "ID", "BID", "QTY" };
    private static final String SOURCE = "MYPUB";
    private static final String CHAIN_NAME = "EUR.PRICE." + SOURCE + ".QUOTES";
	private static final Logger logger = LoggerFactory.getLogger(ChainSubscriber.class);
    private static final String PUBLISH_PREFIX = "ION_TMP:";
	
	private RedisPublisher publisher;
	
    public ChainSubscriber(String[] args) {
        // create the initial configuration used to start the engine.
        System.out.println("Starting Chain Subscriber");
		
		MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
		
		RedisConfig config = new RedisConfig();
		
        try {
            Jedis pubJedis = config.getJedisClient();
            publisher = new RedisPublisher(pubJedis, PUBLISH_PREFIX);
			System.out.println("In ChainSubscriber try loop");
			// Start the engine and get back the instance of Mkv (unique during the
            // life of a component).
            Mkv mkv = Mkv.start(qos);
            // place the permanent subscription
            subscribeToChain();
			
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
        }
    }
    
    public static void main(String[] args) {
		System.out.println("Starting Main");
        ChainSubscriber subscriber = new ChainSubscriber(args);
    }
    
    private void subscribeToChain() {
        MkvSubscribeManager subman = Mkv.getInstance().getSubscribeManager();
        MkvPersistentChain pChain = subman.persistentSubscribe(CHAIN_NAME, this, this, _fields);
    }
    
    public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, 
            boolean isSnapshot) { }
    
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, 
            boolean isSnapshot) {
        try {
            String out = "Record " + mkvRecord.getName() + " : ";
            int cursor = mkvSupply.firstIndex();
            while (cursor!=-1) {
                out +=  mkvRecord.getMkvType().getFieldName(cursor) +
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
    
    public void onSupply(MkvChain mkvChain, String recordName, int position,
            MkvChainAction mkvChainAction) {
        // Do nothing. Permanent subscription will take care of record subscriptions.
        // Put here the custom logic.
    }
}