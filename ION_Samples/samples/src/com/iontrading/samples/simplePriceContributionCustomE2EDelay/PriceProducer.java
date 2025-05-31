/*
 * Price Contribution Solution
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
package com.iontrading.samples.simplePriceContributionCustomE2EDelay;

import java.util.Date; 
import java.util.concurrent.atomic.AtomicInteger;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvProperties;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.properties.MkvStringProperty;
import com.iontrading.mkv.qos.MkvQoS;

/*
 * This sample is composed by three different components:
 * 
 * - PriceGenerator: 
 *      It publishes a set of instrument prices and periodically updates them.
 *      Random values are used to simulate instrument names and prices.
 *      The PriceProducer is the downstream component that subscribes those prices.
 *      
 * - PriceProducer:
 *      It subscribes all the available instrument prices and simulates an internal computation for each update.
 *      At the end of the internal computation, it republishes an extended version of the record, 
 *      passing by the information about original supply timestamps to the downstream component.
 *      The PriceConsumer is the downstream component that subscribes the extended prices.
 *      
 * - PriceConsumer:
 *      It subscribes all the available extended price information and simulates an internal computation for each update.
 *      At the end of the internal computation, it reports a Custom End-To-End delay using the supply timestamps
 *      forwarded from the PriceGenerator down to the end PriceConsumer, passing by the PriceProducer.
 * 
 * The Custom End-To-End delay is computed using the MkvE2EDelaySampler API.
 */
public class PriceProducer implements MkvPlatformListener, MkvPublishListener {
	private Mkv mkv           = null;
    private String source     = null;
    private MkvType type      = null;
    private AtomicInteger seq = new AtomicInteger(0);

    public static void main(String[] args) { new PriceProducer(args); }

    public PriceProducer(String[] args) {
        MkvQoS qos = new MkvQoS();
        
        qos.setArgs(args);        
        qos.setPlatformListeners(new MkvPlatformListener[] { this });
        qos.setPublishListeners(new MkvPublishListener[] { this });
        
        try { mkv = Mkv.start(qos); } catch (Exception e) {}
    }

    public void onComponent(final MkvComponent component, final boolean registered) {}
    public void onConnect(final String component, final boolean connected)   		{}
    public void onMain(final MkvPlatformEvent mkvPlatformEvent)               		{
        switch (mkvPlatformEvent.intValue()) {
        	case MkvPlatformEvent.START_code: break;
        	case MkvPlatformEvent.REGISTER_code: System.out.println(new Date() + " " + "REGISTER"); setup(); break;
        	default: break;
        }
    }

    void setup() {
        try {
            MkvProperties props = mkv.getProperties();
            props.registerProperty(new MkvStringProperty("source", "PXE", "Component source"));
            source = props.getProperty("source");

            type = new MkvType(source + "_PRICE", 
            		new String[] { 
            			"E2ETStamp", "E2ETStampExt", "E2EOriginator", 
            			"Instrument", "Bid", "Ask",
            			"ComputedInfo"
            		}, 
            		new MkvFieldType[] { 
            			MkvFieldType.INT, MkvFieldType.REAL, MkvFieldType.STR, 
            			MkvFieldType.STR, MkvFieldType.REAL, MkvFieldType.REAL,
            			MkvFieldType.INT
            		}
            );
            type.publish();
        } catch (Exception e) {}
    }
    
    void doSomeWork() {}
    
    public void onPublishIdle(final String component, final boolean start)          		{}
    public void onSubscribe(final MkvObject obj)                              			  {}
    public void onPublish(final MkvObject object, final boolean start, final boolean dwl) {
		if (!object.isLocal() && 
        	object.getMkvObjectType() == MkvObjectType.RECORD && 
        	start && object.getName().startsWith("ANY.CM_DEPTH.")) {
            try {
                ((MkvRecord)object)
                	.subscribe(new MkvRecordListener() {
                		
                		public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {}
                		public void onFullUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
                			try {
                	           	/* 
                	           	 * Handle each price update and simulate an internal computation for each update.
                	           	 * At the end of the internal computation, it republishes an extended version of the record, 
                	           	 * passing by the information about original supply timestamps to the downstream component.
                	           	 */                				
                				String instrument = record.getSupply().getString(record.getMkvType().getFieldIndex("Instrument"));
                				double bid = record.getSupply().getDouble(record.getMkvType().getFieldIndex("Bid0"));
                				double ask = record.getSupply().getDouble(record.getMkvType().getFieldIndex("Ask0"));
                				
                				String recordName = "ANY.PRICE." + source + "." + instrument;
                				
                				MkvRecord contribRecord = mkv.getPublishManager().getMkvRecord(recordName);
                				if(contribRecord == null) {
                					contribRecord = new MkvRecord(recordName, type.getName());
                					contribRecord.publish();
                				}
                				
                				/* do something here */
                				doSomeWork();
                				
                				contribRecord.supply(MkvSupplyFactory.create(new Object[] { 
                						record.getLastTStamp(), record.getLastMUTStamp(), 
                						record.getOrig(), 
                						instrument, bid, ask,
                						seq.addAndGet(1)
                				}));                				
                			} catch(Exception e) {}
                        }
                	});
            } catch (Exception e) {}
        }
    }
}
