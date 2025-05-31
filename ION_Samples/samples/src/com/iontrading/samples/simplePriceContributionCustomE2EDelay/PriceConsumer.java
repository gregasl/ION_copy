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

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvE2EDelaySampler;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.PrivateMkvProxy;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
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
public class PriceConsumer implements MkvPlatformListener, MkvPublishListener {
	private Mkv mkv = null;
	
    public static void main(String[] args) { new PriceConsumer(args); }

    public PriceConsumer(String[] args) {
        MkvQoS qos = new MkvQoS();
        
        qos.setArgs(args);        
        qos.setPlatformListeners(new MkvPlatformListener[] { this });
        qos.setPublishListeners(new MkvPublishListener[] { this });
        
        try { mkv = Mkv.start(qos); } catch (Exception e) {}
    }

    public void onComponent(final MkvComponent component, final boolean registered) {}
    public void onConnect(final String component, final boolean connected)    {}
    public void onMain(final MkvPlatformEvent mkvPlatformEvent)               {
        switch (mkvPlatformEvent.intValue()) {
        	case MkvPlatformEvent.START_code: break;
        	case MkvPlatformEvent.REGISTER_code: System.out.println(new Date() + " " + "REGISTER"); setup(); break;
        	default: break;
        }
    }

    void setup() {}
    
    void doSomeWork() {}
    
    public void onPublishIdle(final String component, final boolean start)          {}
    public void onSubscribe(final MkvObject obj)                              {}
    public void onPublish(final MkvObject object, final boolean start, final boolean dwl) {
        if (!object.isLocal() && 
        	object.getMkvObjectType() == MkvObjectType.RECORD && 
        	start && object.getName().startsWith("ANY.PRICE.")) {
            try {
                ((MkvRecord)object).subscribe(new MkvRecordListener() {
                    public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {}
                    public void onFullUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
                	    try {
	                	/* 
	            	       	 * Handle each extended price information and simulate an internal computation for each update.
	            	       	 * At the end of the internal computation, it reports a Custom End-To-End delay using the supply timestamps
	            	      	 * forwarded from the PriceGenerator down to the end PriceConsumer, passing by the PriceProducer. 
	            	       	 */     
	                			
                	        /* do something here */
                		doSomeWork();
	                			
                                /* Report the Custom End-To-End latency, from the Feeder down to the Consumer */
                		String peer;
                		MkvE2EDelaySampler sampler;
                		              		
                        /**
                         *  1. - Compute delays based on application-level supply fields 
                         */
                        String originator = record.getSupply().getString(record.getMkvType().getFieldIndex("E2EOriginator"));
                        originator += "|" + record.getOrig();
                        
                        int timestamp = record.getSupply().getInt(record.getMkvType().getFieldIndex("E2ETStamp"));
                        double timestampext = record.getSupply().getDouble(record.getMkvType().getFieldIndex("E2ETStampExt"));
                                                        
                         sampler = mkv.getPlatform().getE2EDelaySampler(originator);
                        if(sampler == null) sampler = mkv.getPlatform().createE2EDelaySampler(originator);                            
                        sampler.sampleSupplyTimestamps(timestamp, timestampext);
                                
                		/**
                		 *  2. - Manipulation of timestamps 
                		 */
                		if(record.getLastMUTStamp() == 0) {
                		    /* Old publisher - backward compatibility code here */
                		    double now = Mkv.currentTimeMillis();
                		    long originalLowResolutionPublisherMarker = record.getLastTStamp();
                		    long localLowResolutionMarker = PrivateMkvProxy.getMkvSupplyTimeStamp(mkv).fromMUTS(now);
                		    
                		    /* NOTE: These 'markers' are NOT timestamps, they have no relatation with EPOCH */
                		    /* 'Markers' can only be used to compute 'best-effort' value for supply latency */                		    
                		    long bestEffortLatency = localLowResolutionMarker - originalLowResolutionPublisherMarker;                		    
                		    double originalPublisherTimestamp = now - bestEffortLatency;
                		}
                		else {
                		    /* Publisher sends the high resolution timestamp, so use it as best possible clock */
                		    double originalPublisherTimestamp = record.getLastMUTStamp();
                		    double localTime = mkv.currentTimeMillis();
                		    
                		    /* Both timestamps are now the highest possible resolution timestamps from the EPOCH */
                		    double absoluteLatency = localTime - originalPublisherTimestamp;
                		}
               		    } catch(Exception e) {}
               		}
               	});
            } catch (Exception e) {
            }
        }
    }
}
