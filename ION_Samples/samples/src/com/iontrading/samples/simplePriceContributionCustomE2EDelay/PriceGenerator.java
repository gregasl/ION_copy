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
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvProperties;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.properties.MkvIntProperty;
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
public class PriceGenerator implements MkvPlatformListener, MkvPublishListener {
	private Mkv mkv           = null;
    private String source     = null;
    private MkvType type      = null;

    public static void main(String[] args) { new PriceGenerator(args); }

    public PriceGenerator(String[] args) {
        MkvQoS qos = new MkvQoS();
        
        qos.setArgs(args);        
        qos.setPlatformListeners(new MkvPlatformListener[] { this });
        qos.setPublishListeners(new MkvPublishListener[] { this });
        
        try { mkv = Mkv.start(qos); } catch (Exception e) {}
    }

    public void onComponent(final MkvComponent component, final boolean registered) {}
    public void onConnect(final String component, final boolean connected)    		{}
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
            props.registerProperty(new MkvStringProperty("source", "BTEC", "Component source"));
            source = props.getProperty("source");

            type = new MkvType(source + "_CM_DEPTH", 
            		new String[] { 
            			"Instrument", "Bid0", "Ask0"
            		}, 
            		new MkvFieldType[] { 
            			MkvFieldType.STR, MkvFieldType.REAL, MkvFieldType.REAL
            		}
            );
            type.publish();

            props.registerProperty(new MkvIntProperty("num_instrument", 10, "Number of instrument prices to simulate"));
            final int numInstrument = props.getIntProperty("num_instrument");
            
            for(int i=0; i<numInstrument; ++i) {
            	try {
            		String instrument = "INSTR_" + i;
            		String recordName = "ANY.CM_DEPTH." + source + "." + instrument;
            	
            		MkvRecord contribRecord = new MkvRecord(recordName, type.getName());
            		contribRecord.publish();
            	} catch(Exception e) {}
			}
            
           	props.registerProperty(new MkvIntProperty("supdelay", 1, "supply delay (ms)", 1, 1000000));
           	final int supdelay = props.getIntProperty("supdelay");
           	
           	/* 
           	 * Periodically feed new prices onto the ION Bus. 
           	 * Random values are used to simulate instrument names and prices.
           	 */
            new Timer(true).scheduleAtFixedRate(new TimerTask() {
	            Random r = new Random();
	            public void run() { 
	            	for(int i=0; i<numInstrument; ++i) {
	            		try {
	            			String instrument = "INSTR_" + i;
	            			String recordName = "ANY.CM_DEPTH." + source + "." + instrument;
	                 		                 	
	            			MkvRecord contribRecord = mkv.getPublishManager().getMkvRecord(recordName);
	            			contribRecord.supply(MkvSupplyFactory.create(new Object[] { 
	            					instrument, r.nextDouble()*1000, r.nextDouble()*1000
	            			}));
	            		} catch(Exception e) {}
	            	}
	            }
            }, 0, supdelay);
        } catch(Exception e) {}
    }
    
    public void onPublishIdle(final String component, final boolean start)    			  {}
    public void onSubscribe(final MkvObject obj)                              			  {}
    public void onPublish(final MkvObject object, final boolean start, final boolean dwl) {}
}
