/*
 * CustomE2EDelayPubSub
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
package com.iontrading.samples.simpleCustomE2EDelay;

import java.util.Date; 
import java.util.Timer;
import java.util.TimerTask;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvE2EDelaySampler;
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
import com.iontrading.mkv.properties.MkvIntProperty;
import com.iontrading.mkv.qos.MkvQoS;

/*
 * This sample shows the basic usage of the MkvE2EDelaySampler API to report custom End-To-End delays
 */
public class CustomE2EDelayPubSub implements MkvPlatformListener, MkvPublishListener {
    public static void main(String[] args) { new CustomE2EDelayPubSub(args); }

    public CustomE2EDelayPubSub(String[] args) {
        MkvQoS qos = new MkvQoS();
        
        qos.setArgs(args);        
        qos.setPlatformListeners(new MkvPlatformListener[] { this });
        qos.setPublishListeners(new MkvPublishListener[] { this });
        
        try { Mkv.start(qos); } catch (Exception e) {}
    }

    public void onComponent(final MkvComponent component, final boolean registered) {}
    public void onConnect(final String component, final boolean connected)    {}
    public void onMain(final MkvPlatformEvent mkvPlatformEvent)               {
        switch (mkvPlatformEvent.intValue()) {
        	case MkvPlatformEvent.START_code: setup(); break;
        	case MkvPlatformEvent.REGISTER_code: System.out.println(new Date() + " " + "REGISTER"); break;
        	default: break;
        }
    }

    void setup() {
        try {
            int supdelay = 1;
            MkvProperties props = Mkv.getInstance().getProperties();
            props.registerProperty(new MkvIntProperty("supdelay", 1, "supply delay (ms)", 1, 1000000));
            supdelay = props.getIntProperty("supdelay");

            final String cn = props.getComponentName();
            
            final MkvType t = new MkvType("TICK_" + cn, new String[] { "F" }, new MkvFieldType[] { MkvFieldType.STR });
            t.publish();
            
            final MkvRecord r = new MkvRecord("ALL.TICK." + cn + ".REC", t.getName());
            r.publish();
            
            new Timer(true).scheduleAtFixedRate(new TimerTask() {
                int c;

                public void run() { try { r.supply(MkvSupplyFactory.create("" + c++)); } catch (Exception e) {} }
            }, 0, supdelay);
        } catch (Exception e) {}
    }
    
    public void onPublishIdle(final String component, final boolean start)          {}
    public void onSubscribe(final MkvObject obj)                              {}
    public void onPublish(final MkvObject object, final boolean start, final boolean dwl) {
        if (!object.isLocal() && 
        	object.getMkvObjectType() == MkvObjectType.RECORD && 
        	start && object.getName().startsWith("ALL.TICK.")) {
            try {
                ((MkvRecord)object)
                	.subscribe(new MkvRecordListener() {
                		public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {}
                		public void onFullUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
                			final String peer = record.getFrom() + "|" + Mkv.getInstance().getProperties().getComponentName();
                			MkvE2EDelaySampler sampler = Mkv.getInstance().getPlatform().getE2EDelaySampler(peer);
                			if(sampler == null)	sampler = Mkv.getInstance().getPlatform().createE2EDelaySampler(peer);
                			
                			sampler.sampleSupplyTimestamps(record.getLastTStamp(), record.getLastMUTStamp());
                        }
                	});
            } catch (Exception e) {}
        }
    }
}
