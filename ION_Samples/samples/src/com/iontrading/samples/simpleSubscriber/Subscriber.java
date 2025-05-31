package com.iontrading.samples.simpleSubscriber;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.io.IOException;

import com.iontrading.mkv.*;
import com.iontrading.mkv.enums.*;
import com.iontrading.mkv.events.*;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

public class Subscriber {
    private static final Logger LOGGER = Logger.getLogger(Subscriber.class.getName());
    
    static {
        try {
            FileHandler fileHandler = new FileHandler("subscriber.log", true); // Append mode
            fileHandler.setFormatter(new SimpleFormatter()); // Simple text format
            LOGGER.addHandler(fileHandler);
            LOGGER.setUseParentHandlers(false); // Prevents logging to console
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize file logging", e);
        }
    }
    
    private static final String SOURCE = "VMO_REPO_US";
    private static final String RECORD_PREFIX = "USD.CM_DEPTH." + SOURCE + ".";
//    private static final String RECORD_ID = "91282CKS9_REG_Fixed";
//    private static final String RECORD_NAME = RECORD_PREFIX + RECORD_ID;
    private static final String RECORD_NAME = RECORD_PREFIX;
    
    public Subscriber(String[] args) {
        LOGGER.info("Initializing Subscriber...");
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        qos.setPlatformListeners(new MkvPlatformListener[] { new PlatformListener() });
        try {
            Mkv mkv = Mkv.start(qos);
            LOGGER.info("Mkv started successfully.");
        } catch (MkvException e) {
            LOGGER.log(Level.SEVERE, "Failed to start Mkv", e);
        }
    }
    
    public static void main(String[] args) {
        LOGGER.info("Starting Subscriber application...");
        new Subscriber(args);
    }
    
    private class PlatformListener implements MkvPlatformListener {
        public void onComponent(MkvComponent comp, boolean start) {}
        public void onConnect(String comp, boolean start) {}
        
        public void onMain(MkvPlatformEvent event) {
            LOGGER.info("Platform event received: " + event);
            if (event.equals(MkvPlatformEvent.START)) {
                Mkv.getInstance().getPublishManager().addPublishListener(new PublishListener());
            }
        }
    }
    
    private class DataListener implements MkvRecordListener {
        public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {}
        
        public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
            try {
                StringBuilder out = new StringBuilder("Record ").append(mkvRecord.getName()).append(" : ");
                int cursor = mkvSupply.firstIndex();
                while (cursor != -1) {
                    out.append(mkvRecord.getMkvType().getFieldName(cursor))
                        .append(" {")
                        .append(mkvSupply.getObject(cursor))
                        .append("} ");
                    cursor = mkvSupply.nextIndex(cursor);
                }
                LOGGER.info((isSnapshot ? "Snp " : "Upd ") + out);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error processing full update", e);
            }
        }
    }
    
    private class PublishListener implements MkvPublishListener {
    	
    	void subscribeRecord(MkvRecord rec) {
    	    try {
    	        LOGGER.info("Available fields for record: " + rec.getName());

    	        MkvType recordType = rec.getMkvType();
    	        List<String> availableFields = new ArrayList<>();

    	        for (int i = 0; i < recordType.size(); i++) {
    	            String fieldName = recordType.getFieldName(i);
    	            availableFields.add(fieldName);
    	            LOGGER.info("Field " + i + ": " + fieldName);
    	        }

    	        // Log available fields for debugging
    	        LOGGER.info("All available fields: " + availableFields);

    	        // Check if the required fields exist before subscribing
    	        if (!availableFields.contains("Bid0") || !availableFields.contains("Ask0")) {
    	            LOGGER.severe("Required fields Bid0 or Ask0 not found. Aborting subscription.");
    	            return;
    	        }

    	        // Subscribe only if fields exist
    	        MkvRecordListener listener = new DataListener();
    	        rec.subscribe(new String[]{"Bid0", "Ask0"}, listener);

    	        LOGGER.info("Successfully subscribed to: " + rec.getName());
    	    } catch (Exception e) {
    	        LOGGER.log(Level.SEVERE, "Error subscribing to record", e);
    	    }
    	}
    	
        public void onPublish(MkvObject mkvObject, boolean start, boolean dwl) {
            LOGGER.info("Publish event: " + mkvObject.getName() + " Start: " + start + " Dwl: " + dwl);
            if (start && !dwl && mkvObject.getMkvObjectType().equals(MkvObjectType.RECORD)) {
                if (mkvObject.getName().equals(RECORD_NAME)) {
                    LOGGER.info("Intercepted publication: " + mkvObject.getName());
                    subscribeRecord((MkvRecord) mkvObject);
                }
            }
        }
        
        public void onPublishIdle(String component, boolean start) {
            LOGGER.info("Publish idle event: Component " + component + " Start: " + start);
            if (start) {
                MkvRecord rec = Mkv.getInstance().getPublishManager().getMkvRecord(RECORD_NAME);
                if (rec != null) {
                    subscribeRecord(rec);
                }
            }
        }
        
        public void onSubscribe(MkvObject mkvObject) {}
    }
}
