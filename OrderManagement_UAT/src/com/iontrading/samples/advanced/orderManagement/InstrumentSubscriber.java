package com.iontrading.samples.advanced.orderManagement;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvPattern;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

import java.util.EnumMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import java.util.logging.Logger;
import java.util.logging.Level;

public class InstrumentSubscriber {

    private static final Logger LOGGER = Logger.getLogger(InstrumentSubscriber.class.getName());
    
    private static final String CM_SOURCE = "VMO_REPO_US";
    private static final String INSTRUMENT_PATTERN = "USD.CM_INSTRUMENT." + CM_SOURCE + ".";
    private static final Set<String> ELECTRONIC_SOURCES = new HashSet<>(Arrays.asList("DEALERWEB_REPO", "BTEC_REPO_US", "FENICS_USREPO"));
    private final Map<String, Map<String, String>> directSourceToNativeIdMap = new ConcurrentHashMap<>();
    
    // Enum to standardize field names for type-safe lookups
    public enum InstrumentField {
        ID, ID0, ID1, ID2, ID3, ID4, ID5, ID6, ID7, ID8, ID9, ID10, ID11, ID12, ID13, ID14, ID15, 
        SRC0, SRC1, SRC2, SRC3, SRC4, SRC5, SRC6, SRC7, SRC8, SRC9, SRC10, SRC11, SRC12, SRC13, SRC14, SRC15,
        ISAON    }
    
    private final Map<String, EnumMap<InstrumentField, Object>> instrumentData = new ConcurrentHashMap<>();
    private boolean dataLoaded = false;
    private List<Runnable> dataLoadedCallbacks = new ArrayList<>();
    private long lastLogTime = System.currentTimeMillis();
    private int recordCounter = 0;
    
    /** Creates a new instance of InstrumentSubscriber */
    public InstrumentSubscriber(String[] args) {
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Initializing InstrumentSubscriber");
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        qos.setPlatformListeners(new MkvPlatformListener[] {new PlatformListener()});
        try {
            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Starting Mkv for InstrumentSubscriber");
            qos.setPublishListeners(new MkvPublishListener[] {new PublishListener()});
            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Setting QoS for InstrumentSubscriber");
            Mkv mkv = Mkv.start(qos);
            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Mkv started for InstrumentSubscriber");
        } catch (MkvException e) {
            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Failed to start Mkv for InstrumentSubscriber");
            e.printStackTrace();
        }
    }

    /**
     * Register a callback to be executed when instrument data is fully loaded
     */
    public synchronized void registerDataLoadedCallback(Runnable callback) {
        if (dataLoaded) {
            // If data is already loaded, execute callback immediately
            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Data already loaded, executing callback immediately");
            callback.run();
        } else {
            // Otherwise, add to the list of callbacks
            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Data not yet loaded, adding callback to queue");
            dataLoadedCallbacks.add(callback);
        }
    }
    
    /**
     * Check if instrument data is fully loaded
     */
    public synchronized boolean isDataLoaded() {
        return dataLoaded;
    }
    
    /**
     * Force data loaded notification (used by timeout mechanism)
     */
    public synchronized void forceDataLoaded() {
        if (!dataLoaded) {
            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Forcing dataLoaded=true with " + instrumentData.size() + " instruments loaded");
            notifyDataLoaded();
        } else {
            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Data already loaded, ignoring force request");
        }
    }
    
    /**
     * Called internally when data loading is complete
     */
    private synchronized void notifyDataLoaded() {
        if (dataLoaded) {
            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Data already loaded, ignoring duplicate notification");
            // If data is already loaded, ignore the notification
            return;
        }
        
        dataLoaded = true;
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Setting dataLoaded=true, executing " + dataLoadedCallbacks.size() + " callbacks");
                
        // Execute all registered callbacks
        int callbackIndex = 0;
        for (Runnable callback : dataLoadedCallbacks) {
            try {
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Executing callback #" + (callbackIndex + 1));
                callback.run();
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Callback #" + (callbackIndex + 1) + " executed successfully");
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error executing callback #" + (callbackIndex + 1));
                e.printStackTrace();
            }
        }
        
        // Clear the callbacks list
        dataLoadedCallbacks.clear();
        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Cleared all callbacks after execution");
    }
    
    /**
     * Gets the ISAON flag value for a specific instrument.
     * 
     * @param instrumentId The instrument identifier
     * @return Boolean value of ISAON flag (true/false) or null if not found
     */
    public Boolean getInstrumentIsAON(String instrumentId) {
        if (instrumentId == null || instrumentId.isEmpty()) {
            return null;
        }
        
        try {
            // Directly access the data using instrumentId as key
            EnumMap<InstrumentField, Object> data = instrumentData.get(instrumentId);
            
            if (data == null) {
                return null;
            }
            
            // Get the ISAON value
            Object isaonValue = data.get(InstrumentField.ISAON);
            
            if (isaonValue == null) {
                return null;
            }
            
            // Convert to Boolean based on the actual data type
            if (isaonValue instanceof Boolean) {
                return (Boolean) isaonValue;
            } else if (isaonValue instanceof Integer) {
                return ((Integer) isaonValue) != 0;
            } else if (isaonValue instanceof String) {
                return "1".equals(isaonValue) || "true".equalsIgnoreCase((String) isaonValue);
            }
            
            return null;
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error getting ISAON flag for instrument: " + instrumentId);
            }
            e.printStackTrace();
            return null;
        }
    }
    
    public String getInstrumentFieldBySourceString(String instrumentId, String sourceId, Boolean isAON) {
        if (LOGGER.isLoggable(Level.INFO)) {
            ApplicationLogging.logAsync(LOGGER, Level.INFO, "getInstrumentFieldBySourceString called with instrumentId: " + instrumentId + ", sourceId: " + sourceId + " isAON: " + isAON);
        }

        // Input validation
        if (instrumentId == null || sourceId == null || instrumentId.isEmpty() || sourceId.isEmpty()) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                ApplicationLogging.logAsync(LOGGER, Level.WARNING, "Invalid instrument or source ID - null or empty");
            }
            return null;
        }

        // First check if we have a direct mapping (optimized path)
        Map<String, String> sourceMap = directSourceToNativeIdMap.get(instrumentId);
        if (sourceMap != null) {
            // Use a combined key with AON suffix for lookup
            String mapKey = sourceId + (isAON ? "_AON" : "");
            String nativeId = sourceMap.get(mapKey);
            if (nativeId != null) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, "Found cached native instrument ID: " + nativeId + 
                        " for source: " + sourceId + " (AON requested: " + isAON + ")");
                }
                return nativeId;
            }
        }
        
        try {
            // Directly access the data using instrumentId as key
            EnumMap<InstrumentField, Object> data = instrumentData.get(instrumentId);
            
            if (data == null) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    ApplicationLogging.logAsync(LOGGER, Level.WARNING, "No data found for instrument: " + instrumentId);
                }
                return null;
            }

            // Find the source index by comparing string values
            int sourceIndex = -1;
            for (int i = 0; i < 11; i++) {  // Search through sources (Src0-Src10)
                try {
                    InstrumentField srcField = InstrumentField.valueOf("SRC" + i);
                    String src = (String) data.get(srcField);
                    
                    // Skip if the source doesn't match what we're looking for
                    if (src == null || !sourceId.equals(src)) continue;
                    
                    // Get the ID for this source/index
                    InstrumentField idField = InstrumentField.valueOf("ID" + i);
                    String id = (String) data.get(idField);
                    if (id == null) continue;
                    
                    // Check if the ID is an AON type (ends with "AON")
                    boolean idIsAon = id.endsWith("AON");
                    
                    // If isAON parameter matches whether this ID is AON, we found our match
                    if (isAON == idIsAon) {
                        sourceIndex = i;
                        break;
                    }
                } catch (Exception e) {
                    // Skip invalid field names
                    continue;
                }
            }
            
            if (sourceIndex == -1) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    ApplicationLogging.logAsync(LOGGER, Level.WARNING, "Source not found: " + sourceId + 
                        " for instrument: " + instrumentId + " (AON requested: " + isAON + ")");
                }
                return null;
            }
            
            // Get the native instrument ID for this source
            InstrumentField idField = InstrumentField.valueOf("ID" + sourceIndex);
            String nativeId = (String) data.get(idField);
            
            // Store this in our direct mapping for future lookups
            if (nativeId != null && ELECTRONIC_SOURCES.contains(sourceId)) {
                // Create the source map if it doesn't exist
                if (sourceMap == null) {
                    sourceMap = new HashMap<>();
                    directSourceToNativeIdMap.put(instrumentId, sourceMap);
                }
                // Store using combination of source and AON flag as key
                String mapKey = sourceId + (isAON ? "_AON" : "");
                sourceMap.put(mapKey, nativeId);
            }
            
            if (LOGGER.isLoggable(Level.INFO)) {
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Found native instrument ID: " + nativeId + 
                    " for source: " + sourceId + " (AON requested: " + isAON + ")");
            }
            return nativeId;
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error in getInstrumentFieldBySourceString", e);
            }
            e.printStackTrace();
            return null;
        }
    }
   
    /**
     * Get total number of instruments loaded
     */
    public int getInstrumentCount() {
        return instrumentData.size();
    }

    private class DataListener implements MkvRecordListener {
        @Override
        public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
            // Process partial updates for instrument data
            processInstrumentUpdate(mkvRecord, mkvSupply, isSnapshot);
        }

        @Override
        public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
            processInstrumentUpdate(mkvRecord, mkvSupply, isSnapshot);
        }
        
        private void processInstrumentUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
            String instrumentId = null;
            try {
                recordCounter++;
                // Periodic logging to avoid flooding
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastLogTime > 5000) {  // Log every 5 seconds
                    if (LOGGER.isLoggable(Level.FINE)) {
                        ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                            "Processing instrument update: " + mkvRecord.getName() + 
                            ", Total instruments processed: " + recordCounter);
                    }
                    lastLogTime = currentTime;
                }
                
                // Create an EnumMap for type-safe, efficient storage
                EnumMap<InstrumentField, Object> recordData = new EnumMap<>(InstrumentField.class);
                
                // Populate the map with field names and values
                int cursor = mkvSupply.firstIndex();
                while (cursor != -1) {
                    try {
                        // Convert string field names to enum (e.g., "Id0" -> ID0)
                        String fieldName = mkvRecord.getMkvType().getFieldName(cursor);
                        Object fieldValue;
                        if (fieldName.startsWith("Id") || fieldName.startsWith("Src")) {
                            fieldValue = mkvSupply.getString(cursor);
                        } else {
                            fieldValue = mkvSupply.getObject(cursor);
                        }
                        
                        if (LOGGER.isLoggable(Level.FINE)) {
                            ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                                "InstrumentUpdate Field: " + fieldName + ", Value: " + fieldValue);
                        }      
                        
                        // Check if this is the main instrument ID field
                        if ("Id".equals(fieldName)) {
                            instrumentId = (String) fieldValue;
                            if (LOGGER.isLoggable(Level.FINE)) {
                                ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                                    "Found instrument ID: " + instrumentId);
                            }
                        }
                        
                        // Convert string field names to enum (e.g., "Id0" -> ID0)
                        try {
                            InstrumentField field = InstrumentField.valueOf(fieldName.toUpperCase());
                            recordData.put(field, fieldValue);
                        } catch (IllegalArgumentException e) {
                            // Log skipped fields at FINE level
                            if (LOGGER.isLoggable(Level.FINE)) {
                                ApplicationLogging.logAsync(LOGGER, Level.FINE, 
                                    "Skipped field: " + fieldName);
                            }
                        }
                    } catch (Exception e) {
                        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                            "Error processing field for instrument: " + 
                            (instrumentId != null ? instrumentId : "Unknown"), e);
                    }
                    cursor = mkvSupply.nextIndex(cursor);
                }
                
                // Validate instrument ID before processing
                if (instrumentId == null || instrumentId.trim().isEmpty()) {
                    ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                        "Skipping update - no valid instrument ID found in record: " + 
                        mkvRecord.getName());
                    return;
                }
                
                // Merge or update existing instrument data
                synchronized (instrumentData) {
                    // Check if instrument already exists
                    EnumMap<InstrumentField, Object> existingData = instrumentData.get(instrumentId);
                    
                    if (existingData == null) {
                        // New instrument, add completely new record
                        instrumentData.put(instrumentId, recordData);
                        
                        if (LOGGER.isLoggable(Level.INFO)) {
                            ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                "Added new instrument: " + instrumentId);
                        }
                    } else {
                        // Merge/update existing instrument data
                        existingData.putAll(recordData);
                        
                        if (LOGGER.isLoggable(Level.INFO)) {
                            ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                "Updated existing instrument: " + instrumentId);
                        }
                    }
                }
                
                // Create direct mappings for interesting sources
                Map<String, String> sourceToNativeMap = new HashMap<>();
                
                // Get existing mappings if available
                Map<String, String> existingSourceMap = directSourceToNativeIdMap.get(instrumentId);
                if (existingSourceMap != null) {
                    // Copy existing mappings to preserve them
                    sourceToNativeMap.putAll(existingSourceMap);
                }
                
                // Track if we found any Dealerweb mappings in this update
                boolean foundDealerwebMapping = false;
                
                for (int i = 0; i < 11; i++) {  // Expanded to check more sources
                    try {
                        InstrumentField srcField = InstrumentField.valueOf("SRC" + i);
                        String src = (String) recordData.get(srcField);
                        
                        // Only store mappings for sources we care about
                        if (src != null && ELECTRONIC_SOURCES.contains(src)) {
                            InstrumentField idField = InstrumentField.valueOf("ID" + i);
                            String nativeId = (String) recordData.get(idField);
                            if (nativeId != null) {
                                // Check if this is an AON ID
                                boolean isAonId = nativeId.endsWith("AON");
                                // Use the composite key format for consistency with lookup
                                String mapKey = src + (isAonId ? "_AON" : "");
                                sourceToNativeMap.put(mapKey, nativeId);
                                
                                // Handle Dealerweb specially - create both AON and non-AON mappings
                                if ("DEALERWEB_REPO".equals(src)) {
                                    foundDealerwebMapping = true;
                                    
                                    if (isAonId) {
                                        // If this is an AON ID, also create a non-AON entry
                                        String nonAonMapKey = src; // No _AON suffix for non-AON key
                                        String nonAonId = nativeId;
                                        if (nonAonId.endsWith("AON")) {
                                            nonAonId = nonAonId.substring(0, nonAonId.length() - 4);
                                        }
                                        sourceToNativeMap.put(nonAonMapKey, nonAonId);
                                        
                                        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                            "Added additional non-AON mapping for Dealerweb: " + 
                                            nonAonMapKey + " -> " + nonAonId);
                                    } else {
                                        // If this is a non-AON ID, also create an AON entry
                                        String aonMapKey = src + "_AON";
                                        String aonId = nativeId;
                                        if (!aonId.endsWith("AON")) {
                                            aonId = aonId + "_AON";
                                        }
                                        sourceToNativeMap.put(aonMapKey, aonId);
                                        
                                        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                            "Added additional AON mapping for Dealerweb: " + 
                                            aonMapKey + " -> " + aonId);
                                    }
                                }
                                
                                if (LOGGER.isLoggable(Level.INFO)) {
                                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                        "Added mapping: " + mapKey + " -> " + nativeId);
                                }
                            }
                        }
                    } catch (Exception e) {
                        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                            "Error creating source mapping for instrument: " + instrumentId, e);
                    }
                }
                
                // If this update contained no Dealerweb mappings, check if we need to apply
                // the special handling to existing Dealerweb mappings
                if (!foundDealerwebMapping) {
                    // Check for existing non-AON Dealerweb mapping without a corresponding AON mapping
                    String dealerwebKey = "DEALERWEB_REPO";
                    String dealerwebAonKey = dealerwebKey + "_AON";
                    
                    if (sourceToNativeMap.containsKey(dealerwebKey) && !sourceToNativeMap.containsKey(dealerwebAonKey)) {
                        // We have a non-AON mapping but no AON mapping - create the AON mapping
                        String nonAonId = sourceToNativeMap.get(dealerwebKey);
                        String aonId = nonAonId;
                        if (!aonId.endsWith("AON")) {
                            aonId = aonId + "_AON";
                        }
                        sourceToNativeMap.put(dealerwebAonKey, aonId);
                        
                        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                            "Added missing AON mapping for existing Dealerweb: " + 
                            dealerwebAonKey + " -> " + aonId);
                    }
                    else if (sourceToNativeMap.containsKey(dealerwebAonKey) && !sourceToNativeMap.containsKey(dealerwebKey)) {
                        // We have an AON mapping but no non-AON mapping - create the non-AON mapping
                        String aonId = sourceToNativeMap.get(dealerwebAonKey);
                        String nonAonId = aonId;
                        if (nonAonId.endsWith("AON")) {
                            nonAonId = nonAonId.substring(0, nonAonId.length() - 4);
                        }
                        sourceToNativeMap.put(dealerwebKey, nonAonId);
                        
                        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                            "Added missing non-AON mapping for existing Dealerweb: " + 
                            dealerwebKey + " -> " + nonAonId);
                    }
                }
                
                // Update direct source mappings
                synchronized (directSourceToNativeIdMap) {
                    if (!sourceToNativeMap.isEmpty()) {
                        directSourceToNativeIdMap.put(instrumentId, sourceToNativeMap);
                    }
                }
                
                // Trigger data loaded if we have a reasonable number of instruments
                if (instrumentData.size() > 1000 && !dataLoaded) {
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                        "Triggered data loaded with " + instrumentData.size() + " instruments");
                    notifyDataLoaded();
                }
                
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
                    "Comprehensive error processing instrument update: " + 
                    (instrumentId != null ? instrumentId : "Unknown Instrument"), e);
            }
        }
    }

    private class PlatformListener implements MkvPlatformListener {
        @Override
        public void onComponent(MkvComponent component, boolean start) {
        }

        @Override
        public void onConnect(String component, boolean start) {
        }

        @Override
        public void onMain(MkvPlatformEvent event) {
            if (event.equals(MkvPlatformEvent.START)) {
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Mkv platform started");
                Mkv.getInstance().getPublishManager().addPublishListener(new PublishListener());
            }
        }
    }

    private class PublishListener implements MkvPublishListener {

    	public void onPublish(MkvObject mkvObject, boolean start, boolean download) {
            if ((start) && (mkvObject.getName().toString().equals(INSTRUMENT_PATTERN)) && 
                    (mkvObject.getMkvObjectType().equals(MkvObjectType.PATTERN))) {
                try {
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, "Found instrument pattern: " + mkvObject.getName());
                    MkvRecordListener listener = new DataListener();
                    String[] fields = {"Id", "Id0", "Id1", "Id2", "Id3", "Id4", "Id5", "Id6", "Id7", "Id8", "Id9", "Id10",
                                     "Src0", "Src1", "Src2", "Src3", "Src4", "Src5", "Src6", "Src7", "Src8", "Src9", "Src10", "IsAon" };
                    ((MkvPattern) mkvObject).subscribe(fields, listener);
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, "Subscribed to instrument pattern: " + mkvObject.getName());
                } catch (Exception e) {
                    ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error subscribing to instrument pattern: " + mkvObject.getName());
                    e.printStackTrace();
                }
            }
        }

        public void onPublishIdle(String component, boolean start) {
            if (!start) {
                // Look for the instrument pattern
                MkvObject mkvObj = Mkv.getInstance().getPublishManager().getMkvObject(INSTRUMENT_PATTERN);
                if ((mkvObj != null) && (mkvObj.getName().toString().equals(INSTRUMENT_PATTERN)) 
                		&& (mkvObj.getMkvObjectType().equals(MkvObjectType.PATTERN))) {
                    try {
                        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Found instrument pattern during idle: " + mkvObj.getName());
                        MkvRecordListener listener = new DataListener();
                        String[] fields = {"Id", "Id0", "Id1", "Id2", "Id3", "Id4", "Id5", "Id6", "Id7", "Id8", "Id9", "Id10", 
                        		"Src0", "Src1", "Src2", "Src3", "Src4", "Src5", "Src6", "Src7", "Src8", "Src9", "Src10", "IsAon" };
                        ((MkvPattern) mkvObj).subscribe(fields, listener);
                        ApplicationLogging.logAsync(LOGGER, Level.INFO, "Subscribed to instrument pattern at idle: " + mkvObj.getName());
                    } catch (Exception e) {
                        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error subscribing to instrument pattern during idle: " + mkvObj.getName());
                        e.printStackTrace();
                    }
                }
                
                // Check if we have enough instruments to consider data loading complete
                if (instrumentData.size() > 2000 && !dataLoaded) {
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, "More than 2000 instruments loaded, considering data loading complete");
                    notifyDataLoaded();
                }
                
                // If this is the end of all downloads, notify data loaded
                if (component.equals(INSTRUMENT_PATTERN)) {
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, "All downloads complete for instrument pattern: " + mkvObj.getName());
                    notifyDataLoaded();
                }
            }
        }

        @Override
        public void onSubscribe(MkvObject mkvObject) {
            // Not needed
        }
    }
}