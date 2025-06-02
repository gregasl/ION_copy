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
    
    private final Object dataLoadedLock = new Object();
    private final Object instrumentDataLock = new Object();

    // Enum to standardize field names for type-safe lookups
    public enum InstrumentField {
        ID, ID0, ID1, ID2, ID3, ID4, ID5, ID6, ID7, ID8, ID9, ID10, ID11, ID12, ID13, ID14, ID15, 
        SRC0, SRC1, SRC2, SRC3, SRC4, SRC5, SRC6, SRC7, SRC8, SRC9, SRC10, SRC11, SRC12, SRC13, SRC14, SRC15,
        ATTRIBUTE0, ATTRIBUTE1, ATTRIBUTE2, ATTRIBUTE3, ATTRIBUTE4, ATTRIBUTE5, ATTRIBUTE6, ATTRIBUTE7, 
        ATTRIBUTE8, ATTRIBUTE9, ATTRIBUTE10, ATTRIBUTE11, ATTRIBUTE12, ATTRIBUTE13, ATTRIBUTE14, ATTRIBUTE15,
        ISAON }
    
    private final Map<String, EnumMap<InstrumentField, Object>> instrumentData = new ConcurrentHashMap<>();
    private boolean dataLoaded = false;
    private List<Runnable> dataLoadedCallbacks = new ArrayList<>();
    private long lastLogTime = System.currentTimeMillis();
    private int recordCounter = 0;
    private int lastNumberIndex = -1;
    
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
    public void registerDataLoadedCallback(Runnable callback) {
        synchronized(dataLoadedLock) {
            if (dataLoaded) {
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Data already loaded, executing callback immediately");
                callback.run();
            } else {
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Data not yet loaded, adding callback to queue");
                dataLoadedCallbacks.add(callback);
            }
        }
    }

    
    /**
     * Check if instrument data is fully loaded
     */
    public boolean isDataLoaded() {
        synchronized(dataLoadedLock) {
            return dataLoaded;
        }
    }

    /**
     * Force data loaded notification (used by timeout mechanism)
     */
    public void forceDataLoaded() {
        synchronized(dataLoadedLock) {
            if (!dataLoaded) {
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Forcing dataLoaded=true with " + instrumentData.size() + " instruments loaded");
                notifyDataLoaded();
            } else {
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Data already loaded, ignoring force request");
            }
        }
    }

    /**
     * Called internally when data loading is complete
     */
    private void notifyDataLoaded() {
        List<Runnable> callbacksToRun = null;
        
        synchronized(dataLoadedLock) {
            if (dataLoaded) {
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Data already loaded, ignoring duplicate notification");
                return;
            }
            
            dataLoaded = true;
            // Make a copy of the callbacks list to avoid holding lock during execution
            callbacksToRun = new ArrayList<>(dataLoadedCallbacks);
            dataLoadedCallbacks.clear();
        }
        
        // Execute callbacks outside of synchronized block
        int callbackIndex = 0;
        for (Runnable callback : callbacksToRun) {
            try {
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Executing callback #" + (callbackIndex + 1));
                callback.run();
                ApplicationLogging.logAsync(LOGGER, Level.INFO, "Callback #" + (callbackIndex + 1) + " executed successfully");
            } catch (Exception e) {
                ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error executing callback #" + (callbackIndex + 1));
                e.printStackTrace();
            }
            callbackIndex++;
        }
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
            String attributeValue = null;  // Store the attribute value for FENICS_USREPO
            
            for (int i = 0; i < lastNumberIndex + 1; i++) {  // Search through sources (Src0-Src10)
                try {
                    InstrumentField srcField = InstrumentField.valueOf("SRC" + i);
                    String src = (String) data.get(srcField);
                    
                    // Skip if the source doesn't match what we're looking for
                    if (src == null || !sourceId.equals(src)) continue;
                    
                    // Special handling for FENICS_USREPO - get the attribute value for this index
                    if ("FENICS_USREPO".equals(src)) {
                        try {
                            InstrumentField attributeField = InstrumentField.valueOf("ATTRIBUTE" + i);
                            Object attrValue = data.get(attributeField);
                            attributeValue = attrValue != null ? attrValue.toString() : null;
                            
                            if (LOGGER.isLoggable(Level.INFO)) {
                                ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                    "Found FENICS_USREPO with ATTRIBUTE " + i + " = " + attributeValue);
                            }
                        } catch (Exception e) {
                            ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                                "Error getting ATTRIBUTE value for FENICS_USREPO at index " + i, e);
                        }
                    }
                    
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
            
            // For FENICS_USREPO, append the attribute value to differentiate multiple instances
            if ("FENICS_USREPO".equals(sourceId) && attributeValue != null && !attributeValue.isEmpty()) {
                // Get the corresponding attribute for this source index
                InstrumentField attributeField = InstrumentField.valueOf("ATTRIBUTE" + sourceIndex);
                Object attributeObj = data.get(attributeField);
                
                if (attributeObj != null) {
                    // Append the attribute value to make the ID unique
                    nativeId = nativeId + "_" + attributeObj.toString();
                    
                    if (LOGGER.isLoggable(Level.INFO)) {
                        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                            "Modified FENICS_USREPO ID with attribute: " + nativeId);
                    }
                }
            }
            
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
                
                for (int i = 0; i < lastNumberIndex + 1; i++) {  // Expanded to check more sources
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
                                
                                // Special handling for FENICS_USREPO source - check Attribute field
                                String effectiveSource = src;
                                if ("FENICS_USREPO".equals(src)) {
                                    try {
                                        InstrumentField attributeField = InstrumentField.valueOf("ATTRIBUTE" + i);
                                        Object attributeValue = recordData.get(attributeField);
                                        
                                        if (attributeValue != null) {
                                            String attrStr = attributeValue.toString();
                                            if ("BGC".equals(attrStr)) {
                                                effectiveSource = "BGC";
                                                if (LOGGER.isLoggable(Level.INFO)) {
                                                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                                        "FENICS_USREPO source with BGC attribute found for " + 
                                                        instrumentId + " at index " + i + ", using BGC as source");
                                                }
                                            } else if ("FENICS_USREPO".equals(attrStr)) {
                                                // Keep as FENICS_USREPO
                                                if (LOGGER.isLoggable(Level.INFO)) {
                                                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                                        "FENICS_USREPO source with FENICS_USREPO attribute found for " + 
                                                        instrumentId + " at index " + i);
                                                }
                                            } else {
                                                if (LOGGER.isLoggable(Level.INFO)) {
                                                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                                        "FENICS_USREPO source with unknown attribute value: " + attrStr + 
                                                        " for " + instrumentId + " at index " + i + ", using default FENICS_USREPO");
                                                }
                                            }
                                        }
                                    } catch (Exception e) {
                                        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
                                            "Error processing Attribute for FENICS_USREPO source: " + 
                                            instrumentId + " at index " + i, e);
                                    }
                                }
                                
                                // Use the composite key format for consistency with lookup
                                String mapKey = effectiveSource + (isAonId ? "_AON" : "");
                                sourceToNativeMap.put(mapKey, nativeId);
                                
                                if (LOGGER.isLoggable(Level.INFO)) {
                                    if (!effectiveSource.equals(src)) {
                                        ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                            "Mapped source " + src + " to effective source " + effectiveSource +
                                            " for instrument: " + instrumentId);
                                    }
                                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
                                        "Added mapping: " + mapKey + " -> " + nativeId);
                                }
                                
                                // Handle Dealerweb specially - create both AON and non-AON mappings
                                if ("DEALERWEB_REPO".equals(effectiveSource)) {
                                    foundDealerwebMapping = true;
                                    
                                    if (isAonId) {
                                        // If this is an AON ID, also create a non-AON entry
                                        String nonAonMapKey = effectiveSource; // No _AON suffix for non-AON key
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
                                        String aonMapKey = effectiveSource + "_AON";
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
    	boolean subscriptionComplete = false;
    	public void onPublish(MkvObject mkvObject, boolean start, boolean download) {
    	    if ((start) && (mkvObject.getName().toString().equals(INSTRUMENT_PATTERN)) &&
    	        (mkvObject.getMkvObjectType().equals(MkvObjectType.PATTERN))) {
    	        try {
    	            ApplicationLogging.logAsync(LOGGER, Level.INFO, "Found instrument pattern: " + mkvObject.getName());
    	            MkvRecordListener listener = new DataListener();
    	            
    	            // Initialize the full field list
    	            List<String> fieldsList = new ArrayList<>(Arrays.asList(
    	            	    "Id", "IsAon",
    	            	    "Id0", "Src0", "Attribute0",
    	            	    "Id1", "Src1", "Attribute1",
    	            	    "Id2", "Src2", "Attribute2",
    	            	    "Id3", "Src3", "Attribute3",
    	            	    "Id4", "Src4", "Attribute4",
    	            	    "Id5", "Src5", "Attribute5",
    	            	    "Id6", "Src6", "Attribute6",
    	            	    "Id7", "Src7", "Attribute7",
    	            	    "Id8", "Src8", "Attribute8",
    	            	    "Id9", "Src9", "Attribute9",
    	            	    "Id10", "Src10", "Attribute10",
    	            	    "Id11", "Src11", "Attribute11",
    	            	    "Id12", "Src12", "Attribute12",
    	            	    "Id13", "Src13", "Attribute13",
    	            	    "Id14", "Src14", "Attribute14",
    	            	    "Id15", "Src15", "Attribute15"
    	            	));
    	            
    	            boolean subscribed = false;
    	            while (!subscribed && !fieldsList.isEmpty()) {
    	                try {
    	                    String[] fields = fieldsList.toArray(new String[0]);
    	                    ((MkvPattern) mkvObject).subscribe(fields, listener);
    	                    subscribed = true;
    	                    lastNumberIndex = getLastNumberIndex(fields);
    	                    subscriptionComplete = true;
    	                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
    	                        "Successfully subscribed to instrument pattern: " + mkvObject.getName() + 
    	                        " with " + fieldsList.size() + " fields");
    	                } catch (Exception e) {
    	                    // If we have any fields left to remove
    	                    if (fieldsList.size() > 1) {
    	                        // Remove the last field (IsAon is the last in initial list)
    	                    	String lastField = fieldsList.remove(fieldsList.size() - 1);
    	                    	String secondLastField = fieldsList.remove(fieldsList.size() - 1);
    	                        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
    	                            "Subscription failed with field: " + lastField + " " + secondLastField +
    	                            ". Retrying with " + fieldsList.size() + " fields");
    	                    } else {
    	                        // If we've removed all fields and still failing, rethrow the exception
    	                        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
    	                            "Failed to subscribe to instrument pattern: " + mkvObject.getName() + 
    	                            " even with minimal fields");
    	                        throw e;
    	                    }
    	                }
    	            }
    	        } catch (Exception e) {
    	            ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
    	                "Error subscribing to instrument pattern: " + mkvObject.getName());
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
        	            List<String> fieldsList = new ArrayList<>(Arrays.asList(
        	            	    "Id", "IsAon",
        	            	    "Id0", "Src0", "Attribute0",
        	            	    "Id1", "Src1", "Attribute1",
        	            	    "Id2", "Src2", "Attribute2",
        	            	    "Id3", "Src3", "Attribute3",
        	            	    "Id4", "Src4", "Attribute4",
        	            	    "Id5", "Src5", "Attribute5",
        	            	    "Id6", "Src6", "Attribute6",
        	            	    "Id7", "Src7", "Attribute7",
        	            	    "Id8", "Src8", "Attribute8",
        	            	    "Id9", "Src9", "Attribute9",
        	            	    "Id10", "Src10", "Attribute10",
        	            	    "Id11", "Src11", "Attribute11",
        	            	    "Id12", "Src12", "Attribute12",
        	            	    "Id13", "Src13", "Attribute13",
        	            	    "Id14", "Src14", "Attribute14",
        	            	    "Id15", "Src15", "Attribute15"
        	            	));
                        
                        boolean subscribed = false;
        	            while (!subscribed && !fieldsList.isEmpty()) {
        	                try {
        	                    String[] fields = fieldsList.toArray(new String[0]);
        	                    ((MkvPattern) mkvObj).subscribe(fields, listener);
        	                    subscribed = true;
        	                    lastNumberIndex = getLastNumberIndex(fields);
        	                    subscriptionComplete = true;
        	                    ApplicationLogging.logAsync(LOGGER, Level.INFO, 
        	                        "Successfully subscribed to instrument pattern at idle: " + mkvObj.getName() + 
        	                        " with " + fieldsList.size() + " fields");
        	                } catch (Exception e) {
        	                    // If we have any fields left to remove
        	                    if (fieldsList.size() > 1) {
        	                        // Remove the last field (IsAon is the last in initial list)
        	                    	String lastField = fieldsList.remove(fieldsList.size() - 1);
        	                    	String secondLastField = fieldsList.remove(fieldsList.size() - 1);
        	                        ApplicationLogging.logAsync(LOGGER, Level.WARNING, 
        	                            "Subscription failed with field during idle: " + lastField + " " + secondLastField +
        	                            ". Retrying with " + fieldsList.size() + " fields");
        	                    } else {
        	                        // If we've removed all fields and still failing, rethrow the exception
        	                        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, 
        	                            "Failed to subscribe to instrument pattern during: " + mkvObj.getName() + 
        	                            " even with minimal fields");
        	                        throw e;
        	                    }
        	                }
        	            }
                    } catch (Exception e) {
                        ApplicationLogging.logAsync(LOGGER, Level.SEVERE, "Error subscribing to instrument pattern during idle: " + mkvObj.getName());
                        e.printStackTrace();
                    }
                }
                
                // Check if we have enough instruments to consider data loading complete
                if (subscriptionComplete && instrumentData.size() > 2000 && !dataLoaded) {
                    ApplicationLogging.logAsync(LOGGER, Level.INFO, "More than 2000 instruments loaded, considering data loading complete");
                    notifyDataLoaded();
                }
                
                // If this is the end of all downloads, notify data loaded
                if (subscriptionComplete && component.equals(INSTRUMENT_PATTERN)) {
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
    
    /**
     * Finds the highest numerical index in the subscribed fields.
     * @param fields Array of field names
     * @return The highest index number found, or -1 if none found
     */
    private int getLastNumberIndex(String[] fields) {
        int maxIndex = -1;
        
        for (String field : fields) {
            // Look for fields that start with "Id" or "Src" followed by digits
            if ((field.startsWith("Id") || field.startsWith("Src")) && field.length() > 2) {
                try {
                    // Extract the numeric part
                    String numberPart = field.replaceAll("^(Id|Src)", "");
                    if (!numberPart.isEmpty()) {
                        int index = Integer.parseInt(numberPart);
                        maxIndex = Math.max(maxIndex, index);
                    }
                } catch (NumberFormatException e) {
                    // Not a numeric field, just continue
                }
            }
        }
        
        return maxIndex;
    }
    
}