package com.iontrading.automatedMarketMaking;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvPattern;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BondEligibilityListener monitors bond eligibility for market making
 * by listening to MKV data streams and determining which bonds are eligible
 * for automated market making.
 */
public class BondEligibilityListener implements MkvRecordListener, MkvPublishListener {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(BondEligibilityListener.class);
    
    // Store eligible bonds
    private final Set<String> eligibleBonds = ConcurrentHashMap.newKeySet();
    
    // Store bond to instrument mapping
    public final Map<String, String> bondToInstrumentMap = new ConcurrentHashMap<>();
    
    // Store bond data
    private final Map<String, BondConsolidatedData> consolidatedBondData = new ConcurrentHashMap<>();
    
    // Listeners for eligibility changes
    private final List<EligibilityChangeListener> eligibilityListeners = new ArrayList<>();
    
    // Scheduler for periodic tasks
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private final AtomicLong lastUpdateTimestamp = new AtomicLong(0);
    private final AtomicLong consecutiveErrorCount = new AtomicLong(0);
    
    /**
     * Constructor
     */
    public BondEligibilityListener() {
        LOGGER.info("BondEligibilityListener initialized");

        // Start periodic eligibility check
        scheduler.scheduleAtFixedRate(
            this::performPeriodicEligibilityCheck,
            60, // Initial delay (seconds)
            300, // Run every 5 minutes
            TimeUnit.SECONDS
        );
        
        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down BondEligibilityListener scheduler");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }));
    }
   


    /**
     * Add eligibility change listener
     */
    public void addEligibilityChangeListener(EligibilityChangeListener listener) {
        synchronized (eligibilityListeners) {
            eligibilityListeners.add(listener);
        }
        LOGGER.debug("Added eligibility change listener: {}", listener.getClass().getSimpleName());
    }
    
    /**
     * Remove eligibility change listener
     */
    public void removeEligibilityChangeListener(EligibilityChangeListener listener) {
        synchronized (eligibilityListeners) {
            eligibilityListeners.remove(listener);
        }
        LOGGER.debug("Removed eligibility change listener: {}", listener.getClass().getSimpleName());
    }
    
    /**
     * Notify listeners of eligibility change
     */
    private void notifyEligibilityChange(String cusip, boolean isEligible, Map<String, Object> bondData) {
        synchronized (eligibilityListeners) {
            for (EligibilityChangeListener listener : eligibilityListeners) {
                try {
                    listener.onEligibilityChange(cusip, isEligible, bondData);
                } catch (Exception e) {
                    LOGGER.error("Error notifying eligibility listener: {}", e.getMessage(), e);
                }
            }
        }
    }
    
    /**
     * Get current set of eligible bonds
     */
    public Set<String> getEligibleBonds() {
        return new HashSet<>(eligibleBonds);
    }
    
    /**
     * Check if a specific bond is eligible
     */
    public boolean isBondEligible(String cusip) {
        return eligibleBonds.contains(cusip);
    }
    
    /**
     * Get instrument ID for a bond
     */
    public String getInstrumentIdForBond(String bondId) {
        return bondToInstrumentMap.get(bondId);
    }
    
    /**
     * Get bond data
     */
    public Map<String, Object> getBondData(String cusip) {
        BondConsolidatedData data = consolidatedBondData.get(cusip);
        return data != null ? data.getConsolidatedView() : null;
    }
    
    /**
     * Add a bond as eligible for market making
     */
    public void addEligibleBond(String cusip, String instrumentId, Map<String, Object> bondData) {
        boolean wasEligible = eligibleBonds.contains(cusip);
        
        eligibleBonds.add(cusip);
        bondToInstrumentMap.put(cusip, instrumentId);
        
        // Update to use consolidatedBondData instead of bondDataMap
        BondConsolidatedData consolidatedData = consolidatedBondData.computeIfAbsent(
            cusip, k -> new BondConsolidatedData(k));
        
        // Add data based on prefix conventions
        for (Map.Entry<String, Object> entry : bondData.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("STATIC_")) {
                consolidatedData.updateStaticData(
                    Collections.singletonMap(key.substring(7), entry.getValue()));
            } else if (key.startsWith("POS_")) {
                consolidatedData.updatePositionData(
                    Collections.singletonMap(key.substring(4), entry.getValue()));
            } else if (key.startsWith("SDS_")) {
                consolidatedData.updateSdsData(
                    Collections.singletonMap(key.substring(4), entry.getValue()));
            } else {
                // Default to static data for unprefixed fields
                consolidatedData.updateStaticData(
                    Collections.singletonMap(key, entry.getValue()));
            }
        }
        
        if (!wasEligible) {
            LOGGER.info("Bond {} added to eligible list (instrument: {})", cusip, instrumentId);
            notifyEligibilityChange(cusip, true, consolidatedData.getConsolidatedView());
        }
    }

        
    /**
     * Remove a bond from eligible list
     */
    public void removeEligibleBond(String cusip) {
        boolean wasEligible = eligibleBonds.contains(cusip);
        
        eligibleBonds.remove(cusip);
        String instrumentId = bondToInstrumentMap.remove(cusip);
        
        // Update to use consolidatedBondData instead of bondDataMap
        BondConsolidatedData bondData = consolidatedBondData.get(cusip);
        Map<String, Object> consolidatedView = bondData != null ? 
            bondData.getConsolidatedView() : null;
        
        if (wasEligible) {
            LOGGER.info("Bond {} removed from eligible list (instrument: {})", cusip, instrumentId);
            notifyEligibilityChange(cusip, false, consolidatedView);
        }
    }
    
    /**
     * Determine if a bond should be eligible based on criteria
     */
    private boolean shouldBondBeEligible(String cusip, Object bondDataObj) {
        try {
            Map<String, Object> bondData;
            
            // Handle different input types
            if (bondDataObj instanceof BondConsolidatedData) {
                // Extract consolidated view from BondConsolidatedData
                bondData = ((BondConsolidatedData) bondDataObj).getConsolidatedView();
            } else if (bondDataObj instanceof Map) {
                // Use provided map directly
                @SuppressWarnings("unchecked")
                Map<String, Object> typedMap = (Map<String, Object>) bondDataObj;
                bondData = typedMap;
            } else {
                LOGGER.warn("Unsupported bond data type for eligibility check: {}", 
                    bondDataObj != null ? bondDataObj.getClass().getName() : "null");
                return false;
            }
            
            // Check if bond has required data
            if (bondData == null || bondData.isEmpty()) {
                LOGGER.debug("Bond {} ineligible: no bond data", cusip);
                return false;
            }
            
            // Check maturity date (must be at least 2 months from now)
            Object maturityObj = bondData.get("Maturity");
            if (maturityObj == null) {
                maturityObj = bondData.get("STATIC_Maturity");
            }
            if (maturityObj != null) {
                try {
                    // Parse the maturity date - adapt format as needed
                    String maturityStr = maturityObj.toString();
                    
                    // Try different date formats
                    java.time.LocalDate maturityDate = null;
                    
                    try {
                        // Try yyyy-MM-dd format
                        maturityDate = java.time.LocalDate.parse(maturityStr);
                    } catch (Exception e1) {
                        try {
                            // Try MM/dd/yyyy format
                            java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("MM/dd/yyyy");
                            maturityDate = java.time.LocalDate.parse(maturityStr, formatter);
                        } catch (Exception e2) {
                            try {
                                // Try yyyyMMdd format
                                java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");
                                maturityDate = java.time.LocalDate.parse(maturityStr, formatter);
                            } catch (Exception e3) {
                                LOGGER.debug("Could not parse maturity date for bond {}: {}", cusip, maturityStr);
                            }
                        }
                    }
                    
                    if (maturityDate != null) {
                        // Calculate if maturity is at least 2 months away
                        java.time.LocalDate twoMonthsFromNow = java.time.LocalDate.now().plusMonths(2);
                        
                        if (maturityDate.isBefore(twoMonthsFromNow)) {
                            LOGGER.debug("Bond {} ineligible: maturity date {} is less than 2 months away", 
                                cusip, maturityDate);
                            return false;
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("Error checking maturity for bond {}: {}", cusip, e.getMessage());
                }
            }
            
            // Check SOMA holdings (must be at least $1 billion)
            Object somaObj = bondData.get("SOMA");
            if (somaObj == null) {
                somaObj = bondData.get("SDS_SOMA");
            }
            
            if (somaObj != null) {
                try {
                    double soma = Double.parseDouble(somaObj.toString());
                    
                    // Check if SOMA is less than $1 billion
                    if (soma < 1_000_000_000) {
                        LOGGER.debug("Bond {} ineligible: SOMA holdings {} is less than $1 billion", 
                            cusip, soma);
                        return false;
                    }
                } catch (Exception e) {
                    LOGGER.warn("Error checking SOMA for bond {}: {}", cusip, e.getMessage());
                }
            }
            
            // Check CalcNetExtPos for the current date (must be at least -$200 million)
            Object calcNetExtPosObj = bondData.get("CalcNetExtPos");
            if (calcNetExtPosObj == null) {
                calcNetExtPosObj = bondData.get("POS_CalcNetExtPos");
            }
            
            if (calcNetExtPosObj != null) {
                try {
                    double calcNetExtPos = Double.parseDouble(calcNetExtPosObj.toString());
                    
                    // Check if CalcNetExtPos is less than -$200 million
                    if (calcNetExtPos < -200_000_000) {
                        LOGGER.debug("Bond {} ineligible: CalcNetExtPos {} is less than -$200 million", 
                            cusip, calcNetExtPos);
                        return false;
                    }
                } catch (Exception e) {
                    LOGGER.warn("Error checking CalcNetExtPos for bond {}: {}", cusip, e.getMessage());
                }
            }
            
            // Check DateStart matches current date if available
            Object dateStartObj = bondData.get("DateStart");
            if (dateStartObj == null) {
                dateStartObj = bondData.get("SDS_DateStart");
            }
            
            if (dateStartObj != null && calcNetExtPosObj != null) {
                try {
                    String dateStartStr = dateStartObj.toString();
                    
                    // Try to parse the DateStart using various formats
                    java.time.LocalDate dateStart = null;
                    
                    try {
                        // Try yyyy-MM-dd format
                        dateStart = java.time.LocalDate.parse(dateStartStr);
                    } catch (Exception e1) {
                        try {
                            // Try MM/dd/yyyy format
                            java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("MM/dd/yyyy");
                            dateStart = java.time.LocalDate.parse(dateStartStr, formatter);
                        } catch (Exception e2) {
                            try {
                                // Try yyyyMMdd format
                                java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");
                                dateStart = java.time.LocalDate.parse(dateStartStr, formatter);
                            } catch (Exception e3) {
                                LOGGER.debug("Could not parse DateStart for bond {}: {}", cusip, dateStartStr);
                            }
                        }
                    }
                    
                    // If we successfully parsed the date, check if it's current
                    if (dateStart != null) {
                        java.time.LocalDate today = java.time.LocalDate.now();
                        
                        // If DateStart is more than 1 day off from today, position data might be stale
                        if (java.time.temporal.ChronoUnit.DAYS.between(dateStart, today) > 1) {
                            LOGGER.debug("Bond {} position data may be stale - DateStart is {}", 
                                cusip, dateStart);
                            // Don't return false here, just log a warning - you can make this stricter if needed
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("Error checking DateStart for bond {}: {}", cusip, e.getMessage());
                }
            }
            
            // Bond passes all criteria
            return true;
            
        } catch (Exception e) {
            LOGGER.error("Error evaluating eligibility for bond {}: {}", cusip, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Periodic eligibility check
     */
    private void performPeriodicEligibilityCheck() {
        try {
            LOGGER.debug("Performing periodic eligibility check for {} bonds", consolidatedBondData.size());
            
            int eligibleCount = 0;
            int ineligibleCount = 0;
            
            // Check all bonds we have data for
            for (Map.Entry<String, BondConsolidatedData> entry : consolidatedBondData.entrySet()) {
                String cusip = entry.getKey();
                BondConsolidatedData bondData = entry.getValue();
                
                boolean shouldBeEligible = shouldBondBeEligible(cusip, bondData);
                boolean currentlyEligible = eligibleBonds.contains(cusip);
                
                if (shouldBeEligible && !currentlyEligible) {
                    // Bond became eligible
                    String instrumentId = extractInstrumentId(cusip, bondData);
                    if (instrumentId != null) {
                        // Using the BondConsolidatedData directly
                        eligibleBonds.add(cusip);
                        bondToInstrumentMap.put(cusip, instrumentId);
                        notifyEligibilityChange(cusip, true, bondData.getConsolidatedView());
                        eligibleCount++;
                        
                        LOGGER.info("Bond {} added to eligible list (instrument: {})", cusip, instrumentId);
                    }
                } else if (!shouldBeEligible && currentlyEligible) {
                    // Bond became ineligible
                    removeEligibleBond(cusip);
                    ineligibleCount++;
                }
            }
            
            if (eligibleCount > 0 || ineligibleCount > 0) {
                LOGGER.info("Eligibility check complete: {} added, {} removed, {} total eligible", 
                    eligibleCount, ineligibleCount, eligibleBonds.size());
            }
            
        } catch (Exception e) {
            LOGGER.error("Error in periodic eligibility check: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Create instrument ID for CUSIP
     */
    private String createInstrumentIdForCusip(String cusip) {
        
        //return cusip + "_REG_Fixed";
        return cusip + "_C_Fixed";
    }

    /**
     * Extract instrument ID from bond data
     * This works with both consolidated data objects and raw extracted data
     */
    private String extractInstrumentId(String cusip, Object bondDataObj) {
        try {
            // Handle the case where we get a BondConsolidatedData object
            if (bondDataObj instanceof BondConsolidatedData) {
                BondConsolidatedData bondData = (BondConsolidatedData) bondDataObj;
                
                // Try each data source in priority order
                
                // 1. Check static data for instrument ID
                Map<String, Object> staticData = bondData.getStaticData();
                if (staticData != null) {
                    Object instrumentId = staticData.get("InstrumentId");
                    if (instrumentId != null) {
                        return instrumentId.toString();
                    }
                }
                
                // 2. Check position data for instrument ID or references
                Map<String, Object> positionData = bondData.getPositionData();
                if (positionData != null) {
                    Object instrumentId = positionData.get("InstrumentId");
                    if (instrumentId != null) {
                        return instrumentId.toString();
                    }
                }
                
                // 3. Check SDS data for any relevant mapping
                Map<String, Object> sdsData = bondData.getSdsData();
                if (sdsData != null) {
                    Object instrumentId = sdsData.get("InstrumentId");
                    if (instrumentId != null) {
                        return instrumentId.toString();
                    }
                }
            }
            // Handle regular Map<String, Object>
            else if (bondDataObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> bondData = (Map<String, Object>) bondDataObj;
                
                // First try direct instrument ID
                Object instrumentId = bondData.get("InstrumentId");
                if (instrumentId != null) {
                    return instrumentId.toString();
                }
                
                // Then try with prefixes
                String[] prefixes = {"STATIC_", "POS_", "SDS_"};
                for (String prefix : prefixes) {
                    Object prefixedId = bondData.get(prefix + "InstrumentId");
                    if (prefixedId != null) {
                        return prefixedId.toString();
                    }
                }
                
                // Look for related fields that might contain instrument ID
                String[] relatedFields = {"SecurityId", "InstrRef", "Id"};
                for (String field : relatedFields) {
                    Object relatedValue = bondData.get(field);
                    if (relatedValue != null && relatedValue.toString().contains("INSTRUMENT")) {
                        return relatedValue.toString();
                    }
                }
            }
            
            // Check if we already have a mapping
            String existingMapping = bondToInstrumentMap.get(cusip);
            if (existingMapping != null) {
                return existingMapping;
            }
            
            // Fallback: construct instrument ID from CUSIP
            LOGGER.debug("Using fallback instrument ID for CUSIP: {}", cusip);
            return "USD.CM_INSTRUMENT.VMO_REPO_US." + cusip + "_C_Fixed";
            
        } catch (Exception e) {
            LOGGER.error("Error extracting instrument ID for bond {}: {}", cusip, e.getMessage(), e);
            return null;
        }
    }
    
    // Add this method to the BondEligibilityListener class
    @Override
    public void onSubscribe(MkvObject mkvObject) {
        // Default implementation - not needed for this listener
        LOGGER.debug("onSubscribe called for object: {}", mkvObject.getName());
    }

    // MkvRecordListener implementation
    @Override
    public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
        // Not implemented for this listener
    }
    
    @Override
    public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
        // Default implementation - not required for this listener
        LOGGER.debug("onPublish called for object: {}, pub_unpub: {}, dwl: {}", 
                    mkvObject.getName(), pub_unpub, dwl);
    }

    @Override
    public void onPublishIdle(String component, boolean start) {
        // Default implementation - not required for this listener
        LOGGER.debug("onPublishIdle called for component: {}, start: {}", component, start);
    }

    @Override
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
            boolean isSnapshot) {
        try {
            // Update monitoring counters
            LOGGER.info("Received full update in BondEligibilityListener for record: {}", mkvRecord.getName());
            lastUpdateTimestamp.set(System.currentTimeMillis());
            
            String recordName = mkvRecord.getName();
            LOGGER.debug("Received record update: {} (snapshot: {})", recordName, isSnapshot);
            
            // Extract CUSIP based on record type
            String cusip = null;
            String recordType = null;
            
            if (recordName.startsWith("ALL.POSITION_US.SDS.")) {
                // Example: ALL.POSITION_US.SDS.91282CMM0:20250530
                cusip = extractCusipFromSdsRecord(recordName);
                recordType = "SDS";
            } 
            else if (recordName.startsWith("USD.CM_BOND.VMO_REPO_US.")) {
                // Example: USD.CM_BOND.VMO_REPO_US.91282CMM0
                cusip = extractCusipFromBondRecord(recordName);
                recordType = "BOND";
            }
            else if (recordName.startsWith("USD.IU_POSITION.VMO_REPO_US.")) {
                // Example: USD.IU_POSITION.VMO_REPO_US.91282CMM0_20250530_STD
                cusip = extractCusipFromPositionRecord(recordName);
                recordType = "POSITION";
            }
            else {
                LOGGER.debug("Ignoring record {}: unrecognized pattern", recordName);
                return;
            }
            
            if (cusip == null) {
                LOGGER.warn("Could not extract CUSIP from record: {}", recordName);
                return;
            }
            
            // Get or create consolidated data object
            BondConsolidatedData bondData = consolidatedBondData.computeIfAbsent(
                cusip, k -> new BondConsolidatedData(k));
            
            // Extract data using cursor iteration
            Map<String, Object> extractedData = extractDataWithCursor(mkvRecord);
            
            // Update appropriate section based on record type
            switch (recordType) {
                case "SDS":
                    LOGGER.debug("Updating SDS data for CUSIP: {}", cusip);
                    bondData.updateSdsData(extractedData);
                    break;
                    
                case "BOND":
                    LOGGER.debug("Updating static bond data for CUSIP: {}", cusip);
                    bondData.updateStaticData(extractedData);
                    
                    // Extract instrument ID if available
                    Object instrumentId = extractedData.get("InstrumentId");
                    if (instrumentId != null) {
                        bondToInstrumentMap.put(cusip, instrumentId.toString());
                    }
                    break;
                    
                case "POSITION":
                    LOGGER.debug("Updating position data for CUSIP: {}", cusip);
                    bondData.updatePositionData(extractedData);
                    break;
            }
            
            // Check if we should update eligibility status
            evaluateBondEligibility(cusip, bondData);
            
        } catch (Exception e) {
            LOGGER.error("Error processing update: {}", e.getMessage(), e);
            consecutiveErrorCount.incrementAndGet();
        }
    }
    
    /**
     * Extract data from record using cursor iteration (following the DepthListener pattern)
     */
    private Map<String, Object> extractDataWithCursor(MkvRecord mkvRecord) {
        Map<String, Object> extractedData = new ConcurrentHashMap<>();
        
        try {
            // Use the supply's cursor mechanism for iteration
            MkvSupply mkvSupply = mkvRecord.getSupply();
            if (mkvSupply == null) {
                LOGGER.warn("No supply available for record: {}", mkvRecord.getName());
                return extractedData;
            }
            
            // Start with the first field index
            int cursor = mkvSupply.firstIndex();
            
            // Track if anything changed
            boolean changed = false;
            
            // Process all fields using cursor iteration
            while (cursor != -1) {
                try {
                    // Get the field name and value using the cursor
                    String fieldName = mkvRecord.getMkvType().getFieldName(cursor);
                    Object fieldValue = mkvSupply.getObject(cursor);
                    
                    // Store the field if it has a name and value
                    if (fieldName != null) {
                        Object oldValue = extractedData.get(fieldName);
                        
                        if (fieldValue != null && !fieldValue.equals(oldValue)) {
                            extractedData.put(fieldName, fieldValue);
                            changed = true;
                            LOGGER.debug("Field updated: {} = {}", fieldName, fieldValue);
                        } else if (fieldValue == null && oldValue != null) {
                            extractedData.put(fieldName, "null");
                            changed = true;
                            LOGGER.debug("Field nulled: {}", fieldName);
                        }
                    }
                    
                    // Move to the next field
                    cursor = mkvSupply.nextIndex(cursor);
                    
                } catch (Exception e) {
                    LOGGER.warn("Error processing field at cursor {}: {}", cursor, e.getMessage());
                    // Try to continue with the next field
                    if (cursor != -1) {
                        cursor = mkvSupply.nextIndex(cursor);
                    } else {
                        break;
                    }
                }
            }
            
            if (changed) {
                LOGGER.debug("Extracted {} fields with changes from record {}", 
                    extractedData.size(), mkvRecord.getName());
            }
            
        } catch (Exception e) {
            LOGGER.error("Error in cursor iteration for record {}: {}", mkvRecord.getName(), e.getMessage(), e);
        }
        
        return extractedData;
    }

    /**
     * Extract CUSIP from SDS record
     * Example: ALL.POSITION_US.SDS.91282CMM0:20250530
     */
    private String extractCusipFromSdsRecord(String recordName) {
        try {
            if (recordName == null) return null;
            
            LOGGER.info("Extracting CUSIP from SDS record: {}", recordName);
            String[] parts = recordName.split("\\.");
            if (parts.length < 4) return null;
            
            String lastPart = parts[3];
            
            // Handle the case where there's a date after CUSIP
            int colonIndex = lastPart.indexOf(':');
            if (colonIndex > 0) {
                lastPart = lastPart.substring(0, colonIndex);
            }
            
            if (isValidCusip(lastPart)) {
                return lastPart;
            }
            
            return null;
        } catch (Exception e) {
            LOGGER.error("Error extracting CUSIP from SDS record: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Extract CUSIP from bond record
     * Example: USD.CM_BOND.VMO_REPO_US.91282CMM0
     */
    private String extractCusipFromBondRecord(String recordName) {
        try {
            if (recordName == null) return null;
            LOGGER.info("Extracting CUSIP from bond record: {}", recordName);
            String[] parts = recordName.split("\\.");
            if (parts.length < 4) return null;
            
            String lastPart = parts[3];
            
            if (isValidCusip(lastPart)) {
                return lastPart;
            }
            
            return null;
        } catch (Exception e) {
            LOGGER.error("Error extracting CUSIP from bond record: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Extract CUSIP from position record
     * Example: USD.IU_POSITION.VMO_REPO_US.91282CMM0_20250530_STD
     */
    private String extractCusipFromPositionRecord(String recordName) {
        try {
            if (recordName == null) return null;
            LOGGER.info("Extracting CUSIP from position record: {}", recordName);
            String[] parts = recordName.split("\\.");
            if (parts.length < 4) return null;
            
            String lastPart = parts[3];
            
            // Extract CUSIP from complex position ID
            String[] subParts = lastPart.split("_");
            if (subParts.length > 0) {
                String potentialCusip = subParts[0];
                if (isValidCusip(potentialCusip)) {
                    return potentialCusip;
                }
            }
            
            return null;
        } catch (Exception e) {
            LOGGER.error("Error extracting CUSIP from position record: {}", e.getMessage(), e);
            return null;
        }
    }
    
    // MkvPublishListener implementation
    public void onSubscribe(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
        // Not implemented for this listener
    }

    /**
     * Validate CUSIP format
     */
    private boolean isValidCusip(String cusip) {
        // Basic validation - adjust as needed
        return cusip != null && cusip.length() == 9;
    }

    /**
     * Evaluate bond eligibility based on consolidated data
     */
    private void evaluateBondEligibility(String cusip, BondConsolidatedData bondData) {
        try {
            boolean shouldBeEligible = shouldBondBeEligible(cusip, bondData);
            boolean currentlyEligible = eligibleBonds.contains(cusip);
            
            if (shouldBeEligible && !currentlyEligible) {
                // Get instrument ID from mapped data or create one
                String instrumentId = bondToInstrumentMap.get(cusip);
                if (instrumentId == null) {
                    instrumentId = createInstrumentIdForCusip(cusip);
                    bondToInstrumentMap.put(cusip, instrumentId);
                }
                
                // Add to eligible list
                eligibleBonds.add(cusip);
                LOGGER.info("Bond {} added to eligible list (instrument: {})", cusip, instrumentId);
                
                // Notify listeners with consolidated data
                notifyEligibilityChange(cusip, true, bondData.getConsolidatedView());
                
            } else if (!shouldBeEligible && currentlyEligible) {
                // Remove from eligible list
                eligibleBonds.remove(cusip);
                LOGGER.info("Bond {} removed from eligible list", cusip);
                
                // Notify listeners
                notifyEligibilityChange(cusip, false, bondData.getConsolidatedView());
            }
            
        } catch (Exception e) {
            LOGGER.error("Error evaluating eligibility for bond {}: {}", cusip, e.getMessage(), e);
        }
    }
    
    /**
     * Get status information for monitoring
     */
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new ConcurrentHashMap<>();
        status.put("eligibleBonds", eligibleBonds.size());
        status.put("totalBonds", consolidatedBondData.size());
        
        // Count bonds by data availability
        int withStaticOnly = 0;
        int withPositionOnly = 0;
        int withSdsOnly = 0;
        int withStaticAndPosition = 0;
        int withStaticAndSds = 0;
        int withPositionAndSds = 0;
        int withAllData = 0;
        
        for (BondConsolidatedData data : consolidatedBondData.values()) {
            boolean hasStatic = data.hasStaticData();
            boolean hasPosition = data.hasPositionData();
            boolean hasSds = data.hasSdsData();
            
            if (hasStatic && hasPosition && hasSds) withAllData++;
            else if (hasStatic && hasPosition) withStaticAndPosition++;
            else if (hasStatic && hasSds) withStaticAndSds++;
            else if (hasPosition && hasSds) withPositionAndSds++;
            else if (hasStatic) withStaticOnly++;
            else if (hasPosition) withPositionOnly++;
            else if (hasSds) withSdsOnly++;
        }
        
        status.put("bondsWithStaticOnly", withStaticOnly);
        status.put("bondsWithPositionOnly", withPositionOnly);
        status.put("bondsWithSdsOnly", withSdsOnly);
        status.put("bondsWithStaticAndPosition", withStaticAndPosition);
        status.put("bondsWithStaticAndSds", withStaticAndSds);
        status.put("bondsWithPositionAndSds", withPositionAndSds);
        status.put("bondsWithAllData", withAllData);
        status.put("listeners", eligibilityListeners.size());
        
        return status;
    }
}