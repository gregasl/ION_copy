package com.iontrading.automatedMarketMaking;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvLog;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;

/**
 * BondEligibilityListener monitors bond eligibility for market making
 * by listening to MKV data streams and determining which bonds are eligible
 * for automated market making.
 */
public class BondEligibilityListener implements MkvRecordListener, MkvPublishListener {
    
    private static MkvLog log = Mkv.getInstance().getLogManager().getLogFile("MarketMaker");
    private static IONLogger logger = new IONLogger(log, 2, "MarketOrder");
    // Store eligible bonds
    private final Set<String> eligibleBonds = ConcurrentHashMap.newKeySet();

    // Store bond to instrument mapping
    public final Map<String, Map<String, String>> bondToInstrumentMaps = new ConcurrentHashMap<>();    

    // Store bond data
    private final Map<String, BondConsolidatedData> consolidatedBondData = new ConcurrentHashMap<>();
    
    // Listeners for eligibility changes
    private final List<EligibilityChangeListener> eligibilityListeners = new ArrayList<>();
    
    // Scheduler for periodic tasks
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private final AtomicLong lastUpdateTimestamp = new AtomicLong(0);
    private final AtomicLong consecutiveErrorCount = new AtomicLong(0);
    
    private static final Pattern SDS_CUSIP_PATTERN = Pattern.compile("ALL\\.POSITION_US\\.SDS\\.([^:]+)(?::.+)?");
    private static final Pattern BOND_CUSIP_PATTERN = Pattern.compile("USD\\.CM_BOND\\.VMO_REPO_US\\.(.+)");
    private static final Pattern POSITION_CUSIP_PATTERN = Pattern.compile("USD\\.IU_POSITION\\.VMO_REPO_US\\.([^_]+)");
    // private static final Pattern MFA_CUSIP_PATTERN = Pattern.compile("ALL\\.STATISTICS\\.MFA\\.MFA_([^_]+)_");

    /**
     * Constructor
     */
    public BondEligibilityListener() {
        logger.info("BondEligibilityListener initialized");


        // Start periodic eligibility check
        scheduler.scheduleAtFixedRate(
            this::performPeriodicEligibilityCheck,
            60, // Initial delay (seconds)
            300, // Run every 5 minutes
            TimeUnit.SECONDS
        );
        
        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down BondEligibilityListener scheduler");
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
        logger.info("Added eligibility change listener: " + listener.getClass().getSimpleName());
    }

    /**
     * Remove eligibility change listener
     */
    public void removeEligibilityChangeListener(EligibilityChangeListener listener) {
        synchronized (eligibilityListeners) {
            eligibilityListeners.remove(listener);
        }
        logger.info("Removed eligibility change listener: " + listener.getClass().getSimpleName());

    }

    /**
     * Notify listeners of eligibility change
     */
    private void notifyEligibilityChange(String Id, boolean isEligible, Map<String, Object> bondData) {

        if (Id == null) {
            logger.error("notifyEligibilityChange: Id is null, cannot notify listeners");
            return;
        }
        
        synchronized (eligibilityListeners) {
            for (EligibilityChangeListener listener : eligibilityListeners) {
                try {
                    listener.onEligibilityChange(Id, isEligible, bondData);
                } catch (Exception e) {
                    logger.error("Error notifying eligibility listener: " + e.getMessage() + " " + e);

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
     * Check if a specific bond is eligible for a specific term code
     */
    public boolean isIdEligible(String Id) {
        if (Id == null) {
            logger.warn("isIdEligible: Id is null");
            return false;
        }
        return isEligible(Id);
    }

    /**
     * Get instrument ID for a bond
     */
    public String getInstrumentIdForBond(String bondId, String termCode) {
        if (bondId == null) {
            logger.warn("getInstrumentIdForBond: bondId is null, termCode=" + termCode);
            return null;
        }
    
        if (termCode == null) {
            logger.warn("getInstrumentIdForBond: termCode is null, bondId=" + bondId);
            return null;
        }

        Map<String, String> instrumentMap = bondToInstrumentMaps.get(bondId);
        if (instrumentMap == null) return null;
    
        return instrumentMap.get(termCode);
    }

    /**
     * Get bond data
     */
    public Map<String, Object> getBondData(String cusip) {
        BondConsolidatedData data = consolidatedBondData.get(cusip);
        return data != null ? data.getConsolidatedView() : null;
    }

    private boolean isEligible(String Id) {
        return eligibleBonds.contains(Id);
    }

    /**
     * Add a bond as eligible for market making
     */
    public void addEligibleBond(String Id, Map<String, Object> bondData) {
        boolean wasEligible = isIdEligible(Id);
        eligibleBonds.add(Id);

        // Update to use consolidatedBondData instead of bondDataMap
        BondConsolidatedData consolidatedData = consolidatedBondData.computeIfAbsent(
            Id, k -> new BondConsolidatedData(k));
        
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
            // } else if (key.startsWith("MFA_")) {
            //     consolidatedData.updateMfaData(
            //         Collections.singletonMap(key.substring(4), entry.getValue()));
            } else {
                // Default to static data for unprefixed fields
                consolidatedData.updateStaticData(
                    Collections.singletonMap(key, entry.getValue()));
            }
        }
        
        if (!wasEligible) {
            logger.info("Bond " + Id + " added to eligible list (iD: " + Id + ")");
   
            notifyEligibilityChange(Id, true, consolidatedData.getConsolidatedView());
        }
    }

    /**
     * Remove a bond from eligible list
     */
    public void removeEligibleBond(String Id) {
        boolean wasEligible = isIdEligible(Id);
        if (wasEligible) {
            eligibleBonds.remove(Id);
        }

        // Update to use consolidatedBondData instead of bondDataMap
        BondConsolidatedData bondData = consolidatedBondData.get(Id);
        Map<String, Object> consolidatedView = bondData != null ? 
            bondData.getConsolidatedView() : null;
        
        if (wasEligible) {
            logger.info("Bond " + Id + " removed from eligible list");

            notifyEligibilityChange(Id, false, consolidatedView);
        }
    }
    
    /**
     * Container for bond eligibility results by term code
     */
    public static class EligibilityResult {
        private final boolean eligibleForTermC;
        private final boolean eligibleForTermREG;
        
        public EligibilityResult(boolean eligibleForTermC, boolean eligibleForTermREG) {
            this.eligibleForTermC = eligibleForTermC;
            this.eligibleForTermREG = eligibleForTermREG;
        }

        public boolean isEligibleForTermC() {
            return eligibleForTermC;
        }
        
        public boolean isEligibleForTermREG() {
            return eligibleForTermREG;
        }
    }

    /**
     * Determine if a bond should be eligible based on criteria
     */
    private EligibilityResult shouldBondBeEligible(String cusip, Object bondDataObj) {
        try {
            Map<String, Object> bondData;
            boolean eligibleC = true;   
            boolean eligibleREG = true;   
            if (!isValidCusip(cusip)) {
                logger.info("Invalid CUSIP for eligibility check: " + cusip);
                return new EligibilityResult(false, false);
            }

            logger.info("Evaluating eligibility for bond & bondDataObj: " + cusip + " " + bondDataObj);

            boolean isStrip = (cusip.contains("91283") || cusip.contains("912800") || cusip.startsWith("912815") || cusip.startsWith("912820") || cusip.startsWith("912821") || cusip.startsWith("912803"));
            if (isStrip) {
                logger.info("Bond " + cusip + " is a strip bond, skipping eligibility check");
                return new EligibilityResult(false, false);
            }   

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
                
                logger.info("Unsupported bond data type for eligibility check: " + 
                    (bondDataObj != null ? bondDataObj.getClass().getName() : "null"));
                return new EligibilityResult(false, false);
            }

            // Check if bond has required data
            if (bondData == null || bondData.isEmpty()) {
                logger.debug("Bond " + cusip + " ineligible: no bond data");
                return new EligibilityResult(false, false);
            }

            // Check maturity date (must be at least 2 months from now)
            Object maturityObj = bondData.get("STATIC_DateMaturity");
            logger.info("Checking maturity date for bond " + cusip + ": " + maturityObj);
            
            java.time.LocalDate maturityDate = null;
            java.time.LocalDate twoMonthsFromNow = java.time.LocalDate.now().plusMonths(2);
            if (maturityObj != null) { 
                try {
                    // First check if it's an integer (MKV date format)
                    if (maturityObj instanceof Integer || maturityObj instanceof Long) {
                        // Convert integer YYYYMMDD format to LocalDate
                        int dateInt = Integer.parseInt(maturityObj.toString());
                        int year = dateInt / 10000;
                        int month = (dateInt % 10000) / 100;
                        int day = dateInt % 100;
                        maturityDate = java.time.LocalDate.of(year, month, day);
                        // Check if maturity date is at least 2 months from now
                        if (maturityDate.isBefore(twoMonthsFromNow)) {
                            
                            logger.info("Bond " + cusip + " ineligible: maturity date " + maturityDate + " is less than 2 months away");
                            
                            return new EligibilityResult(false, false);
                        }
                        
                        logger.info("Parsed maturity date from integer: " + dateInt + " → " + maturityDate);
                        
                    } else {
                        String maturityStr = maturityObj.toString();
                        // Try different string date formats in sequence
                        try {
                            // Try yyyy-MM-dd format
                            maturityDate = java.time.LocalDate.parse(maturityStr);
                        } catch (Exception e1) {
                            try {
                                // Try MM/dd/yyyy format
                                java.time.format.DateTimeFormatter formatter = 
                                    java.time.format.DateTimeFormatter.ofPattern("MM/dd/yyyy");
                                maturityDate = java.time.LocalDate.parse(maturityStr, formatter);
                                // Check if maturity date is at least 2 months from now
                                if (maturityDate.isBefore(twoMonthsFromNow)) {
                                    logger.info("Bond " + cusip + " ineligible: maturity date " + maturityDate + " is less than 2 months away");
                                    return new EligibilityResult(false, false);
                                }
                            } catch (Exception e2) {
                                try {
                                    // Try yyyyMMdd format as string
                                    java.time.format.DateTimeFormatter formatter = 
                                        java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");
                                    maturityDate = java.time.LocalDate.parse(maturityStr, formatter);
                                    // Check if maturity date is at least 2 months from now
                                    if (maturityDate.isBefore(twoMonthsFromNow)) {
                                        logger.info("Bond " + cusip + " ineligible: maturity date " + maturityDate + " is less than 2 months away");
                                        return new EligibilityResult(false, false);
                                    }
                                } catch (Exception e3) {
                                    logger.info("Could not parse maturity date for bond " + cusip + ": " + maturityStr);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Error parsing maturity date: " + e.getMessage());
                }
            } else {
                logger.info("No maturity date found for bond " + cusip);
                return new EligibilityResult(false, false);
            }

            // Check SOMA holdings (must be at least $1 billion)
            Object somaObj = bondData.get("SDS_SOMA");
            logger.info("Checking SOMA holdings for bond " + cusip + ": " + somaObj);

            if (somaObj != null) {
                try {
                    double soma = Double.parseDouble(somaObj.toString());
                    logger.info("Parsed SOMA value: " + somaObj + " → " + String.format("%.2f", soma));

                    // Check if SOMA is less than $1 billion
                    if (soma < 1_000_000_000) {
                        logger.info("Bond " + cusip + " ineligible: SOMA holdings " + soma + " is less than $1 billion");
                        return new EligibilityResult(false, false);
                    }
                } catch (Exception e) {
                    logger.warn("Error checking SOMA for bond " + cusip + ": " + e.getMessage());
                }
            }

            // Check POS for C (must be at least -$200 million)
            // Check CalcNetExtPos for the current date (must be at least -$200 million)
            Object termCodeObj = bondData.get("POS_TermCode");
            Object calcNetExtPosObj = bondData.get("POS_CalcNetExtPos");
            logger.info("Checking CalcNetExtPos for bond " + cusip + ": " + calcNetExtPosObj);

            if (calcNetExtPosObj != null && termCodeObj != null) {
                try {
                    double calcNetExtPos = Double.parseDouble(calcNetExtPosObj.toString());
                    String termCode = termCodeObj.toString();
                    logger.info("Parsed CalcNetExtPos value, TermCode: " + calcNetExtPosObj + " → $" + String.format("%,.2f", calcNetExtPos) + ", " + termCode);
                    // Check if CalcNetExtPos is less than -$200 million
                    if (calcNetExtPos < -200_000_000 && "C".equals(termCode)) {
                        logger.info("Bond " + cusip + " ineligible: CalcNetExtPos " + calcNetExtPos + " is less than -$200 million");
                        return new EligibilityResult(false, false);
                    }
                } catch (Exception e) {
                    logger.warn("Error checking CalcNetExtPos for bond " + cusip + ": " + e.getMessage());
                }
            }

            // Bond eligibility result
            return new EligibilityResult(eligibleC, eligibleREG);

        } catch (Exception e) {
            logger.error("Error evaluating eligibility for bond " + cusip + ": " + e.getMessage() + " " + e);
            return new EligibilityResult(false, false);
        }
    }

    /**
     * Helper method to safely get MFA record
     */
    // @SuppressWarnings("unchecked")
    // private Map<String, Object> getMfaRecord(Map<String, Object> mfaInfo, String key) {
    //     if (mfaInfo == null) return null;
    //     Object obj = mfaInfo.get(key);
    //     if (obj instanceof Map) {
    //         return (Map<String, Object>) obj;
    //     }
    //     return null;
    // }

    /**
     * Periodic eligibility check
     */
    private void performPeriodicEligibilityCheck() {
        try {
            logger.info("Performing periodic eligibility check for " + consolidatedBondData.size() + " bonds");

            int eligibleCCount = 0;
            int ineligibleCCount = 0;
            int eligibleREGCount = 0;
            int ineligibleREGCount = 0;
            
            // Check all bonds we have data for
            for (Map.Entry<String, BondConsolidatedData> entry : consolidatedBondData.entrySet()) {
                BondConsolidatedData bondData = entry.getValue();
                String cusip = bondData.getCusip();
                EligibilityResult shouldBeEligible = shouldBondBeEligible(cusip, bondData);
                
                String IdC = cusip + "_C_Fixed";
                String IdREG = cusip + "_REG_Fixed";
                
                boolean currentlyEligibleC = isIdEligible(IdC);
                boolean currentlyEligibleREG = isIdEligible(IdREG);

                    // Handle C term changes
                if (shouldBeEligible.eligibleForTermC && !currentlyEligibleC) {
                    // Bond became eligible
                    addEligibleBond(IdC, bondData.getConsolidatedView());
                    eligibleCCount++;
                    logger.info("Bond added to eligible list (instrument: " + IdC + ")");

                } else if (!shouldBeEligible.eligibleForTermC && currentlyEligibleC) {
                    // Bond became ineligible
                    removeEligibleBond(IdC);
                    ineligibleCCount++;
                    logger.info("Bond removed from eligible list (termCode: " + "C" + ")");
                }

                if (shouldBeEligible.eligibleForTermREG && !currentlyEligibleREG) {
                    addEligibleBond(IdREG, bondData.getConsolidatedView());
                    eligibleREGCount++;
                    logger.info("Bond added to eligible list (instrument: " + IdREG + ")");
                } else if (!shouldBeEligible.eligibleForTermREG && currentlyEligibleREG) {
                    // Bond remains ineligible
                    removeEligibleBond(IdREG);
                    ineligibleREGCount++;
                    logger.info("Bond removed from eligible list (termCode: " + "REG" + ")");
                }
            }

            if (eligibleCCount > 0 || ineligibleCCount > 0 || eligibleREGCount > 0 || ineligibleREGCount > 0) {
                logger.info("Eligibility check complete: "+ eligibleCCount +" added, "+ ineligibleCCount +" removed, "+ eligibleBonds.size() +" total eligible");
            }
            
        } catch (Exception e) {
            logger.error("Error in periodic eligibility check: " + e.getMessage() + " " + e);
        }
    }
    
    // Add this method to the BondEligibilityListener class
    @Override
    public void onSubscribe(MkvObject mkvObject) {
        // Default implementation - not needed for this listener
        logger.debug("onSubscribe called for object: " + mkvObject.getName());

    }

    // MkvRecordListener implementation
    @Override
    public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {
        // Not implemented for this listener
    }
    
    @Override
    public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
        // Default implementation - not required for this listener
        logger.debug("onPublish called for object: " + mkvObject.getName() + ", pub_unpub: " + pub_unpub + ", dwl: " + dwl);
    }

    @Override
    public void onPublishIdle(String component, boolean start) {
        // Default implementation - not required for this listener
        logger.debug("onPublishIdle called for component: " + component + ", start: " + start);
    }

    @Override
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
            boolean isSnapshot) {

        logger.info("BondEligibilityListener received MKV record");
        

        try {
            // Update monitoring counters
                logger.info("Received full update in BondEligibilityListener for record: " + mkvRecord.getName());
           
            lastUpdateTimestamp.set(System.currentTimeMillis());
            
            String recordName = mkvRecord.getName();
            logger.info("Received record update: " + recordName + " (snapshot: " + isSnapshot + ")");
            

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
            // else if (recordName.startsWith("ALL.STATISTICS.MFA.")) {
            //     // Example: ALL.STATISTICS.MFA.MFA_912810RW0_C_Fixed_TODAY
            //     cusip = extractCusipFromMfaRecord(recordName);
            //     recordType = "MFA";
            // }
            else {
                logger.debug("Ignoring record " + recordName + ": unrecognized pattern");
                return;
            }

            if (cusip == null) {
                logger.warn("Could not extract CUSIP from record: " + recordName);
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
                    logger.info("Updating SDS data for CUSIP: " + cusip);
                    bondData.updateSdsData(extractedData);
                    logger.info("Extracted SDS data for CUSIP " + cusip + ": " + extractedData);
                    break;

                case "BOND":
                    logger.info("Updating static bond data for CUSIP: " + cusip);
                    bondData.updateStaticData(extractedData);
                    logger.info("Extracted static bond data for CUSIP " + cusip + ": " + extractedData);
                    // Extract instrument ID if available
                    Object instrumentId = extractedData.get("InstrumentId");
                    if (instrumentId != null) {
                        bondToInstrumentMaps.computeIfAbsent(cusip, k -> new ConcurrentHashMap<>()).put("C", instrumentId.toString());
                    }
                    break;
                    
                case "POSITION":
                    logger.info("Updating position data for CUSIP: " + cusip);
                    bondData.updatePositionData(extractedData);
                    logger.info("Extracted position data for CUSIP " + cusip + ": " + extractedData);
                    break;

                // case "MFA":
                //     logger.info("Updating MFA data for CUSIP: {}", cusip);
                //     bondData.updateMfaData(extractedData);
                //     logger.info("Extracted MFA data for CUSIP {}: {}", cusip, extractedData);
                //     break;
            }

            // Check if we should update eligibility status
            evaluateBondEligibility(cusip, bondData);
            
        } catch (Exception e) {
            logger.error("Error processing update: " + e.getMessage() + " " + e);
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
                logger.warn("No supply available for record: " + mkvRecord.getName());
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
                            logger.debug("Field updated: " + fieldName + " = " + fieldValue);
                        } else if (fieldValue == null && oldValue != null) {
                            extractedData.put(fieldName, "null");
                            changed = true;
                            logger.debug("Field nulled: " + fieldName);
                        }
                    }

                    // Move to the next field
                    cursor = mkvSupply.nextIndex(cursor);
                    
                } catch (Exception e) {
                    logger.warn("Error processing field at cursor " + cursor + ": " + e.getMessage());
                    // Try to continue with the next field
                    if (cursor != -1) {
                        cursor = mkvSupply.nextIndex(cursor);
                    } else {
                        break;
                    }
                }
            }
            
            if (changed) {
                logger.debug("Extracted " + extractedData.size() + " fields with changes from record " + mkvRecord.getName());
            }

        } catch (Exception e) {
            logger.error("Error in cursor iteration for record " + mkvRecord.getName() + ": " + e.getMessage() + " " + e);
        }

        return extractedData;
    }

    /**
     * Extract CUSIP from SDS record
     * Example: ALL.POSITION_US.SDS.91282CMM0:20250530
     */
    private String extractCusipFromSdsRecord(String recordName) {
        Matcher matcher = SDS_CUSIP_PATTERN.matcher(recordName);
        return matcher.matches() ? matcher.group(1) : null;
    }

    /**
     * Extract CUSIP from bond record
     * Example: USD.CM_BOND.VMO_REPO_US.91282CMM0
     */
    private String extractCusipFromBondRecord(String recordName) {
        Matcher matcher = BOND_CUSIP_PATTERN.matcher(recordName);
        return matcher.matches() ? matcher.group(1) : null;
    }

    /**
     * Extract CUSIP from MFA record
     * Example: ALL.STATISTICS.MFA.MFA_912810RW0_C_Fixed_TODAY
     */
    // private String extractCusipFromMfaRecord(String recordName) {
    //     try {
    //         if (recordName == null) return null;
            
    //         Matcher matcher = MFA_CUSIP_PATTERN.matcher(recordName);
    //         if (matcher.find()) {
    //             String potentialCusip = matcher.group(1);
    //             if (isValidCusip(potentialCusip)) {
    //                 return potentialCusip;
    //             }
    //         }
            
    //         return null;
    //     } catch (Exception e) {
    //         if (logger.isErrorEnabled()) {
    //             logger.error("Error extracting CUSIP from MFA record: {}", e.getMessage(), e);
    //         }
    //         return null;
    //     }
    // }
    
    /**
     * Extract CUSIP from position record
     * Example: USD.IU_POSITION.VMO_REPO_US.91282CMM0_20250530_STD
     */
    private String extractCusipFromPositionRecord(String recordName) {
        Matcher matcher = POSITION_CUSIP_PATTERN.matcher(recordName);
        return matcher.find() ? matcher.group(1) : null;
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
        return cusip != null && cusip.length() == 9 && cusip.startsWith("9") && !cusip.endsWith("W")  && !cusip.endsWith("R");
    }

    /**
     * Return VMO Id format
     */
    private String IdfromCUSIP (String cusip, String termCode) {
        // Basic validation - adjust as needed
        if (cusip == null || termCode == null) {
            logger.warn("Cannot create ID from null values: cusip=" + cusip + ", termCode=" + termCode);
            return null;
        }

        if (isValidCusip(cusip)) {
            return cusip + "_" + termCode + "_Fixed";
        }
        return null;
    }

    /**
     * Evaluate bond eligibility based on consolidated data
     */
    private void evaluateBondEligibility(String cusip, BondConsolidatedData bondData) {
        try {
            EligibilityResult shouldBeEligible = shouldBondBeEligible(cusip, bondData);
            String IdC = IdfromCUSIP(cusip, "C");
            String IdREG = IdfromCUSIP(cusip, "REG");
            if (IdC == null || IdREG == null) {
                logger.warn("Invalid ID format for CUSIP " + cusip + ": CUSIP=" + cusip + ", IdC=" + IdC + ", IdREG=" + IdREG);
                return;
            }
            boolean currentlyEligibleC = isIdEligible(IdC);
            boolean currentlyEligibleREG = isIdEligible(IdREG);

            if (shouldBeEligible.eligibleForTermC && !currentlyEligibleC) {
                // Add bond as eligible for the specified term code
                addEligibleBond(IdC, bondData.getConsolidatedView());
                logger.info("Bond added to eligible C list (instrument: " + IdC + ")");
            } else if (!shouldBeEligible.eligibleForTermC && currentlyEligibleC) {
                removeEligibleBond(IdC);
                logger.info("Bond " + IdC + " removed from eligible C list");
            }
        
            if (shouldBeEligible.eligibleForTermREG && !currentlyEligibleREG) {
                // Add bond as eligible for REG term
                addEligibleBond(IdREG, bondData.getConsolidatedView());
                logger.info("Bond added to eligible REG list (instrument: " + IdREG + ")");
            } else if (!shouldBeEligible.eligibleForTermREG && currentlyEligibleREG) {
                // Remove from eligible REG list
                removeEligibleBond(IdREG);
                logger.info("Bond removed from eligible REG list (instrument: " + IdREG + ")");
            }

        } catch (Exception e) {
            logger.error("Error evaluating eligibility for bond " + cusip + ": " + e.getMessage() + " " + e);
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
        // int withMfaOnly = 0; // MFA is not currently tracked in status
        // int withMfaAndPosition = 0; // MFA + Position is not currently tracked in status
        int withAllData = 0;
        
        for (BondConsolidatedData data : consolidatedBondData.values()) {
            boolean hasStatic = data.hasStaticData();
            boolean hasPosition = data.hasPositionData();
            boolean hasSds = data.hasSdsData();
            // boolean hasMfa = data.hasMfaData();

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
        // status.put("bondsWithMfaOnly", withMfaOnly);
        // status.put("bondsWithMfaAndPosition", withMfaAndPosition);
        status.put("bondsWithAllData", withAllData);
        status.put("listeners", eligibilityListeners.size());
        
        return status;
    }
}