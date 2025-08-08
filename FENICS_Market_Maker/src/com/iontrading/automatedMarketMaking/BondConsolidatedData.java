package com.iontrading.automatedMarketMaking;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class BondConsolidatedData {
    // Common identifier
    private final String cusip;
    
    // Data from CM_BOND source
    private Map<String, Object> staticData = new ConcurrentHashMap<>();
    
    // Data from IU_POSITION source
    private Map<String, Object> positionData = new ConcurrentHashMap<>();
    
    // Data from SDS source
    private Map<String, Object> sdsData = new ConcurrentHashMap<>();
    
    // Data from ASL source
    private Map<String, Object> aslData = new ConcurrentHashMap<>();

    // Data from MFA source
    // private Map<String, Object> mfaData = new ConcurrentHashMap<>();

    // Timestamps for each data source
    private long staticDataTimestamp = 0;
    private long positionDataTimestamp = 0;
    private long sdsDataTimestamp = 0;
    private long aslDataTimestamp = 0;
    // private long mfaDataTimestamp = 0;

    public BondConsolidatedData(String cusip) {
        this.cusip = cusip;
    }
    
    public String getCusip() {
        return cusip;
    }
    
    public void updateStaticData(Map<String, Object> data) {
        staticData.putAll(data);
        staticDataTimestamp = System.currentTimeMillis();
    }
    
    public void updatePositionData(Map<String, Object> data) {
        positionData.putAll(data);
        positionDataTimestamp = System.currentTimeMillis();
    }
    
    public void updateSdsData(Map<String, Object> data) {
        sdsData.putAll(data);
        sdsDataTimestamp = System.currentTimeMillis();
    }

    public void updateAslData(Map<String, Object> data) {
        aslData.putAll(data);
        aslDataTimestamp = System.currentTimeMillis();
    }

    // public void updateMfaData(Map<String, Object> data) {
    //     mfaData.putAll(data);
    //     mfaDataTimestamp = System.currentTimeMillis();
    // }

    public Map<String, Object> getStaticData() {
        return staticData;
    }
    
    public Map<String, Object> getPositionData() {
        return positionData;
    }
    
    public Map<String, Object> getSdsData() {
        return sdsData;
    }

    public Map<String, Object> getAslData() {
        return aslData;
    }

    // public Map<String, Object> getMfaData() {
    //     return mfaData;
    // }
    
    public boolean hasStaticData() {
        return !staticData.isEmpty();
    }
    
    public boolean hasPositionData() {
        return !positionData.isEmpty();
    }
    
    public boolean hasSdsData() {
        return !sdsData.isEmpty();
    }

    public boolean hasAslData() {
        return !aslData.isEmpty();
    }

    // public boolean hasMfaData() {
    //     return !mfaData.isEmpty();
    // }

    public long getStaticDataTimestamp() {
        return staticDataTimestamp;
    }
    
    public long getPositionDataTimestamp() {
        return positionDataTimestamp;
    }
    
    public long getSdsDataTimestamp() {
        return sdsDataTimestamp;
    }

    public long getAslDataTimestamp() {
        return aslDataTimestamp;
    }

    // public long getMfaDataTimestamp() {
    //     return mfaDataTimestamp;
    // }

    /**
     * Get a consolidated view of all data
     */
    public Map<String, Object> getConsolidatedView() {
        Map<String, Object> consolidated = new ConcurrentHashMap<>();
        
        // Add each data source with appropriate prefixes
        for (Map.Entry<String, Object> entry : staticData.entrySet()) {
            consolidated.put("STATIC_" + entry.getKey(), entry.getValue());
        }
        
        for (Map.Entry<String, Object> entry : positionData.entrySet()) {
            consolidated.put("POS_" + entry.getKey(), entry.getValue());
        }
        
        for (Map.Entry<String, Object> entry : sdsData.entrySet()) {
            consolidated.put("SDS_" + entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Object> entry : aslData.entrySet()) {
            consolidated.put("ASL_" + entry.getKey(), entry.getValue());
        }

        // for (Map.Entry<String, Object> entry : mfaData.entrySet()) {
        //     consolidated.put("MFA_" + entry.getKey(), entry.getValue());
        // }

        // Add metadata
        consolidated.put("CUSIP", cusip);
        consolidated.put("LastStaticUpdate", staticDataTimestamp);
        consolidated.put("LastPositionUpdate", positionDataTimestamp);
        consolidated.put("LastSdsUpdate", sdsDataTimestamp);
        consolidated.put("LastAslUpdate", aslDataTimestamp);
        // consolidated.put("LastMfaUpdate", mfaDataTimestamp);
        consolidated.put("IsComplete", hasStaticData() && hasPositionData() && hasSdsData() && hasAslData());

        return consolidated;
    }
}