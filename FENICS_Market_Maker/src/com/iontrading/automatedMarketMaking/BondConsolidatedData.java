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
    
    // Timestamps for each data source
    private long staticDataTimestamp = 0;
    private long positionDataTimestamp = 0;
    private long sdsDataTimestamp = 0;
    
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
    
    public Map<String, Object> getStaticData() {
        return staticData;
    }
    
    public Map<String, Object> getPositionData() {
        return positionData;
    }
    
    public Map<String, Object> getSdsData() {
        return sdsData;
    }
    
    public boolean hasStaticData() {
        return !staticData.isEmpty();
    }
    
    public boolean hasPositionData() {
        return !positionData.isEmpty();
    }
    
    public boolean hasSdsData() {
        return !sdsData.isEmpty();
    }
    
    public long getStaticDataTimestamp() {
        return staticDataTimestamp;
    }
    
    public long getPositionDataTimestamp() {
        return positionDataTimestamp;
    }
    
    public long getSdsDataTimestamp() {
        return sdsDataTimestamp;
    }
    
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
        
        // Add metadata
        consolidated.put("CUSIP", cusip);
        consolidated.put("LastStaticUpdate", staticDataTimestamp);
        consolidated.put("LastPositionUpdate", positionDataTimestamp);
        consolidated.put("LastSdsUpdate", sdsDataTimestamp);
        consolidated.put("IsComplete", hasStaticData() && hasPositionData() && hasSdsData());
        
        return consolidated;
    }
}