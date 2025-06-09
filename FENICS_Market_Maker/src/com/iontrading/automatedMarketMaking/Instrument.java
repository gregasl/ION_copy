/*
 * Instrument
 *
 * Java bean representing instrument mapping data.
 *
 */

package com.iontrading.automatedMarketMaking;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instrument is a data container class representing instrument mapping data
 * including source mappings and AON flags.
 * 
 * This is a simple Java bean with getters and setters to hold the
 * instrument configuration data.
 */
public class Instrument {

    private static final Logger LOGGER = LoggerFactory.getLogger(Instrument.class);

    /**
     * The instrument ID this Instrument object represents.
     * This is immutable once set in the constructor.
     */
    private final String instrumentId;
    
    // Core instrument fields
    private String id;
    private Boolean is0AON, is1AON, is2AON, is3AON, is4AON, is5AON, is6AON, is7AON, is8AON, is9AON, is10AON, is11AON, is12AON, is13AON, is14AON, is15AON;
    
    // ID fields (Id0-Id15)
    private String id0, id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13, id14, id15;
    
    // Source fields (Src0-Src15)
    private String src0, src1, src2, src3, src4, src5, src6, src7, src8, src9, src10, src11, src12, src13, src14, src15;
    
    // Attribute fields (Attribute0-Attribute15)
    private String attribute0, attribute1, attribute2, attribute3, attribute4, attribute5, attribute6, attribute7;
    private String attribute8, attribute9, attribute10, attribute11, attribute12, attribute13, attribute14, attribute15;
    
    // Source fields (Src0-Src15)
    private String qtyMin0, qtyMin1, qtyMin2, qtyMin3, qtyMin4, qtyMin5, qtyMin6, qtyMin7, qtyMin8, qtyMin9, qtyMin10, qtyMin11, qtyMin12, qtyMin13, qtyMin14, qtyMin15;

    // Cache for source to native ID mappings
    private final Map<String, String> sourceToNativeIdMap = new HashMap<>();
    
    /**
     * Creates a new Instrument instance for the specified instrument.
     * 
     * @param instrumentId The ID of the instrument this Instrument represents
     */
    public Instrument(String instrumentId) {
        this.instrumentId = instrumentId;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Created new Instrument instance for: {}", instrumentId);
        }
    }

    /**
     * @return The instrument ID this Instrument represents
     */
    public String getInstrumentId() {
        return instrumentId;
    }
    
    // Core field getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public Boolean get0IsAON() { return is0AON; }
    public void setIs0AON(Boolean isAON) { this.is0AON = isAON; }
    
    public Boolean get1IsAON() { return is1AON; }
    public void setIs1AON(Boolean isAON) { this.is1AON = isAON; }

    public Boolean get2IsAON() { return is2AON; }
    public void setIs2AON(Boolean isAON) { this.is2AON = isAON; }
    
    public Boolean get3IsAON() { return is3AON; }
    public void setIs3AON(Boolean isAON) { this.is3AON = isAON; }
    
    public Boolean get4IsAON() { return is4AON; }
    public void setIs4AON(Boolean isAON) { this.is4AON = isAON; }

    public Boolean get5IsAON() { return is5AON; }
    public void setIs5AON(Boolean isAON) { this.is5AON = isAON; }

    public Boolean get6IsAON() { return is6AON; }
    public void setIs6AON(Boolean isAON) { this.is6AON = isAON; }
    
    public Boolean get7IsAON() { return is7AON; }
    public void setIs7AON(Boolean isAON) { this.is7AON = isAON; }

    public Boolean get8IsAON() { return is8AON; }
    public void setIs8AON(Boolean isAON) { this.is8AON = isAON; }

    public Boolean get9IsAON() { return is9AON; }
    public void setIs9AON(Boolean isAON) { this.is9AON = isAON; }
    
    public Boolean get10IsAON() { return is10AON; }
    public void setIs10AON(Boolean isAON) { this.is10AON = isAON; }
    
    public Boolean get11IsAON() { return is11AON; }
    public void setIs11AON(Boolean isAON) { this.is11AON = isAON; }

    public Boolean get12IsAON() { return is12AON; }
    public void setIs12AON(Boolean isAON) { this.is12AON = isAON; }

    public Boolean get13IsAON() { return is13AON; }
    public void setIs13AON(Boolean isAON) { this.is13AON = isAON; }

    public Boolean get14IsAON() { return is14AON; }
    public void setIs14AON(Boolean isAON) { this.is14AON = isAON; }

    public Boolean get15IsAON() { return is15AON; }
    public void setIs15AON(Boolean isAON) { this.is15AON = isAON; }

    // ID field getters and setters
    public String getId0() { return id0; }
    public void setId0(String id0) { this.id0 = id0; }
    
    public String getId1() { return id1; }
    public void setId1(String id1) { this.id1 = id1; }
    
    public String getId2() { return id2; }
    public void setId2(String id2) { this.id2 = id2; }
    
    public String getId3() { return id3; }
    public void setId3(String id3) { this.id3 = id3; }
    
    public String getId4() { return id4; }
    public void setId4(String id4) { this.id4 = id4; }
    
    public String getId5() { return id5; }
    public void setId5(String id5) { this.id5 = id5; }
    
    public String getId6() { return id6; }
    public void setId6(String id6) { this.id6 = id6; }
    
    public String getId7() { return id7; }
    public void setId7(String id7) { this.id7 = id7; }
    
    public String getId8() { return id8; }
    public void setId8(String id8) { this.id8 = id8; }
    
    public String getId9() { return id9; }
    public void setId9(String id9) { this.id9 = id9; }
    
    public String getId10() { return id10; }
    public void setId10(String id10) { this.id10 = id10; }
    
    public String getId11() { return id11; }
    public void setId11(String id11) { this.id11 = id11; }
    
    public String getId12() { return id12; }
    public void setId12(String id12) { this.id12 = id12; }
    
    public String getId13() { return id13; }
    public void setId13(String id13) { this.id13 = id13; }
    
    public String getId14() { return id14; }
    public void setId14(String id14) { this.id14 = id14; }
    
    public String getId15() { return id15; }
    public void setId15(String id15) { this.id15 = id15; }
    
    // Source field getters and setters
    public String getSrc0() { return src0; }
    public void setSrc0(String src0) { this.src0 = src0; }
    
    public String getSrc1() { return src1; }
    public void setSrc1(String src1) { this.src1 = src1; }
    
    public String getSrc2() { return src2; }
    public void setSrc2(String src2) { this.src2 = src2; }
    
    public String getSrc3() { return src3; }
    public void setSrc3(String src3) { this.src3 = src3; }
    
    public String getSrc4() { return src4; }
    public void setSrc4(String src4) { this.src4 = src4; }
    
    public String getSrc5() { return src5; }
    public void setSrc5(String src5) { this.src5 = src5; }
    
    public String getSrc6() { return src6; }
    public void setSrc6(String src6) { this.src6 = src6; }
    
    public String getSrc7() { return src7; }
    public void setSrc7(String src7) { this.src7 = src7; }
    
    public String getSrc8() { return src8; }
    public void setSrc8(String src8) { this.src8 = src8; }
    
    public String getSrc9() { return src9; }
    public void setSrc9(String src9) { this.src9 = src9; }
    
    public String getSrc10() { return src10; }
    public void setSrc10(String src10) { this.src10 = src10; }
    
    public String getSrc11() { return src11; }
    public void setSrc11(String src11) { this.src11 = src11; }
    
    public String getSrc12() { return src12; }
    public void setSrc12(String src12) { this.src12 = src12; }
    
    public String getSrc13() { return src13; }
    public void setSrc13(String src13) { this.src13 = src13; }
    
    public String getSrc14() { return src14; }
    public void setSrc14(String src14) { this.src14 = src14; }
    
    public String getSrc15() { return src15; }
    public void setSrc15(String src15) { this.src15 = src15; }
    
    // QtyMin field getters and setters
    public String getQtyMin0() { return qtyMin0; }
    public void setQtyMin0(String qtyMin0) { this.qtyMin0 = qtyMin0; }

    public String getQtyMin1() { return qtyMin1; }
    public void setQtyMin1(String qtyMin1) { this.qtyMin1 = qtyMin1; }

    public String getQtyMin2() { return qtyMin2; }
    public void setQtyMin2(String qtyMin2) { this.qtyMin2 = qtyMin2; }

    public String getQtyMin3() { return qtyMin3; }
    public void setQtyMin3(String qtyMin3) { this.qtyMin3 = qtyMin3; }

    public String getQtyMin4() { return qtyMin4; }
    public void setQtyMin4(String qtyMin4) { this.qtyMin4 = qtyMin4; }

    public String getQtyMin5() { return qtyMin5; }
    public void setQtyMin5(String qtyMin5) { this.qtyMin5 = qtyMin5; }

    public String getQtyMin6() { return qtyMin6; }
    public void setQtyMin6(String qtyMin6) { this.qtyMin6 = qtyMin6; }

    public String getQtyMin7() { return qtyMin7; }
    public void setQtyMin7(String qtyMin7) { this.qtyMin7 = qtyMin7; }

    public String getQtyMin8() { return qtyMin8; }
    public void setQtyMin8(String qtyMin8) { this.qtyMin8 = qtyMin8; }

    public String getQtyMin9() { return qtyMin9; }
    public void setQtyMin9(String qtyMin9) { this.qtyMin9 = qtyMin9; }

    public String getQtyMin10() { return qtyMin10; }
    public void setQtyMin10(String qtyMin10) { this.qtyMin10 = qtyMin10; }

    public String getQtyMin11() { return qtyMin11; }
    public void setQtyMin11(String qtyMin11) { this.qtyMin11 = qtyMin11; }

    public String getQtyMin12() { return qtyMin12; }
    public void setQtyMin12(String qtyMin12) { this.qtyMin12 = qtyMin12; }

    public String getQtyMin13() { return qtyMin13; }
    public void setQtyMin13(String qtyMin13) { this.qtyMin13 = qtyMin13; }

    public String getQtyMin14() { return qtyMin14; }
    public void setQtyMin14(String qtyMin14) { this.qtyMin14 = qtyMin14; }

    public String getQtyMin15() { return qtyMin15; }
    public void setQtyMin15(String qtyMin15) { this.qtyMin15 = qtyMin15; }

    // Attribute field getters and setters
    public String getAttribute0() { return attribute0; }
    public void setAttribute0(String attribute0) { this.attribute0 = attribute0; }
    
    public String getAttribute1() { return attribute1; }
    public void setAttribute1(String attribute1) { this.attribute1 = attribute1; }
    
    public String getAttribute2() { return attribute2; }
    public void setAttribute2(String attribute2) { this.attribute2 = attribute2; }
    
    public String getAttribute3() { return attribute3; }
    public void setAttribute3(String attribute3) { this.attribute3 = attribute3; }
    
    public String getAttribute4() { return attribute4; }
    public void setAttribute4(String attribute4) { this.attribute4 = attribute4; }
    
    public String getAttribute5() { return attribute5; }
    public void setAttribute5(String attribute5) { this.attribute5 = attribute5; }
    
    public String getAttribute6() { return attribute6; }
    public void setAttribute6(String attribute6) { this.attribute6 = attribute6; }
    
    public String getAttribute7() { return attribute7; }
    public void setAttribute7(String attribute7) { this.attribute7 = attribute7; }
    
    public String getAttribute8() { return attribute8; }
    public void setAttribute8(String attribute8) { this.attribute8 = attribute8; }
    
    public String getAttribute9() { return attribute9; }
    public void setAttribute9(String attribute9) { this.attribute9 = attribute9; }
    
    public String getAttribute10() { return attribute10; }
    public void setAttribute10(String attribute10) { this.attribute10 = attribute10; }
    
    public String getAttribute11() { return attribute11; }
    public void setAttribute11(String attribute11) { this.attribute11 = attribute11; }
    
    public String getAttribute12() { return attribute12; }
    public void setAttribute12(String attribute12) { this.attribute12 = attribute12; }
    
    public String getAttribute13() { return attribute13; }
    public void setAttribute13(String attribute13) { this.attribute13 = attribute13; }
    
    public String getAttribute14() { return attribute14; }
    public void setAttribute14(String attribute14) { this.attribute14 = attribute14; }
    
    public String getAttribute15() { return attribute15; }
    public void setAttribute15(String attribute15) { this.attribute15 = attribute15; }
    
    /**
     * Gets the native instrument ID for a specific source and AON flag.
     * This method searches through the Id0-Id15 and Src0-Src15 fields to find the matching source,
     * then returns the corresponding native ID.
     * 
     * @param sourceId The source identifier (e.g., "DEALERWEB_REPO", "BTEC_REPO_US", "FENICS_USREPO")
     * @param isAON Whether to look for AON or non-AON variant
     * @return The native instrument ID or null if not found
     */
    public String getInstrumentFieldBySourceString(String sourceId, Boolean isAON) {
        if (sourceId == null || sourceId.isEmpty()) {
            LOGGER.warn("Instrument.getInstrumentFieldBySourceString received null/empty sourceId");
            return null;
        }

        // Check cache first
        String cacheKey = sourceId + (isAON ? "_AON" : "");
        String cachedResult = sourceToNativeIdMap.get(cacheKey);
        if (cachedResult != null) {
            LOGGER.info("Found cached mapping for {}, source={}, isAON={}: {}", instrumentId, sourceId, isAON, cachedResult);
            return cachedResult;
        }

        // Log the contents of the sourceToNativeIdMap for debugging
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Source mapping cache for {} contains {} entries:", instrumentId, sourceToNativeIdMap.size());
            for (Map.Entry<String, String> entry : sourceToNativeIdMap.entrySet()) {
                LOGGER.debug("  {} -> {}", entry.getKey(), entry.getValue());
            }
        }

        try {
            // Search through all source/ID pairs
            LOGGER.info("Searching through source/ID pairs for {} looking for source={}", instrumentId, sourceId);

            for (int i = 0; i <= 15; i++) {
                String src = getSourceByIndex(i);
                String nativeId = getIdByIndex(i);
                Boolean isNativeIdAON = getAONByIndex(i);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Checking index {}: src={}, nativeId={}, isAON={}", i, src, nativeId, isNativeIdAON);
                }

                if (src == null || !sourceId.equals(src) || nativeId == null) {
                    continue;
                }

                LOGGER.info("Found matching source at index {}: {}", i, src);
                
                // Check if AON matches
                if (isAON != null && isNativeIdAON != null && isAON != isNativeIdAON) {
                    LOGGER.info("Skipping index {} because AON mismatch: expected={}, found={}", 
                        i, isAON, isNativeIdAON);
                    continue;
                }

                // Special handling for FENICS_USREPO
                if ("FENICS_USREPO".equals(src)) {
                    String attributeValue = getAttributeByIndex(i);
                    LOGGER.debug("FENICS_USREPO found, checking attribute value: {}", attributeValue);
                    
                    // Only use this entry if attribute also matches FENICS_USREPO
                    if ("BGC".equals(attributeValue)) {
                        LOGGER.info("Skipping FENICS_USREPO entry because attribute doesn't match: {}", attributeValue);
                        continue;
                    } else if ("FENICS_USREPO".equals(attributeValue)) {
                        LOGGER.info("Using FENICS_USREPO entry because attribute matches: {}", attributeValue);
                        return nativeId;
                    } 
                } else {
                    // For all other sources, return the native ID directly
                    LOGGER.info("Returning native ID for source {}: {}", src, nativeId);
                    return nativeId;
                }
            }

            // No matches found after full search
            LOGGER.warn("No matching instrument found for source: {}, AON: {}, instrument: {}", 
                sourceId, isAON, instrumentId);

            // Log the actual source and ID fields to help diagnose
            StringBuilder sb = new StringBuilder();
            sb.append("Source fields for ").append(instrumentId).append(":\n");
            for (int i = 0; i <= 15; i++) {
                String src = getSourceByIndex(i);
                String nativeId = getIdByIndex(i);
                String attr = getAttributeByIndex(i);
                Boolean isAttrAON = getAONByIndex(i);

                if (src != null && !src.isEmpty()) {
                    sb.append("  Index ").append(i)
                    .append(": src=").append(src)
                    .append(", id=").append(nativeId)
                    .append(", attr=").append(attr)
                    .append(", isAttrAON=").append(isAttrAON)
                    .append("\n");
                }
            }
            LOGGER.info("Source fields for {}:\n{}", instrumentId, sb.toString());

            return null;
            
        } catch (Exception e) {
            LOGGER.error("Error getting instrument field for source: {}", sourceId, e);
            return null;
        }
    }
    
/**
 * Gets the minimum quantity for a specific source.
 * This method searches through the Src0-Src15 fields to find the matching source,
 * then returns the corresponding QtyMin value.
 * 
 * @param sourceId The source identifier (e.g., "DEALERWEB_REPO", "BTEC_REPO_US", "FENICS_USREPO")
 * @return The minimum quantity as a double, or -1 if not found or invalid
 */
public double getMinimumQuantityBySource(String sourceId) {
    if (sourceId == null || sourceId.isEmpty()) {
        LOGGER.warn("Instrument.getMinimumQuantityBySource received null/empty sourceId");
        return -1;
    }

    try {
        // Search through all source/QtyMin pairs
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Searching for minimum quantity for source: {} in instrument: {}", sourceId, instrumentId);
        }

        for (int i = 0; i <= 15; i++) {
            String src = getSourceByIndex(i);
            String qtyMinStr = getQtyMinByIndex(i);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Checking index {}: src={}, qtyMin={}", i, src, qtyMinStr);
            }

            // Skip if source doesn't match or is null
            if (src == null || !sourceId.equals(src)) {
                continue;
            }

            LOGGER.info("Found matching source at index {}: {}", i, src);

            // Special handling for FENICS_USREPO (same logic as getInstrumentFieldBySourceString)
            if ("FENICS_USREPO".equals(src)) {
                String attributeValue = getAttributeByIndex(i);
                LOGGER.debug("FENICS_USREPO found, checking attribute value: {}", attributeValue);
                
                // Only use this entry if attribute also matches FENICS_USREPO
                if ("BGC".equals(attributeValue)) {
                    LOGGER.info("Skipping FENICS_USREPO entry because attribute doesn't match: {}", attributeValue);
                    continue;
                } else if (!"FENICS_USREPO".equals(attributeValue)) {
                    LOGGER.info("Skipping FENICS_USREPO entry because attribute doesn't match: {}", attributeValue);
                    continue;
                }
            }

            // Parse and return the minimum quantity
            if (qtyMinStr != null && !qtyMinStr.isEmpty()) {
                try {
                    double qtyMin = Double.parseDouble(qtyMinStr);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Found minimum quantity for source {}: {}", sourceId, qtyMin);
                    }
                    return qtyMin;
                } catch (NumberFormatException e) {
                    LOGGER.warn("Invalid QtyMin format for source {} at index {}: {}", sourceId, i, qtyMinStr);
                    // Continue searching in case there's another valid entry
                }
            } else {
                LOGGER.warn("QtyMin is null/empty for source {} at index {}", sourceId, i);
                // Continue searching in case there's another valid entry
            }
        }

        // No valid matches found
        LOGGER.warn("No minimum quantity found for source: {} in instrument: {}", sourceId, instrumentId);

        // Log all source fields for debugging (only if debug is enabled)
        if (LOGGER.isDebugEnabled()) {
            logAllSourceFields();
        }

        return -1;
        
    } catch (Exception e) {
        LOGGER.error("Error getting minimum quantity for source: {} in instrument: {}", 
            sourceId, instrumentId, e);
        return -1;
    }
}

/**
 * Gets the minimum quantity for a specific source with a fallback default.
 * This is a convenience method that provides a fallback value if no specific minimum is found.
 * 
 * @param sourceId The source identifier
 * @param defaultMinimum The default minimum to return if source-specific minimum is not found
 * @return The minimum quantity for the source, or defaultMinimum if not found
 */
public double getMinimumQuantityBySource(String sourceId, double defaultMinimum) {
    double sourceMinimum = getMinimumQuantityBySource(sourceId);
    if (sourceMinimum <= 0) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("No specific minimum found for source {}, using default: {}", 
                sourceId, defaultMinimum);
        }
        return defaultMinimum;
    }
    return sourceMinimum;
}

/**
 * Gets all minimum quantities mapped by source for this instrument.
 * Useful for configuration validation and debugging.
 * 
 * @return Map of source -> minimum quantity
 */
public Map<String, Double> getAllMinimumQuantitiesBySource() {
    Map<String, Double> result = new HashMap<>();
    
    try {
        for (int i = 0; i <= 15; i++) {
            String src = getSourceByIndex(i);
            String qtyMinStr = getQtyMinByIndex(i);
            
            if (src != null && !src.isEmpty() && qtyMinStr != null && !qtyMinStr.isEmpty()) {
                // Apply same FENICS_USREPO filtering logic
                if ("FENICS_USREPO".equals(src)) {
                    String attributeValue = getAttributeByIndex(i);
                    if (!"FENICS_USREPO".equals(attributeValue)) {
                        continue; // Skip this entry
                    }
                }
                
                try {
                    double qtyMin = Double.parseDouble(qtyMinStr);
                    result.put(src, qtyMin);
                } catch (NumberFormatException e) {
                    LOGGER.warn("Invalid QtyMin format for source {} at index {}: {}", src, i, qtyMinStr);
                }
            }
        }
    } catch (Exception e) {
        LOGGER.error("Error getting all minimum quantities for instrument: {}", instrumentId, e);
    }
    
    return result;
}

/**
 * Helper method to log all source fields for debugging
 */
private void logAllSourceFields() {
    StringBuilder sb = new StringBuilder();
    sb.append("All source/QtyMin fields for ").append(instrumentId).append(":\n");
    
    for (int i = 0; i <= 15; i++) {
        String src = getSourceByIndex(i);
        String qtyMin = getQtyMinByIndex(i);
        String attr = getAttributeByIndex(i);
        Boolean isAON = getAONByIndex(i);
        
        if (src != null && !src.isEmpty()) {
            sb.append("  Index ").append(i)
              .append(": src=").append(src)
              .append(", qtyMin=").append(qtyMin)
              .append(", attr=").append(attr)
              .append(", isAON=").append(isAON)
              .append("\n");
        }
    }
    
    LOGGER.debug("Source/QtyMin mapping for {}:\n{}", instrumentId, sb.toString());
}
    
    /**
     * Helper method to get source by index
     */
    private String getSourceByIndex(int index) {
        switch (index) {
            case 0: return src0;
            case 1: return src1;
            case 2: return src2;
            case 3: return src3;
            case 4: return src4;
            case 5: return src5;
            case 6: return src6;
            case 7: return src7;
            case 8: return src8;
            case 9: return src9;
            case 10: return src10;
            case 11: return src11;
            case 12: return src12;
            case 13: return src13;
            case 14: return src14;
            case 15: return src15;
            default: return null;
        }
    }
        
    /**
     * Helper method to get qtyMin by index
     */
    private String getQtyMinByIndex(int index) {
        switch (index) {
            case 0: return qtyMin0;
            case 1: return qtyMin1;
            case 2: return qtyMin2;
            case 3: return qtyMin3;
            case 4: return qtyMin4;
            case 5: return qtyMin5;
            case 6: return qtyMin6;
            case 7: return qtyMin7;
            case 8: return qtyMin8;
            case 9: return qtyMin9;
            case 10: return qtyMin10;
            case 11: return qtyMin11;
            case 12: return qtyMin12;
            case 13: return qtyMin13;
            case 14: return qtyMin14;
            case 15: return qtyMin15;
            default: return null;
        }
    }

    /**
     * Helper method to get ID by index
     */
    private String getIdByIndex(int index) {
        switch (index) {
            case 0: return id0;
            case 1: return id1;
            case 2: return id2;
            case 3: return id3;
            case 4: return id4;
            case 5: return id5;
            case 6: return id6;
            case 7: return id7;
            case 8: return id8;
            case 9: return id9;
            case 10: return id10;
            case 11: return id11;
            case 12: return id12;
            case 13: return id13;
            case 14: return id14;
            case 15: return id15;
            default: return null;
        }
    }
    
    /**
     * Helper method to get attribute by index
     */
    private String getAttributeByIndex(int index) {
        switch (index) {
            case 0: return attribute0;
            case 1: return attribute1;
            case 2: return attribute2;
            case 3: return attribute3;
            case 4: return attribute4;
            case 5: return attribute5;
            case 6: return attribute6;
            case 7: return attribute7;
            case 8: return attribute8;
            case 9: return attribute9;
            case 10: return attribute10;
            case 11: return attribute11;
            case 12: return attribute12;
            case 13: return attribute13;
            case 14: return attribute14;
            case 15: return attribute15;
            default: return null;
        }
    }
    
    private Boolean getAONByIndex(int index) {
        switch (index) {
            case 0: return is0AON;
            case 1: return is1AON;
            case 2: return is2AON;
            case 3: return is3AON;
            case 4: return is4AON;
            case 5: return is5AON;
            case 6: return is6AON;
            case 7: return is7AON;
            case 8: return is8AON;
            case 9: return is9AON;
            case 10: return is10AON;
            case 11: return is11AON;
            case 12: return is12AON;
            case 13: return is13AON;
            case 14: return is14AON;
            case 15: return is15AON;
            default: return null;
        }
    }

    /**
     * Builds the source mapping cache for quick lookups.
     * This should be called after all fields are populated.
     */
    public void buildSourceMappings() {
        LOGGER.info("Building source mappings for instrument: {}", instrumentId);
        
        // Log the field values for mapping
        StringBuilder sb = new StringBuilder();
        sb.append("Source fields for building mappings:\n");
        
        // Clear existing mappings to avoid stale data
        sourceToNativeIdMap.clear();
        
        // Now try with reflection for all indexes
        for (int i = 0; i <= 15; i++) {
            String src = null;
            String id = null;
            String attr = null;
            Double qtyMin = null;
            Boolean instrumentIsAon = null;
            try {

                    java.lang.reflect.Method getSrc = this.getClass().getMethod("getSrc" + i);
                    java.lang.reflect.Method getId = this.getClass().getMethod("getId" + i);
                    java.lang.reflect.Method getAttr = this.getClass().getMethod("getAttribute" + i);
                    java.lang.reflect.Method getAON = this.getClass().getMethod("get" + i + "IsAON");
                    java.lang.reflect.Method getQtyMin = this.getClass().getMethod("getQtyMin" + i);

                    src = (String) getSrc.invoke(this);
                    id = (String) getId.invoke(this);
                    attr = (String) getAttr.invoke(this);
                    instrumentIsAon = (Boolean) getAON.invoke(this);
                    qtyMin = (Double) getQtyMin.invoke(this);

            } catch (Exception e) {
                sb.append("  Error accessing field at index ").append(i)
                .append(": ").append(e.getMessage()).append("\n");
                // Print full stack trace to console for debugging
                LOGGER.error("Error accessing field at index {}", i, e);
                continue;
            }
            
            if (src != null && !src.isEmpty() && id != null) {
                
                // Build regular mapping
                String cacheKey = src;
                if (instrumentIsAon) {
                    cacheKey += "_AON";
                }
                                
                if ("FENICS_USREPO".equals(src)) {
                    String attributeValue = getAttributeByIndex(i);
                    LOGGER.debug("FENICS_USREPO found, checking attribute value: {}", attributeValue);
                    
                    // Only use this entry if attribute also matches FENICS_USREPO
                    if ("BGC".equals(attributeValue)) {
                        LOGGER.info("Skipping FENICS_USREPO entry because attribute doesn't match: {}", attributeValue);
                        continue;
                    } else if ("FENICS_USREPO".equals(attributeValue)) {
                        LOGGER.info("Using FENICS_USREPO entry because attribute matches: {}", attributeValue);
                        sourceToNativeIdMap.put(cacheKey, id);
                    }
                } else {
                	sourceToNativeIdMap.put(cacheKey, id);
                }
            }
        }
        
        // Log the resulting cache
        sb.append("Built source mappings (").append(sourceToNativeIdMap.size())
        .append(" entries):\n");
        for (Map.Entry<String, String> entry : sourceToNativeIdMap.entrySet()) {
            sb.append("  ").append(entry.getKey())
            .append(" -> ").append(entry.getValue())
            .append("\n");
        }

        LOGGER.info("Source mappings built for instrument: {}", instrumentId);
    }
    
    /**
     * Returns a string representation of this Instrument object.
     */
    @Override
    public String toString() {
        return "Instrument{instrumentId='" + instrumentId + "', " +
               "id='" + id + "', " +
               "mappings=" + sourceToNativeIdMap.size() + "}";
    }
}