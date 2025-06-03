package com.iontrading.automatedMarketMaking;

/**
 * GCBest provides enhanced market data representation for GC
 * while maintaining compatibility with the original Best class structure.
 */
public class GCBest extends Best {
    
    // Arrays to store depth levels (1-9, since level 0 is in parent class)
    private final double[] additionalAskPrices = new double[9];
    private final double[] additionalBidPrices = new double[9];
    private final String[] additionalAskSources = new String[9];
    private final String[] additionalBidSources = new String[9];
    
    public GCBest(String instrumentId) {
        super(instrumentId);
    }
    
    // Methods for depth level access (levels 1-9)
    public double getAskPrice(int level) {
        if (level == 0) {
            return getAsk();
        }
        if (level > 0 && level < 10) {
            return additionalAskPrices[level - 1];
        }
        throw new IllegalArgumentException("Invalid depth level: " + level);
    }
    
    public void setAskPrice(int level, double price) {
        if (level == 0) {
            setAsk(price);
        } else if (level > 0 && level < 10) {
            additionalAskPrices[level - 1] = Math.round(price * 100.0) / 100.0;
        } else {
            throw new IllegalArgumentException("Invalid depth level: " + level);
        }
    }
    
    public double getBidPrice(int level) {
        if (level == 0) {
            return getBid();
        }
        if (level > 0 && level < 10) {
            return additionalBidPrices[level - 1];
        }
        throw new IllegalArgumentException("Invalid depth level: " + level);
    }
    
    public void setBidPrice(int level, double price) {
        if (level == 0) {
            setBid(price);
        } else if (level > 0 && level < 10) {
            additionalBidPrices[level - 1] = Math.round(price * 100.0) / 100.0;
        } else {
            throw new IllegalArgumentException("Invalid depth level: " + level);
        }
    }
    
    public String getAskSource(int level) {
        if (level == 0) {
            return getAskSrc();
        }
        if (level > 0 && level < 10) {
            return additionalAskSources[level - 1];
        }
        throw new IllegalArgumentException("Invalid depth level: " + level);
    }
    
    public void setAskSource(int level, String source) {
        if (level == 0) {
            setAskSrc(source);
        } else if (level > 0 && level < 10) {
            additionalAskSources[level - 1] = source;
        } else {
            throw new IllegalArgumentException("Invalid depth level: " + level);
        }
    }
    
    public String getBidSource(int level) {
        if (level == 0) {
            return getBidSrc();
        }
        if (level > 0 && level < 10) {
            return additionalBidSources[level - 1];
        }
        throw new IllegalArgumentException("Invalid depth level: " + level);
    }
    
    public void setBidSource(int level, String source) {
        if (level == 0) {
            setBidSrc(source);
        } else if (level > 0 && level < 10) {
            additionalBidSources[level - 1] = source;
        } else {
            throw new IllegalArgumentException("Invalid depth level: " + level);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SpecializedBest{instrumentId='").append(getInstrumentId()).append("'\n");
        
        // Add market depth information
        for (int i = 0; i < 10; i++) {
            sb.append(String.format("Level %d - Bid: %.2f (%s) | Ask: %.2f (%s)\n",
                i,
                getBidPrice(i), getBidSource(i),
                getAskPrice(i), getAskSource(i)));
        }
        return sb.toString();
    }

    public GCLevelResult getGCLevel() {
        // Check each level
        for (int i = 0; i < 10; i++) {
            double currentAskPrice = getAskPrice(i);
            double currentBidPrice = getBidPrice(i);
            String currentAskSource = getAskSource(i);
            String currentBidSource = getBidSource(i);

            // Skip if current price is 0 or source is null
            if ((currentAskPrice == 0 && currentBidPrice == 0) ||
                (currentAskSource == null && currentBidSource == null)) {
                continue;
            }

            // Look for matches in subsequent levels
            for (int j = i + 1; j < 10; j++) {
                Double matchedBid = null;
                Double matchedAsk = null;
                
                // Check ask side for match
                if (currentAskPrice != 0 && 
                    currentAskPrice == getAskPrice(j) && 
                    currentAskSource != null && 
                    getAskSource(j) != null && 
                    !currentAskSource.equals(getAskSource(j))) {
                    matchedAsk = currentAskPrice;
                }

                // Check bid side for match
                if (currentBidPrice != 0 && 
                    currentBidPrice == getBidPrice(j) && 
                    currentBidSource != null && 
                    getBidSource(j) != null && 
                    !currentBidSource.equals(getBidSource(j))) {
                    matchedBid = currentBidPrice;
                }
                
                // If we found either a matching bid or ask, return the result
                if (matchedBid != null || matchedAsk != null) {
                    return new GCLevelResult(matchedBid, matchedAsk);
                }
            }
        }
        
        return null;  // No matching prices with different sources found
    }
}