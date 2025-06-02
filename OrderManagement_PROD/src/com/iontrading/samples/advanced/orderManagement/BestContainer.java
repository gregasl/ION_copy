package com.iontrading.samples.advanced.orderManagement;

public class BestContainer {
    private final Best best;
    private final GCBest gcBestCash;
    private final GCBest gcBestReg;
    
    public BestContainer(Best best, GCBest gcBestCash, GCBest gcBestReg) {
        this.best = best;
        this.gcBestCash = gcBestCash;
        this.gcBestReg = gcBestReg;
    }
    
    public Best getBest() { return best; }
    public GCBest getGcBestCash() { return gcBestCash; }
    public GCBest getGcBestReg() { return gcBestReg; }
 }