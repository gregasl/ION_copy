/*
 * R4Q
 *
 * ION Trading U.K. Limited supplies this software code is for testing purposes
 * only. The scope of this software code is exclusively limited to the
 * demonstration of the structure of an application using the ION(tm) Common
 * Market and ION Trading U.K. Limited does not guarantee the correct behavior
 * of any deployed application using this software code.
 * This software code has not been thoroughly tested under all conditions.
 * ION, therefore, cannot guarantee or imply reliability, serviceability, or
 * function of this software.
 * Any use of this software outside of this defined scope is the sole
 * responsibility of the user.
 *
 * ION Trading ltd (2005)
 */

package com.iontrading.samples.advanced.cne;

import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.events.MkvTransactionCallEvent;
import com.iontrading.mkv.events.MkvTransactionCallListener;
import com.iontrading.mkv.helper.MkvSupplyBuilder;

public class R4Q {
    
    public static final int VERB_BUY = 0;
    public static final int VERB_SELL = 1;
    public static final int VERB_UNSET = -1;
    
    private String Id;
    
    private int CNEReserved = -1;
    private String CNELockOwner;
    private String TraderLockOwner;
    private String CustFirm;
    private int RFQNLegs = -1;
    private String RFQTypeStr;
    private int DFDDealerActions = -1;
    
    private String InstrumentId;
    private int MaturityDate = -1;
    private int RFQDateSettl = -1;
    private int RFQDaysSettl = -1;
    private int RFQVerb = VERB_UNSET;
    private double RFQCustValue;
    private double RFQQty;
    private String StatusStr;
    private String ANTraderList;
    
    private int RFQClosed = 0;
    
    private MkvRecord mkvRecord;
    
    private CNELogic logic;
    
    private boolean locked;
    
    private final MkvTransactionCallListener genericTransListener;
    
    /** Creates a new instance of R4Q */
    public R4Q(CNELogic logic) {
        this.logic = logic;
        genericTransListener = new GeneralTransactionResult();
    }
    
    public String getId() {
        return Id;
    }
    
    public void setId(String Id) {
        this.Id = Id;
    }
    
    public int getCNEReserved() {
        return CNEReserved;
    }
    
    public void setCNEReserved(int CNEReserved) {
        this.CNEReserved = CNEReserved;
    }
    
    public String getCNELockOwner() {
        return CNELockOwner;
    }
    
    public void setCNELockOwner(String CNELockOwner) {
        this.CNELockOwner = CNELockOwner;
    }
    
    public String getTraderLockOwner() {
        return TraderLockOwner;
    }
    
    public void setTraderLockOwner(String traderLockOwner) {
        if ((this.TraderLockOwner!=null) && 
                !this.TraderLockOwner.equals(traderLockOwner)) {
            
            this.TraderLockOwner = traderLockOwner;
            logic.traderLockedRFQ(this);
        } else {
            this.TraderLockOwner = traderLockOwner;
        }
    }
    
    public String getCustFirm() {
        return CustFirm;
    }
    
    public void setCustFirm(String CustFirm) {
        this.CustFirm = CustFirm;
    }
    
    public int getRFQNLegs() {
        return RFQNLegs;
    }
    
    public void setRFQNLegs(int RFQNLegs) {
        this.RFQNLegs = RFQNLegs;
    }
    
    public String getRFQTypeStr() {
        return RFQTypeStr;
    }
    
    public void setRFQTypeStr(String RFQTypeStr) {
        this.RFQTypeStr = RFQTypeStr;
    }
    
    public int getDFDDealerActions() {
        return DFDDealerActions;
    }
    
    public void setDFDDealerActions(int DFDDealerActions) {
        this.DFDDealerActions = DFDDealerActions;
    }
    
    public String getInstrumentId() {
        return InstrumentId;
    }
    
    public void setInstrumentId(String InstrumentId) {
        this.InstrumentId = InstrumentId;
    }
    
    public int getMaturityDate() {
        return MaturityDate;
    }
    
    public void setMaturityDate(int MaturityDate) {
        this.MaturityDate = MaturityDate;
    }
    
    public int getRFQDateSettl() {
        return RFQDateSettl;
    }
    
    public void setRFQDateSettl(int RFQDateSettl) {
        this.RFQDateSettl = RFQDateSettl;
    }
    
    public int getRFQDaysSettl() {
        return RFQDaysSettl;
    }
    
    public void setRFQDaysSettl(int RFQDaysSettl) {
        this.RFQDaysSettl = RFQDaysSettl;
    }
    
    public int getRFQVerb() {
        return RFQVerb;
    }
    
    public void setRFQVerb(int RFQVerb) {
        this.RFQVerb = RFQVerb;
    }
    
    public double getRFQCustValue() {
        return RFQCustValue;
    }
    
    public void setRFQCustValue(double RFQCustValue) {
        this.RFQCustValue = RFQCustValue;
    }
    
    public double getRFQQty() {
        return RFQQty;
    }
    
    public void setRFQQty(double RFQQty) {
        this.RFQQty = RFQQty;
    }
    
    public synchronized MkvRecord getMkvRecord() {
    	if (mkvRecord != null && !mkvRecord.isValid())
    		mkvRecord = null;
    	
        return mkvRecord;
    }
    
    public synchronized void setMkvRecord(MkvRecord mkvRecord) {
    	if (mkvRecord.isValid())
    		this.mkvRecord = mkvRecord;
    	else
    		this.mkvRecord = null;
    }
    
    public void lock() {
        System.out.println("-> RFQ: Locking RFQ {" + getId() + "}");
        heartBeat(true);
    }
    
    public void unLock(int resultCode, String resultString) {
        System.out.println("-> RFQ: UnLocking RFQ {" + getId() + "}");
        
        MkvRecord rec = getMkvRecord();
        if (rec!=null) {
            try {
                MkvSupplyBuilder valuesBuilder = new MkvSupplyBuilder(rec);
                valuesBuilder.setField("CNELockOwner", "");
                valuesBuilder.setField("CNEResultCode", new Integer(resultCode));
                valuesBuilder.setField("CNEResultMessage", resultString);
                rec.transaction(valuesBuilder.getSupply(), new GeneralTransactionResult());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public void heartBeat(boolean lock) {
    	MkvRecord rec = getMkvRecord();
    	
        if (rec!=null) {
            try {
                MkvSupplyBuilder valuesBuilder = new MkvSupplyBuilder(rec);
                valuesBuilder.setField("CNELockOwner", "ion");
                if(lock) {
                	rec.transaction(valuesBuilder.getSupply(), new LockTransactionResult());
                } else {
                	rec.transaction(valuesBuilder.getSupply(), genericTransListener);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    private void notifyLock() {
        logic.rfqLocked(this);
    }
    
    public void addTrader(String trader) {
    	MkvRecord rec = getMkvRecord();
    	
        if (rec!=null) {
            try {
                System.out.println("-> RFQ: RFQ {" + getId() + "} " +
                        "Appending trader {" + trader + "}");
                MkvSupplyBuilder valuesBuilder = new MkvSupplyBuilder(rec);
                String tList = (getANTraderList().equals("")?("/" + trader + "/"):(getANTraderList() + trader + "/"));
                valuesBuilder.setField("ANTraderList", tList);
                rec.transaction(valuesBuilder.getSupply(), genericTransListener);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public void reject() {
    	MkvRecord rec = getMkvRecord();
    	
        if (rec!=null) {
            try {
                System.out.println("-> RFQ: RFQ {" + getId() + "} Rejecting");
                MkvSupplyBuilder valuesBuilder = new MkvSupplyBuilder(rec);
                valuesBuilder.setField("ActionStr", "DealerReject");
                rec.transaction(valuesBuilder.getSupply(), genericTransListener);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public void reply(boolean setAutoUpdate, double initialPrice) {
    	MkvRecord rec = getMkvRecord();
    	
        if (rec!=null) {
            try {
                System.out.println("-> RFQ: RFQ {" + getId() + "} Set autoreply" + 
                        (setAutoUpdate?" and autoupdate":""));
                MkvSupplyBuilder valuesBuilder = new MkvSupplyBuilder(rec);
                valuesBuilder.setField("ANAutoReply", new Integer(1));
                if (initialPrice!=0) {
                    valuesBuilder.setField("ANDealerValue", new Double(initialPrice));
                }
                if (setAutoUpdate) {
                    valuesBuilder.setField("ANAutoUpdate", new Integer(1));
                }
                valuesBuilder.setField("ActionStr", "DealerQuote");
                valuesBuilder.setField("DealerTrader", "andrea");
                valuesBuilder.setField("ANDealGoodForTime", new Integer(MarketDef.ON_THE_WIRE_TIME));
                rec.transaction(valuesBuilder.getSupply(), genericTransListener);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public void changePrice(double price) {
    	MkvRecord rec = getMkvRecord();
    	
        if (rec!=null) {
            try {
                //System.out.println("Set price " + getId() + " " + price);
                MkvSupplyBuilder valuesBuilder = new MkvSupplyBuilder(rec);
                valuesBuilder.setField("ANDealerValue", new Double(price));
                rec.transaction(valuesBuilder.getSupply(), genericTransListener);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public int getRFQClosed() {
        return RFQClosed;
    }
    
    public void setRFQClosed(int RFQClosed) {
        this.RFQClosed = RFQClosed;
    }
    
    public boolean isClosed() {
        return getRFQClosed()==1;
    }
    
    public boolean isCNEReserved() {
        return getCNEReserved()==1 && "".equals(getCNELockOwner());
    }
    
    public boolean isAcceptedSubject() {
        return "CustAcceptedSubject".equalsIgnoreCase(getStatusStr());
    }
    
    public boolean isSubject() {
        return "QuoteSubject".equalsIgnoreCase(getStatusStr());
    }
    
    public void setStatusStr(String statusStr) {
        StatusStr = statusStr;
    }
    
    public String getStatusStr() {
        return StatusStr;
    }

    public String getANTraderList() {
        return ANTraderList;
    }

    public void setANTraderList(String ANTraderList) {
        this.ANTraderList = ANTraderList;
    }
    
    private class GeneralTransactionResult implements MkvTransactionCallListener {
        
        public void onResult(MkvTransactionCallEvent event, byte code, String message) {
            if(code!=0) {
                System.out.println("-> RFQ: Transaction result {" + getId() + "} " + message);
            }
        }
    }
    
    private class LockTransactionResult implements MkvTransactionCallListener {
        
        public void onResult(MkvTransactionCallEvent event, byte code, String message) {
            if(code!=0) {
                System.out.println("-> RFQ: Lock Transaction result (" + getId() + ") " + message);
            } else if (!locked) {
                locked = true;
                notifyLock();
            }
        }
    }
}
