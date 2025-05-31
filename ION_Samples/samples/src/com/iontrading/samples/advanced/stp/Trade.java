/*
 * Trade
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

package com.iontrading.samples.advanced.stp;

import java.util.Calendar;

import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSubscribeProxy;
import com.iontrading.mkv.helper.MkvSupplyUtils;

public class Trade
{
    public static final String[] FIELDS = 
            new String[] {
			    "Id",            
			    "Date",          
			    "Time",          
			    "Qty",           
			    "DateSettl",     
			    "Trader",        
			    "CPTrader",      
			    "CPMember",      
			    "TradeNo",       
			    "Code",          
			    "QtyNominal",    
			    "CurrencyStr",   
			    "VerbStr",       
			    "StatusStr",     
			    "AggressedStr",  
			    "TypeStr",       
			    "ValueTypeStr",  
			    "Value",         
			    "ValueFmt"       };
    
    
    private static MkvSubscribeProxy proxy;

    public static void initProxy() throws MkvException {
    	if (proxy == null) {
            System.out.println("Creating Proxy for Trades");
            proxy = new MkvSubscribeProxy(Trade.class);
        }        
    }
    
    private String 	id;
    private Calendar tradeTimeStamp;
    private double 	qty;
    private Calendar dateSettl;
    private String 	trader;
    private String 	CPTrader;
    private String 	CPMember;
    private int 	tradeNo;
    private String 	code;
    private double 	qtyNominal;
    private String 	currencyStr;
    private String 	verbStr;
    private String 	statusStr;

    private String 	aggressedStr;
    private String 	typeStr;

    private String 	valueTypeStr;
    private double 	value;
    private int 	valueFmt;
    
    public Trade() {
    }

    public void update(MkvRecord rec, MkvSupply supply) throws MkvException {
    	Trade.initProxy();
    	proxy.update(rec, supply, this);
    }
	
	public String toString() {
		return "TRADE : id {" + getId() + "} " +
                "instrId {" + getCode() + "} " +
                "verb {" + getVerbStr() + "} " +
                "qty {" + getQty() + "} value {" + getValue() + "}";		
	}

	/**
	 * @return Returns the _date.
	 */
	public Calendar getDate() {
		return tradeTimeStamp;
	}

	/**
	 * @param _date The _date to set.
	 */
	public void setDate(int _date) {
		if (tradeTimeStamp==null) {
			tradeTimeStamp = Calendar.getInstance();
		}
		
		MkvSupplyUtils.setMkvDate(tradeTimeStamp, _date);
	}

	/**
	 * @return Returns the _dateSettl.
	 */
	public Calendar getDateSettl() {
		return dateSettl;
	}

	/**
	 * @param settl The _dateSettl to set.
	 */
	public void setDateSettl(int settl) {
		if (dateSettl==null) {
			dateSettl = Calendar.getInstance();
		}
		
		MkvSupplyUtils.setMkvDate(tradeTimeStamp, settl);
		MkvSupplyUtils.setMkvTime(tradeTimeStamp, 0);
	}

	/**
	 * @return Returns the _time.
	 */
	public Calendar getTime() {
		return tradeTimeStamp;
	}

	/**
	 * @param _time The _time to set.
	 */
	public void setTime(int _time) {
		if (tradeTimeStamp==null) {
			tradeTimeStamp = Calendar.getInstance();
		}
		
		MkvSupplyUtils.setMkvTime(tradeTimeStamp, _time);
	}

	/**
	 * @return Returns the aggressedStr.
	 */
	public String getAggressedStr() {
		return aggressedStr;
	}

	/**
	 * @param aggressedStr The aggressedStr to set.
	 */
	public void setAggressedStr(String aggressedStr) {
		this.aggressedStr = aggressedStr;
	}

	/**
	 * @return Returns the code.
	 */
	public String getCode() {
		return code;
	}

	/**
	 * @param code The code to set.
	 */
	public void setCode(String code) {
		this.code = code;
	}

	/**
	 * @return Returns the cPMember.
	 */
	public String getCPMember() {
		return CPMember;
	}

	/**
	 * @param member The cPMember to set.
	 */
	public void setCPMember(String member) {
		CPMember = member;
	}

	/**
	 * @return Returns the cPTrader.
	 */
	public String getCPTrader() {
		return CPTrader;
	}

	/**
	 * @param trader The cPTrader to set.
	 */
	public void setCPTrader(String trader) {
		CPTrader = trader;
	}

	/**
	 * @return Returns the currencyStr.
	 */
	public String getCurrencyStr() {
		return currencyStr;
	}

	/**
	 * @param currencyStr The currencyStr to set.
	 */
	public void setCurrencyStr(String currencyStr) {
		this.currencyStr = currencyStr;
	}

	/**
	 * @return Returns the id.
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id The id to set.
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return Returns the qty.
	 */
	public double getQty() {
		return qty;
	}

	/**
	 * @param qty The qty to set.
	 */
	public void setQty(double qty) {
		this.qty = qty;
	}

	/**
	 * @return Returns the qtyNominal.
	 */
	public double getQtyNominal() {
		return qtyNominal;
	}

	/**
	 * @param qtyNominal The qtyNominal to set.
	 */
	public void setQtyNominal(double qtyNominal) {
		this.qtyNominal = qtyNominal;
	}

	/**
	 * @return Returns the statusStr.
	 */
	public String getStatusStr() {
		return statusStr;
	}

	/**
	 * @param statusStr The statusStr to set.
	 */
	public void setStatusStr(String statusStr) {
		this.statusStr = statusStr;
	}

	/**
	 * @return Returns the tradeNo.
	 */
	public int getTradeNo() {
		return tradeNo;
	}

	/**
	 * @param tradeNo The tradeNo to set.
	 */
	public void setTradeNo(int tradeNo) {
		this.tradeNo = tradeNo;
	}

	/**
	 * @return Returns the trader.
	 */
	public String getTrader() {
		return trader;
	}

	/**
	 * @param trader The trader to set.
	 */
	public void setTrader(String trader) {
		this.trader = trader;
	}

	/**
	 * @return Returns the typeStr.
	 */
	public String getTypeStr() {
		return typeStr;
	}

	/**
	 * @param typeStr The typeStr to set.
	 */
	public void setTypeStr(String typeStr) {
		this.typeStr = typeStr;
	}

	/**
	 * @return Returns the value.
	 */
	public double getValue() {
		return value;
	}

	/**
	 * @param value The value to set.
	 */
	public void setValue(double value) {
		this.value = value;
	}

	/**
	 * @return Returns the valueFmt.
	 */
	public int getValueFmt() {
		return valueFmt;
	}

	/**
	 * @param valueFmt The valueFmt to set.
	 */
	public void setValueFmt(int valueFmt) {
		this.valueFmt = valueFmt;
	}

	/**
	 * @return Returns the valueTypeStr.
	 */
	public String getValueTypeStr() {
		return valueTypeStr;
	}

	/**
	 * @param valueTypeStr The valueTypeStr to set.
	 */
	public void setValueTypeStr(String valueTypeStr) {
		this.valueTypeStr = valueTypeStr;
	}

	/**
	 * @return Returns the verbStr.
	 */
	public String getVerbStr() {
		return verbStr;
	}

	/**
	 * @param verbStr The verbStr to set.
	 */
	public void setVerbStr(String verbStr) {
		this.verbStr = verbStr;
	}
}
