package com.iontrading.samples.advanced.hugepub;

import java.util.Random;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyBuilder;
import com.iontrading.mkv.qos.MkvQoS;

public class HugePub {
    
    private static final String SOURCE = "MYPUB";
    private static final String RECORD_PREFIX = "EUR.PRICE." + SOURCE + ".";
    
    private static final String CHAIN_ID = "QUOTES";
        
    private static final String TYPE_NAME = SOURCE + "_Quote";

    private String[] names;
    private MkvFieldType[] types;
    private int numRec;
    private int[] fieldIds;
    private MkvType theType;
    
    /** Creates a new instance of ChainPublisher */
    public HugePub(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        try {
            // Start the engine and get back the instance of Mkv (unique during the
            // life of a component).
            Mkv.start(qos);
            
            // publish the type and the chain
            if (publishType()) {
                initChain(CHAIN_ID);
                new TheTimer().start();
            } else {
                // type has not been published
            }
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        new HugePub(args);
    }

    /**
     * Publish the chain.
     * @param chainId The id of the chain.
     */
    private MkvChain publishChain(String chainId) {
        try {
            MkvChain chain = new MkvChain(RECORD_PREFIX + chainId, TYPE_NAME);
            chain.publish();
            return chain;
        } catch (MkvException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Publish the type.
     * @retun true if the type is correctly published, false otherwise (for example
     *  the type has already been published).
     */
    private boolean publishType() {
        try {
        	String _names = Mkv.getInstance().getProperties().getProperty("names");
        	if (_names==null || _names.length()==0) {
        		_names = "S0 R0 R1 I0";
        	}
        	_names = "ID " + _names;

        	String _types = Mkv.getInstance().getProperties().getProperty("types");
        	if (_types==null || _types.length()==0) {
        		_types = "S R R I";
        	}
        	_types = "I " + _types;
        	
        	names = _names.split("\\s");
        	String[] __types = _types.split("\\s");
        	types = new MkvFieldType[__types.length];
        	fieldIds = new int[__types.length];
        	for (int i = 0; i < __types.length; i++) {
				if ("R".equals(__types[i])) types[i] = MkvFieldType.REAL;
				else if ("S".equals(__types[i])) types[i] = MkvFieldType.STR;
				else if ("I".equals(__types[i])) types[i] = MkvFieldType.INT;
				fieldIds[i] = i;
			}
        	
            theType = new MkvType(TYPE_NAME, names, types);
            theType.publish();
            return true;
        } catch (MkvException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Publish the chain, records and append the records to the chain.
     * @param chainId   The id of the chain.
     * @param recordIds The array of the record ids.
     */
    private void initChain(String chainId) {
        MkvChain chain = publishChain(chainId);
        
        numRec = Mkv.getInstance().getProperties().getIntProperty("numrec");
        
        
        Mkv.getInstance().getPublishManager().publishStop();
        if (chain!=null) {
            // the chain has been published correctly
            for (int i=0; i<numRec; i++) {
                MkvRecord record = publishRecord(i);
                if (record!=null) {
                    // the record has been published correctly.
                    // appending it to the chain.
                    chain.add(record.getName());
                }
            }
        }
        Mkv.getInstance().getPublishManager().publishStart();
    }

    /**
     * Publish a record given its id.
     * @param id    The record id (ex. EUR.PRICE.SOURCE.FGBLH3)
     */
    private MkvRecord publishRecord(int id) {
        try {
            MkvRecord rec = new MkvRecord(RECORD_PREFIX + "RECORD" + id, TYPE_NAME);
            rec.publish();
            supplyRecord(rec, id);
            return rec;
        } catch (MkvException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Update a record with the given values.
     * @param record  The record to be updated
     * @param ask   The ask value.
     * @param bid   The bid value.
     * @param qty   The qty value.
     */
    private void supplyRecord(MkvRecord record, int id) {
    	Object values[] = new Object[names.length];
    	values[0] = new Integer(id);
    	for (int i = 1; i<values.length; i++) {
    		switch (types[i].intValue()) {
    			case MkvFieldType.INT_code:
    				values[i] = genRandomInt(i);
    				break;
	    		case MkvFieldType.REAL_code:
	    			values[i] = genRandomDbl(i);
    				break;
	    		case MkvFieldType.STR_code:
	    			values[i] = genRandomStr(i);
    				break;
    			default:
    				break;
    		}
    	}
    	
        try {
            record.supply(fieldIds, values);
            // ok, the record has been updated
        } catch (MkvException e) {
            e.printStackTrace();
            // a problem prevented the record to be updated
        }
    }
    
    private Random random = new Random();
    
    private String genRandomStr(int i) {
    	return "RECORDSTR" + random.nextInt();
    }
    
    private Integer genRandomInt(int i) {
    	return new Integer(random.nextInt());
    }

    private Double genRandomDbl(int i) {
    	return new Double(random.nextInt());
    }
    
    private long updates = 0;
    
    private void randomUpdate(int numUpd, int numFields) throws Exception 
    {
    	MkvPublishManager man = Mkv.getInstance().getPublishManager();
    	for (int i = 0; i < numUpd; i++) {
    		int recNo = (int)((double)(numRec-1) * random.nextDouble() + .5);
    		MkvSupplyBuilder builder = new MkvSupplyBuilder(theType);
    		for (int j = 0; j<numFields; j++) {
    			int f = (int)((double)(types.length-2) * random.nextDouble() + .5) + 1;
    			switch (types[f].intValue()) {
    			case MkvFieldType.INT_code:
    				builder.setField(f, genRandomInt(f));
    				break;
    			case MkvFieldType.REAL_code:
    				builder.setField(f, genRandomDbl(f));
    				break;
    			case MkvFieldType.STR_code:
    				builder.setField(f, genRandomStr(f));
    				break;
    			}
    		}
    		MkvRecord rec = (MkvRecord)man.getMkvRecords().get(recNo);
  			rec.supply(builder.getSupply());
  			updates++;
		}
    }
    
    private class TheTimer extends Thread
    {
    	int period = -1;
    	int numRecordsToUpdate = 0;
    	int numFieldsToUpdate = 0;

    	TheTimer() {
    		setDaemon(true);
    	}
    	
    	public void run() {
    		period = Mkv.getInstance().getProperties().getIntProperty("period");
    		numRecordsToUpdate = Mkv.getInstance().getProperties().getIntProperty("updrecs");
    		numFieldsToUpdate = Mkv.getInstance().getProperties().getIntProperty("updfields");
    		if (numRecordsToUpdate==0) numRecordsToUpdate = 1;
    		if (numFieldsToUpdate==0) numFieldsToUpdate = 1;
    		try {
	    		while (true) {
	    			randomUpdate(numRecordsToUpdate, numFieldsToUpdate);
	    			if (period>0) {
	    				Thread.sleep(period);
	    			}
	    		}
    		} catch (Exception w) {
    			w.printStackTrace();
    		}
    	}
    }
}
