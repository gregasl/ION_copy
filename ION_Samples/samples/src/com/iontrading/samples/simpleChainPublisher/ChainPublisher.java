/*
 * ChainPublisher
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

package com.iontrading.samples.simpleChainPublisher;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

public class ChainPublisher {
    
    private static final String SOURCE = "MYPUB";
    private static final String RECORD_PREFIX = "EUR.PRICE." + SOURCE + ".";
    
    private static final String[] RECORD_ID = {
        "FGBLH3", "FGBLH2", "FGBLH1"};
    private static final String CHAIN_ID = "QUOTES";
        
    private static final String TYPE_NAME = SOURCE + "_Quote";
        
    private static final String[] FIELD_NAMES = { "ID", "ASK", "BID", "QTY" };
    private static final MkvFieldType[] FIELD_TYPES = {
        MkvFieldType.STR, MkvFieldType.REAL,
        MkvFieldType.REAL, MkvFieldType.REAL };
                    
    private static final int ID = 0;
    private static final int ASK = 1;
    private static final int BID = 2;
    private static final int QTY = 3;

    /** Creates a new instance of ChainPublisher */
    public ChainPublisher(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        try {
            // Start the engine and get back the instance of Mkv (unique during the
            // life of a component).
            Mkv mkv = Mkv.start(qos);
            
            // publish the type and the chain
            if (publishType()) {
                initChain(CHAIN_ID, RECORD_ID);
            } else {
                // type has not been published
            }
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        ChainPublisher publisher = new ChainPublisher(args);
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
            MkvType type = new MkvType(TYPE_NAME, FIELD_NAMES, FIELD_TYPES);
            type.publish();
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
    private void initChain(String chainId, String[] recordIds) {
        MkvChain chain = publishChain(chainId);
        if (chain!=null) {
            // the chain has been published correctly
            for (int i=0; i<recordIds.length; i++) {
                MkvRecord record = publishRecord(recordIds[i]);
                if (record!=null) {
                    // the record has been published correctly.
                    // appending it to the chain.
                    chain.add(record.getName());
                    // supplying the initial values for the record.
                    supplyRecord(record, 99.9, 100.1, 5 + i);
                }
            }
        }
    }

    /**
     * Publish a record given its id.
     * @param id    The record id (ex. EUR.PRICE.SOURCE.FGBLH3)
     */
    private MkvRecord publishRecord(String id) {
        try {
            MkvRecord rec = new MkvRecord(RECORD_PREFIX + id, TYPE_NAME);
            rec.publish();
            int fields[] = { ID };
            Object values[] = { id };
            rec.supply(fields, values);
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
    private void supplyRecord(MkvRecord record, double ask, double bid, double qty) {
        int fields[] = {ASK, BID, QTY};
        Object values[] = {new Double(ask), new Double(bid), new Double(qty)};
        try {
            record.supply(fields, values);
            // ok, the record has been updated
        } catch (MkvException e) {
            e.printStackTrace();
            // a problem prevented the record to be updated
        }
    }
}
