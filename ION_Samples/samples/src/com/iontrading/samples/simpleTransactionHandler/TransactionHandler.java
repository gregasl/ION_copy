/*
 * TransactionHandler
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

package com.iontrading.samples.simpleTransactionHandler;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.events.MkvTransactionCallEvent;
import com.iontrading.mkv.events.MkvTransactionListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyBuilder;
import com.iontrading.mkv.qos.MkvQoS;

public class TransactionHandler implements MkvTransactionListener {
    
    private static final String SOURCE = "MYPUB";
    private static final String RECORD_PREFIX = "EUR.PRICE." + SOURCE + ".";
    private static final String RECORD_ID = "FGBLH3";
    private static final String TYPE_NAME = SOURCE + "_Quote";
    
    private static final String[] FIELD_NAMES = { "ID", "ASK", "BID", "QTY" };
    private static final MkvFieldType[] FIELD_TYPES = {
        MkvFieldType.STR, MkvFieldType.REAL,
        MkvFieldType.REAL, MkvFieldType.REAL };

    private static final int ID = 0;
    private static final int ASK = 1;
    private static final int BID = 2;
    private static final int QTY = 3;

    private MkvRecord record;

    public TransactionHandler(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        qos.setTransactionListeners(new MkvTransactionListener[] {this});
        try {
            // Start the engine and get back the instance of Mkv (unique during the
            // life of a component).
            Mkv mkv = Mkv.start(qos);
            
            publishType();
            publishRecord(RECORD_ID);
            supplyRecord(RECORD_ID, 100.50, 99.75, 5);
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        TransactionHandler publisher = new TransactionHandler(args);
    }

    /**
     * Publish the type.
     */
    private void publishType() {
        try {
            MkvType type = new MkvType(TYPE_NAME, FIELD_NAMES, FIELD_TYPES);
            type.publish();
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    /**
     * Publish a record given its id.
     * @param id    The record id (ex. EUR.PRICE.SOURCE.FGBLH3)
     */
    private void publishRecord(String id) {
        try {
            MkvRecord rec = new MkvRecord(RECORD_PREFIX + RECORD_ID, TYPE_NAME);
            rec.publish();
            int fields[] = { ID };
            Object values[] = { id };
            rec.supply(fields, values);
            record = rec;
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    /**
     * Update a record with the given values.
     * @param id    The record id (ex. EUR.PRICE.SOURCE.FGBLH3)
     * @param ask   The ask value.
     * @param bid   The bid value.
     * @param qty   The qty value.
     */
    private void supplyRecord(String id, double ask, double bid, double qty) {
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

    /**
     * suporting transaction on the QTY
     */
    public void onCall(MkvTransactionCallEvent event) {

        System.out.println("Called transaction by " + event.getCaller());

        MkvSupply supply = event.getSupply();

        try {
            if (supply.isSet(QTY)) {
                MkvRecord rec = event.getRecord();
                MkvSupplyBuilder supBuilder = new MkvSupplyBuilder(rec);
                supBuilder.setField(QTY, supply.getObject(QTY));
                rec.supply(supBuilder.getSupply());
                event.setResult((byte)0, "OK");
            } else {
                event.setResult((byte)-1, "Field unsupported");
            }
        } catch (MkvException e) {
            // handle the exception
            e.printStackTrace();

            // try send back an error
            try {
                event.setResult((byte)-99, "Error " + e);
            } catch (MkvException ee) {
            }
        }
    }
}