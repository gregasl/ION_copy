/*
 * Recordset
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

package com.iontrading.samples.recordset;

import java.util.Arrays;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.MkvRecordset;
import com.iontrading.mkv.MkvSubscriptionRequestListener;
import com.iontrading.mkv.MkvSubscriptionRequest;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.MkvType;

/**
 *
 * @author lveraldi
 */
public class Recordset {

    /** Creates a new instance of Recordset */
    public Recordset(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        try {
            // Start the engine and get back the instance of Mkv (unique during the
            // life of a component).
            Mkv mkv = Mkv.start(qos);

			// Publish the Recordset as soon as the application starts,
			// using a custom MkvSuscriptionRequestListener
			// to react to remote record subscriptions matching the Recordset name
            MkvType type = new MkvType("BTEC_DM_DEPT_TYPE",
            	new String[] { "ID", "ASK", "BID" },
            	new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.REAL, MkvFieldType.REAL});
            type.publish();

            MkvRecordset recordset = new MkvRecordset("EUR.CM_DEPTH.BTEC.", "BTEC_DM_DEPT_TYPE", new SubcriptionRequestListener());
            recordset.publish();

        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Recordset(args);
    }

    class SubcriptionRequestListener implements MkvSubscriptionRequestListener {
	    public void onSubscriptionRequest(MkvRecordset recordset, MkvSubscriptionRequest request) {
	        // Handle the request, either synchronously or asynchronously,
                // based on business logics
                System.out.println("Request for subscription of Record {" + request.getRecordName() + "} " +
                                   "Fields {" + Arrays.toString(request.getFieldNames()) + "} " +
                                   "From Component {" + request.getSubscriber() + "}");

                // The publisher can take decision about each subscription request
                // based on:
                // - the record name and field name list of the requested subscription
                // - the component and/or the corresponding user name this subscription request comes from
                // - entitlement criteria that applies to the specific record or component or user name
                // - the content of the specific record fields (content based entitlements)

                // Accept the subscription by providing a snapshot supply for the requested fields
                recordset.acceptSubscription(request, MkvSupplyFactory.create(new Object[] { 0.0f, 1.0f }));

                // or Reject the subscription with error
                recordset.rejectSubscription(request, "Component is not entitled!");

                // The publisher can produce supplies for any record matching the published Recordset
                // without the corresponding MkvRecord object
                // This can be done using the static supplyRecord method of the MkvRecordset object
                try {
					recordset.supplyRecord("EUR.CM_INSTRUMENT.SRC.INSTR_001", MkvSupplyFactory.create(new Object[] { 0.0f }));
				} catch(Exception e) {
					e.printStackTrace();
				}
           }
    }
}
