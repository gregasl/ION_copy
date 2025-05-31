/*
 * FunctionCaller
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

package com.iontrading.samples.simpleFunctionCaller;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionCallListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.qos.MkvQoS;

public class FunctionCaller {
    
    private static final String SOURCE = "MYPUB";
    private static final String FUNCTION_AVG_ID = "Avg";
    private static final String FUNCTION_MIN_ID = "Min";
    
    private MkvFunction avgFn;
    
    /** Creates a new instance of Subscriber */
    public FunctionCaller(String[] args) {
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        qos.setPublishListeners(new MkvPublishListener[] {new PublishListener()});
        try {
            Mkv mkv = Mkv.start(qos);
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        FunctionCaller sub = new FunctionCaller(args);
    }
    
    /**
     * Call the function and register a listener for the result of that individual call
     */
    private void callAvg() {
        if (avgFn==null) {
            System.out.println("The Avg function is not available!");
        } else {
            MkvSupply args = MkvSupplyFactory.create(new Object[] {
                new Double(2.5), new Double(3.5)});
            try {
                // Call the function specifing a function result handler for each call
                // The instance of handler will discriminate the different calls.
                avgFn.call(args, new AvgFunctionListener());
                System.out.println("Function Avg called succesfully");
            } catch (MkvException e) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * Handle the function result events
     */
    private class AvgFunctionListener implements MkvFunctionCallListener {
        public void onError(MkvFunctionCallEvent mkvFunctionCallEvent, 
                byte errCode, String errStr) {
            
            System.out.println("Error result for Avg function: " +
                    "code {" + errCode + "} message {" + errStr + "}");
        }
        
        public void onResult(
                MkvFunctionCallEvent mkvFunctionCallEvent, MkvSupply result) {
            
            try {
                System.out.println("Result for Avg function {" + result.getDouble(0) + "}");
            } catch (MkvException e) {
                e.printStackTrace();
            }
        }
    }
    
    private class PublishListener implements MkvPublishListener {
        public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
            // don't want to react to each publication but will look in the data
            // dictionary for the needed functions in the onPublishIdle event.
        }
        
        public void onPublishIdle(String component, boolean start) {
            if (start) {
                // looking for the Avg functions
                MkvFunction _avgFn = Mkv.getInstance().getPublishManager().getMkvFunction(
                        SOURCE + "_" + FUNCTION_AVG_ID);
                // if the function is in the data dictionary it just call it.
                if (_avgFn!=null) {
                    avgFn = _avgFn;
                    callAvg();
                }
            }
        }
        
        public void onSubscribe(MkvObject mkvObject) {
            // not interested in this event because our component is a pure subscriber
            // and is not supposed receiving request for subscriptions.
        }
    }
}
