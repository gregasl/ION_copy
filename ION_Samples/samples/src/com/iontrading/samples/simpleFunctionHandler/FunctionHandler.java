/*
 * FunctionHandler
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

package com.iontrading.samples.simpleFunctionHandler;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.qos.MkvQoS;

public class FunctionHandler {
    
    /** Creates a new instance of SimpleFunctionHandler */
    public FunctionHandler(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        try {
            // Start the engine and get back the instance of Mkv (unique during the
            // life of a component).
            Mkv mkv = Mkv.start(qos);
            publishFunctions();
         } catch (MkvException e) {
            e.printStackTrace();
        }
    }
    
    private static final String SOURCE = "MYPUB";
    private static final String FUNCTION_AVG_ID = "Avg";
    private static final String FUNCTION_MIN_ID = "Min";
    
    public static void main(String[] args) {
        FunctionHandler fhandler = new FunctionHandler(args);
    }
    
    /**
     * Publish the type.
     */
    private void publishFunctions() {
        try {
            MkvFunction avg_function = new MkvFunction(
                    // the name of the function compliant to the notation SOURCE_NAME
                    SOURCE + "_" + FUNCTION_AVG_ID, 
                    // The return type
                    MkvFieldType.REAL,          
                    // the names of the arguments
                    new String[] {"Arg1", "Arg2"},  
                    // the argument types
                    new MkvFieldType[] {MkvFieldType.REAL, MkvFieldType.REAL},  
                    // help
                    "Calculate the average of two real values",
                    // the listener that will handle the requests
                    new AvgFunctionHandler());      
                    
            avg_function.publish();

            MkvFunction min_function = new MkvFunction(
                    SOURCE + "_" + FUNCTION_MIN_ID,
                    MkvFieldType.REAL,
                    new String[] {"Arg1", "Arg2"},
                    new MkvFieldType[] {MkvFieldType.REAL, MkvFieldType.REAL},
                    "Calculate the minimum of two real values",
                    new MinFunctionHandler());
                    
            min_function.publish();
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * This object is used to implement the logic behind the function Avg
     */
    private class AvgFunctionHandler implements MkvFunctionListener {
        public void onCall(MkvFunctionCallEvent mkvFunctionCallEvent) {
            // If the handler supplies more than a published function some logic must
            // be implemented to distinguish the different functions by name.
            try {
                MkvSupply argsWrapper = mkvFunctionCallEvent.getArgs();
                double arg0 = argsWrapper.getDouble(0);
                double arg1 = argsWrapper.getDouble(1);
                System.out.println("Called function Avg: arg1 {" +
                        arg0 + "} arg2 {" + arg1 + "}");
                double avg = (arg0 + arg1) / 2.0;
                System.out.println("Returning the result {" + avg + "}");
                mkvFunctionCallEvent.setResult(MkvSupplyFactory.create(avg));
            } catch (MkvException e) {
                // handle different scenarios of exception.
                try {
                    mkvFunctionCallEvent.setError((byte)-1, "Unexpected exception {" + e + "}");
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }
        }
    }
    
    /**
     * This object is used to implement the logic behind the function Min
     */
    private class MinFunctionHandler implements MkvFunctionListener {
        public void onCall(MkvFunctionCallEvent mkvFunctionCallEvent) {
            // If the handler supplies more than a published function some logic must
            // be implemented to distinguish the different functions by name.
            try {
                MkvSupply argsWrapper = mkvFunctionCallEvent.getArgs();
                double arg0 = argsWrapper.getDouble(0);
                double arg1 = argsWrapper.getDouble(1);
                System.out.println("Called function Min: arg1 {" +
                        arg0 + "} arg2 {" + arg1 + "}");
                double min = (arg0 > arg1) ? arg1 : arg0;
                System.out.println("Returning the result {" + min + "}");
                mkvFunctionCallEvent.setResult(MkvSupplyFactory.create(min));
            } catch (MkvException e) {
                // handle different scenarios of exception.
                try {
                    mkvFunctionCallEvent.setError((byte)-1, "Unexpected exception {" + e + "}");
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }
        }
    }
}
