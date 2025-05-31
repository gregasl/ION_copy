/*
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
 * ION Trading ltd (2017)
 */


package com.iontrading.samples.xrsClient;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.events.MkvAvailabilityListener;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionCallListener;
import com.iontrading.mkv.helper.MkvSupplyFactory;

/**
 * Takes care about a function call.
 */
public class FunctionCall {
	
    /**
     * Calls an ION Bus function with given parameters.
     */
    public static void call(final String functionName, final FunctionCallback<Object[]> callback, final Object... parameters) {
        final MkvPublishManager pubm = Mkv.getInstance().getPublishManager();

        MkvAvailabilityListener availabilityListener = new MkvAvailabilityListener() {
            public void onPublish(MkvObject object, boolean start) {
                if (!start || object.getMkvObjectType() != MkvObjectType.FUNCTION) {
                    return;
                }
                MkvFunction mkvFunction = (MkvFunction) object;

                System.out.printf("Calling function %s(%s)", functionName, Arrays.asList(parameters).toString());

                try {
                    mkvFunction.call(MkvSupplyFactory.create(parameters), new MkvFunctionCallListener() {
                        public void onResult(MkvFunctionCallEvent event, MkvSupply supply) {
                            System.out.println("Function result received.");
                            Object[] result = SupplyUtils.toArray(supply);
                            String[] result0 = result[0].toString().split(":");
                            int errorCode = Integer.parseInt(result0[0]);
                            String errorMessage = result0[1];
                            callback.onSuccess(event, result, errorCode, errorMessage);
                        }

                        public void onError(MkvFunctionCallEvent event, byte errcode, String error) {
                            System.err.println("Function error received.");
                            callback.onFailure(event, new InvocationTargetException(null, error));
                        }
                    });
                } catch (Exception ex) {
                    throw new RuntimeException("Exception when calling function", ex);
                }

                pubm.removeAvailabilityListener(this);
            }
        };

        pubm.addAvailabilityListener(functionName, availabilityListener);
    }
}
