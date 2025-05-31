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

import java.util.Arrays;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.events.MkvMQSubscribeEvent;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

/**
 * Main class for the xRS sample client.
 * For further information about the XRS Protocol, 
 * please refer to https://support.iongroup.com/library/content/core-technology/p9077836/p9085122/xrs-interface/accessing-xrs/running-queries/ 
 */
public class XRSClient {

    public static void main(String[] args) throws MkvException {
        // Initialize and start ION bus library
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        Mkv.start(qos);
        new XRSClient();
    }

    public XRSClient() {
        searchCounterpartiesByCountry("US");
    }

    private void searchCounterpartiesByCountry(String filter) {

        FunctionCall.call("CPS_SearchCounterparty", new FunctionCallback<Object[]>() {
            public void onSuccess(Object event, Object[] result, int errorCode, String errorMessage) {
                System.out.println("xRS function call result: " + Arrays.asList(result).toString());
                
                String queueName = null;
                
                for(int i=0; i<result.length; ++i) {
                    if("QueueName".equalsIgnoreCase(result[i].toString())) {
                        queueName = result[i+1].toString();
                        break;
                    }
                }
                
                if(queueName == null) {
                    throw new IllegalStateException("Function call did not return Message Queue name! Result: " + Arrays.asList(result).toString());
               }
                
                new XRSQueueSubscriber(queueName, new String[] { "Id", "CodeMapping", "AddressList", "ContactsList", "SoftDeleted" }, 
                        new XRSQueueCallback() {
                    public void onEvent(MkvMQSubscribeEvent event) {
                        // TODO Map this event onto the application business logic                        
                    }

                    public void onDownloadBegin() {
                        System.out.println("Query Download Begin");
                        // TODO Map this event onto the application business logic                        
                    }

                    public void onDownloadEnd() {
                        System.out.println("Query Download End");
                        // TODO Map this event onto the application business logic                        
                    }

                    public void onDeleteAll() {
                        System.out.println("Query Reset");
                        // TODO Map this event onto the application business logic                        
                    }

                    public void onQueryStatus(QueryStatus status, String details) {
                        System.out.println("Query Status Changed: " + status + " Details: " + details);
                    }

                    public void onError(int code, String message) {
                        System.out.println("Query ERROR! Code: " + code + " Message: " + message);
                    }

                    public void onException(Throwable throwable) {
                        System.out.println("Query EXCEPTION! " + throwable);
                        throwable.printStackTrace();
                    }

                    public void onException(MkvMQSubscribeEvent event, Throwable throwable) {
                        System.out.println("Query EXCEPTION! " + throwable);
                        throwable.printStackTrace();
                    }
                });
            }

            public void onFailure(Object event, Exception exception) {
                exception.printStackTrace();
            }

        }, 
                //Specify the set of required fields that will be actually subscribed, to optimized the initial snapshot processing on Server-side
                "ARRAY.-Q_SubFields.5.STR", "Id", "CodeMapping", "AddressList", "ContactsList", "SoftDeleted",
                //For XRS Filtering Criteria,
                //refer to https://support.iongroup.com/library/content/core-technology/p9077836/p9085122/xrs-interface/accessing-xrs/running-queries/filter-criteria/
                "ARRAY.~:Country.1.STR", filter);  
                                             
    }
}
