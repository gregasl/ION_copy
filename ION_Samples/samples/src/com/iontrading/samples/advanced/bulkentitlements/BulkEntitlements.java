/*
 * BulkEntitlements
 *
 * Perform a bulk request to evaluate the entitlement permissions
 * on a custom stream of resources.
 *
 * ION Trading U.K. Limited supplies this software code for testing purposes
 * only. The scope of this software code is exclusively limited to the
 * demonstration of the structure of an application using the ION(tm) Common
 * Market and ION Trading U.K. Limited does not guarantee the correct behavior
 * of any deployed application using this software code. This software code has
 * not been thoroughly tested under all conditions. ION, therefore, cannot
 * guarantee or imply reliability, serviceability, or function of this software.
 * Any use of this software outside of this defined scope is the sole
 * responsibility of the user.
 *
 * Copyright 2005 ION Trading srl. All rights reserved.
 */
 package com.iontrading.samples.advanced.bulkentitlements;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.bulkentitlements.IMkvBulkEntitlementRequest;
import com.iontrading.mkv.bulkentitlements.IMkvBulkEntitlementRequestListener;
import com.iontrading.mkv.bulkentitlements.IMkvBulkEntitlementResourceStreamFactory;
import com.iontrading.mkv.bulkentitlements.IMkvBulkEntitlementResourceStream;
import com.iontrading.mkv.bulkentitlements.IMkvBulkEntitlementResourceCallback;
import com.iontrading.mkv.bulkentitlements.MkvBulkEntitlementRequest;
import com.iontrading.mkv.bulkentitlements.MkvEntitlementEngine.MkvSingleEntitlementResult;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.qos.MkvQoS;

public class BulkEntitlements {
    private Mkv mkv;

    public static void main(String commands[]) { new BulkEntitlements(commands); }

    public BulkEntitlements(String[] commands) {
        try {
            MkvQoS qos = new MkvQoS();
            qos.setArgs(commands);
            mkv = Mkv.start(qos);
            mkv.getPlatform().addPlatformListener(new MyPlatformListener());
        } catch (Exception e) { e.printStackTrace(); }
    }

    class MyPlatformListener implements MkvPlatformListener {
        public void onMain(MkvPlatformEvent event) {
            if (event == MkvPlatformEvent.START) {
                resources = new CopyOnWriteArraySet<String>();
                for(int i=0; i<1000; ++i) resources.add("Resource_" + i);

                /**
                 * Server applications can start the Bulk Query Server
                 * so that the Java API will publish the pertinent <source>_bulkQuery function
                 * on the ION Bus and handle the incoming requests.
                 *
                 * For each request, a Message Queue of results is created automatically
                 * by the Java API and all permissions are added as records inside the Message Queue
                 *
                 * Using the startBulkQueryService, all the management of requests and replies via Message Queue
                 * is delegated to the Java API
                 */
                IMkvBulkEntitlementResourceStreamFactory myFactory = new IMkvBulkEntitlementResourceStreamFactory() {
                    public void openResourceStream(String resourceFilter, IMkvBulkEntitlementResourceCallback callback) {
                        IMkvBulkEntitlementResourceStream stream = new MyStream(resources, resourceFilter, callback);
                        callback.onStreamStart(stream);
                    }
                };

                mkv.getEntitlementManager().startBulkQueryService("TRADE_SERVER", myFactory);

                /**
                 * Alternatively, more sophisticated client applications may decide
                 * to fire a query to the Java API and receive the result set
                 * on an application-level listener for further processing and filtering
                 *
                 * This use case is particularly useful if  the application needs to apply
                 * a customized business logics on top of the query results
                 */
                IMkvBulkEntitlementRequestListener myListener = new IMkvBulkEntitlementRequestListener() {
                    public void onQueryStarted(IMkvBulkEntitlementRequest request) {
                        System.out.println("Query " + request + " started now");
                    }

                    public void onResourceEvaluated(IMkvBulkEntitlementRequest request, String resource, String feature, MkvSingleEntitlementResult result) {
                        System.out.println("Resource Evaluated for Query " + request + ": Resource={" + resource + "} Permission={" + result +"}");
                    }

                    public void onQueryEnded(IMkvBulkEntitlementRequest request) {
                        System.out.println("Query " + request + " completed now");
                    }

                    public void onQueryTimeout(IMkvBulkEntitlementRequest request) {
                        System.out.println("ERROR! Query " + request + " has timeout");
                    }

                    public void onQueryError(IMkvBulkEntitlementRequest request, byte errorCode, String errorMessage) {
                        System.out.println("ERROR! Query " + request + " has ended with failure, ErrorCode={" + errorCode + "} ErrorMessage={" + errorMessage + "}");
                    }
                };

                mkv.getEntitlementManager().bulkQuery(new MkvBulkEntitlementRequest("TRADE_SERVER", "mkv"), myFactory,
                        "A.B", myListener);
            }
        }
        public void onComponent(MkvComponent component, boolean registered) {}
        public void onConnect(String component, boolean connected) {}
    }

    class MyStream implements IMkvBulkEntitlementResourceStream {
        private volatile boolean paused = false;
        private volatile boolean canceled = false;

        MyStream(final Collection<String> data, final String resourceFilter, final IMkvBulkEntitlementResourceCallback callback) {
            new Thread(new Runnable() {
                public void run() {
                    try {
                        for(String resource : data) {
                            if(resource.startsWith(resourceFilter))
                                callback.onResourceAvailable(resource);

                            if(paused) {
                                try { synchronized(MyStream.this) { MyStream.this.wait(); } }
                                catch(Exception e) { e.printStackTrace(); }
                            }

                            if(canceled) break;
                        }
                    } finally { callback.onStreamEnd(); }
                }
            }).start();
        }

        public void pause() throws UnsupportedOperationException  { synchronized(this) { paused = true; } }
        public void resume() throws UnsupportedOperationException { synchronized(this) { paused = false; this.notifyAll(); } }
        public void cancel()                                      { canceled = true; resume(); }
    }

    CopyOnWriteArraySet<String> resources;
}
