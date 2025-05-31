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

import com.iontrading.mkv.events.MkvMQSubscribeEvent;

/**
 * A generic callback to react to XRS Query associated events 
 */
public interface XRSQueueCallback {

    /**
     * The status of the XRS query
     * For a details explanation of the state machine for the query, 
     * please refer to https://support.iongroup.com/library/content/core-technology/p9077836/p9085122/xrs-interface/accessing-xrs/running-queries/monitoring-the-query-status/
     */
    enum QueryStatus {
        SNAPSHOT, LIVE, STALE
    };


    /**
     * Event raised for each incoming supply from the XRS query result
     * @param event the incoming XRS supply
     */
    void onEvent(MkvMQSubscribeEvent event);

    /**
     * Defines the beginning of the initial query snapshot download
     */
    void onDownloadBegin();

    /**
     * Defines the completion of the query snapshot download
     */
    void onDownloadEnd();
    
    
    /**
     * Notifies that the set of entities for this XRS query shall be reset
     */
    void onDeleteAll();

    /**
     * Notifies the transitions of the underlying XRS query
     * @param status The new status of this XRS query
     * @param details An optional string with a human readable description of the status
     */
    void onQueryStatus(QueryStatus status, String details);

    /**
     * Notifies of any application errors from the Server
     * @param code the error code as provided by the Server
     * @param message the message string as provided by the Server
     */
    void onError(int code, String message);

    /**
     * Notifies of any unexpected exception during the XRS event processing
     * @param throwable the underlying exception thrown during processing
     */
    void onException(Throwable throwable);

    /**
     * Notifies of any unexpected exception during the XRS event processing
     * @param event the MkvMQSubscribeEvent that triggers the exception
     * @param throwable the underlying exception thrown during processing
     */
    void onException(MkvMQSubscribeEvent event, Throwable throwable);
}
