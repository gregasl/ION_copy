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
import com.iontrading.mkv.enums.MkvMQAction;
import com.iontrading.mkv.events.MkvMQSubscribeEvent;
import com.iontrading.mkv.events.MkvMQSubscribeListener;
import com.iontrading.mkv.messagequeue.MkvMQSubscribe;
import com.iontrading.mkv.messagequeue.MkvMQSubscribeConf;
import com.iontrading.samples.xrsClient.XRSQueueCallback.QueryStatus;

/**
 * Handles XRS Query subscription and associated events
 */
public class XRSQueueSubscriber {

    /**
     * Subscribes to a MQ with a given name. For further reference about XRS
     * protocol design, please refer to
     */
    public XRSQueueSubscriber(final String queueName, final String[] fields, final XRSQueueCallback callback) {
        System.out.println(String.format("Subscribing to the queue %s", queueName));
        MkvMQSubscribeListener listener = new MkvMQSubscribeListener() {
            public void onUpdate(MkvMQSubscribeEvent event) {
                System.out.println("Processing MQ event: " + toString(event));

                switch (event.getAction().intValue()) {
                case MkvMQAction.USER_code:
                    String parts[] = event.getRecord().split(":");
                    int code = Integer.parseInt(parts[0]);
                    String message = parts[1];
                    String messageParts[] = message.split("\\|");
                    String customMessage = (messageParts.length > 1) ? messageParts[1] : "";

                    switch (code) {
                    case 0:
                        callback.onQueryStatus(QueryStatus.SNAPSHOT, customMessage);
                        break;

                    case 1:
                        callback.onQueryStatus(QueryStatus.LIVE, customMessage);
                        break;

                    case 2:
                        callback.onQueryStatus(QueryStatus.STALE, customMessage);

                        /**
                         * When the XRS query switches to STALE, the client
                         * needs to close the underlying MessageQueue
                         * subscription, to free associated resources
                         */
                        event.getMessageQueueSubscribe().unsubscribe();
                        break;

                    case 14:
                        callback.onDownloadBegin();
                        break;

                    case 15:
                        callback.onDownloadEnd();
                        break;

                    default:
                        callback.onError(code, message);
                        break;
                    }

                case MkvMQAction.ADD_code:
                case MkvMQAction.REWRITE_code:
                case MkvMQAction.DELETE_code:
                    try {
                        callback.onEvent(event);
                    } catch (Throwable e) {
                        callback.onException(event, e);
                    }
                    break;

                case MkvMQAction.BATCHSTART_code:
                case MkvMQAction.BATCHEND_code:
                    break;

                case MkvMQAction.KILL_code:
                    try {
                        callback.onDeleteAll();
                    } catch (Throwable e) {
                        callback.onException(event, e);
                    }
                    break;

                default:
                    break;
                }
            }

            private String toString(MkvMQSubscribeEvent event) {
                Object[] objects = SupplyUtils.toArray(event.getSupply());
                return "Action = " + event.getAction() + ", Record = " + event.getRecord() + ", Supply = " + Arrays.asList(objects).toString();
            }
        };

        try {
            final MkvMQSubscribe subscription = Mkv.getInstance().getMQManager().createSubscription(queueName, listener, new MkvMQSubscribeConf());

            if (fields != null) {
                subscription.subscribe(fields);
            } else {
                subscription.subscribe();
            }
        } catch (Throwable throwable) {
            callback.onException(throwable);
        }
    }

}
