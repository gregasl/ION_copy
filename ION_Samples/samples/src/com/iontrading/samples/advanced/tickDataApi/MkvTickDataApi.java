package com.iontrading.samples.advanced.tickDataApi;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvE2ETickData;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.qos.MkvQoS;

/*
 * TickByTickApi
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
 * ION Trading ltd (2014)
 */
public class MkvTickDataApi implements MkvPlatformListener, MkvPublishListener {

    /**
     * Timer task event. Produces the delay distributions to be updated at a
     * fixed time
     */
    private class TickDataTask extends TimerTask {
        private final double[] delays = new double[100];
        int i = 0;

        /**
         * Constructor.
         */
        public TickDataTask() {

            // the delay array simulates the round trip delays produced by a
            // gateway that exchanges messages with a market. 99% of messages
            // have an End-To-End delay smaller than 1 millisecond and 1% have a
            // delay higher than 1 millisecond.
            double d = 1.001;
            for (int i = 0; i < delays.length; i++) {
                delays[i] = d;
                d -= 0.001;
            }
        }

        @Override
        public void run() {

            // add delay into DelayDistribution objects. These objects will be
            // used by Percentile API to compute percentiles.
            delayDistribution1.add(delays[getNextIndex()]);
            delayDistribution2.add(delays[getNextIndex()]);
            delayDistribution3.add(delays[getNextIndex()]);
        }

        private int getNextIndex() {
            if (i == delays.length - 1)
                i = 0;
            else
                i++;

            return i;
        }
    }

    private MkvE2ETickData delayDistribution1 = null;
    private MkvE2ETickData delayDistribution2 = null;
    private MkvE2ETickData delayDistribution3 = null;

    public MkvTickDataApi(String[] args) {
        MkvQoS qos = new MkvQoS();

        qos.setArgs(args);
        qos.setPlatformListeners(new MkvPlatformListener[] { this });
        qos.setPublishListeners(new MkvPublishListener[] { this });

        try {
            Mkv.start(qos);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.iontrading.mkv.events.MkvPlatformListener#onMain(com.iontrading.mkv
     * .enums.MkvPlatformEvent)
     */
    public void onMain(MkvPlatformEvent event) {
        switch (event.intValue()) {
        case MkvPlatformEvent.START_code:
            System.out.println(new Date() + " " + "START");
            break;
        case MkvPlatformEvent.STOP_code:
            System.out.println(new Date() + " " + "stopped");
            break;
        case MkvPlatformEvent.REGISTER_IDLE_code:
            System.out.println(new Date() + " " + "REGISTER_IDLE");
            break;
        case MkvPlatformEvent.REGISTER_code:
            System.out.println(new Date() + " " + "REGISTER");

            // Create the DelayDistribution objects
            delayDistribution1 = new MkvE2ETickData("MarketDelayAbsolute", "MDATA", "BBG_MDATA");
            delayDistribution2 = new MkvE2ETickData("MarketDelayAdjusted", "MDATA", "BBG_MDATA", 1);
            delayDistribution3 = new MkvE2ETickData("MarketDepth", "MDATA", "BBG_MDATA", 2);

            // Add them into Percentile API
            Mkv.getInstance().getTickDataCollector().register(delayDistribution1);
            Mkv.getInstance().getTickDataCollector().register(delayDistribution2);
            Mkv.getInstance().getTickDataCollector().register(delayDistribution3);

            Timer timer = new Timer(true);
            timer.scheduleAtFixedRate(new TickDataTask(), 0, 300);

            break;
        case MkvPlatformEvent.UNREGISTER_code:
            System.out.println(new Date() + " " + "UNREGISTER");
            break;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.iontrading.mkv.events.MkvPublishListener#onPublish(com.iontrading
     * .mkv.MkvObject, boolean, boolean)
     */
    public void onPublish(MkvObject object, boolean start, boolean dwl) {

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.iontrading.mkv.events.MkvPublishListener#onPublishIdle(java.lang.
     * String, boolean)
     */
    public void onPublishIdle(String component, boolean start) {

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.iontrading.mkv.events.MkvPublishListener#onSubscribe(com.iontrading
     * .mkv.MkvObject)
     */
    public void onSubscribe(MkvObject obj) {

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.iontrading.mkv.events.MkvPlatformListener#onComponent(com.iontrading
     * .mkv.MkvComponent, boolean)
     */
    public void onComponent(MkvComponent component, boolean registered) {

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.iontrading.mkv.events.MkvPlatformListener#onConnect(java.lang.String,
     * boolean)
     */
    public void onConnect(String component, boolean connected) {

    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        new MkvTickDataApi(args);
    }
}
