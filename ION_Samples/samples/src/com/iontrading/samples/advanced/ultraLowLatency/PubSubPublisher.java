/*
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
 * ION Trading ltd (2006-2007)
 */

package com.iontrading.samples.advanced.ultraLowLatency;

import java.util.Random;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvPattern;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.qos.MkvQoS;

/**
 * This sample shows how to leverage the Ultra Low Latency message passing on same-host connections. 
 * The application code is a standard Publisher code, 
 * since the usage of Ultra Low Latency message passing is completely transparent to the application.
 * 
 * Publisher and Subscriber needs to be running on the same-host and connected with one another. 
 *
 * To enable the Ultra Low Latency data transfer among components running on the same host 
 * both the Publisher and Subscriber component shall be configured with the following setting:
 *
 *   mkv.ultra_low_latency = 1
 *   
 * A shared memory driver process needs to be running on the same host: 
 * see the "Java API Ultra Low Latency Driver" package associated with Java API product 
 * on the ION Tracker Website, for further information.
 * 
 * Latency performance can be inspected using the ION Performance Meter tool, 
 * focusing on the E2E Latencies table.
 * 
 * @author lveraldi
 *
 */
public class PubSubPublisher {

    private Random random = new Random();
    private String instruments[] = new String[] {
            "EUR_ab3m_25y", "EUR_ab6m_14y", "EUR_ab6m_01y", "EUR_ab3m_03y",
            "EUR_ab3m_05y", "EUR_ab6m_08y", "EUR_ab6m_02y", "EUR_ab3m_20y"
    };
    private int incrementalSupplyFields[];

    private MkvType type;
    private MkvChain chain;
    private MkvPattern pattern;
    private MkvRecord records[];

    public static void main(String[] args) {
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);

        try {
            /* Register the component onto the ION Platform */
            Mkv.start(qos);

            /* Start business level activity */
            new PubSubPublisher();
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    public PubSubPublisher() throws MkvException {
        publishAll();
        startIncrementalSupplyThread();
    }

    private void publishAll() throws MkvException {
        /* Publish a type, chain, pattern and a set of records that will be incrementally updated at regular intervals */
        String source = Mkv.getInstance().getProperties().getProperty("source");

        type = new MkvType("Type",
                new String[] {
                    "Id", "Trader", "Active", "Mid", "NLegs",
                    "L0_InstrumentId", "L0_MidPrice", "L0_MidYield",
                    "L1_InstrumentId", "L1_MidPrice", "L1_MidYield"
                },
                new MkvFieldType[] {
                    MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.INT, MkvFieldType.REAL, MkvFieldType.INT,
                    MkvFieldType.STR, MkvFieldType.REAL, MkvFieldType.REAL,
                    MkvFieldType.STR, MkvFieldType.REAL, MkvFieldType.REAL
                });
        type.publish();

        incrementalSupplyFields = new int[] {
                type.getFieldIndex("L0_MidPrice"), type.getFieldIndex("L0_MidYield"),
                type.getFieldIndex("L1_MidPrice"), type.getFieldIndex("L1_MidYield")
        };

        chain = new MkvChain("EUR.QUOTE." + source + ".QUOTE", "Type");
        chain.publish();

        pattern = new MkvPattern("EUR.QUOTE." + source + ".", "Type");
        pattern.publish();

        /* Instantiate all the records and add them to the chain */
        records = new MkvRecord[instruments.length];
        for (int i = 0; i < instruments.length; ++i) {
            records[i] = new MkvRecord("EUR.QUOTE." + source + "." + instruments[i], "Type");
            records[i].publish();
            supplyAll(records[i]);
            chain.add(records[i].getName());
        }
    }

    private void startIncrementalSupplyThread() {
        /* Start an independent thread for incremental record supplies */
        (new Thread() {
            public void run() {
                while (true) {
                    try {
                        for (MkvRecord record : records)
                            supply(record);
                    } catch(Exception e) {
                        e.printStackTrace();
                    }

                    try {
                        Thread.sleep(200);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    private void supplyAll(MkvRecord record) throws MkvException {
        /* Supply the initial record snapshot */
        record.supply(MkvSupplyFactory.create(new Object[] {
                record.getName(), "Trader1", 1, random.nextDouble(), 2,
                "Leg0", random.nextDouble(), random.nextDouble(),
                "Leg1", random.nextDouble(), random.nextDouble()
        }));
    }

    private void supply(MkvRecord record) throws MkvException {
        /* Perform incremental record supply */
        record.supply(incrementalSupplyFields, new Object[] {
                random.nextDouble(), random.nextDouble(),
                random.nextDouble(), random.nextDouble()
        });
    }
}
