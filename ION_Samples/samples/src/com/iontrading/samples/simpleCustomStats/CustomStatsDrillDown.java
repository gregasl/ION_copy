/*
 * CustomStats
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
package com.iontrading.samples.simpleCustomStats;

import com.iontrading.mkv.*;
import com.iontrading.mkv.qos.*;
import com.iontrading.mkv.enums.*;
import com.iontrading.mkv.events.*;
import com.iontrading.mkv.helper.*;
import com.iontrading.mkv.exceptions.*;

import java.util.Random;
import java.text.DecimalFormat;



/*
 * This sample describes how to report Common Market performance indicators as Custom Statistics
 */
public class CustomStatsDrillDown
{
        public static Mkv mkv;
        public static int statime;
        
        public static String[] VMO_MARKETS = new String[] { "BTEC", "CME", "ESPEED" };

        static class MyRandomUtils
        {
                private static Random generator = new Random();
                private static DecimalFormat formatter = new DecimalFormat("###.##");

                public static int getInt(int max)
                { return generator.nextInt(max); }

                public static double getReal(double max)
                {
                        try { return formatter.parse(formatter.format(generator.nextDouble()*max)).doubleValue(); }
                        catch(Exception e) { return 0.0; }
                }
        }

        /*
         * Periodically report VMO statistics
         */
        static class VMOListener implements MkvCustomStatsListener
        {
                private MkvSupplyBuilder builder = null;

                public void produceStats(MkvCustomStatsTable table)
                {
                        if(builder == null)
                                builder = new MkvSupplyBuilder(table.getType());

                        int    ndepth  = 0;
                        int    ntrades = 0;
                        double mlavg   = 0;

                        builder.setField(table.getType().getFieldIndex("Component"),     Mkv.getInstance().getProperties().getComponentName());
                        
                        /* Report aggregated statistics for all markets */
                        builder.setField(table.getType().getFieldIndex("NTrades"),       (int) (ntrades = (MyRandomUtils.getInt(100) / statime)));
                        builder.setField(table.getType().getFieldIndex("MDLatencyMin"),  (int) (MyRandomUtils.getInt(20)));
                        builder.setField(table.getType().getFieldIndex("MDLatencyMax"),  (int) (mlavg + MyRandomUtils.getInt((int)(100 - mlavg))));
                        builder.setField(table.getType().getFieldIndex("MDLatencyAvg"),  (double) (mlavg = 20.0 / 60.0 * ndepth));

                        table.send(builder.getSupply());
                }
        }

        /*
         * Periodically report VMO statistics
         */
        static class VMODrillDownListener implements MkvCustomStatsListener
        {
                private MkvSupplyBuilder builder = null;

                public void produceStats(MkvCustomStatsTable table)
                {
                        if(builder == null)
                                builder = new MkvSupplyBuilder(table.getType());

                        int    ndepth  = 0;
                        int    ntrades = 0;
                        double mlavg   = 0;

                        builder.setField(table.getType().getFieldIndex("Component"),     Mkv.getInstance().getProperties().getComponentName());
                        
                        /* Report a specific statistics for each market */
                        for(String mrkt : VMO_MARKETS) {
                            builder.setField(table.getType().getFieldIndex("Market"),        mrkt);
                            builder.setField(table.getType().getFieldIndex("NTrades"),       (int) (ntrades = (MyRandomUtils.getInt(100) / statime)));
                            builder.setField(table.getType().getFieldIndex("MDLatencyMin"),  (int) (MyRandomUtils.getInt(20)));
                            builder.setField(table.getType().getFieldIndex("MDLatencyMax"),  (int) (mlavg + MyRandomUtils.getInt((int)(100 - mlavg))));
                            builder.setField(table.getType().getFieldIndex("MDLatencyAvg"),  (double) (mlavg = 20.0 / 60.0 * ndepth));

                            table.send(builder.getSupply());
                        }
                }
        }

        /*
         * Define the Custom Stats
         */
        private static void defineCustomStatsTables() throws MkvException
        {
                /*
                 * VMOTypeCustom is an extension
                 * of the Standard Component statistics
                 */
                MkvType vmoTypeCustom = new MkvType("VMOTypeCustom",
                      new String[] { "Component", "NTrades", "MDLatencyMin", "MDLatencyMax", "MDLatencyAvg" },
                      new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.REAL });

                Mkv.getInstance().getCustomStatsManager()
                    .createTable("vmostats", vmoTypeCustom, new VMOListener())
                    .extendTable(MkvCustomStatsManager.COMPONENTS_TABLE);

                /*
                 * VMOTypeCustomDrillDown is a drill down of the previous table
                 * on a per-market basis
                 */
                MkvType vmoTypeCustomDrillDown = new MkvType("VMOTypeCustomDrillDown",
                        new String[] { "Component", "Market", "NTrades", "MDLatencyMin", "MDLatencyMax", "MDLatencyAvg" },
                        new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.REAL });
                
                Mkv.getInstance().getCustomStatsManager()
                    .createTable("vmostats_details", vmoTypeCustomDrillDown, new VMODrillDownListener())
                    .extendTable(MkvCustomStatsManager.COMPONENTS_TABLE)
                    .addKeyFields(new String[] { MkvCustomStatsManager.COMPONENTS_KEY_FIELD, "Market" });
        }

        /*
         * Main Application entry point
         */
        public static void main(String commands[])
        {
                try
                {
                        MkvQoS qos = new MkvQoS();
                        qos.setArgs(commands);

                        mkv = Mkv.start(qos);

                        try
                        { statime = Mkv.getInstance().getProperties().getIntProperty("STATIME"); }
                        catch(Exception e)
                        {
                                System.out.println("Exception: " + e + "\n");
                                e.printStackTrace();
                        }

                        defineCustomStatsTables();
                }
                catch (Exception e)
                {
                        System.out.println("Exception: " + e + "\n");
                        e.printStackTrace();
                }
        }
}
