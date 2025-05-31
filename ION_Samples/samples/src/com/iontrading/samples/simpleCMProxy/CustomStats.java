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

package com.iontrading.samples.simpleCMProxy;

import com.iontrading.mkv.*;
import com.iontrading.mkv.qos.*;
import com.iontrading.mkv.enums.*;
import com.iontrading.mkv.events.*;
import com.iontrading.mkv.helper.*;
import com.iontrading.mkv.exceptions.*;

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

import java.util.Random;
import java.text.DecimalFormat;


/*
 * This sample describes how to report Custom Statistics on behalf of other components
 *
 * This application listens to registration/de-registration events from MOCK components on the ION Bus
 * and for each running MOCK exposes a set of Custom Statistics.
 *
 * The set of Custom Statistics exposed is the one defined by Common Market.
 */
public class CustomStats
{
	public static Mkv mkv;
	public static int statime;
	public static Set mocks = new HashSet();

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

	static class Listener implements MkvPlatformListener
	{
		public void onComponent(MkvComponent mkvComponent, boolean onoff)
		{
			if(mkvComponent.getType().contains("MOCK"))
			{
				synchronized(mocks){
					if(onoff) mocks.add(mkvComponent.getName());
					else mocks.remove(mkvComponent.getName());
				}
			}
		}

	    public void onConnect(String component, boolean onoff) {}
	    public void onMain(MkvPlatformEvent mkvPlatformEvent) {}
	}

	/*
	 * Periodically report Common Market statistics on behalf of another component
	 */
	static class CommonMarketListener implements MkvCustomStatsListener
	{
		private MkvSupplyBuilder builder = null;

		public void produceStats(MkvCustomStatsTable table)
 	 	{
 	 		if(builder == null)
 	 			builder = new MkvSupplyBuilder(table.getType());

 	 		synchronized(mocks){
 	 			
				Iterator iter = mocks.iterator();
		        while(iter.hasNext())
		        {
		        	String mockName = (String)iter.next();
	
					int    ndepth  = 0;
		    		int    ntrades = 0;
		    		double lavg    = 0;
		    		double mlavg   = 0;
	
			  		builder.setField(table.getType().getFieldIndex("Component"),     mockName); // The component we report stats on behalf of
			  		builder.setField(table.getType().getFieldIndex("Market"),        "COMMONMARKET");
	
			  		builder.setField(table.getType().getFieldIndex("NDepthUpd"),     (int) (ndepth = ((3000 + MyRandomUtils.getInt(3000)) / statime)));
			  		builder.setField(table.getType().getFieldIndex("NTrades"),       (int) (ntrades = (MyRandomUtils.getInt(100) / statime)));
			  		builder.setField(table.getType().getFieldIndex("NOrdTr"),        (int) (ntrades + (MyRandomUtils.getInt(1000 - ntrades)) / statime));
					builder.setField(table.getType().getFieldIndex("NTickers"),      (int) (ntrades + (MyRandomUtils.getInt(1000 - ntrades)) / statime));
	
			  		builder.setField(table.getType().getFieldIndex("NOrdAddIn"),     (int) (0));
			  		builder.setField(table.getType().getFieldIndex("NOrdAddOut"),    (int) (0));
			  		builder.setField(table.getType().getFieldIndex("NOrdDelIn"),     (int) (0));
			  		builder.setField(table.getType().getFieldIndex("NOrdDelOut"),    (int) (0));
			  		builder.setField(table.getType().getFieldIndex("NOrdRwtIn"),     (int) (0));
			  		builder.setField(table.getType().getFieldIndex("NOrdRwtOut"),    (int) (0));
	
			  		builder.setField(table.getType().getFieldIndex("LatencyAvg"),    (double) (lavg = (MyRandomUtils.getReal(20))));
			  		builder.setField(table.getType().getFieldIndex("LatencyMin"),    (int) (MyRandomUtils.getInt(20)));
			  		builder.setField(table.getType().getFieldIndex("LatencyMax"),    (int) (lavg + (MyRandomUtils.getInt((int)(200 - lavg)))));
			  		builder.setField(table.getType().getFieldIndex("LatencyMaxAbs"), (int) (0));
			  		builder.setField(table.getType().getFieldIndex("LatencyTot"),    (int) (0));
	
					builder.setField(table.getType().getFieldIndex("MDLatencyAvg"),  (double) (mlavg = 20.0 / 60.0 * ndepth));
					builder.setField(table.getType().getFieldIndex("MDLatencyMin"),  (int) (MyRandomUtils.getInt(20)));
					builder.setField(table.getType().getFieldIndex("MDLatencyMax"),  (int) (mlavg + MyRandomUtils.getInt((int)(100 - mlavg))));
	
			  		table.send(builder.getSupply());
				}
 	 		}
  	 	}
	}

	/*
	 * Define the Custom Stats
	 */
	private static void defineCustomStatsTables() throws MkvException
	{
		/*
		 * CMTypeCustom is an extension
		 * of the Standard Component statistics
		 */
		MkvType cmTypeCustom = new MkvType("CMTypeCustom",
	              new String[] { "Component", "Market",
	              				 "NDepthUpd", "NTrades", "NOrdTr", "NTickers",
	              				 "NOrdAddIn", "NOrdAddOut", "NOrdDelIn", "NOrdDelOut", "NOrdRwtIn", "NOrdRwtOut",
	              				 "LatencyMin", "LatencyMax", "LatencyMaxAbs", "LatencyAvg", "LatencyTot",
	              				 "MDLatencyMin", "MDLatencyMax", "MDLatencyAvg" },
	              new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.STR,
	              					   MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT,
	              					   MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT,
	              					   MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.REAL, MkvFieldType.INT,
	              					   MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.REAL });

		Mkv.getInstance().getCustomStatsManager()
			.createTable("cmstatistics", cmTypeCustom, new CommonMarketListener())
			.extendTable(MkvCustomStatsManager.COMMONMARKET_TABLE);
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
			qos.setPlatformListeners(new MkvPlatformListener[] { new Listener() });

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
