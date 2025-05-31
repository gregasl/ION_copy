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
package com.iontrading.samples.simpleCMDrillDown;

import com.iontrading.mkv.*;
import com.iontrading.mkv.qos.*;
import com.iontrading.mkv.enums.*;
import com.iontrading.mkv.events.*;
import com.iontrading.mkv.helper.*;
import com.iontrading.mkv.exceptions.*;

import java.util.Vector;
import java.util.Random;
import java.text.DecimalFormat;


/*
 * This sample describes how to produce a drill down table to expose Common Market statistics
 * on a per-trader base.
 *
 * The overall Common Markets performance indicators are broken-down into specific values
 * for each of the active traders
 */
public class CustomStats
{
	static class MyRandomUtils
	{
		private static Random generator = new Random();
		private static DecimalFormat formatter = new DecimalFormat("###.##");

		public static int getInt()
		{ return generator.nextInt(1000); }

		public static double getReal()
		{
			try { return formatter.parse(formatter.format(generator.nextDouble()*100)).doubleValue(); }
			catch(Exception e) { return 0.0; }
		}
	}


	public static Mkv mkv;
	public static Vector traders = new Vector();

	static {
		traders.add("Carlo");
		traders.add("Julio");
		traders.add("Fred");
	}




	/*
	 * Create a new Drill Down Table
	 */
	static class CustomStatsDrillDownListener implements MkvCustomStatsListener
	{
		private MkvSupplyBuilder builder = null;
		public void produceStats(MkvCustomStatsTable table)
 	 	{
 	 		if(builder == null)
 	 		{
 	 			builder = new MkvSupplyBuilder(table.getType());
 		  		builder.setField(table.getType().getFieldIndex(MkvCustomStatsManager.COMPONENTS_KEY_FIELD),
 	        		             Mkv.getInstance().getProperties().getComponentName());
 	 		}

			for(int i=0; i<traders.size(); ++i)
			{
				/* Compute custom indicator values here */

	  			builder.setField(table.getType().getFieldIndex("Trader"), (String)traders.get(i));
		  		builder.setField(table.getType().getFieldIndex("NTrades"), MyRandomUtils.getInt());
		  		builder.setField(table.getType().getFieldIndex("NOrdTr"), MyRandomUtils.getInt());
		  		builder.setField(table.getType().getFieldIndex("NOrdAddIn"), MyRandomUtils.getInt());
		  		builder.setField(table.getType().getFieldIndex("NOrdAddOut"), MyRandomUtils.getInt());
		  		builder.setField(table.getType().getFieldIndex("NOrdDelIn"), MyRandomUtils.getInt());
		  		builder.setField(table.getType().getFieldIndex("NOrdDelOut"), MyRandomUtils.getInt());
		  		builder.setField(table.getType().getFieldIndex("NOrdRwtIn"), MyRandomUtils.getInt());
		  		builder.setField(table.getType().getFieldIndex("NOrdRwtOut"), MyRandomUtils.getInt());

		  		builder.setField(table.getType().getFieldIndex("LatencyMin"), MyRandomUtils.getInt());
				builder.setField(table.getType().getFieldIndex("LatencyMax"), MyRandomUtils.getInt());
				builder.setField(table.getType().getFieldIndex("LatencyMaxAbs"), MyRandomUtils.getInt());
				builder.setField(table.getType().getFieldIndex("LatencyAvg"), MyRandomUtils.getReal());
				builder.setField(table.getType().getFieldIndex("LatencyTot"), MyRandomUtils.getInt());
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
		 * CMDrillDownType is a new drill-down table
		 * for additional dimensions on the CM Stats
		 */
		MkvType customTypeDrillDown = new MkvType("CMDrillDownType",
	              new String[]       { MkvCustomStatsManager.COMPONENTS_KEY_FIELD, "Trader",
	                    			   "NTrades", "NOrdTr",
				  	              	   "NOrdAddIn", "NOrdAddOut", "NOrdDelIn", "NOrdDelOut", "NOrdRwtIn", "NOrdRwtOut",
  	  	                 			   "LatencyMin", "LatencyMax", "LatencyMaxAbs", "LatencyAvg", "LatencyTot" },
	              new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.STR,
	              				       MkvFieldType.INT, MkvFieldType.INT,
	              				       MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT,
	              				       MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.INT, MkvFieldType.REAL, MkvFieldType.INT });

		Mkv.getInstance().getCustomStatsManager()
			.createTable("CMDrillDown", customTypeDrillDown, new CustomStatsDrillDownListener())
			.extendTable(MkvCustomStatsManager.COMPONENTS_TABLE)
			.addKeyFields(new String[] { "Trader" });
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

			defineCustomStatsTables();
		}
		catch (Exception e)
		{
			System.out.println("Exception: " + e + "\n");
			e.printStackTrace();
		}
	}
}

