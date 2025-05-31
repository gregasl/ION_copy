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
package com.iontrading.samples.simpleVirtualHost;

import com.iontrading.mkv.*;
import com.iontrading.mkv.qos.*;
import com.iontrading.mkv.enums.*;
import com.iontrading.mkv.events.*;
import com.iontrading.mkv.helper.*;
import com.iontrading.mkv.exceptions.*;

import java.util.Random;
import java.text.DecimalFormat;


/*
 * This is a very basic example of how to produce a Virtual Record inside the Hosts table
 *
 * This component simulates a Daemon and reports standard statistics about the machine
 * it is running onto, assuming no real Daemon is in charge of doing it
 *
 * This scenario may be used to monitor MMI.NET instances, which typically don't run
 * on Daemon-controlled hosts
 */
public class CustomStats
{
	public static Mkv mkv;


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
	 * Create a virtual record
	 */
	static class CustomStatsVirtualListener implements MkvCustomStatsListener
	{
		private MkvSupplyBuilder builder = null;
		public void produceStats(MkvCustomStatsTable table)
 	 	{
 	 		if(builder == null)
 	 		{
 	 			builder = new MkvSupplyBuilder(table.getType());
 		  		builder.setField(table.getType().getFieldIndex(MkvCustomStatsManager.HOSTS_KEY_FIELD),
 	        		             "VirtualHost");
 	        	builder.setField(table.getType().getFieldIndex("Host"), "17.105.211.123");
  			}

			/* Compute custom indicator values here */

	  		builder.setField(table.getType().getFieldIndex("NumProcs"), MyRandomUtils.getInt(1000));
	  		builder.setField(table.getType().getFieldIndex("CpuPct"), MyRandomUtils.getReal(100));
	  		builder.setField(table.getType().getFieldIndex("MemPct"), MyRandomUtils.getReal(100));

	  		table.send(builder.getSupply());
  	 	}
	}

	/*
	 * Define the Custom Stats
	 */
	private static void defineCustomStatsTables() throws MkvException
	{
		/*
		 * MyType is an extension
		 * of the Standard Component statistics
		 */
		MkvType customTypeExtend = new MkvType("MyType",
	              new String[]       { MkvCustomStatsManager.HOSTS_KEY_FIELD, "Host", "NumProcs", "CpuPct", "MemPct" },
	              new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.INT, MkvFieldType.REAL, MkvFieldType.REAL });

		Mkv.getInstance().getCustomStatsManager()
			.createTable("hinfo", customTypeExtend, new CustomStatsVirtualListener())
			.extendTable(MkvCustomStatsManager.HOSTS_TABLE);
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

