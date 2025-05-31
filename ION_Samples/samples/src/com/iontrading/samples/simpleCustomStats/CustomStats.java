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

import java.util.Vector;
import java.util.Random;
import java.text.DecimalFormat;


/*
 * This sample describes a complete scenario of an ION Custom Component
 * that operates as a proxy between the ION Bus and an external world,
 * for example Web-based browsers in the ION.Web application.
 *
 * This component:
 * - exposes additional statistics for its performance (this is done in the 'IONWebExtend' table)
 * - provides a detailed view on a per-client base (this is done in the 'IONWebDrillDown' table)
 * - add a virtual record for each connection to external components (this is done in the 'IONWebVirtual' table)
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
	public static Vector clients = new Vector();

	static {
		clients.add("WebBrowser_mkv");
		clients.add("External_client");
		clients.add("Mozilla_mkv");
	}



	/*
	 * How to Extend the Components Table with additional performance indicators
	 */
	static class CustomStatsExtendListener implements MkvCustomStatsListener
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

			/* Compute custom indicator values here */

	  		builder.setField(table.getType().getFieldIndex("NumClients"), clients.size());
	  		builder.setField(table.getType().getFieldIndex("Bandwith"), MyRandomUtils.getReal());
	  		builder.setField(table.getType().getFieldIndex("NumSessions"), MyRandomUtils.getInt());
  			table.send(builder.getSupply());
  	 	}
	}




	/*
	 * How to Create a new Drill Down Table
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

			for(int i=0; i<clients.size(); ++i)
			{
				/* Compute custom indicator values here */

				builder.setField(table.getType().getFieldIndex("Client"), (String)clients.get(i));
				builder.setField(table.getType().getFieldIndex("Subscriptions"), MyRandomUtils.getInt());
				builder.setField(table.getType().getFieldIndex("Updates"), MyRandomUtils.getInt());
				table.send(builder.getSupply());
			}
  	 	}
	}




	/*
	 * How to Create a virtual record
	 */
	static class CustomStatsVirtualListener implements MkvCustomStatsListener
	{
		private MkvSupplyBuilder builder = null;
		public void produceStats(MkvCustomStatsTable table)
 	 	{
 	 		if(builder == null)
 	 		{
 	 			builder = new MkvSupplyBuilder(table.getType());
 		  		builder.setField(table.getType().getFieldIndex(MkvCustomStatsManager.CONNECTIONS_KEY_FIELD_FROM),
 	        		             Mkv.getInstance().getProperties().getComponentName());
  			}

			for(int i=0; i<clients.size(); ++i)
			{
				/* Compute custom indicator values here */

				builder.setField(table.getType().getFieldIndex(MkvCustomStatsManager.CONNECTIONS_KEY_FIELD_TO),
								 (String)clients.get(i));

				builder.setField(table.getType().getFieldIndex("RTDAvg"), MyRandomUtils.getInt());
				builder.setField(table.getType().getFieldIndex("RTDMsg"), MyRandomUtils.getInt());

				builder.setField(table.getType().getFieldIndex("BytesIn"), MyRandomUtils.getReal());
				builder.setField(table.getType().getFieldIndex("BytesOut"), MyRandomUtils.getReal());

				builder.setField(table.getType().getFieldIndex("MsgRecRecv"), MyRandomUtils.getReal());
				builder.setField(table.getType().getFieldIndex("MsgRecSent"), MyRandomUtils.getReal());

				table.send(builder.getSupply());
			}
  	 	}
	}




	/*
	 * How to define the Custom Stats
	 */
	private static void defineCustomStatsTables() throws MkvException
	{
		/*
		 * IONWebExtendType is an extension
		 * of the Standard Component statistics
		 */
		MkvType customTypeExtend = new MkvType("IONWebExtendType",
	              new String[]       { MkvCustomStatsManager.COMPONENTS_KEY_FIELD, "NumClients", "Bandwith", "NumSessions" },
	              new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.INT, MkvFieldType.REAL, MkvFieldType.INT });

		Mkv.getInstance().getCustomStatsManager()
			.createTable("IONWebExtend", customTypeExtend, new CustomStatsExtendListener())
			.extendTable(MkvCustomStatsManager.COMPONENTS_TABLE);



		/*
		 * IONWebDrillDown is a new drill-down table
		 * for additional dimensions on the Component
		 */
		MkvType customTypeDrillDown = new MkvType("IONWebDrillDownType",
	              new String[]       { MkvCustomStatsManager.COMPONENTS_KEY_FIELD, "Client", "Subscriptions", "Updates" },
	              new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.INT, MkvFieldType.INT });

		Mkv.getInstance().getCustomStatsManager()
			.createTable("IONWebDrillDown", customTypeDrillDown, new CustomStatsDrillDownListener())
			.extendTable(MkvCustomStatsManager.COMPONENTS_TABLE)
			.addKeyFields(new String[] { "Client" });


		/*
		 * IONWebVirtual is used to produce virtual records
		 * in the Connections table
		 */
		MkvType customTypeVirtual = new MkvType("IONWebVirtual",
	              new String[]       {
						MkvCustomStatsManager.CONNECTIONS_KEY_FIELD_FROM, MkvCustomStatsManager.CONNECTIONS_KEY_FIELD_TO,
						"RTDAvg", "RTDMsg",
						"BytesIn", "BytesOut",
						"MsgRecRecv", "MsgRecSent"
				  },
	              new MkvFieldType[] {
						MkvFieldType.STR, MkvFieldType.STR,
						MkvFieldType.INT, MkvFieldType.INT,
						MkvFieldType.REAL, MkvFieldType.REAL,
						MkvFieldType.REAL, MkvFieldType.REAL
				  }
		);

		Mkv.getInstance().getCustomStatsManager()
			.createTable(MkvCustomStatsManager.CONNECTIONS_TABLE, customTypeVirtual, new CustomStatsVirtualListener())
			.extendTable(MkvCustomStatsManager.CONNECTIONS_TABLE);
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

