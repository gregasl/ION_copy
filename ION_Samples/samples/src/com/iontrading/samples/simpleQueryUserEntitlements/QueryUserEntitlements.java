/*
 * SimpleQueryUserEntitlements
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
 * ION Trading ltd (2009)
 */

package com.iontrading.samples.simpleQueryUserEntitlements;


import com.iontrading.mkv.*;
import com.iontrading.mkv.events.*;
import com.iontrading.mkv.enums.*;
import com.iontrading.mkv.qos.MkvQoS;


public class QueryUserEntitlements
{
	private static class QueryClosure implements MkvEntitlementsQueryExecuteListener
	{
		public void onQueryExecuted(MkvEntitlements.Query query,
					MkvEntitlementPermission permission,
					MkvEntitlements.EntitlementsIterator iterator)
		{
			if (permission.intValue() == MkvEntitlementPermission.ALLOW_code)
			{
				System.out.println("Allowed:");

				while (iterator.hasNext())
				{
					MkvEntitlements.Element curr[] = (MkvEntitlements.Element[])iterator.next();
					System.out.println("Entitled Element: " + curr[0] + "." + curr[1] + "." + curr[2] + " - Feature=" + curr[3]);
				}
			}
			else System.out.println("DENY! [" + permission.toString() + "]");
		}
	}

	private static class ESListener implements MkvEntitlementServiceListener
	{
		Mkv mkv;
		MkvEntitlements.Query query;

		public ESListener(Mkv m, MkvEntitlements.Query q)
		{
			mkv = m;
			query = q;
		}

		public void onServiceUp() { } 

		public void onServiceAvailable() 
		{
			query.setUser("mkv");
			query.setEntSource("__SYSTEM_MY_ENTSRV_103");
			mkv.getEntitlementManager().eval(query, new QueryClosure());
		}

		public void onServiceDown() { }
	}

	public static void main(String commands[])
	{
		MkvEntitlements.Query q = new MkvEntitlements.Query(false);
		MkvEntitlements.Element srv, cont, obj;

		srv = q.addService("TRADESERVER", "TRADESERVER_DEV");
		cont = q.addContainer(srv, "Books");
		obj = q.addObject(cont, "ISRAEL");
		obj = q.addObject(cont, "TREASURY");

		q.addFeature(srv, "Trade");
		q.addFeature(srv, "View");
		q.addFeature(srv, "TradeOnBehalfOf");

		MkvEntitlements.EntitlementsIterator iterator = new MkvEntitlements.EntitlementsIterator(q);
		while (iterator.hasNext())
		{
			MkvEntitlements.Element curr[] = (MkvEntitlements.Element[])iterator.next();
			System.out.println("Entitled Element: " + curr[0] + "." + curr[1] + "." + curr[2] + " - Feature=" + curr[3]);
		}

		try
		{
			MkvQoS qos = new MkvQoS();
			qos.setArgs(commands);

			Mkv mkv = Mkv.start(qos);
			mkv.getEntitlementManager().addEntitlementServiceListener(new ESListener(mkv, q));
		}
		catch (Exception e) { System.out.println("Exception on starting!"); }
	}
}