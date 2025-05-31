/*
 * GenericByFeatureEntitlements
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

package com.iontrading.samples.simpleGenericByFeatureEntitlements;

import com.iontrading.mkv.*;
import com.iontrading.mkv.qos.*;
import com.iontrading.mkv.enums.*;
import com.iontrading.mkv.events.*;

public class GenericByFeatureEntitlements
{
	private static boolean go = false;

	private static class EvalListener implements MkvEntitlementRequestEvalListener
	{
		public void onRequestEvaluated(MkvEntitlementRequest request, MkvEntitlementPermission permission)
		{
			System.out.println("Request " + request + " evaluated remotely with permission " + permission);
		}
	}

	private static class EvalListListener implements MkvEntitlementListEvalListener
	{
		public void onAuthorizationRule(MkvEntitlementRequest request, String resource, String feature, MkvEntitlementPermission permission)
		{
			System.out.println("Rule found for " + request + ": [" + resource + "]." + feature + " = " + permission);
		}

		public void onEnumerationEnded(MkvEntitlementRequest request)
		{
			System.out.println("Rule enumeration ended!");
		}
	}

	private static class Thr extends Thread
	{
		Mkv mkv;

		Thr(Mkv mkv) { this.mkv = mkv; }

		public void run()
		{
			while (true)
			{
				if (go)
				{
					System.out.println("System is available, querying the cache");

					/* Creating a new generic by-feature request to be evaluated */
					MkvEntitlementRequest req = new MkvEntitlementRequest("userId",
						"XTP_US", "Europe.UK.London.Desk4.Account5.Instrument10",
						"Trade");

					/* First try to query the cache */
					MkvEntitlementPermission perm = mkv.getEntitlementManager().cacheQuery(req);

					if (perm == null)
					{
						System.out.println("Cache is empty, querying the server");

						/* Do remote quering */
						mkv.getEntitlementManager().eval(req, new EvalListener());
					}
					else System.out.println("Cache is loaded");

					/* Try a user rule listing from remote server */
					MkvEntitlementRequest reqList = new MkvEntitlementRequest("userId", "XTP_US");
					reqList.setFeature("Trade");

					/* Do remote quering */
					mkv.getEntitlementManager().eval(req, new EvalListListener());



				}
				else
				{
					System.out.println("System is not available, waiting...");
				}

				try { sleep(500); } catch (Exception e) { }
			}
		}
	}

	private static class ESListener implements MkvEntitlementServiceListener
	{
		Mkv mkv;

		ESListener(Mkv mkv) { this.mkv = mkv; }

		public void onServiceUp() { }
		public void onServiceAvailable()
		{
			go = true;
		}
		public void onServiceDown()
		{
			go = false;
		}
	}

	public static void main(String commands[])
	{
		try
		{
			MkvQoS qos = new MkvQoS();
			qos.setArgs(commands);

			Mkv mkv = Mkv.start(qos);
			mkv.getEntitlementManager().addEntitlementServiceListener(new ESListener(mkv));

			new Thr(mkv).start();
		}
		catch (Exception e) { }
	}
}
