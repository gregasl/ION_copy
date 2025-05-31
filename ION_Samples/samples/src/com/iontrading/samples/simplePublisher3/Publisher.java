/*
 * Publisher
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

package com.iontrading.samples.simplePublisher3;

import com.iontrading.mkv.*;
import com.iontrading.mkv.events.*;
import com.iontrading.mkv.qos.*;
import com.iontrading.mkv.exceptions.*;
import com.iontrading.mkv.enums.*;

public class Publisher implements MkvPlatformListener
{
	Mkv mkv;

	public Publisher()
	{
	}
	public static void main(String[] args)
	{
		Publisher sc = new Publisher();
		sc.startMkv(args);
	}
	private void startMkv(String[] args) {
		MkvQoS qos = new MkvQoS();
		qos.setPlatformListeners(new MkvPlatformListener[] {this});
		qos.setArgs(args);
		try {
			mkv = Mkv.start(qos);
		} catch (MkvException e) {
			e.printStackTrace();
		}
	}
		
	static final String TYPE_NAME = "MYBUP_Quote";
	static final String[] names = new String[] { "ID", "ASK", "BID", "QTY" };
	static final MkvFieldType[] types =  new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.REAL, MkvFieldType.REAL, MkvFieldType.REAL };
	static final Object[] values = new Object[] { "FGBLH3", new Double(99.9), new Double(99.8), new Double(10) };
	
	/*
	 * This class extends MkvRecord implementing the MkvSubscribeable interface
	 * instances of this class will be notified on the record being subscribed
	 */
	class MyRecord extends MkvRecord implements MkvSubscribeable {
		final boolean[] alreadySupplied = { false, false, false, false }; 
		MyRecord(String recordName, String typeName) throws MkvException {
			super(recordName, TYPE_NAME);
		}

		public void onSubscribe() {
			/*
			 * We supply a field only if someone has actually subscribed to it
			 */
			for (int i = 0; i < names.length; ++i) {
				if (!alreadySupplied[i] && isRemotelySubscribed(new String[] { names[i]} )) {
					try {
						/*
						 * in this simple example we supply fields one at a time
						 */
						supply(new String[] {names[i]}, new Object[] { values[i]} );
						alreadySupplied[i] = true;
						System.out.println("Supplying field " + names[i]);
					} catch (MkvException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
		
	private void initialize() {
		try {
			MkvType mkvType = new MkvType(TYPE_NAME,
					names,
					types);
			mkvType.publish();
			MyRecord myRecord = new MyRecord("EUR.QUOTE.MYPUB.FGBLH3", TYPE_NAME);
			myRecord.publish();
		}
		catch (MkvException e) {
			e.printStackTrace();
		}

	}


	public void onComponent(MkvComponent component, boolean registered) { }
	public void onConnect(String component, boolean connected) { }


	public void onMain(MkvPlatformEvent mkvPlatformEvent) {
		switch(mkvPlatformEvent.intValue()) {
		case MkvPlatformEvent.START_code:
			System.out.println("START");
			break;
		case MkvPlatformEvent.STOP_code:
			System.out.println("STOP");
			break;
		case MkvPlatformEvent.REGISTER_IDLE_code:
		{
			System.out.println("IDLE_REGISTER");
			break;
		}
		case MkvPlatformEvent.REGISTER_code:
			initialize();
			System.out.println("REGISTER");
			break;
		}
	}
}

