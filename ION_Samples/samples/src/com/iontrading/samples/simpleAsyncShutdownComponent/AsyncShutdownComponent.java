/*
 * Component which exploits the shutdown protocol 
 * in order to request an asynchronous shutdown to the daemon.
 * An asynchronous shutdown allows the component to delay the stop event
 * until it has performed all the operations to be done before stopping.
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

package com.iontrading.samples.simpleAsyncShutdownComponent;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.enums.MkvShutdownMode;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

public class AsyncShutdownComponent {

	/** Creates a new instance of the component */
	public AsyncShutdownComponent(String[] args) {

		MkvQoS qos = new MkvQoS();
		qos.setArgs(args);
		qos.setPlatformListeners(new MkvPlatformListener[] { new PlatformListener() });

		try {
			Mkv.start(qos);
		} catch (MkvException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		new AsyncShutdownComponent(args);
	}

	private class PlatformListener implements MkvPlatformListener {

		private int readyToStop = 2;

		public void onMain(MkvPlatformEvent event) {

			switch (event.intValue()) {

			case MkvPlatformEvent.START_code:
				break;

			case MkvPlatformEvent.STOP_code:
				break;

			case MkvPlatformEvent.SHUTDOWN_REQUEST_code:

				/*
				 * Handling a stop request coming from the daemon. If the
				 * component wishes to do some stuff before stopping, it should
				 * send a request for asynchronous shutdown to the daemon,
				 * otherwise it can request a synchronous shutdown (not
				 * mandatory).
				 * 
				 * Here we simulate the condition of component ready/not ready
				 * to stop by using the counter readyToStop.
				 */
				try {
					if (--readyToStop > 0)
						Mkv.getInstance().shutdown(MkvShutdownMode.ASYNC,
								"Not yet ready to stop, please wait...");
					else
						/*
						 * Not mandatory, but it may be useful to inform the
						 * daemon about the stop message.
						 */
						Mkv.getInstance().shutdown(MkvShutdownMode.SYNC,
								"Component is stopping...");
				} catch (MkvException e) {
					e.printStackTrace();
				}
				break;
			}

		}

		public void onComponent(MkvComponent comp, boolean start) {

		}

		public void onConnect(String comp, boolean start) {

		}
	}

}
