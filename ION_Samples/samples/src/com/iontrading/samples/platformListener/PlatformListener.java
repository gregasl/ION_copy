/*
 * SimpleLog
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

package com.iontrading.samples.platformListener;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

public class PlatformListener implements MkvPlatformListener {

	/** Creates a new instance of PlatformListener */
	public PlatformListener(String[] args) {
		// create the initial configuration used to start the engine.
		MkvQoS qos = new MkvQoS();
		qos.setArgs(args);
		qos.setPlatformListeners(new MkvPlatformListener[] { this });
		try {
			// Start the engine and get back the instance of Mkv (unique during
			// the
			// life of a component).
			Mkv mkv = Mkv.start(qos);
		} catch (MkvException e) {
			e.printStackTrace();
		}
	}

	public void onComponent(MkvComponent mkvComponent, boolean onoff) {
		System.out.println("Component " + mkvComponent.getName() + " ["
				+ mkvComponent.getType() + "]"
				+ (onoff ? " joined the platform" : " left the platform"));
	}

	public void onConnect(String component, boolean onoff) {
		System.out.println("Connection with " + component + " has been "
				+ (onoff ? "Created" : "Disrupted"));
	}

	public void onMain(MkvPlatformEvent mkvPlatformEvent) {
		switch (mkvPlatformEvent.intValue()) {
		case MkvPlatformEvent.START_code:
			System.out.println("START");
			break;
		case MkvPlatformEvent.STOP_code:
			System.out.println("STOP");
			// Start the procedure fro gracesfully shutdwon the component
			// Save states, free resources, close connections ...
			break;
		case MkvPlatformEvent.REGISTER_IDLE_code:
			System.out.println("IDLE_REGISTER");
			break;
		case MkvPlatformEvent.REGISTER_code:
			System.out.println("REGISTER");
			break;
		case MkvPlatformEvent.UNREGISTER_code:
                    System.out.println("UNREGISTER!");
                    break;
		}
	}

	public static void main(String[] args) {
		new PlatformListener(args);
	}
}
