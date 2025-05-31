/*
 * OnBehalfOfRegistrationListener
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

/* this example shows how to register a virtual component component on the platform */

package com.iontrading.samples.advanced.onBehalfOfRegistration;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvLicenseManager;
import com.iontrading.mkv.MkvPlatform;
import com.iontrading.mkv.enums.MkvOnBehalfOfRegistrationResult;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvLicenseEvent;
import com.iontrading.mkv.events.MkvLicenseListener;
import com.iontrading.mkv.events.MkvOnBehalfOfRegistrationListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.qos.MkvQoS;

public class OnBehalfOfRegistration implements MkvPlatformListener {

	public OnBehalfOfRegistration(String[] args) {
		MkvQoS qos = new MkvQoS();
		qos.setArgs(args);
		qos.setPlatformListeners(new MkvPlatformListener[] { this });
		try {
			Mkv mkv = Mkv.start(qos);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) {
		new OnBehalfOfRegistration(args);
	}

	public void onComponent(MkvComponent component, boolean registered) {
	}

	public void onConnect(String component, boolean connected) {

	}

	
	class OnBehalfOfRegistrationListener implements MkvOnBehalfOfRegistrationListener {
		public void onCall(MkvOnBehalfOfRegistrationResult res, final String token) {
			System.out.println("OnBehalfOfRegistrationListener {" + res.toString() + "}");
		}
	}

	void testRegisterOnBehalfOf() {
		MkvPlatform plat = Mkv.getInstance().getPlatform();
		plat.registerOnBehalfOf("COMPONENT1234", "mkv", "mkvpwd", "PUB",
								new OnBehalfOfRegistrationListener());
	}

	public void onMain(MkvPlatformEvent mkvPlatformEvent) {
		switch (mkvPlatformEvent.intValue()) {
		case MkvPlatformEvent.START_code:
			Mkv.getInstance().getPlatform().register();
			break;
		case MkvPlatformEvent.STOP_code:
			break;
		case MkvPlatformEvent.REGISTER_IDLE_code:
			System.out.println("REGISTER_IDLE");
			break;
		case MkvPlatformEvent.REGISTER_code:
			System.out.println("REGISTER");
			break;
		case MkvPlatformEvent.UNREGISTER_code:
			System.out.println("UNREGISTER");
			break;
		}
	}

}
