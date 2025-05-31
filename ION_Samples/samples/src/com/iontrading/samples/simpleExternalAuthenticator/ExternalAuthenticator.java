/*
 * ExternalAuthenticator
 *
 * ION Trading U.K. Limited supplies this software code is for testing purposes
 * only. The scope of this software code is exclusively limited to the
 * demonstration of the structure of an application using the ION(tm) Common
 * Market and ION Trading U.K. Limited does not guarantee the correct behaviour
 * of any deployed application using this software code.
 * This software code has not been thoroughly tested under all conditions.
 * ION, therefore, cannot guarantee or imply reliability, service ability, or
 * function of this software.
 * Any use of this software outside of this defined scope is the sole
 * responsibility of the user.
 *
 * ION Trading ltd (2011)
 */

/*
 * This example shows how to create a component behaving as an external authenticator.
 * To make the authenticator works correctly, put in its initial configuration properties the key "mkv.syscomponent", and set its value to 1.
 * Furthermore, the daemon master of the platform to which this component registers, must be configured to use this external authenticator.
 * All the needed informations regarding this configuration step can be found in the "Platform Tools: User Guide and Reference" document, chapter "Using External Authentication".
 */
package com.iontrading.samples.simpleExternalAuthenticator;


import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.qos.MkvQoS;

public class ExternalAuthenticator implements MkvPlatformListener {

	public static void main(String[] args) {

		ExternalAuthenticator auth = new ExternalAuthenticator(args);
	}


	/* Creates a new instance of ExternalAuthenticator */
	public ExternalAuthenticator(String[] args) {

		/* Create the initial configuration used to start the engine. */
		MkvQoS qos = new MkvQoS();
		qos.setPlatformListeners(new MkvPlatformListener[] { this });
		qos.setArgs(args);

		try {

			/*
			 * Start the engine and get back the instance of Mkv (unique during
			 * the life of a component.
			 */
			Mkv mkv = Mkv.start(qos);

		} catch (MkvException e) {
			e.printStackTrace();
		}
	}


	/**
	 * Publish the Authentication function.
	 */
	private void publishAuthenticationFunction() {

		try {

			MkvFunction avg_function = new MkvFunction(
			/* The function name (notation COMPONENTNAME.Authenticate) */
			Mkv.getInstance().getProperties().getComponentName() + ".Authenticate",
			/* The return type */
			MkvFieldType.STR,
			/* The function arguments */
			new String[] { "UserName", "Password" },
			/* The arguments types */
			new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.STR },
			/* An help message */
			"Checks a user login given username and password.",
			/* The listener that will handle the requests */
			new LoginRequestHandler());

			avg_function.publish();

		} catch (MkvException e) {
			e.printStackTrace();
		}

	}


	public void onMain(MkvPlatformEvent event) {

		switch (event.intValue()) {

			case MkvPlatformEvent.START_code :

				System.out.println("START");
				publishAuthenticationFunction();
				break;

			case MkvPlatformEvent.STOP_code :

				System.out.println("STOP");
				/*
				 * Start the procedure for gracefully shutdown the component
				 * Save states, free resources, close connections ...
				 */
				break;

			case MkvPlatformEvent.REGISTER_IDLE_code :

				System.out.println("IDLE_REGISTER");
				break;

			case MkvPlatformEvent.REGISTER_code :

				System.out.println("REGISTER");
				break;

			case MkvPlatformEvent.UNREGISTER_code :

				System.out.println("UNREGISTER");
				break;

			default:
				break;
		}
	}


	public void onComponent(MkvComponent mkvComponent, boolean onoff) {

	}


	public void onConnect(String component, boolean onoff) {

	}

	/**
	 * This object is used to implement the logic behind the authentication
	 * function.
	 */
	private class LoginRequestHandler implements MkvFunctionListener {

		@Override
		public void onCall(MkvFunctionCallEvent event) {

			try {

				MkvSupply args = event.getArgs();

				String resString = "0:Ok";

				String username = args.getString(0);
				String password = args.getString(1);

				System.out.println("Received a login request from " + event.getCaller() + " for user '"
						+ (username != null ? username : "null") + "'.");

				/*
				 * Implement the authentication logic. Here are just made basic
				 * checks for username and password correctness.
				 */
				if (username == null || username.isEmpty()) {
					resString = "-1:wrong user";
				} else {
					if (password == null || password.isEmpty()) {
						resString = "-2:wrong pwd";
					}
				}

				System.out.println("Returning the result {" + resString + "}.");

				event.setResult(MkvSupplyFactory.create(resString));

			} catch (MkvException e) {

				/* Handle different scenarios of exception. */
				try {
					event.setError((byte) -1, "Unexpected exception {" + e + "}");
				} catch (Exception ee) {
					ee.printStackTrace();
				}
			}
		}
	}
}
