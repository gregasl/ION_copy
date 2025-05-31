package com.iontrading.samples.simpleGenericAuditClient;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.enums.MkvGenericAuditAction;
import com.iontrading.mkv.enums.MkvGenericAuditResult;
import com.iontrading.mkv.enums.MkvGenericAuditServerError;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvGenericAuditAvailabilityListener;
import com.iontrading.mkv.events.MkvGenericAuditEvent;
import com.iontrading.mkv.events.MkvGenericAuditListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

/*
 * SimpleGenericAuditClient
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
public class SimpleGenericAuditClient implements MkvPlatformListener,
		MkvGenericAuditAvailabilityListener {

	private int evtCount = 0;

	public static void main(String[] args) {

		new SimpleGenericAuditClient(args);
	}

	public SimpleGenericAuditClient(String[] args) {

		MkvQoS qos = new MkvQoS();
		qos.setArgs(args);
		qos.setPlatformListeners(new MkvPlatformListener[] { this });
		try {
			Mkv.start(qos);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onMain(MkvPlatformEvent platformEvent) {

		switch (platformEvent.intValue()) {

		case MkvPlatformEvent.START_code:
			/*
			 * Here we add a new GenericAuditAvailabilityListener for
			 * notifications about the Generic Audit Server availability.
			 */
			Mkv.getInstance().getPlatform()
					.setGenericAuditAvailabilityListener(this);
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

	/*
	 * Method invoked by the Api when the availability of the Generic Audit
	 * Server changes. In this example we just wait for the Audit Server
	 * availability, and then send three simple Audit Messages. However the user
	 * might implement a more complex, multi-threaded mechanism for managing the
	 * production and logging of Generic Audit Events.
	 */
	public void onAvailability(boolean start) {

		if (start) {

			while (evtCount < 3) {

				String[] keys = new String[] { "key1", "key2", "key3" };
				String[] oldValues = new String[] { "0", "0", "0" };
				String[] newValues = new String[] { "1", "1", "1" };

				// Initializing the event the information to be logged.
				MkvGenericAuditEvent event = new MkvGenericAuditEvent();

				// Mandatory fields
				event.setCategory("APPLICATION");
				event.setEntity("SETTING");
				event.setEntityId("EUR.MTX_TRADER.TRVET_ALE.BTEC_traderTest");
				event.setTransactionId("1");
				event.setResult(MkvGenericAuditResult.SUCCESS);

				// Optional field
				event.setDescription("Additional details for the event");

				// Optional fields, auto-filled by the server if not set by the
				// application
				event.setAffectedSource("MMI_SRC_NAME");
				event.setAffectedComponent("MMI_COMP_NAME");
				event.setAffectedHost("MMI_HOST_NAME");

				// Optional fields, auto-filled by the server if not set
				event.setUpdaterUser("SYSADM_USER");
				event.setUpdaterComponent("SYSADM_COMP_ANAME");
				event.setUpdaterHost("SYSADM_HOST_ANAME");

				/*
				 * Simulating the usage of some common actions such as INSERT,
				 * AMEND, and DELETE.
				 */
				switch (evtCount) {

				case 0:
					// Mandatory field
					event.setAction(MkvGenericAuditAction.INSERT);
					// Optional field
					event.setKeyValuePairs(keys, null, newValues);
					break;

				case 1:
					event.setAction(MkvGenericAuditAction.AMEND);
					event.setKeyValuePairs(keys, oldValues, newValues);
					break;

				case 2:
					event.setAction(MkvGenericAuditAction.DELETE);
					event.setKeyValuePairs(keys, oldValues, null);
					break;

				default:
					break;
				}

				++evtCount;

				try {

					boolean res = Mkv
							.getInstance()
							.getPlatform()
							.logGenericAuditEvent(event,
									new MkvGenericAuditListener() {

										@Override
										public void onResult(
												MkvGenericAuditEvent evt,
												MkvGenericAuditServerError err,
												String errorMsg) {

											if (err == MkvGenericAuditServerError.AUDIT_SERVER_ERROR) {
												System.err.println("Server returned the error {"
														+ errorMsg
														+ "} on logging the generic audit event for entityId "
														+ evt.getEntityId());
												/*
												 * Here the user might implement
												 * their own logic for re-trying
												 * the task at a later time.
												 */
											} else {

												System.out.println("Generic Audit Event with entityId "
														+ evt.getEntityId()
														+ " successfully logged");
											}
										}
									});

					if (res) {
						System.out
								.println("Auditing request sent succesfully.");
					} else {
						System.err
								.println("Failed while performing the auditing request the Server!");
					}

				} catch (MkvException e) {

					System.err
							.println("Invalid data has been set into the Audit Event!");
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void onComponent(MkvComponent component, boolean registered) {

	}

	@Override
	public void onConnect(String component, boolean connected) {

	}
}
