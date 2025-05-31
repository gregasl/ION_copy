/*
 * SimpleOnBehalfOfRegistration
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
package com.iontrading.samples.simpleOnBehalfOfRegistration;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.enums.onbehalfof.MkvOnBehalfOfAuthenticationResult;
import com.iontrading.mkv.enums.onbehalfof.MkvOnBehalfOfRegistrationResult;
import com.iontrading.mkv.enums.onbehalfof.MkvOnBehalfOfUnregistrationResult;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.onbehalfof.MkvOnBehalfOfAuthenticationWithPasswordResponseEvent;
import com.iontrading.mkv.events.onbehalfof.MkvOnBehalfOfAuthenticationWithPasswordResponseListener;
import com.iontrading.mkv.events.onbehalfof.MkvOnBehalfOfAuthenticationWithTokenResponseEvent;
import com.iontrading.mkv.events.onbehalfof.MkvOnBehalfOfAuthenticationWithTokenResponseListener;
import com.iontrading.mkv.events.onbehalfof.MkvOnBehalfOfRegistrationListener;
import com.iontrading.mkv.events.onbehalfof.MkvOnBehalfOfUnregistrationListener;
import com.iontrading.mkv.onbehalfof.api.MkvOnBehalfOfRegistrationFinalizer;
import com.iontrading.mkv.qos.MkvQoS;


/*
 * This is a very basic example of how to register a virtual component onto the ION Platform.
 * Registration proceeds in two phases:
 * - first one, is required to authenticate the user credentials
 * - second one, is to complete the actual registration and obtain the token.
 */
public class SimpleOnBehalfOfRegistration
{
    private static Mkv mkv;

    private static void registerVirtualComponentUsingStandardCredentials(final String componentName, final String userAccount, final String password) {
        mkv.getPlatform().registerOnBehalfOfUsingPassword(userAccount, password, null, null, "PUB", "localhost", new MkvOnBehalfOfAuthenticationWithPasswordResponseListener() {
            public void onAuthenticationSuccess(MkvOnBehalfOfAuthenticationWithPasswordResponseEvent responseEvent, MkvOnBehalfOfRegistrationFinalizer registrationFinalizer) {
                System.out.println("[OnBehalfOf] Using Password -- Credential check SUCCEEDED: User = " + responseEvent.getUsername() + ", Groups = " + responseEvent.getGroupList());

                System.out.println("[OnBehalfOf] Using Password -- Performing actual registration");
                registrationFinalizer.completeRegistration(componentName, "1.0.0", "1.0.0", "Win10 x64", new MkvOnBehalfOfRegistrationListener() {
                    public void onRegistrationSuccess(String authenticationToken) {
                        System.out.println("[OnBehalfOf] Using Password -- Registration COMPLETED! Token = " + authenticationToken);

                        performUnregistration(componentName, authenticationToken);
                    }

                    @Override
                    public void onRegistrationFailure(MkvOnBehalfOfRegistrationResult errorResult) {
                        System.out.println("[OnBehalfOf] Using Password -- Registration FAILED! Error = " + errorResult);
                    }
                });
            }

            public void onAuthenticationFailure(MkvOnBehalfOfAuthenticationResult res, String customErrMsg) {
                System.out.println("[OnBehalfOf] Using Password -- Credential check FAILED! Error = " + res.toString() + ", Custom Message = " + customErrMsg);
            }
        });
    }

    private static void registerVirtualComponentUsingEnterpriseAuthenticationToken(final String componentName, final String serviceId, final String tokenEncoding, final String tokenValue) {
        mkv.getPlatform().registerOnBehalfOfWithAuthenticationToken(serviceId, tokenEncoding, tokenValue, null, "PUB", "loalhost", new MkvOnBehalfOfAuthenticationWithTokenResponseListener() {
            public void onAuthenticationSuccess(MkvOnBehalfOfAuthenticationWithTokenResponseEvent responseEvent, MkvOnBehalfOfRegistrationFinalizer registrationFinalizer) {
                System.out.println("[OnBehalfOf] Using Enteprise Authentication -- Credential check SUCCEEDED: User = " + responseEvent.getUsername() + ", Groups = " + responseEvent.getGroupList());

                System.out.println("[OnBehalfOf] Using Enteprise Authentication -- Performing actual registration");
                registrationFinalizer.completeRegistration(componentName, "1.0.0", "1.0.0", "Win10 x64", new MkvOnBehalfOfRegistrationListener() {
                    public void onRegistrationSuccess(String authenticationToken) {
                        System.out.println("[OnBehalfOf] Using Enteprise Authentication -- Registration COMPLETED! Token = " + authenticationToken);

                        performUnregistration(componentName, authenticationToken);
                    }

                    @Override
                    public void onRegistrationFailure(MkvOnBehalfOfRegistrationResult errorResult) {
                        System.out.println("[OnBehalfOf] Using Enteprise Authentication -- Registration FAILED! Error = " + errorResult);
                    }
                });
            }

            @Override
            public void onAuthenticationFailure(MkvOnBehalfOfAuthenticationResult res, String customErrMsg) {
                System.out.println("[OnBehalfOf] Using Enteprise Authentication -- Credential check FAILED! Error = " + res.toString() + ", Custom Message = " + customErrMsg);
            }
        });
    }

    private static void performUnregistration(final String componentName, final String authenticationToken) {
        mkv.getPlatform().unregisterOnBehalfOf(componentName, authenticationToken, new MkvOnBehalfOfUnregistrationListener() {
            public void onUnregistrationSuccess() {
                System.out.println("[OnBehalfOf] Unregistration -- Operation COMPLETED!");
            }

            public void onUnregistrationFailure(MkvOnBehalfOfUnregistrationResult errorResult) {
                System.out.println("[OnBehalfOf] Unregistration -- Operation FAILED! Error = " + errorResult);
            }
        });
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
			qos.setPlatformListeners(new MkvPlatformListener[] { new MkvPlatformListener() {
                @Override
                public void onMain(MkvPlatformEvent event) {
                    if(event == MkvPlatformEvent.REGISTER_IDLE) {
                        System.out.println("[OnBehalfOf] Using Password -- Requesting a virtual component registration, this will be processed asynchronously");
                        registerVirtualComponentUsingStandardCredentials("VirtualComponent1", "mkv", mkv.getProperties().getPasswordProperty("user.pwd", "PWDTOOL_ENCRYPTION_SEED"));

                        System.out.println("[OnBehalfOf] Using Enteprise Authentication -- Requesting a virtual component registration, this will be processed asynchronously");
                        registerVirtualComponentUsingEnterpriseAuthenticationToken("VirtualComponent2", "KERBEROS", "BASE64", "ABC");
                    }
                }

                @Override
                public void onConnect(String component, boolean connected) {}
                public void onComponent(MkvComponent component, boolean registered) {}
            }});

			mkv = Mkv.start(qos);
		}
		catch (Exception e)
		{
			System.out.println("Exception: " + e + "\n");
			e.printStackTrace();
		}
	}
}

