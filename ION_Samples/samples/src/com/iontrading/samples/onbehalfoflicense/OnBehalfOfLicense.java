/*
 * OnBehalfOfLicense
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

package com.iontrading.samples.onbehalfoflicense;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvLicenseManager;
import com.iontrading.mkv.enums.MkvOnBehalfOfFeatureLicenseError;
import com.iontrading.mkv.enums.MkvOnBehalfOfRegistrationResult;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvOnBehalfOfFeatureLicenseEvent;
import com.iontrading.mkv.events.MkvOnBehalfOfFeatureLicenseListener;
import com.iontrading.mkv.events.MkvOnBehalfOfRegistrationListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

/**
 *
 * @author lveraldi
 */
public class OnBehalfOfLicense {

    private static final String MAIN_VIRTUAL_LICENSE = "WebClient";
    private static final String BY_FEATURE_LICENSE = "WebByFeature";
    
    private Mkv mkv;
    private final Map<String /* Virtual Component Name */, 
        Map<String /* By-Feature License Name */, Integer /* Number of Licenses */>> virtualComponentMap = 
        new HashMap<String, Map<String, Integer>>();
    
    public static void main(String[] args) {
        new OnBehalfOfLicense(args);
    }

    /** Creates a new instance of OnBehalfOfLicense */
    public OnBehalfOfLicense(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        qos.setPlatformListeners(new MkvPlatformListener[] { new OnBehalfOfLicenseMkvPlatformListener() });
        
        try {
            // 
            // Registering an On-Behalf-Of component.
            // This component will also consume a by-feature license.
            //
            final String virtualComponentName = "VirtualWebGUI";
            Map<String, Integer> licenses = new HashMap<String, Integer>();
            licenses.put(BY_FEATURE_LICENSE, 10);

            virtualComponentMap.put(virtualComponentName, licenses);

            //
            // Start the engine and get back the instance of Mkv (unique during the
            // life of a component).
            //
            mkv = Mkv.start(qos);
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }
    
    class OnBehalfOfLicenseMkvPlatformListener implements MkvPlatformListener {

        public void onMain(MkvPlatformEvent event) {
            switch(event.intValue()) {
                //
                // The Daemon will clean-up all On-Behalf-Of components as well as 
                // all their associated by-feature licenses when we unregister 
                // from the ION Platform.
                //
                // For this reason, on the Register IDLE event we need to retry 
                // the acquisition of all On-Behalf-Of licenses.
                //
                case MkvPlatformEvent.REGISTER_IDLE_code: {
                    for(Map.Entry<String, Map<String, Integer>> entry : virtualComponentMap.entrySet()) {
                        final String componentName = entry.getKey();
                        mkv.getPlatform().registerOnBehalfOf(componentName, 
                                "Administrator", "mkvpwd", 
                                MAIN_VIRTUAL_LICENSE, 
                                new OnBehalfOfLicenseListener(componentName));
                    }
                }
                
                default: break;
            }
            
        }

        public void onComponent(MkvComponent component, boolean registered) {}
        public void onConnect(String component, boolean connected) {}
    }
    
    class OnBehalfOfLicenseListener implements MkvOnBehalfOfRegistrationListener {
        
        private String componentName;
        
        public OnBehalfOfLicenseListener(String componentName) {
            this.componentName = componentName;
        }
        
        public void onCall(MkvOnBehalfOfRegistrationResult res, String token) {
            switch(res.intValue()) {
                case MkvOnBehalfOfRegistrationResult.REGISTRATION_SUCCESSFUL_code: {
                    //
                    // Registration is successful: we can now acquire the by-feature licenses
                    // associated to this virtual component.
                    //
                    mkv.getLicenseManager().addOnBehalfOfFeatureLicenseListener(componentName, new OnBehalfOfByFeatureLicenseListener(componentName));

                    System.out.println("Registration successful for the Virtual Component: {" + componentName + "} Result: {" + res.toString() + "} Token: {" + token + "}");
                    Map<String, Integer> licenses = virtualComponentMap.get(componentName);
                    for(Map.Entry<String, Integer> entry : licenses.entrySet()) {
                        try {
                            mkv.getLicenseManager().requestOnBehalfOf(componentName, token, entry.getKey(), entry.getValue());
                        } catch(Exception e) {
                            System.out.println("ByFeature License for On-Behalf-Of component request failed: Exception: {" + e + "}");
                            e.printStackTrace();
                        }
                    }
                } break;
                
                default: {
                    System.out.println("Failed to register the Virtual Component: {" + componentName + "} Result: {" + res.toString() + "}");
                } break;
            }
        } 
    }
    
    class OnBehalfOfByFeatureLicenseListener implements MkvOnBehalfOfFeatureLicenseListener {

        private String componentName;
        
        public OnBehalfOfByFeatureLicenseListener(String componentName) {
            this.componentName = componentName;
        }
        
        public void onAcquired(MkvOnBehalfOfFeatureLicenseEvent event) {
            System.out.println("ByFeature License for On-Behalf-Of component {" + componentName + "} acquired successfully: "
                    + "Acquired: {" + event.getAcquired() + "} "
                    + "TotalAcquired: {" + mkv.getLicenseManager().getAcquiredOnBehalfOf(componentName, event.getFeature()) + "}");
        }

        public void onFailed(MkvOnBehalfOfFeatureLicenseEvent event, MkvOnBehalfOfFeatureLicenseError error, String message) {
            System.out.println("ByFeature License for On-Behalf-Of component {" + componentName + "} acquired FAILED!: "
                    + "Error: {" + error.toString() + "} Message: {" + message + "}");
        }

        public void onLost(Collection<String> lostFeatures) {
            System.out.println("ByFeature License for On-Behalf-Of component {" + componentName + "} LOST!: "
                    + "ByFeature Licenses: {" + lostFeatures.toString() + "}");
        }
    }
}
