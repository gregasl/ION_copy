/*
 * PasswordServerServiceCredentials

 *
 * Retrieve service credentials from the Daemon Password Server.
 *
 * ION Trading U.K. Limited supplies this software code for testing purposes
 * only. The scope of this software code is exclusively limited to the
 * demonstration of the structure of an application using the ION(tm) Common
 * Market and ION Trading U.K. Limited does not guarantee the correct behavior
 * of any deployed application using this software code. This software code has
 * not been thoroughly tested under all conditions. ION, therefore, cannot
 * guarantee or imply reliability, serviceability, or function of this software.
 * Any use of this software outside of this defined scope is the sole
 * responsibility of the user.
 *
 * Copyright 2005 ION Trading srl. All rights reserved.
 */
package com.iontrading.samples.advanced.passwordServerServiceCredentials;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvProperties;
import com.iontrading.mkv.MkvServicePasswordListener;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.properties.MkvStringProperty;
import com.iontrading.mkv.qos.MkvQoS;

public class PasswordServerServiceCredentials {

    private static final String PWD_SERVER_SERVICEID_PROPERTY_NAME = "DBSERVICE";
    private static final String PWD_SERVER_SERVICEID_DEFAULT = "";
    
    static Mkv mkv;
    static String serviceId;
    
    public static void main(String[] args) {
        MkvQoS qos = new MkvQoS();
        qos.setArgs(args);
        qos.setPlatformListeners(new MkvPlatformListener[] { new MyPlatformListener() });
        
        try {
            mkv = Mkv.start(qos);
            MkvProperties pm = mkv.getProperties();
            
            // Registers custom property
            pm.registerProperty(new MkvStringProperty(
                            PWD_SERVER_SERVICEID_PROPERTY_NAME, 
                            PWD_SERVER_SERVICEID_DEFAULT, 
                            "The Password Server serviceId to retrieve the DB credentials"));
            serviceId = pm.getProperty(PWD_SERVER_SERVICEID_PROPERTY_NAME);
            
        } catch (MkvException e) {
            System.err.println("Mkv.start: fatal error");
            e.printStackTrace();
        }

    }
    
    static class MyPlatformListener implements MkvPlatformListener {
        @Override
        public void onMain(MkvPlatformEvent event) {
            if(event == MkvPlatformEvent.REGISTER_IDLE) {
                //Call the Daemon Password Server to retrieve the required credentials
                mkv.getProperties().getServicePassword(serviceId, new MyServicePasswordListener());                
                //Result will come back asynchronously, as soon as the Daemon Password Server has processed the request
            }
        }

        @Override
        public void onComponent(MkvComponent component, boolean registered) {}

        @Override
        public void onConnect(String component, boolean connected) {}
        
    }
    
    static class MyServicePasswordListener implements MkvServicePasswordListener {
        @Override
        public void onRequestCompleted(String serviceName, String userName, String password) {
            //Credentials have been retrieved and are now ready to be used to connect to the DBMS!
            System.out.println("Service Credentials for ID: " + serviceId + " : retrieved successfully!");
            //Use "userName" and "password" secrets here
        }

        @Override
        public void onRequestFailed(byte errCode, String errorMessage) {
            //Request to retrieve the service credentials has failed!
            System.out.println("Failed to retrieve Service Credential for ID: " + serviceId + " : " + errCode + ":" + errorMessage);
        }
        
    }
}
