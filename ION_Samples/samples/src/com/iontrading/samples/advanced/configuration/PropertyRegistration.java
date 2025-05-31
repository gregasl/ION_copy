/*
 * PropertyRegistration
 *
 * Handle configuration variable.
 * The VAR5 variable supports a custom logic to react on change requests.
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
package com.iontrading.samples.advanced.configuration;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvProperties;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.properties.MkvIntProperty;
import com.iontrading.mkv.properties.MkvStringProperty;
import com.iontrading.mkv.qos.MkvQoS;

public class PropertyRegistration {
    
    private static String var1;
    private static int var2;
    private static int var3;
    private static String var4;
    private static int var5;
    
    public static void main(String[] args) {
        MkvQoS q = new MkvQoS();
        q.setArgs(args);
        
        try {
            Mkv mkv = Mkv.start(q);
            MkvProperties pm = mkv.getProperties();
            
            // Registers custom properties
            pm.registerProperty(new MkvStringProperty("VAR1", "VAR1_Default",
                "A string variable"));
            pm.registerProperty(new MkvIntProperty("VAR2", 0,
                "An int variable"));
            pm.registerProperty(new MkvIntProperty("VAR3", -1,
                "One more int variable. This one is ranged", -100, 100));
            pm.registerProperty(new MkvStringProperty("VAR4", "BUY",
                "An enum variable, can be BUY or SELL", "BUY|SELL"));
            pm.registerProperty(new CustomProperty("VAR5", 4,
                "Custom logic variable. Change me from SysAdmin"));
            
            // Gets custom properties values from environment
            var1 = pm.getProperty("VAR1");
            var2 = pm.getIntProperty("VAR2");
            var3 = pm.getIntProperty("VAR3");
            var4 = pm.getProperty("VAR4");
            var5 = pm.getIntProperty("VAR5");
            
            System.out.println("The value of VAR1 is " + var1);
            System.out.println("The value of VAR2 is " + var2);
            System.out.println("The value of VAR3 is " + var3);
            System.out.println("The value of VAR4 is " + var4);
            System.out.println("The value of VAR5 is " + var5);
            
        } catch (MkvException e) {
            System.err.println("Mkv.start: fatal error");
            e.printStackTrace();
        }
    }
    
    /**
     * Change the valu of the value of the variable VAR5
     */
    public static void setVar5(int aVar5) {
        var5 = aVar5;
        System.out.println("The variable VAR5 has been changed to " + var5);
    }
}
