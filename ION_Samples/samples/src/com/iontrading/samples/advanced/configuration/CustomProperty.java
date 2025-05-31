/*
 * CustomProperty
 *
 * CustomProperty extends MkvIntProperty and add some more validation logic.
 * Accepts only even values.
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

import com.iontrading.mkv.properties.MkvIntProperty;

public class CustomProperty extends MkvIntProperty {
    
    public CustomProperty(String name, int def, String description) {
        super(name, def, description);
    }
    
    public boolean acceptValue(String value) {
        // put the custom logic here...
        int var = Integer.parseInt(value);
        if ((var%2)==0) {
            System.out.println("New value [" + value + "] for property ["
                + getName() + "] accepted.");
            PropertyRegistration.setVar5(var);
            return true;
        } else {
            System.out.println("New value [" + value + "] for property ["
                + getName() + "] rejected.");
            return false;
        }
    }
}

