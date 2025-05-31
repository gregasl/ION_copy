/*
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
 * ION Trading ltd (2017)
 */

package com.iontrading.samples.xrsClient;

import java.util.ArrayList;

import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.exceptions.MkvException;

/**
 * Helps with {@link MkvSupply}.
 */
public class SupplyUtils {
    /**
     * Converts {@link MkvSupply} to {@link Object} array.
     */
    public static Object[] toArray(MkvSupply supply) {
        ArrayList<Object> result = new ArrayList<Object>();
        for (int i = supply.firstIndex(); i >= 0; i = supply.nextIndex(i)) {
            try {
                Object object = supply.getObject(i);
                if (MkvFieldType.STR.equals(supply.getType(i))) {
                    object = supply.getString(i);
                }

                result.add(object);
            } catch (MkvException ex) {
                ex.printStackTrace();
            }
        }
        return result.toArray(new Object[0]);
    }
}
