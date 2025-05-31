/*
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
 * ION Trading ltd (2006-2007)
 */

package com.iontrading.samples.benchmarks;

import java.util.GregorianCalendar;

import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.helper.MkvSupplyUtils;

public class SupplyFlow {

	private final Object[][] supplies;
	private final int[] indexes;

	public SupplyFlow(MkvType t, int flow, int strlen) {
		supplies = new Object[flow][];

		GregorianCalendar cal = new GregorianCalendar();

		Object values[] = new Object[t.size()];
		indexes = new int[t.size()];

		for (int j = 0; j < t.size(); j++) {
			indexes[j] = j;
		}

		for (int i = 0; i < flow; i++) {
			cal.add(GregorianCalendar.DATE, 1);
			cal.add(GregorianCalendar.SECOND, 1);

			for (int j = 0; j < t.size(); j++) {
				switch (t.getFieldType(j).intValue()) {
					case MkvFieldType.REAL_code :
						values[j] = new Double(10.0 + 0.1 * i);
						break;
					case MkvFieldType.INT_code :
						values[j] = new Integer(i);
						break;
					case MkvFieldType.DATE_code :
						values[j] = new Integer(MkvSupplyUtils.getMkvDate(cal));
						break;
					case MkvFieldType.TIME_code :
						values[j] = new Integer(MkvSupplyUtils.getMkvTime(cal));
						break;
					case MkvFieldType.STR_code : {
						StringBuffer tmp = new StringBuffer();
						tmp.append(i);
						tmp.append(' ');
						while (tmp.length() < strlen) {
							tmp.append((char) ((' ' + (i % 94)) & 0xff));
						}
						values[j] = new String(tmp);
						break;
					}
				}
			}

			supplies[i] = values.clone();
		}
	}

	public Object[] getSupply(int idx) {
		return supplies[Math.abs(idx % supplies.length)];
	}

	public int[] getIndexes() {
		return indexes;
	}
}