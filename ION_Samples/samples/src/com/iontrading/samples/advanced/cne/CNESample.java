/*
 * CNESample
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

package com.iontrading.samples.advanced.cne;

import java.util.HashMap;
import java.util.Map;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvPersistentChain;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSubscribeManager;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvChainAction;
import com.iontrading.mkv.events.MkvChainListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSubscribeProxy;
import com.iontrading.mkv.qos.MkvQoS;

public class CNESample {
	private final CNELogic logic = new CNELogic();

	private final String applicationId = Integer.toHexString((int) System
			.currentTimeMillis() % 1000);

	public static void main(String[] args) {
		MkvQoS qos = new MkvQoS();
		qos.setArgs(args);
		try {
			// starts the API
			Mkv.start(qos);

			new CNESample();
			// set up permanent subscription to the depths and to
			// the order chain
		} catch (MkvException e) {
			e.printStackTrace();
		}
	}

	/** Creates a new instance of OrderManagement */
	public CNESample() {
		subscribeToR4QChain();
	}

	public String getApplicationId() {
		return applicationId;
	}

	/**
	 * Set up a permanent subscription to the r4q chain. The chain to subscribe
	 * is the one of the user logged in.
	 */
	private void subscribeToR4QChain() {
		MkvSubscribeManager mkvSubscribe = Mkv.getInstance()
				.getSubscribeManager();
		MkvPersistentChain chain = mkvSubscribe.persistentSubscribe(MarketDef
				.getName("CM_R4Q", "RFQ"), new CNEChainListener(),
				new CNERecordListener(), MarketDef.RFQ_FIELDS);
	}

	class CNEChainListener implements MkvChainListener {
		public void onSupply(MkvChain chain, String record, int pos,
				MkvChainAction action) {
			if (action.equals(MkvChainAction.APPEND)
					|| action.equals(MkvChainAction.INSERT)) {
				logic.initBenchmark(record);
			}
		}
	}

	class CNERecordListener implements MkvRecordListener {
		private final MkvSubscribeProxy rfqProxy = new MkvSubscribeProxy(
				R4Q.class, MarketDef.RFQ_FIELDS);

		private final Map rfqs = new HashMap();

		private R4Q getRfq(String recordName) {
			R4Q rfq = (R4Q) rfqs.get(recordName);
			if (rfq == null) {
				rfq = new R4Q(logic);
				setRfq(recordName, rfq);
			}
			return rfq;
		}

		private void setRfq(String recordName, R4Q rfq) {
			rfqs.put(recordName, rfq);
		}

		/**
		 * The component doesn't need to listen to partial updates
		 */
		public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
				boolean isSnap) {
		}

		/**
		 * Get the updates for the CM_ORDER records. If the received update is
		 * related to an order that has a cached MarketOrder object counterpart
		 * the supply event is forwarded.
		 */
		public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply,
				boolean isSnap) {
			R4Q rfq = getRfq(mkvRecord.getName());
			rfq.setMkvRecord(mkvRecord);

			try {
				rfqProxy.update(mkvRecord, mkvSupply, rfq);
				logic.check(rfq);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
