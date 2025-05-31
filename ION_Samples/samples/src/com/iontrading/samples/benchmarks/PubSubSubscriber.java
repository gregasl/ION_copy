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

import java.util.HashMap;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvLog;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvProperties;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.PrivateMkvProxy;
import com.iontrading.mkv.enums.MkvChainAction;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.events.MkvChainListener;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionCallListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.properties.MkvIntProperty;
import com.iontrading.mkv.properties.MkvStringProperty;
import com.iontrading.mkv.qos.MkvQoS;

public class PubSubSubscriber {
	private final Mkv mkv = Mkv.getInstance();

	private static MkvLog myLog;
	private final MkvSupply ack_supply = MkvSupplyFactory.create(1);

        private String prefix;

	/**
         * Launches PubSubSubscriber application.
         *
         * @param args
         *            command-line parameters
         */
        public static void main(String[] args) {
                try {
                        MkvQoS qos = new MkvQoS();
                        qos.setArgs(args);
                        qos.setAutoRegister(false);

                        Mkv mkv = Mkv.start(qos);
                        myLog = mkv.getLogManager().createLogFile("Subcriber");
                        myLog.add("Subcriber Logs");

                        new PubSubSubscriber();
                        mkv.getPlatform().register();
                } catch (MkvException e) {
                        e.printStackTrace();
                        // System.exit(-1);
                }
        }
        
	public PubSubSubscriber() {
	        MkvProperties p = mkv.getProperties();

                // Register properties for benchmarking dimensions
                p.registerProperty(new MkvIntProperty("FLOW", 256, "", 1, 10000000));

                mkv.getPublishManager().addPublishListener(new JSubPublishListener(mkv));
	}

	/**
	 * Collects benchmarking info about every received update of the phase,
	 * traces a summary at the end of the phase, and calls again the
	 * 'RatePublisher_nextRate' function to activate the next phase.
	 */
	class JSubPublishListener implements MkvPublishListener {
	    
		private final MkvSupply start_supply;
		private final String compname;
		private final HashMap<String, AckCallListener> ack_listeners = new HashMap<String, AckCallListener>();
		private final int flow;

		JSubPublishListener(Mkv mkv) {
			compname = mkv.getProperties().getComponentName();
			start_supply = MkvSupplyFactory.create(compname);
			flow = mkv.getProperties().getIntProperty("FLOW");
		}

		public void onPublish(MkvObject object, boolean start, boolean dwl) {
		        // Don't handle unpublish actions
			if (!start) return;

			if (object.getMkvObjectType() == MkvObjectType.FUNCTION) {
				MkvFunction fun = (MkvFunction) object;

				try {
					if (fun.getName().endsWith("_start")) {
						String orig = fun.getName().substring(0, fun.getName().indexOf('_'));

						synchronized (ack_listeners) {
							if (ack_listeners.containsKey(orig)) {
								return; // already registered
							}

							AckCallListener flner = new AckCallListener();
							ack_listeners.put(orig, flner);
						}

						fun.call(start_supply, new StartCallListener());
					} else if (fun.getName().endsWith("_" + compname + "_ack")) {
						String orig = fun.getName().substring(0, fun.getName().indexOf('_'));
						mkv.getPlatform().startFlushBlock();

						try {
							for (int i = flow; i >= 0; i--) {
								fun.call(ack_supply, ack_listeners.get(orig));
							}
						} finally {
							mkv.getPlatform().endFlushBlock();
						}
					}
				} catch (MkvException e) {
					e.printStackTrace();
					// System.exit(-1);
				}
			} else if (object.getMkvObjectType() == MkvObjectType.CHAIN) {
				MkvChain chain = (MkvChain) object;

				if (chain.getName().startsWith(prefix)) {
					System.out.print("chainname: " + chain.getName());
					Mkv.getInstance().getSubscribeManager().persistentSubscribe(chain.getName(),
									new MkvChainListener() {
                                                                            public void onSupply(MkvChain chain, String record, int pos, MkvChainAction action) {}
                                                                        },
									new PubSubRecListener());
				}
			}
		}

		public void onPublishIdle(String component, boolean start) {}
		public void onSubscribe(MkvObject object) {}
	}
	
        class StartCallListener implements MkvFunctionCallListener {

                public void onResult(MkvFunctionCallEvent event, MkvSupply result) {
                        try {
                                 prefix = result.getString(0);
                        } catch (MkvException e) {}
                }
                public void onError(MkvFunctionCallEvent event, byte errorCode, String errorMessage) {}
        }
    
        class AckCallListener implements MkvFunctionCallListener {
            
                public void onResult(MkvFunctionCallEvent event, MkvSupply result) {
    
                        mkv.getPlatform().startFlushBlock();
                        try {
                                event.getFunction().call(ack_supply, this);
                        } catch (MkvException e) {
                                e.printStackTrace();
                                // System.exit(-1);
                        } finally {
                                mkv.getPlatform().endFlushBlock();
                        }
                }

                public void onError(MkvFunctionCallEvent event, byte errorCode, String errorMessage) {}
        }
    
        class PubSubRecListener implements MkvRecordListener {

            public void onPartialUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {}
            public void onFullUpdate(MkvRecord record, MkvSupply supply, boolean isSnapshot) {}
        }	
}
