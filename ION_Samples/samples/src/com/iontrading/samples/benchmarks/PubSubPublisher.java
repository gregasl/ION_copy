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

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvLog;
import com.iontrading.mkv.MkvPattern;
import com.iontrading.mkv.MkvProperties;
import com.iontrading.mkv.MkvPublishManager;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.PrivateMkvProxy;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.properties.MkvIntProperty;
import com.iontrading.mkv.properties.MkvStringProperty;
import com.iontrading.mkv.qos.MkvQoS;

public class PubSubPublisher {

        private final Mkv mkv = Mkv.getInstance();
	private final String this_comp = mkv.getProperties().getComponentName();
        private static MkvLog myLog;

	public static void main(String[] args) {
	        MkvQoS qos = new MkvQoS();
	        qos.setArgs(args);
	        qos.setAutoRegister(false);
	    
	        try {
	                Mkv mkv = Mkv.start(qos);
	                        
	                myLog = mkv.getLogManager().createLogFile("Publisher");
	                myLog.add("Publisher Logs");

	                new PubSubPublisher();
	                mkv.getPlatform().register();
	        } catch (MkvException e) {
	                e.printStackTrace();
	                Mkv.stop();
	                // System.exit(-1);
	        }
	}

	public PubSubPublisher() throws MkvException {
		MkvProperties p = mkv.getProperties();
		
		// Register properties for benchmarking dimensions
		p.registerProperty(new MkvStringProperty("RECPREFIX", "CURR.INSTRUMENT." + p.getComponentName() + ".TEST_", ""));
                p.registerProperty(new MkvIntProperty("NUMREC", 1000, "", 1, 1000000));
                p.registerProperty(new MkvIntProperty("FLOW", 256, "", 1, 10000000));
                p.registerProperty(new MkvIntProperty("STRLEN", 32, "", 1, 32000));
                p.registerProperty(new MkvStringProperty("FIELDNAMES", "BID ASK BIDQ ASKQ BS AS CODE NUM", ""));
                p.registerProperty(new MkvStringProperty("FIELDTYPES", "R R R R R R S I", ""));
                                
		// Publish data
		String typename = "T_" + this_comp;
		MkvType type = createType(typename, p.getProperty("FIELDNAMES"), p.getProperty("FIELDTYPES"));
		type.publish();

		MkvFunction fun = new MkvFunction(this_comp + "_start", MkvFieldType.STR, 
		        new String[] { "Compname" },
		        new MkvFieldType[] { MkvFieldType.STR }, 
		        new StartListener());
		fun.publish();
	}
	
	public static MkvType createType(String typename, String fieldnames, String fieldtypes) throws MkvException {
            String[] fnames_strs = fieldnames.split(" ");
            String[] ftypes_strs = fieldtypes.split(" ");

            int len = fnames_strs.length;

            String[] names = new String[len + 2];
            MkvFieldType[] types = new MkvFieldType[len + 2];

            names[0] = "updCounter";
            types[0] = MkvFieldType.INT;
            names[1] = "updTimeStamp";
            types[1] = MkvFieldType.INT;

            for (int i = 0; i < len; i++) {
                    String t = ftypes_strs[i];
                    names[i + 2] = fnames_strs[i];

                    if (t.equalsIgnoreCase("S")) types[i + 2] = MkvFieldType.STR;
                    else if (t.equalsIgnoreCase("R")) types[i + 2] = MkvFieldType.REAL;
                    else if (t.equalsIgnoreCase("I")) types[i + 2] = MkvFieldType.INT;
                    else if (t.equalsIgnoreCase("D")) types[i + 2] = MkvFieldType.DATE;
                    else if (t.equalsIgnoreCase("T")) types[i + 2] = MkvFieldType.TIME;
                    else types[i + 2] = MkvFieldType.INT;
            }

            return new MkvType(typename, names, types);
	}

	class StartListener implements MkvFunctionListener {
	    
		int counter;

		public void onCall(MkvFunctionCallEvent event) {
			try {
			        MkvProperties p = mkv.getProperties();
			        String prefix = p.getProperty("RECPREFIX");
			    
				String remote_comp = event.getArgs().getString(0);
				String typename = "T_" + this_comp;
				String chainname = prefix + this_comp + "_" + remote_comp;

				if (Mkv.getInstance().getPublishManager().getMkvChain(chainname) != null) {
					event.setError((byte) -1, "-1:Already registered");
					return; // Already registered
				}

				event.setResult(MkvSupplyFactory.create(prefix));

				// Publish one chain and numrec records for this component
				MkvChain chain = new MkvChain(chainname, typename);
				chain.publish();

				MkvPattern pattern = new MkvPattern(prefix.substring(0, prefix.lastIndexOf('.') + 1), typename);
				pattern.publish();

				synchronized (this) {
					for (int i = 0; i < p.getIntProperty("NUMREC"); i++) {
						MkvRecord r = new MkvRecord(prefix + "_" + this_comp + counter++, typename);
						r.publish();
						chain.add(r.getName());
					}
				}

				AckListener acklner = new AckListener(chain, remote_comp);
				
				MkvFunction fun = new MkvFunction(this_comp + "_" + remote_comp + "_ack", MkvFieldType.INT, new String[] { "n" }, new MkvFieldType[] { MkvFieldType.INT }, acklner);
				fun.publish();
			} catch (Throwable e) {
				e.printStackTrace(System.out);
				// System.exit(-1);
			}
		}
	}

	class AckListener implements MkvFunctionListener {
	    
                private final int flow = mkv.getProperties().getIntProperty("FLOW");
                private final int numrec = mkv.getProperties().getIntProperty("NUMREC");               
                private final int strlen = mkv.getProperties().getIntProperty("STRLEN");

                private final SupplyFlow supplyflow;                
		private final MkvSupply res = MkvSupplyFactory.create(mkv.getProperties().getIntProperty("FLOW"));
		private final MkvRecord[] recs;
		
		private int[] tosupply;
		private int toupd;
		private int counter;

		public AckListener(MkvChain chain, String remote_comp) {
			MkvPublishManager pm = mkv.getPublishManager();

			supplyflow = new SupplyFlow(pm.getMkvType("T_" + this_comp), flow, strlen);

			recs = new MkvRecord[chain.size()];

			tosupply = new int[chain.size()];

			for (int i = 0; i < recs.length; i++) {
				recs[i] = pm.getMkvRecord((String) chain.get(i));
			}
		}

		public void onCall(MkvFunctionCallEvent event) {
			boolean result = true;
			try {
				for (int i = 0; i < flow; i++) {
					Object[] sup = supplyflow.getSupply(tosupply[toupd]);
					sup[0] = Integer.valueOf(counter);
					int time = PrivateMkvProxy.getMkvSupplyTimeStamp(mkv).get();
					sup[1] = time;
					recs[toupd].supply(supplyflow.getIndexes(), sup);

					toupd = ++toupd % numrec;
					tosupply[toupd] = ++tosupply[toupd] % flow;
					counter++;
				}
				event.setResult(res);
			} catch (MkvException e) {
				e.printStackTrace();
				//System.exit(-1);
			}
		}
	}
}
