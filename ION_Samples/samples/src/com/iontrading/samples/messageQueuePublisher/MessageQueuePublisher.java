/*
 * MessageQueuePublisher
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
 * ION Trading ltd (2009)
 */

package com.iontrading.samples.messageQueuePublisher;


import java.text.DecimalFormat;
import java.util.Random;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.messagequeue.MkvMQConf;
import com.iontrading.mkv.messagequeue.MkvMQ;
import com.iontrading.mkv.qos.MkvQoS;

public class MessageQueuePublisher implements MkvPlatformListener {

	private Mkv mkv;

    private static final String SOURCE = "MYPUB";
    private static final String FUNCTION_ID = "GetLastTradesByTrader";



	/** Creates a new instance of MessageQueuePublisher */
	public MessageQueuePublisher(String[] args) {
		// create the initial configuration used to start the engine.
		MkvQoS qos = new MkvQoS();
		qos.setArgs(args);
		qos.setPlatformListeners(new MkvPlatformListener[] { this });
		try {
			// Start the engine and get back the instance of Mkv (unique during
			// the
			// life of a component).
			mkv = Mkv.start(qos);
		} catch (MkvException e) {
			e.printStackTrace();
		}
	}

	public void onComponent(MkvComponent mkvComponent, boolean onoff) {
	}

	public void onConnect(String component, boolean onoff) {

	}

	public void onMain(MkvPlatformEvent mkvPlatformEvent) {
		switch (mkvPlatformEvent.intValue()) {
		case MkvPlatformEvent.START_code:
			break;
		case MkvPlatformEvent.STOP_code:
			break;
		case MkvPlatformEvent.REGISTER_IDLE_code:
			break;
		case MkvPlatformEvent.REGISTER_code:
			try {
	            MkvFunction function = new MkvFunction(
	                    // the name of the function compliant to the notation SOURCE_NAME
	            		SOURCE + "_" + FUNCTION_ID,
	                    // The return type
	            		MkvFieldType.STR,
	            		// the name of the argument
	            		new String[] {"Arg"},
	            		// the type of the argument
	            		new MkvFieldType[] {MkvFieldType.STR},
	            		// help string
	                    "Return some fake trade records given a trader's name",
	                    // the listener that will handle the requests
	                    new FunctionHandler());
	            function.publish();
			}
			catch (MkvException e){
				e.printStackTrace();
			}

			break;
		}
	}

	public static void main(String[] args) {
		new MessageQueuePublisher(args);
	}

    private class FunctionHandler implements MkvFunctionListener {
    	final MkvType mkvType;
    	FunctionHandler()
    	{
    		MkvType _mkvType = null;

    		/* create MkvType for dynamically created message queue */
    		try {
        		_mkvType = new MkvType("TRADETYPE",
        				new String [] { "Code", "Price", "Qty", "VerbStr" },
        				new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.REAL, MkvFieldType.REAL, MkvFieldType.STR } );
    		}
    		catch (MkvException e){
    			;
    		}
    		finally {
    			mkvType = _mkvType;
    		}
    	}



    	public void onCall(MkvFunctionCallEvent mkvFunctionCallEvent) {
            try {
                MkvSupply argsWrapper = mkvFunctionCallEvent.getArgs();
                StringBuffer mqName = new StringBuffer();
                /* generate a random name for a dynamically created message queue */
                mqName.append("ANY.SPLITRES.").append(SOURCE).append(".TEMP_").append(Math.abs(new Random().nextInt()));
                System.out.println("Returning the trades on queue {" + mqName.toString() + "}");
                /* retrieve input string */
                String argument = argsWrapper.getString(0);

                /* create a message queue with the random generated name */
                MkvMQ mkvMQ = mkv.getMQManager().create(mqName.toString(), mkvType, new MkvMQConf());

                long seed = argument.hashCode();
                Random r = new Random(seed);
            	DecimalFormat df = new DecimalFormat("0000000000");

                for (int i = 0; i < ((seed % 10) + 1); ++i)
                	mkvMQ.put(MkvSupplyFactory.create(new Object[] {
   							"CH" + new String(df.format(Math.abs(r.nextInt()) % 10000)),
   							new Double(90. + 2 * r.nextDouble()),
   							new Double(10000 * ( + 1)),
   							new String((r.nextInt() % 2 == 0) ? "Buy" : "Sell") }));


                /* close the queue with a 10 second timeout
                 * the queue will be destroyed when all data items are retrieved or when the timeout elapses,
                 * whichever happens first
                 */
                mkvMQ.flush();
                mkvMQ.close(10);
                /* the name of the queue is returned as the function call result so that the caller can subscribe to it */
                mkvFunctionCallEvent.setResult(MkvSupplyFactory.create(new Object[] { "0:Ok", "QueueName", mqName.toString() }));
            } catch (MkvException e) {
                try {
                    mkvFunctionCallEvent.setError((byte)-1, "Unexpected exception {" + e + "}");
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }
        }
    }
}
