/*
 * MessageQueueSubscriber
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

package com.iontrading.samples.messageQueueSubscriber;



import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvMQSubscribeAction;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionCallListener;
import com.iontrading.mkv.events.MkvMQSubscribeEvent;
import com.iontrading.mkv.events.MkvMQSubscribeListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.messagequeue.MkvMQSubscribe;
import com.iontrading.mkv.messagequeue.MkvMQSubscribeConf;
import com.iontrading.mkv.qos.MkvQoS;

public class MessageQueueSubscriber {


	private static final String SOURCE = "MYPUB";
	private static final String FUNCTION_ID = "GetLastTradesByTrader";


	private class MQSubscribeListener implements MkvMQSubscribeListener {
		final String queueName;
		public MQSubscribeListener(String queueName) {
			this.queueName = queueName;
		}
		/* handler for message queue related events */
		public void onUpdate(MkvMQSubscribeEvent event) {
			try {
				switch (event.getSubscribeAction().intValue()) {
				case MkvMQSubscribeAction.SUPPLY_code:
					/* a record was received from the queue */
					System.out.println("Reading result from ephemeral message queue {" + queueName +
						"} -> Code: {" +  event.getSupply().getString(0) +
						"} Price: {" + event.getSupply().getDouble(1) +
						"} Qty: {" + event.getSupply().getDouble(2) +
						"} VerbStr: {" + event.getSupply().getString(3));

					break;
				case MkvMQSubscribeAction.CLOSE_code:
					/* the publisher has closed the queue - no more data will be received from it */
					System.out.println("Ephemeral message queue {" + queueName + "} has been closed");
				default:
					;
				}
			}
			catch(MkvException e) {
				e.printStackTrace();
			}

		}
	}




	private class PublishListener implements MkvPublishListener {
		public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
		}

		public void onPublishIdle(String component, boolean start) {
			if (start) {
				/* attempt calling function */
				MkvFunction fun = Mkv.getInstance().getPublishManager().getMkvFunction(
						SOURCE + "_" + FUNCTION_ID);
				callFun(fun);
			}
		}

		public void onSubscribe(MkvObject mkvObject) {
		}
	}

	public void start(String[] args) {
		MkvQoS qos = new MkvQoS();
		qos.setArgs(args);
		/* set up listener for publish events */
		qos.setPublishListeners(new MkvPublishListener[] { new PublishListener() });
		try {
			Mkv.start(qos);
		} catch (MkvException e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) {
		new MessageQueueSubscriber().start(args);
	}

	private void callFun(MkvFunction fun) {
		if (fun != null) {
			MkvSupply args = MkvSupplyFactory.create(new Object[] {
					new String("Jill")});
			try {
				/* invoke function */
				fun.call(args, new FunctionListener());
				System.out.println("Function GetLastTradesByTrader called succesfully");
			} catch (MkvException e) {
				e.printStackTrace();
			}
		}
	}

	private class FunctionListener implements MkvFunctionCallListener {
		public void onError(MkvFunctionCallEvent mkvFunctionCallEvent,
				byte errCode, String errStr) {

			System.out.println("Error result for GetLastTradesByTrader function: " +
					"code {" + errCode + "} message {" + errStr + "}");
		}

		public void onResult(
				MkvFunctionCallEvent mkvFunctionCallEvent, MkvSupply result) {
			/* on servicing the function call,
			 * MessageQueuePublisher publishes some data onto a dynamically created queue,
			 * whose name it returns as the function result */
			try {
				String mainResult = result.getString(0);
				if(mainResult.startsWith("0:")) {
					String resultMQ = result.getString(2);
					System.out.println("Publisher is returning result for GetLastTradesByTrader function on ephemeral message queue {" + resultMQ + "}");
					/* subscribe the queue */
					MkvMQSubscribe mkvMQSubscribe =
						Mkv.getInstance().getMQManager().createSubscription(resultMQ,
								/* set up a listener for data supplied to the queue */
								new MQSubscribeListener(resultMQ),
								/* use defaults for subscription */
								new MkvMQSubscribeConf());
					mkvMQSubscribe.subscribe();
				}
				else {
					System.out.println("Publisher returned error: {" + mainResult + "}");
				}
			} catch (MkvException e) {
				e.printStackTrace();
			}
		}
	}

}


