package com.iontrading.samples.advanced.mqtochain;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.MkvLog;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.enums.MkvMQAction;
import com.iontrading.mkv.enums.MkvMQSubscribeAction;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionCallListener;
import com.iontrading.mkv.events.MkvMQSubscribeEvent;
import com.iontrading.mkv.events.MkvMQSubscribeListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.messagequeue.MkvMQSubscribe;
import com.iontrading.mkv.messagequeue.MkvMQSubscribeConf;
import com.iontrading.mkv.qos.MkvQoS;

class MyUtil {
	static public MkvLog myLog;
}

class UtilSplit
{
	public String Currency;
	public String Instrument;
	public String Source;
	public String Name;

	UtilSplit(String FullName){
		String results[] = FullName.split("\\.");
		if(results.length > 0){
			Name=results[3];
		}

		if(results.length > 1){
			Source=results[2];
		}

		if(results.length > 2){
			Instrument=results[1];
		}

		if(results.length > 3){
			Currency=results[0];
		}
	}
}

class ChainPublish
{
	MkvType 			NewType;
	MkvType				OldType;
	String[]			FieldNames;
	MkvFieldType[] 		FieldTypes;
	String				NewTypeName;
	MkvChain			NewChain;
	String 				NewChainName;
	String 				NewSource;
	UtilSplit			OldRecordSeg;
	FunctionParams		Params;


	ChainPublish(MkvMQSubscribeEvent event, FunctionParams Params) {

		this.Params=Params;
		OldType = event.getMessageQueueSubscribe().getMkvType();
		NewSource = Mkv.getInstance().getProperties().getComponentName();
		NewTypeName = NewSource + "_" + OldType.getName();
		NewType = Mkv.getInstance().getPublishManager().getMkvType(NewTypeName);

		StringBuffer fields = new StringBuffer();

		if(NewType == null) {
			FieldNames = new String[OldType.size()];
			FieldTypes = new MkvFieldType[OldType.size()];

			for (int i = 0; i < OldType.size(); i++) {
				FieldNames[i]=OldType.getFieldName(i);
				FieldTypes[i]=OldType.getFieldType(i);
				fields.append(FieldNames[i] + ":" + FieldTypes[i] + ", ");
			}

	        try {
	        	MyUtil.myLog.add("Publishing type " + NewTypeName + " " + fields.toString());
	        	NewType = new MkvType(NewTypeName, FieldNames, FieldTypes);
	        	NewType.publish();
	        	MyUtil.myLog.add("Published type " + NewTypeName);
	        } catch (MkvException e) {
	        	MyUtil.myLog.add("Unable to publish type " + NewTypeName,e);
	        }
		} else {
			MyUtil.myLog.add("Type " + NewTypeName + " already published, reusing it.");
		}

		OldRecordSeg = new UtilSplit(event.getMessageQueueSubscribe().getMkvRecord().getName());

		NewChainName = OldRecordSeg.Currency + "." + OldRecordSeg.Instrument + "." + NewSource + "." + Params.ChainName;

		NewChain = Mkv.getInstance().getPublishManager().getMkvChain(NewChainName);

		if(NewChain == null) {
			try {
				MyUtil.myLog.add("Publishing chain " + NewChainName + " of type " + NewTypeName);
				NewChain = new MkvChain(NewChainName,NewType.toString());
				NewChain.publish();
				MyUtil.myLog.add("Published chain " + NewChainName + " of type " + NewTypeName);
	        } catch (MkvException e) {
	        	MyUtil.myLog.add("Unable to publish chain " + NewChainName,e);
	        }
		} else {
			MyUtil.myLog.add("Chain " + NewChainName + " already published");
		}
	}

	private void ManageKILL(MkvMQSubscribeEvent event) {

		MyUtil.myLog.add("Unpublishing the chain " + NewChainName);
		Iterator iter = NewChain.iterator();
		while(iter.hasNext()) {
			 Object val = iter.next();
			 MkvRecord rec = Mkv.getInstance().getPublishManager().getMkvRecord(val.toString());
			 MyUtil.myLog.add("Removing the record " + val.toString() + " from the chain " + NewChainName);
			 NewChain.remove(val.toString());
			 try {
				 MyUtil.myLog.add("Unpublishing the record " + val.toString());
				 rec.unpublish();
			 } catch (MkvException e) {
				 MyUtil.myLog.add("Unable to unpublish the record " + val.toString(),e);
			 }
		}
		try {
			MyUtil.myLog.add("Unpublishing the chain " + NewChainName);
			NewChain.unpublish();
			NewChain.publish();
		} catch (MkvException e) {
			MyUtil.myLog.add("Unable to unpublish the chain " + NewChainName,e);
		}
	}

	private void ManageDELETE(MkvMQSubscribeEvent event) {
		String recordName = getRecordName(event);
		MyUtil.myLog.add("Removing the record " + recordName + " from the chain " + NewChainName);
		NewChain.remove(recordName);
		MkvRecord rec = Mkv.getInstance().getPublishManager().getMkvRecord(recordName);
		try {
			MyUtil.myLog.add("Unpublishing the record " + recordName);
			rec.unpublish();
		} catch (MkvException e) {
			MyUtil.myLog.add("Unable to unpublish the record " + recordName,e);
		}
	}

	private String getRecordName(MkvMQSubscribeEvent event) {
		UtilSplit split = new UtilSplit(event.getRecord());

		return OldRecordSeg.Currency + "." +  OldRecordSeg.Instrument + "." + NewSource + "." + split.Name;
	}

	private void ManageUPDATE(MkvMQSubscribeEvent event) {
		String NewRecordName=getRecordName(event);
		MkvRecord rec=null;
		if(event.getAction() == MkvMQAction.REWRITE) {
			MyUtil.myLog.add("Updating the record " + NewRecordName + " belonging to the chain " + NewChainName);
			rec = Mkv.getInstance().getPublishManager().getMkvRecord(NewRecordName);
			MyUtil.myLog.add("Updated the record " + NewRecordName + " belonging to the chain " + NewChainName);
		}

		if(event.getAction() == MkvMQAction.ADD) {

			rec = Mkv.getInstance().getPublishManager().getMkvRecord(NewRecordName);

			// In case of an ADD event, the record shouldn't be already published.

			try {
				if(rec == null) {
					// Should always enter here
					rec = new MkvRecord(NewRecordName,NewType.getName());
					MyUtil.myLog.add("Publishing the record " + NewRecordName);
					rec.publish();
					MyUtil.myLog.add("Adding the record " + NewRecordName + " to the chain" + NewChainName);
					NewChain.add(rec.getName());
				}
			} catch(MkvException e) {
				MyUtil.myLog.add("Unable to Add/Publish the record " + NewRecordName + " to the chain" + NewChainName,e);
			}
		}

		try {
			MyUtil.myLog.add("Supplying the record " + NewRecordName);
			rec.supply(event.getSupply());

			StringBuffer values = new StringBuffer();

			for (int i = 0; i < OldType.size(); i++) {
				try {
					values.append(NewType.getFieldName(i) + ":"+event.getSupply().getObject(i)+",");
				} catch (MkvException e) {
					e.printStackTrace(System.out);
				}
			}

			MyUtil.myLog.add("Supplied the record " + NewRecordName + "with "+ values);

		} catch(MkvException e) {
			MyUtil.myLog.add("Unable to supply the record " + NewRecordName,e);
		}
	}

	public void Supply(MkvMQSubscribeEvent event) {

		if(event.getAction() == MkvMQAction.KILL) {
			ManageKILL(event);
			return;
		}

		if(event.getAction() == MkvMQAction.REWRITE || event.getAction() == MkvMQAction.ADD) {
			ManageUPDATE(event);
			return;
		}

		if(event.getAction() == MkvMQAction.DELETE) {
			ManageDELETE(event);
			return;
		}
	}
}

class PrintSupplyQueueListener implements MkvMQSubscribeListener {

	private int 			counter;
	private ChainPublish	chainPublish;
	FunctionParams 			Params;

	PrintSupplyQueueListener(FunctionParams Params) {
		counter=0;
		this.Params=Params;
	}

	public void onUpdate(MkvMQSubscribeEvent event) {
		switch (event.getSubscribeAction().intValue()) {
		case MkvMQSubscribeAction.SUPPLY_code:
			if(counter == 0) {
				chainPublish = new ChainPublish(event, Params);
			}
			chainPublish.Supply(event);
			counter++;

			break;

		case MkvMQSubscribeAction.CLOSE_code:

			MyUtil.myLog.add("Close - Received " + counter + " rows");

			break;

		case MkvMQSubscribeAction.IDLE_code:
			MyUtil.myLog.add("Idle - Received " + counter + " rows");
			break;
		}
	}
}

class MQResultSubscribe implements MkvFunctionCallListener {

	private FunctionParams Params;
	MQResultSubscribe(FunctionParams Params) {
		this.Params=Params;
	}
	public void onError(MkvFunctionCallEvent event, byte errcode, String error) {
		MyUtil.myLog.add("Function call " + event.toString() + " from" + event.getCaller() +  " error "+ error + " for request " + event.getId());
	}

	public void onResult(MkvFunctionCallEvent event, MkvSupply result) {
		// Format is <errorcode>:<errorstring>:[<fieldname,fieldvalue>]
		String queuename="";
		try {
			MyUtil.myLog.add("Function call result " + event.toString() + " is " + result.getString(0)+ " from" + event.getCaller() +  " error for request " + event.getId());

			if (result.getString(0).equals("OK")) {
				MyUtil.myLog.add("Function call result error " + result.getString(0) + " is " + result.getString(0)+ " from" + event.getCaller() +  " error for request " + event.getId());
				throw new MkvException("Function failed");
			}


			if(result.getString(1).equals("QueueName")) {
				queuename = result.getString(2);
			} else {
				MyUtil.myLog.add("Function call result: Syntax Error QueueName expected instead of " + result.getString(1) + " is " + result.getString(0)+ " from" + event.getCaller() +  " error for request " + event.getId());
				throw new MkvException("Syntax error: QueueName expected");
			}


			if (queuename == null) {
				MyUtil.myLog.add("Function call result: Syntax Error cannot parse queue name from" + event.getCaller() +  " error for request " + event.getId());
				throw new MkvException("Syntax error: cannot parse queue name");
			}

			MyUtil.myLog.add("Subscribing to " + queuename + "...");

			MkvMQSubscribeConf conf = new MkvMQSubscribeConf();
			conf.setTimeout(0); // no timeout
			conf.setExpireOnClose(true);
			conf.setExpireOnUnpublish(true);

			MkvMQSubscribe sub = Mkv.getInstance().getMQManager().createSubscription(queuename,
					new PrintSupplyQueueListener(Params), conf);
			sub.subscribe(); // subscribe all fields
		} catch (MkvException e) {
			MyUtil.myLog.add("Unable to subscribe " + queuename + "...",e);
		}
	}
}

class FunctionParams
{
	public MkvSupply 	Args;
	public String 		FunctionName;
	public String 		ChainName;
}

class MQQueryExecutor implements java.lang.Runnable {
// there is no need to implements Runnable

	class MQFunctionListener implements MkvPublishListener {
		FunctionParams params;

		public MQFunctionListener(FunctionParams params) {
			MyUtil.myLog.add(" MQFunctionListener started ...");
			this.params = params;
			Mkv mkv = Mkv.getInstance();
			mkv.getPublishManager().addPublishListener(this);
			if(mkv.getPublishManager().getMkvFunction(params.FunctionName)!= null) {
				makeQuery();
			}
		}

		public void onPublish(MkvObject obj, boolean start, boolean dwl) {
			System.out.println(obj.getName());
			if ( start && obj.getMkvObjectType() == MkvObjectType.FUNCTION && obj.getName().equals(params.FunctionName)) {
				makeQuery();
			}
		}

		private void makeQuery() {
			try {
				Mkv mkv = Mkv.getInstance();
				MkvFunction queryfun = mkv.getPublishManager().getMkvFunction(params.FunctionName);

				if (queryfun == null) {
					MyUtil.myLog.add(params.FunctionName + " function not available");
				} else {
					MyUtil.myLog.add("Calling " + params.FunctionName);
					queryfun.call(params.Args,new MQResultSubscribe(params));

					MyUtil.myLog.add("Requesting...");
				}
			} catch (MkvException e) {
				MyUtil.myLog.add("Exception while calling " + params.FunctionName,e);
				System.exit(-1);
			}

		}
		public void onPublishIdle(String arg0, boolean arg1) {
		}

		public void onSubscribe(MkvObject arg0) {
		}
	}

	// end MQFunctionListener

	public MQQueryExecutor()
	{
		MyUtil.myLog.add(" MQQueryExecutor started ...");
		new Thread(this).start();
	}

	
	// configuration file format
	// <chain to publish> <function to call to obtain queue> <fun arg0 type> <fun arg 0 val> <fun arg1 type> <fun arg 1 val> ...
	private FunctionParams ReadConfigInit()
	{
		MyUtil.myLog.add("Reading the file config.init");

		String results[];
		int i,j;
		Object[] objs;

		FunctionParams ret = new FunctionParams();
		try {
			FileInputStream fin = new FileInputStream("config.init");
			BufferedReader in = new BufferedReader(new InputStreamReader(fin));

			String s = "";
			while ( (s = in.readLine()) != null) {
				MyUtil.myLog.add("Read the following line from config.init" + s);
				results = s.split(" |\\t");
				ret.ChainName = results[0];
				ret.FunctionName = results[1];
				objs = new Object[results.length];
				for(i=2,j=0;i<results.length;j++,i+=2){
					if(results[i].equals("STR")) {
						objs[j] = new String(results[i+1]);
					}
					if(results[i].equals("INT")) {
						objs[j] = new Integer(results[i+1]);
					}
					if(results[i].equals("REAL")) {
						objs[j] = new Double(results[i+1]);
					}
					if(results[i].equals("DATE")) {
						objs[j] = new Integer(results[i+1]);
					}
					if(results[i].equals("TIME")) {
						objs[j] = new Integer(results[i+1]);
					}
				}
				ret.Args =  MkvSupplyFactory.create(objs);
			}
		} catch(Exception e) {
			MyUtil.myLog.add("Exception while reading config.init",e);
		}

		return ret;
	}

	public void run()
	{
		try {
			// if Runnable is not implemented the MQFunctionListener object can be created into the
			// constructor of MQQueryExecutor.

			FunctionParams params = ReadConfigInit();
			new MQFunctionListener(params);

		} catch (NumberFormatException e) {
			MyUtil.myLog.add("Invalid Number",e);
			System.out.println("Invalid number");
		}
	}
}



public class MQToChain implements MkvPlatformListener {
	public static void main(String[] args) {
		try {

			// Launch the Mkv api
			MkvQoS qos = new MkvQoS();
			qos.setArgs(args);
			qos.setPlatformListeners(new MkvPlatformListener[] {new MQToChain()});
			Mkv mkv = Mkv.start(qos);


		} catch (MkvException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		} catch (Exception e) {
			System.out.println(e.toString());
		}
	}

	public void onComponent(MkvComponent arg0, boolean arg1) {

	}

	public void onConnect(String arg0, boolean arg1) {

	}

	@Override
	public void onMain(MkvPlatformEvent arg0) {
        switch(arg0.intValue()) {
        case MkvPlatformEvent.START_code:
        	MyUtil.myLog = Mkv.getInstance().getLogManager().createLogFile("MQTOCHAIN");
        	MyUtil.myLog.add("STARTing MQTOCHAIN ...");
        	new MQQueryExecutor();
            break;
        }
	}
}
