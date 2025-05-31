package com.iontrading.samples.simpleByFeatureService;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvChain;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvType;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.qos.MkvQoS;
import com.iontrading.proguard.annotation.Keep;
import com.iontrading.proguard.annotation.KeepPublicClassMemberNames;

@Keep
@KeepPublicClassMemberNames
public class SimpleByFeatureService {
	/*************************************
	 * This is the code that will change *
	 * according to your business domain *
	 *************************************/
	private static final String SOURCE = "Demo";

	private static String xmlFile =
		"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" +
		"<ns:ByFeatureService xmlns:ns=\"http://www.iontrading.com/entitlementserver/\" name=\"" + SOURCE + "\" version=\"1.0\">" +
			"<Dictionary name=\"ANY.METADATA." + SOURCE + ".RESOURCES\" type=\"CHAIN\">" +
				"<Field name=\"Level1\" level=\"1\"/>" +
				"<Field name=\"Level2\" level=\"2\"/>" +
				"<Field name=\"Level3\" level=\"3\"/>" +
				"<Field name=\"Level4\" level=\"4\"/>" +
			"</Dictionary>" +
			"<FeatureList name=\"ANY.METADATA."+ SOURCE + ".FEATURES\" type=\"CHAIN\">" +
			"<Field name=\"FeatureName\" description=\"FeatureDesc\"/>" +
			"</FeatureList>" +
		"</ns:ByFeatureService>";

	private static String[][] resources = new String[][] {
		{ "TRADESERVER", "*", "TRADESERVER_DEMO", "GVC2"}, { "TRADESERVER", "*", "TRADESERVER_DEMO", "GVCC"},
		{ "TRADESERVER", "*", "TRADESERVER_DEMO", "GVCF"}, { "TRADESERVER", "*", "TRADESERVER_DEMO", "GVD2"},
		{ "TRADESERVER", "*", "TRADESERVER_DEMO", "GVDC"}, { "TRADESERVER", "*", "TRADESERVER_DEMO", "GVDF"},
		{ "TRADESERVER", "*", "TRADESERVER_X", "GVC2"}, { "TRADESERVER", "*", "TRADESERVER_X", "GVCC"},
		{ "TRADESERVER", "*", "TRADESERVER_X", "GVCF"},	{ "TRADESERVER", "*", "TRADESERVER_X", "GVD2"},
		{ "TRADESERVER", "*", "TRADESERVER_X", "GVDC"}, { "TRADESERVER", "*", "TRADESERVER_X", "GVDF"},
	};

	private static String[][] features = new String[][] {
		{"Trade", "Trade Parent Feature Description"},
		{"Trade/Edit", "Trade/Edit Feature Description"},
		{"Trade/Process", "Trade/Process Feature Description"},
		{"Trade/OnBehalfOf", "Trade/OnBehalfOf Feature Description"},
		{"View", "View Feature Description"},
		{"Query", "Query Feature Description"}
	};

	/********************************************************
	 * BEGIN: THIS IS CONSTANT CODE FOR PSH Bus Interaction *
	 ********************************************************/
	private static Mkv mkvApplication;

	public static void main(String[] args) {
		try {
			MkvQoS qos = new MkvQoS();
	    	    	qos.setArgs(args);
			qos.setComponentVersion(1,0,0,0);
			qos.setAutoRegister(true);
			qos.setPlatformListeners(new MkvPlatformListener[] { new PlatformListener() });

            		mkvApplication = Mkv.start(qos);
		} catch(Throwable e) { e.printStackTrace(); }
	}

	static class PlatformListener implements MkvPlatformListener
	{
		@Override
		public void onComponent(MkvComponent comp, boolean flag) {}
		@Override
		public void onConnect(String comp, boolean flag) {}

		@Override
		public void onMain(MkvPlatformEvent event) {
	        	switch(event.intValue()) {
	        		case MkvPlatformEvent.REGISTER_code:
	        			publishIntrospectiveFunction();
	        			publishResourceHierachyChain();
	        			publishFeatureChain();
	        		break;
		        }
		}
	}

	static class FunctionHandler implements MkvFunctionListener
	{
		public void onCall(MkvFunctionCallEvent mkvFunctionCallEvent) {
		    try { mkvFunctionCallEvent.setResult(MkvSupplyFactory.create("0:" + xmlFile)); }
		    catch(Throwable e) {
				e.printStackTrace();
				try { mkvFunctionCallEvent.setError((byte)-1, "Unexpected exception {" + e + "}"); }
				catch(Throwable ee) { ee.printStackTrace(); }
		    }
		}
	}

	private static void publishIntrospectiveFunction() {
		try {
		    new MkvFunction(
				SOURCE + "_getByFeatureConfiguration",
				MkvFieldType.STR, new String[] {}, new MkvFieldType[] {},
				"Get the ByFeature Configuration XML for this component",
				new FunctionHandler())
		    .publish();
		} catch(Throwable e) { e.printStackTrace(); }
	}

	private static void publishResourceHierachyChain() {
		try {
		    MkvType type = new MkvType(SOURCE + "_HierarchyType",
				new String[] { "Level1", "Level2", "Level3", "Level4" },
				new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.STR, MkvFieldType.STR });
		    type.publish();

		    MkvChain chain = new MkvChain("ANY.METADATA." + SOURCE + ".RESOURCES", type.getName());
		    chain.publish();

		    for(int i=0; i<resources.length; ++i) {
			MkvRecord rec = new MkvRecord("ANY.METADATA." + SOURCE + ".RESOURCE_" + i, type.getName());
			rec.publish();
			rec.supply(MkvSupplyFactory.create(resources[i]));
			chain.add(rec.getName());
		    }
		} catch(Throwable e) { e.printStackTrace(); }
	}

	private static void publishFeatureChain() {
		try {
		    MkvType type = new MkvType(SOURCE + "_ResourceType",
				new String[] { "FeatureName", "FeatureDesc" },
				new MkvFieldType[] { MkvFieldType.STR, MkvFieldType.STR });
		    type.publish();

		    MkvChain chain = new MkvChain("ANY.METADATA." + SOURCE + ".FEATURES", type.getName());
		    chain.publish();

		    for(int i=0; i<features.length; ++i) {
			MkvRecord rec = new MkvRecord("ANY.METADATA." + SOURCE + ".FEATURE_" + i, type.getName());
			rec.publish();
			rec.supply(MkvSupplyFactory.create(features[i]));
			chain.add(rec.getName());
		    }
		} catch(Throwable e) { e.printStackTrace(); }
	}
	/********************************************************
	 * END:   THIS IS CONSTANT CODE FOR PSH Bus Interaction *
	 ********************************************************/
}
