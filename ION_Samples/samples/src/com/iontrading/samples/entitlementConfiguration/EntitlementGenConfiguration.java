package com.iontrading.samples.entitlementConfiguration;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvFunction;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionCallListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.qos.MkvQoS;

public class EntitlementGenConfiguration
{
	/* These are the configuration interfaces published by the ES */
	private static String getEntitlementServerAddFunctionName(String source) { return source + ".TableAdd"; }
	private static String getEntitlementServerRwtFunctionName(String source) { return source + ".TableRwt"; }
	private static String getEntitlementServerSetFunctionName(String source) { return source + ".TableSet"; }
	private static String getEntitlementServerDelFunctionName(String source) { return source + ".TableDel"; }

	/* This must be a unique rule id. SysAdmin uses a timestamp plus a sequential number */
	private static String getUniqueRuleId(Mkv mkv) { return mkv.getProperties().getComponentName()+"_"+System.currentTimeMillis(); }

	/* Boolean: 1 = ALLOW, 0 = DENY */
	private enum Permission { DENY, ALLOW };

	/* 0 = User, 1 = Group, 2 = Profile */
	private enum EntityType { USER, GROUP, PROFILE };

	private static class PublishListener implements MkvPublishListener
	{
		private static final String USER_CONF_NAME = "user";
		private static final String ES_SOURCE_CONF_NAME = "es_source";

		private static final String WSS_NAMESPACE = "WSS_SUITE";
		private static final String WSS_RESOURCE = "BookABC";
		private static final String WSS_FEATURE = "Trade/Edit";

		private final Mkv mkv;
		private final String esSource;

		private MkvFunction addFunction = null;
		private MkvFunction rwtFunction = null;
		private MkvFunction setFunction = null;
		private MkvFunction delFunction = null;

		private boolean serverIsAvailable = false;

		PublishListener(Mkv mkv)
		{
			this.mkv = mkv;
			this.esSource = mkv.getProperties().getProperty(ES_SOURCE_CONF_NAME);
		}

		public void onPublish(MkvObject object, boolean start, boolean dwl) {}
		public void onSubscribe(MkvObject obj) {}

		public void onPublishIdle(String component, boolean start)
		{
			/* Check if the ES is ready on PublishIdle */
			addFunction = mkv.getPublishManager().getMkvFunction(getEntitlementServerAddFunctionName(esSource));
			rwtFunction = mkv.getPublishManager().getMkvFunction(getEntitlementServerRwtFunctionName(esSource));
			setFunction = mkv.getPublishManager().getMkvFunction(getEntitlementServerSetFunctionName(esSource));
			delFunction = mkv.getPublishManager().getMkvFunction(getEntitlementServerDelFunctionName(esSource));

			/* Here the server is ready to accept configuration requests */
			serverIsAvailable = addFunction != null && rwtFunction != null && setFunction != null && delFunction != null;
			if(serverIsAvailable)
			{
				String ruleId = getUniqueRuleId(mkv);
				String userName = mkv.getProperties().getProperty(USER_CONF_NAME);

				/* All parameters should be specified for ADD. The operation fails if the rules already exists or it is a duplicate. */
				Object[] addConfigurationArguments = new Object[] {
					"ES_C_GENAUTHORIZATION",                  /* The configuration table you want to change */
					"ID", ruleId,                             /* A unique rule id to add to the ES collection */
					"Namespace", WSS_NAMESPACE,               /* The Namespace you want to configure: should be same you actually use in the queries */
					"Resource", WSS_RESOURCE,                 /* A resource of the collection */
					"Feature", WSS_FEATURE,                   /* A feature of the collection */
					"Permission", Permission.DENY.ordinal(),  /* Boolean: 1 = ALLOW, 0 = DENY */
					"UserGroup", EntityType.USER.ordinal(),   /* 0 = User, 1 = Group, 2 = Profile */
					"UserGroupName", userName                 /* The user/group/profile name */
				};

				/* Only changes should be specified for RWT. The operation fails if the rule does not exist or it is a duplicate. */
				Object[] rwtConfigurationArguments = new Object[] {
					"ES_C_GENAUTHORIZATION",                  /* The configuration table you want to change */
					"ID", ruleId,                             /* A unique rule id to add to the ES collection */
					"Permission", Permission.ALLOW.ordinal()  /* Boolean: 1 = ALLOW, 0 = DENY */
				};

				/* All parameters should be specified for SET. It simulates ADD or RWT on purpose. */
				Object[] setConfigurationArguments = new Object[] {
					"ES_C_GENAUTHORIZATION",                  /* The configuration table you want to change */
					"ID", ruleId,                             /* A unique rule id to add to the ES collection */
					"Namespace", WSS_NAMESPACE,               /* The Namespace you want to configure: should be same you actually use in the queries */
					"Resource", WSS_RESOURCE,                 /* A resource of the collection */
					"Feature", WSS_FEATURE,                   /* A feature of the collection */
					"Permission", Permission.DENY.ordinal(),  /* Boolean: 1 = ALLOW, 0 = DENY */
					"UserGroup", EntityType.USER.ordinal(),   /* 0 = User, 1 = Group, 2 = Profile */
					"UserGroupName", userName                 /* The user/group/profile name */
				};

				/* Just the rule is should be specified for DEL. It fails if the rule does not exist. */
				Object[] delConfigurationArguments = new Object[] {
					"ES_C_GENAUTHORIZATION",                  /* The configuration table you want to change */
					"ID", ruleId,                             /* A unique rule id to add to the ES collection */
				};

				try
				{
					MkvFunctionCallListener listener = new MkvFunctionCallListener() {
						public void onResult(MkvFunctionCallEvent event, MkvSupply result)
						{ /* Configuration was successfully accepted and stored by ES! */ }

						public void onError(MkvFunctionCallEvent event, byte errcode, String error)
						{ throw new RuntimeException("Configuration call failed with error:" + error); }
					};

					addFunction.call(MkvSupplyFactory.create(addConfigurationArguments), listener);
					setFunction.call(MkvSupplyFactory.create(rwtConfigurationArguments), listener);
					setFunction.call(MkvSupplyFactory.create(setConfigurationArguments), listener);
					delFunction.call(MkvSupplyFactory.create(delConfigurationArguments), listener);
				} catch(Exception e) { e.printStackTrace(); }
			}
		}
	}

	public static void main(String commands[])
	{
		try
		{
			MkvQoS qos = new MkvQoS();
			qos.setArgs(commands);
			Mkv mkv = Mkv.start(qos);
			mkv.getPublishManager().addPublishListener(new PublishListener(mkv));
		}
		catch (Exception e) { e.printStackTrace(); }
	}
}
