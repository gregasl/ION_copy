#!/bin/bash
SAMPLE_NAME=
WORKINGDIR=
MAINCLASS=
MAINCLASS2=
MAINCLASS3=
SRC=

function sample_build {
	echo Building [$SAMPLE_NAME] sample
	echo     \> WORKINGDIR=RUN_$WORKINGDIR
	echo     \> MAINCLASS=$MAINCLASS

	if [ ! -d build ] 
	then
		\mkdir build
  	fi
  	
	if [ ! -d build/classes ] 
	then
		\mkdir build/classes
  	fi

	echo "javac -cp ../jmkv*.jar -d build/classes $SRC/*.java"
	javac -cp ../jmkv*.jar -d build/classes $SRC/*.java
	echo
	echo
}

function sample_run {
	echo Running [$SAMPLE_NAME] sample
	cd RUN_$WORKINGDIR
	echo "java -cp ../../*:../build/classes $MAINCLASS"
	java -cp ../../*:../build/classes $MAINCLASS &
	
	if [ ! $MAINCLASS2 = "" ]
	then
		echo "java -cp ../../*:../build/classes $MAINCLASS2 -init mkv2.jinit"
		java -cp ../../*:../build/classes $MAINCLASS2 -init mkv2.jinit &
	fi
	
	if [ ! $MAINCLASS3 = "" ]
	then
		echo "java -cp ../../*:../build/classes $MAINCLASS3 -init mkv3.jinit"
		java -cp ../../*:../build/classes $MAINCLASS3 -init mkv3.jinit &
	fi
	
	cd ..
}

function sample_help {
	echo
	echo ---------------------------------------------------------------------------------------------
	echo ION Java API Samples
	echo ---------------------------------------------------------------------------------------------
	echo "PlatformListener:                      Example of MkvPlatformListener implementation"
	echo 
	echo "SimpleLog:                             Example of MkvPlatformListener implementation tracing events in a custom log file"
	echo 
	echo "SimplePublisher:                       The SimplePublisher sample publishes a type, a record and fill the record with values"
	echo 
	echo "SimplePublisher2:                      Same as SimplePublisher but using the new MkvSupplyProxy mechanism"
	echo 
	echo "SimplePublisher3:                      Same as SimplePublisher, excepts it uses a custom extended MkvRecord implementing the Subscribeable interface"
	echo
	echo "SimpleTimerSupplier:                   The SimpleTimerSupplier is similar to the SimplePublisher sample but implements a mechanism based on the MkvTimer"
	echo 
	echo "SimpleChainPublisher:                  The SimpleChainPublisher sample publishes a type, a set record and a chain"
	echo 
	echo "SimplePatternPublisher:                The SimplePatternPublisher sample publishes a type, a set record and a pattern"
	echo 
	echo "SimpleSubscriber:                      The SimpleSubscriber listens for publications and when it receives a record with a given name it susbcribes to it"
	echo 
	echo "SimpleSubscriber2:                     Same as SimpleSubscriber but using the new MkvSupplyProxy mechanism"
	echo 
	echo "SimpleChainSubscriber:                 The SimpleChainSubscriber listens for publications and when it receives a chain with a given name it subscribe to it"
	echo
	echo "SimplePatternSubscriber:               The SimplePatternSubscriber listens for publications and when it receives a pattern with a given name it subscribe to it"
	echo 
	echo "SimplePermChainSubscriber:             The subscription to the chain is performed registering a permanent subscription at startup"
	echo 
	echo "SimpleFunctionHandler:                 The SimpleFunctionHandler publishes two functions: Avg and Min"
	echo 
	echo "SimpleTransactionHandler:              The SimpleTransactionHandler sample publishes a record and listen for transactions on the fields"
	echo 
	echo "SimpleFunctionCaller:                  The SimpleFunctionCaller exploits the function exposed by the SimpleFunctionHandler"
	echo 
	echo "SimpleTransactionCaller:               This sample starts a timer with period 500ms that request for a transaction on a field"
	echo 
	echo "MessageQueuePublisher:                 The MessageQueuePublisher publishes a function; when this function is invoked, it publishes some records on a dynamically created message queue"
	echo 
	echo "MessageQueueSubscriber:                It invokes the function published by the MessageQueuePublisher, then proceeds to subscribe the data from the message queue"
	echo 
	echo "SimpleCustomStats:                     This sample shows how to use the Custom Stats interfaces to report custom statistics"
	echo
	echo "SimpleCMStats:                         This sample shows how to use the Custom Stats interfaces to report Common Market Statistics"
	echo
	echo "SimpleCMDrillDown:                     This sample shows how to use the Custom Stats interfaces to report a per-trader drill down of Common Market Statistics"
	echo
	echo "SimpleCMProxy:                         This sample shows how to use the Custom Stats interfaces to report Common Market Statistics on behalf of MOCKs"
	echo
	echo "SimpleVirtualHost:                     This sample shows how to use the Custom Stats interfaces to report standard statistics for a Virtual Host"
	echo
	echo "CommonMarketCustomStats:               This sample shows how to use the Custom Stats interfaces to report custom statistics for Common Market"
	echo
	echo "SimpleExternalAuthenticator:           This sample shows how to implement a component behaving as an external authenticator for the platform users"
	echo
	echo "SimpleAsyncShutdownComponent:          This sample shows how to implement a component able to request an asynchronous shutdown to the daemon"
	echo
	echo "SimpleQueryUserEntitlements:           This sample shows how to use the advanced query interface of the Entitlement Server"
	echo
	echo "SimpleGenericAuditClient:              This sample shows how to implement a component able to log Audit Events to a Generic Audit Server"
	echo
	echo "PersistentSubscription:                This sample shows how to use the Persistent Subscription interface and its lifecycle events"
	echo
	echo "Recordset:                             This sample shows how to publish a Recordset object"
	echo
	echo "EntitlementConfiguration:              This sample shows how to configure the ION Entitlement Server, programmatically"
	echo
	echo "SimplePriceContributionCustomE2EDelay: This sample shows how to use the Custom End-To-End Delay API, to report custom performance statitics to the ION Daemon"
	echo
	echo "SimpleOnBehalfOfLicense:               This sample shows how to use the on-behalf-of API for license management"
	echo
	echo "XRSClient:                             This sample shows how to interact with an XRS publisher"
	echo
	echo "AdvConfigHandling:                     This sample exploit the component configuration features offered by the API"
	echo 
	echo "AdvOrderManagement:                    The sample works together with a CM compliant gateway"
	echo 
	echo "AdvSTP:                                This sample subscribes to the Common Market TRADE chain using a permanent subscription"
	echo 
	echo "AdvMQToChain:                          This sample illustrates how to republish records data from a queue onto a chain"
	echo 
	echo "AdvOnBehalfOfRegistration:             This sample shows how to register a virtual component component on the platform"
	echo 
	echo "AdvTickDataApi:                        This sample shows an example of usage of the Percentile API"
	echo
	echo "AdvUltraLowLatency:                    These samples show the configuration of Publisher and Subscriber to use Ultra Low Latency message passing on same-host connections"
	echo
	echo "AdvCNE:                                This sample shows how to subscribe R4Q records from a CMI gateway"
	echo 
	echo "AdvBulkEntitlements:                   This sample shows how to perform a bulk entitlement query to the ION Entitlement Server"
	echo
	echo "AdvByFeatureReport:                    This sample shows how to query the ION Entitlement Server to produce a comprehensive report"
	echo
	echo "AdvChainSnapshot:                      This sample shows how to subscribe to a chain and wait for all the records to be downloaded"
	echo 
	echo "PasswordServerServiceCredentials:      This smple shows how to use the Daemon Password Server to retrieve service credentials"
	echo 
	echo "Benchmarks:                            This sample includes a Publisher and a Subscriber for supply throughput benchmarking"
	echo 
	echo "SimpleOnBehalfOfRegistration:          This sample shows how to register a virtual component on the platform using the on-behalf-of registration"
	echo 
	echo ---------------------------------------------------------------------------------------------
	echo 
	echo 
	echo "Usage: build_sample.bat [all | sample_name]"
	echo 
	echo 
}

if [ "$1" = "PlatformListener" -o "$1" = "all" ]
then
	SAMPLE_NAME=PlatformListener
	WORKINGDIR=PlatformListener
	MAINCLASS=com.iontrading.samples.platformListener.PlatformListener
	SRC=src/com/iontrading/samples/platformListener
	sample_build
fi

if [ "$1" = "SimpleLog" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleLog
	WORKINGDIR=SimpleLog
	MAINCLASS=com.iontrading.samples.simpleLog.SimpleLog
	SRC=src/com/iontrading/samples/simpleLog
	sample_build
fi

if [ "$1" = "SimplePublisher" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimplePublisher
	WORKINGDIR=SimplePublisher
	MAINCLASS=com.iontrading.samples.simplePublisher.Publisher
	SRC=src/com/iontrading/samples/simplePublisher
	sample_build
fi

if [ "$1" = "SimplePublisher2" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimplePublisher2
	WORKINGDIR=SimplePublisher2
	MAINCLASS=com.iontrading.samples.simplePublisher2.Publisher
	SRC=src/com/iontrading/samples/simplePublisher2
	sample_build
fi

if [ "$1" = "SimplePublisher3" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimplePublisher3
	WORKINGDIR=SimplePublisher3
	MAINCLASS=com.iontrading.samples.simplePublisher3.Publisher
	SRC=src/com/iontrading/samples/simplePublisher3
	sample_build
fi

if [ "$1" = "SimpleTimerSupplier" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleTimerSupplier
	WORKINGDIR=SimpleTimerSupplier
	MAINCLASS=com.iontrading.samples.simpleTimerSupplier.TimerPublisher
	SRC=src/com/iontrading/samples/simpleTimerSupplier
	sample_build
fi

if [ "$1" = "SimpleChainPublisher" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleChainPublisher
	WORKINGDIR=SimpleChainPublisher
	MAINCLASS=com.iontrading.samples.simpleChainPublisher.ChainPublisher
	SRC=src/com/iontrading/samples/simpleChainPublisher
	sample_build
fi

if [ "$1" = "SimplePatternPublisher" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimplePatternPublisher
	WORKINGDIR=SimplePatternPublisher
	MAINCLASS=com.iontrading.samples.simplePatternPublisher.Publisher
	SRC=src/com/iontrading/samples/simplePatternPublisher
	sample_build
fi

if [ "$1" = "SimpleSubscriber" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleSubscriber
	WORKINGDIR=SimpleSubscriber
	MAINCLASS=com.iontrading.samples.simpleSubscriber.Subscriber
	SRC=src/com/iontrading/samples/simpleSubscriber
	sample_build
fi

if [ "$1" = "SimpleSubscriber2" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleSubscriber2
	WORKINGDIR=SimpleSubscriber2
	MAINCLASS=com.iontrading.samples.simpleSubscriber2.Subscriber
	SRC=src/com/iontrading/samples/simpleSubscriber2
	sample_build
fi

if [ "$1" = "SimpleChainSubscriber" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleChainSubscriber
	WORKINGDIR=SimpleChainSubscriber
	MAINCLASS=com.iontrading.samples.simpleChainSubscriber.ChainSubscriber
	SRC=src/com/iontrading/samples/simpleChainSubscriber
	sample_build
fi

if [ "$1" = "SimplePatternSubscriber" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimplePatternSubscriber
	WORKINGDIR=SimplePatternSubscriber
	MAINCLASS=com.iontrading.samples.simplePatternSubscriber.Subscriber
	SRC=src/com/iontrading/samples/simplePatternSubscriber
	sample_build
fi

if [ "$1" = "SimplePermChainSubscriber" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimplePermChainSubscriber
	WORKINGDIR=SimplePermChainSubscriber
	MAINCLASS=com.iontrading.samples.simplePermChainSubscriber.ChainSubscriber
	SRC=src/com/iontrading/samples/simplePermChainSubscriber
	sample_build
fi

if [ "$1" = "SimpleFunctionHandler" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleFunctionHandler
	WORKINGDIR=SimpleFunctionHandler
	MAINCLASS=com.iontrading.samples.simpleFunctionHandler.FunctionHandler
	SRC=src/com/iontrading/samples/simpleFunctionHandler
	sample_build
fi

if [ "$1" = "SimpleTransactionHandler" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleTransactionHandler
	WORKINGDIR=SimpleTransactionHandler
	MAINCLASS=com.iontrading.samples.simpleTransactionHandler.TransactionHandler
	SRC=src/com/iontrading/samples/simpleTransactionHandler
	sample_build
fi

if [ "$1" = "SimpleFunctionCaller" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleFunctionCaller
	WORKINGDIR=SimpleFunctionCaller
	MAINCLASS=com.iontrading.samples.simpleFunctionCaller.FunctionCaller
	SRC=src/com/iontrading/samples/simpleFunctionCaller
	sample_build
fi

if [ "$1" = "SimpleTransactionCaller" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleTransactionCaller
	WORKINGDIR=SimpleTransactionCaller
	MAINCLASS=com.iontrading.samples.simpleTransactionCaller.TransactionCaller
	SRC=src/com/iontrading/samples/simpleTransactionCaller
	sample_build
fi

if [ "$1" = "MessageQueuePublisher" -o "$1" = "all" ]
then
	SAMPLE_NAME=MessageQueuePublisher
	WORKINGDIR=MessageQueuePublisher
	MAINCLASS=com.iontrading.samples.messageQueuePublisher.MessageQueuePublisher
	SRC=src/com/iontrading/samples/messageQueuePublisher
	sample_build
fi

if [ "$1" = "MessageQueueSubscriber" -o "$1" = "all" ]
then
	SAMPLE_NAME=MessageQueueSubscriber
	WORKINGDIR=MessageQueueSubscriber
	MAINCLASS=com.iontrading.samples.messageQueueSubscriber.MessageQueueSubscriber
	SRC=src/com/iontrading/samples/messageQueueSubscriber
	sample_build
fi

if [ "$1" = "SimpleCustomStats" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleCustomStats
	WORKINGDIR=SimpleCustomStats
	MAINCLASS=com.iontrading.samples.simpleCustomStats.CustomStats
	SRC=src/com/iontrading/samples/simpleCustomStats
	sample_build
fi

if [ "$1" = "SimpleCMStats" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleCMStats
	WORKINGDIR=SimpleCMStats
	MAINCLASS=com.iontrading.samples.simpleCMStats.CustomStats
	SRC=src/com/iontrading/samples/simpleCMStats
	sample_build
fi

if [ "$1" = "SimpleCMDrillDown" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleCMDrillDown
	WORKINGDIR=SimpleCMDrillDown
	MAINCLASS=com.iontrading.samples.simpleCMDrillDown.CustomStats
	SRC=src/com/iontrading/samples/simpleCMDrillDown
	sample_build
fi

if [ "$1" = "SimpleCMProxy" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleCMProxy
	WORKINGDIR=SimpleCMProxy
	MAINCLASS=com.iontrading.samples.simpleCMProxy.CustomStats
	SRC=src/com/iontrading/samples/simpleCMProxy
	sample_build
fi

if [ "$1" = "SimpleVirtualHost" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleVirtualHost
	WORKINGDIR=SimpleVirtualHost
	MAINCLASS=com.iontrading.samples.simpleVirtualHost.CustomStats
	SRC=src/com/iontrading/samples/simpleVirtualHost
	sample_build
fi

if [ "$1" = "CommonMarketCustomStats" -o "$1" = "all" ]
then
	SAMPLE_NAME=CommonMarketCustomStats
	WORKINGDIR=CommonMarketCustomStats
	MAINCLASS=com.iontrading.samples.commonMarketCustomStats.CustomStats
	SRC=src/com/iontrading/samples/commonMarketCustomStats
	sample_build
fi

if [ "$1" = "SimpleExternalAuthenticator" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleExternalAuthenticator
	WORKINGDIR=SimpleExternalAuthenticator
	MAINCLASS=com.iontrading.samples.simpleExternalAuthenticator.ExternalAuthenticator
	SRC=src/com/iontrading/samples/simpleExternalAuthenticator
	sample_build
fi

if [ "$1" = "SimpleAsyncShutdownComponent" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleAsyncShutdownComponent
	WORKINGDIR=SimpleAsyncShutdownComponent
	MAINCLASS=com.iontrading.samples.simpleAsyncShutdownComponent.AsyncShutdownComponent
	SRC=src/com/iontrading/samples/simpleAsyncShutdownComponent
	sample_build
fi

if [ "$1" = "SimpleQueryUserEntitlements" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleQueryUserEntitlements
	WORKINGDIR=SimpleQueryUserEntitlements
	MAINCLASS=com.iontrading.samples.simpleQueryUserEntitlements.QueryUserEntitlements
	SRC=src/com/iontrading/samples/simpleQueryUserEntitlements
	sample_build
fi

if [ "$1" = "SimpleGenericAuditClient" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleGenericAuditClient
	WORKINGDIR=SimpleGenericAuditClient
	MAINCLASS=com.iontrading.samples.simpleGenericAuditClient.SimpleGenericAuditClient
	SRC=src/com/iontrading/samples/simpleGenericAuditClient
	sample_build
fi

if [ "$1" = "PersistentSubscription" -o "$1" = "all" ]
then
	SAMPLE_NAME=PersistentSubscription
	WORKINGDIR=PersistentSubscription
	MAINCLASS=com.iontrading.samples.persistentSubscription.PersistentSubscription
	SRC=src/com/iontrading/samples/persistentSubscription
	sample_build
fi

if [ "$1" = "Recordset" -o "$1" = "all" ]
then
	SAMPLE_NAME=Recordset
	WORKINGDIR=Recordset
	MAINCLASS=com.iontrading.samples.recordset.Recordset
	SRC=src/com/iontrading/samples/recordset
	sample_build
fi

if [ "$1" = "EntitlementConfiguration" -o "$1" = "all" ]
then
	SAMPLE_NAME=EntitlementConfiguration
	WORKINGDIR=EntitlementConfiguration
	MAINCLASS=com.iontrading.samples.entitlementConfiguration.EntitlementConfiguration
	MAINCLASS2=com.iontrading.samples.entitlementConfiguration.EntitlementGenConfiguration
	SRC=src/com/iontrading/samples/entitlementConfiguration
	sample_build
fi

if [ "$1" = "SimplePriceContributionCustomE2EDelay" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimplePriceContributionCustomE2EDelay
	WORKINGDIR=SimplePriceContributionCustomE2EDelay
	MAINCLASS=com.iontrading.samples.simplePriceContributionCustomE2EDelay.PriceGenerator
	MAINCLASS2=com.iontrading.samples.simplePriceContributionCustomE2EDelay.PriceProducer
	MAINCLASS3=com.iontrading.samples.simplePriceContributionCustomE2EDelay.PriceConsumer
	SRC=src/com/iontrading/samples/simplePriceContributionCustomE2EDelay
	sample_build
fi

if [ "$1" = "SimpleOnBehalfOfLicense" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleOnBehalfOfLicense
	WORKINGDIR=SimpleOnBehalfOfLicense
	MAINCLASS=com.iontrading.samples.onbehalfoflicense.OnBehalfOfLicense
	SRC=src/com/iontrading/samples/onbehalfoflicense
	sample_build
fi

if [ "$1" = "XRSClient" -o "$1" = "all" ]
then
	SAMPLE_NAME=XRSClient
	WORKINGDIR=XRSClient
	MAINCLASS=com.iontrading.samples.xrsClient.XRSClient
	SRC=src/com/iontrading/samples/xrsClient
	sample_build
fi

if [ "$1" = "AdvConfigHandling" -o "$1" = "all" ]
then
	SAMPLE_NAME=AdvConfigHandling
	WORKINGDIR=AdvConfigHandling
	MAINCLASS=com.iontrading.samples.advanced.configuration.PropertyRegistration
	SRC=src/com/iontrading/samples/advanced/configuration
	sample_build
fi

if [ "$1" = "AdvOrderManagement" -o "$1" = "all" ]
then
	SAMPLE_NAME=AdvOrderManagement
	WORKINGDIR=AdvOrderManagement
	MAINCLASS=com.iontrading.samples.advanced.orderManagement.OrderManagement
	SRC=src/com/iontrading/samples/advanced/orderManagement
	sample_build
fi

if [ "$1" = "AdvSTP" -o "$1" = "all" ]
then
	SAMPLE_NAME=AdvSTP
	WORKINGDIR=AdvSTP
	MAINCLASS=com.iontrading.samples.advanced.stp.TradeChainSubscriber
	SRC=src/com/iontrading/samples/advanced/stp
	sample_build
fi

if [ "$1" = "AdvMQToChain" -o "$1" = "all" ]
then
	SAMPLE_NAME=AdvMQToChain
	WORKINGDIR=AdvMQToChain
	MAINCLASS=com.iontrading.samples.advanced.mqtochain.MQToChain
	SRC=src/com/iontrading/samples/advanced/mqtochain
	sample_build
fi

if [ "$1" = "AdvOnBehalfOfRegistration" -o "$1" = "all" ]
then
	SAMPLE_NAME=AdvOnBehalfOfRegistration
	WORKINGDIR=AdvOnBehalfOfRegistration
	MAINCLASS=com.iontrading.samples.advanced.onBehalfOfRegistration.OnBehalfOfRegistration
	SRC=src/com/iontrading/samples/advanced/onBehalfOfRegistration
	sample_build
fi

if [ "$1" = "AdvTickDataApi" -o "$1" = "all" ]
then
	SAMPLE_NAME=AdvTickDataApi
	WORKINGDIR=AdvTickDataApi
	MAINCLASS=com.iontrading.samples.advanced.tickDataApi.MkvTickDataApi
	SRC=src/com/iontrading/samples/advanced/tickDataApi
	sample_build
fi

if [ "$1" = "AdvUltraLowLatency" -o "$1" = "all" ]
then
	SAMPLE_NAME=AdvUltraLowLatency
	WORKINGDIR=AdvUltraLowLatency
	MAINCLASS=com.iontrading.samples.advanced.ultraLowLatency.PubSubPublisher
	MAINCLASS2=com.iontrading.samples.advanced.ultraLowLatency.PubSubSubscriber
	SRC=src/com/iontrading/samples/advanced/ultraLowLatency
	sample_build
fi

if [ "$1" = "AdvCNE" -o "$1" = "all" ]
then
	SAMPLE_NAME=AdvCNE
	WORKINGDIR=AdvCNE
	MAINCLASS=com.iontrading.samples.advanced.cne.CNESample
	SRC=src/com/iontrading/samples/advanced/cne
	sample_build
fi

if [ "$1" = "AdvBulkEntitlements" -o "$1" = "all" ]
then
	SAMPLE_NAME=AdvBulkEntitlements
	WORKINGDIR=AdvBulkEntitlements
	MAINCLASS=com.iontrading.samples.advanced.bulkentitlements.BulkEntitlements
	SRC=src/com/iontrading/samples/advanced/bulkentitlements
	sample_build
fi

if [ "$1" = "AdvByFeatureReport" -o "$1" = "all" ]
then
	SAMPLE_NAME=AdvByFeatureReport
	WORKINGDIR=AdvByFeatureReport
	MAINCLASS=com.iontrading.samples.advanced.byfeaturereport.ByFeatureReport
	SRC=src/com/iontrading/samples/advanced/byfeaturereport
	sample_build
fi

if [ "$1" = "AdvChainSnapshot" -o "$1" = "all" ]
then
	SAMPLE_NAME=AdvChainSnapshot
	WORKINGDIR=AdvChainSnapshot
	MAINCLASS=com.iontrading.samples.advanced.chainSnapshot.WaitChainSnapshot
	SRC=src/com/iontrading/samples/advanced/chainSnapshot
	sample_build
fi

if [ "$1" = "Benchmarks" -o "$1" = "all" ]
then
	SAMPLE_NAME=Benchmarks
	WORKINGDIR=Benchmarks
	MAINCLASS=com.iontrading.samples.benchmarks.PubSubPublisher
	MAINCLASS2=com.iontrading.samples.benchmarks.PubSubSubscriber
	SRC=src/com/iontrading/samples/benchmarks
	sample_build
fi

if [ "$1" = "SimpleOnBehalfOfRegistration" -o "$1" = "all" ]
then
	SAMPLE_NAME=SimpleOnBehalfOfRegistration
	WORKINGDIR=SimpleOnBehalfOfRegistration
	MAINCLASS=com.iontrading.samples.simpleOnBehalfOfRegistration.SimpleOnBehalfOfRegistration
	SRC=src/com/iontrading/samples/simpleOnBehalfOfRegistration
	sample_build
fi

if [ "$1" = "PasswordServerServiceCredentials" -o "$1" = "all" ]
then
	SAMPLE_NAME=PasswordServerServiceCredentials
	WORKINGDIR=PasswordServerServiceCredentials
	MAINCLASS=com.iontrading.samples.advanced.passwordServerServiceCredentials.PasswordServerServiceCredentials
	SRC=src/com/iontrading/samples/advanced/passwordServerServiceCredentials
	sample_build
fi

if [ "$WORKINGDIR" = "" ]
then
	sample_help
	exit 1
fi

if [ ! "$1" = "all" ]
then
	sample_run
fi
