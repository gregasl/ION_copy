ION Java API examples
---------------------

The sample suite shows the main features of the ION Java API.
The distribution contains the following directory/files:
- readme.txt			This file.
- src				Contains the java sources of the samples.
- RUN_<name of the sample>	The working dir of the samples, with the mkv.jinit file to run them on the ION Platform.
- build_sample.bat		A script file to compile the samples on a Windows machine
- build_sample.sh		A script file to compile the samples on a Linux machine


How to build the samples
------------------------
Prerequisite to build and run the samples is the availability of a Java distribution >= 1.6.
The JAVA_HOME environment variable shall be properly set to point to the insalled Java version.

- On a Windows machine: 
    Use the batch script to build the samples. 
    You can specify the 'all' option, to build all the distributed samples.
    You can specify a sample name as option, to build and run that specific sample only.

    Example:    
      build_sample.bat [all | sample_name]
      
- On a Linux machine:
    Use the bash shell script to build the samples. 
    You can specify the 'all' option, to build all the distributed samples.
    You can specify a sample name as option, to build and run that specific sample only.

    Example:    
      build_sample.sh [all | sample_name]


List of available examples
--------------------------
- PlatformListener (package com.iontrading.samples.platformListener)
	Example of MkvPlatformListener implementation.

- SimpleLog (package com.iontrading.samples.simpleLog)
	Example of MkvPlatformListener implementation tracing events in a custom log file.

- SimplePublisher (package com.iontrading.samples.simplePublisher)
	The SimplePublisher sample publishes a type, a record and fill the record with values. 
	The record can be subscribed to by a MKV client or by the SimpleSubscriber sample.

- SimplePublisher2 (package com.iontrading.samples.simplePublisher2)
	Same as SimplePublisher but using the new MkvSupplyProxy mechanism.
	The mechanism allow defining a binding between objects and records.

- SimplePublisher3 (package com.iontrading.samples.simplePublisher3)
	Same as SimplePublisher, excepts it uses a custom extended MkvRecord implementing the Subscribeable interface; 
	this way it is able to supply a field only if it has been been actually subscribed;
	the sample application can be tested against SimplePublisher2.

- SimpleTimerSupplier (package com.iontrading.samples.simpleTimerSupplier)
	The SimpleTimerSupplier is similar to the SimplePublisher sample but implements	a mechanism 
	based on the MkvTimer object to provide updates for the record at timer.

- SimpleChainPublisher (package com.iontrading.samples.simpleChainPublisher)
	The SimpleChainPublisher sample publishes a type, a set record and a chain. 
	The records are appended to the chain.
	The chain can be subscribed to through the MKV client or using the SimpleChainSusbcriber sample.

- SimplePatternPublisher (package com.iontrading.samples.simplePatternPublisher)
	The SimplePatternPublisher sample publishes a type, a set record and a pattern.
	The pattern can be subscribed to through the MKV client or using the SimplePatternSusbcriber sample.

- SimpleSubscriber (package com.iontrading.samples.simpleSubscriber)
	The SimpleSubscriber listens for publications and when it receives a record with a given name 
	it susbcribes to some fields then listen for updates on that record.
	Can be used together with the SimplePublisher or SimpleTimerSupplier.

- SimpleSubscriber2 (package com.iontrading.samples.simpleSubscriber)
	Same as SimpleSubscriber but using the new MkvSupplyProxy mechanism.
	The mechanism allow defining a binding between objects and records.

- SimpleChainSubscriber (package com.iontrading.samples.simpleChainSubscriber)
	The SimpleChainSubscriber listens for publications and when it receives a chain with a given name 
	it subscribe to it. It then listens for chain updates events 
	and on that base subscribes to the records belonging to the chain. 
	The SimpleChainSubscriber listens for the record updates as well.
	Can be used together with the SimpleChainPublisher.

- SimplePatternSubscriber (package com.iontrading.samples.simplePatternSubscriber)
	The SimplePatternSubscriber listens for publications and when it receives a pattern with a given name 
	it subscribe to it. It then listens for record updates events.
	Can be used together with the SimplePatternPublisher.

- SimplePermChainSubscriber (package com.iontrading.samples.simplePermChainSubscriber)
	The SimpleChainSubscriber listen for chain and record supply events. 
	The subscription to the chain is performed registering a permanent subscription at startup.
	The permanent subscription deals transparently with the subscription of the records that belong to the chain.
	Can be used together with the SimpleChainPublisher.

- SimpleFunctionHandler (package com.iontrading.samples.simpleFunctionHandler)
	The SimpleFunctionHandler publishes two functions: Avg and Min that respectively calculate 
	the average and the minimum of the real values.
	The functions can be called through the MKV client or using the sample SimpleFunctionCaller.

- SimpleTransactionHandler (package com.iontrading.samples.simpleTransactionHandler)
	The SimpleTransactionHandler sample publishes a record and listen for transactions on the fields. 
	It accepts the transaction that field and supplies back the value.

- SimpleFunctionCaller (package com.iontrading.samples.simpleFunctionCaller)
	The SimpleFunctionCaller exploits the function exposed by the SimpleFunctionHandler.

- SimpleTransactionCaller (package com.iontrading.samples.simpleTransactionCaller)
	The SimpleTransactionCaller is based on SimpleSubscriber: 
	it subscribes to the record published by the SimpleTransactionHandler and listen for its supplies.
	Further more this sample starts a timer with period 500ms that request for a transaction on a field.

- MessageQueuePublisher (package com.iontrading.samples.messageQueuePublisher)
	The MessageQueuePublisher publishes a function; when this function is invoked,
	it publishes some records on a dynamically created message queue whose name is returned 
	as the function result.

- MessageQueueSubscriber (package com.iontrading.samples.messageQueueSubscriber)
	The MessageQueueSubscriber is designed to work together with the MessageQueuePublisher;
	upon connecting it invokes the function published by the MessageQueuePublisher,
	thus obtaining the name for a dynamically created message queue; 
	the MessageQueueSubscriber then proceeds to subscribe the data from the message queue.

- SimpleCustomStats (package com.iontrading.samples.simpleCustomStats)
	This sample shows how to use the Custom Stats interfaces to report custom statistics.

- SimpleCMStats (package com.iontrading.samples.simpleCMStats)
	This sample shows how to use the Custom Stats interfaces to report Common Market Statistics.

- SimpleCMDrillDown (package com.iontrading.samples.simpleCMDrillDown)
	This sample shows how to use the Custom Stats interfaces to report a per-trader drill down of Common Market Statistics.

- SimpleCMProxy (package com.iontrading.samples.simpleCMProxy)
	This sample shows how to use the Custom Stats interfaces to report Common Market Statistics on behalf of MOCKs.

- SimpleVirtualHost (package com.iontrading.samples.simpleVirtualHost)
	This sample shows how to use the Custom Stats interfaces to report standard statistics for a Virtual Host.

- CommonMarketCustomStats (package com.iontrading.samples.commonMarketCustomStats)
	This sample shows how to use the Custom Stats interfaces to report custom statistics for Common Market.

- SimpleExternalAuthenticator (package com.iontrading.samples.simpleExternalAuthenticator)
	This sample shows how to implement a component behaving as an external authenticator for the platform users.
	This simple authenticator listens for login requests made by the daemon 
	and responds with a string containing the operation outcome. 

- SimpleAsyncShutdownComponent (package com.iontrading.samples.simpleAsyncShutdownComponent)
	This sample shows how to implement a component able to request an asynchronous shutdown to the daemon.
	When receiving a stop request from the daemon, the component checks its internal state,
	and in case it has any operations to perform before stopping, 
	it replies to the daemon requesting an asynchronous shutdown.

- SimpleQueryUserEntitlements (package com.iontrading.samples.advanced.simpleQueryUserEntitlements)
	This sample shows how to use the advanced query interface of the Entitlement Server. 
	The QueryUserEntitlements() is an exported function of the Entitlement Server 
	to perform multiple entitlement checks at once.
	This sample uses the API to build a proper query and communicate to the Entitlement Server.

- SimpleGenericAuditClient (package com.iontrading.samples.simpleGenericAuditClient)
	This sample shows how to implement a component able to log Audit Events to a Generic Audit Server.
	The components waits for the Generic Audit Server availability, and then sends some simple Audit Messages to the server.

- PersistentSubscription (package com.iontrading.samples.persistentSubscription)
	This sample shows how to use the Persistent Subscription interface and its lifecycle events 
	to have a declarative subscriber code.
	Persistent subscriptions are convenient because they do not require the application 
	to implement event driven behavior, thus abstracting the application business logic 
	from the technical details entailed by a distributed system.

- Recordset (package com.iontrading.samples.recordset)
	This sample shows how to publish a Recordset object. 
	A Recordset object allow to have a single object published on the ION Platform standing for 
	multiple records without the need to publish these records as well; 
	in addition to reduced usage of network and memory resources, 
	support for Recordset allows for on demand supply of data on the publisher side.
	Since the publisher component is notified of every subscription requests hitting the Recordset, 
	the mechanism allows for enforcement of entitlement and content-based entitlement on the publisher side. 

- EntitlementConfiguration (package com.iontrading.samples.entitlementConfiguration)
	This sample shows how to configure the ION Entitlement Server, programmatically.

- SimplePriceContributionCustomE2EDelay (package com.iontrading.samples.simplePriceContributionCustomE2EDelay)
	This sample shows how to use the Custom End-To-End Delay API, 
	to report custom performance statitics to the ION Daemon. 
	The reported statistic information can be visualized and navigated via the ION Performance Meter tool.

- SimpleOnBehalfOfLicense (package com.iontrading.samples.onbehalfoflicense)
	This sample shows how to use the on-behalf-of API for license management.

- XRSClient (package com.iontrading.samples.xrsClient)
	This sample shows how to interact with an XRS publisher.

- AdvConfigHandling (package com.iontrading.samples.advanced.configuration)
(ADVANCED)
	This sample exploit the component configuration features offered by the API. 
	The configuration variables supported are registered to the API and they are available
	for read or read/write to the SysAdmin tool.
	The example supports the run time change of a configuration variable (VAR5).
	Start the sample and through the SysAdmin try to change the variable VAR5 
	providing an even integer (the component will accept the change)
	and an odd integer (the component will reject the change).

- AdvOrderManagement (package com.iontrading.samples.advanced.orderManagement)
(ADVANCED)
	The sample works together with a CM compliant gateway. 
	It subscribes to the depths of the instrument configured in the MarketDef static class 
	and to the order chain.
	Depending on the MarketDef settings the component reacts to best changes 
	aggressing the prices or places a permanent order at startup.
	The order are monitored till they are no longer able to trade 
	(completely filled, deleted, etc.).
	The component exploit the permanent subscription mechanism.
	Change the MarketDef in order to access different gateways.

- AdvSTP (package com.iontrading.samples.advanced.stp)
(ADVANCED)
	This sample subscribes to the Common Market TRADE chain using a permanent subscription.
	Records values then are cached using instances of a custom java class.
	The component exploit the supply proxy mechanism.

- MQToChain (package com.iontrading.samples.advanced.mqtochain)
(ADVANCED)
	This sample illustrates how to republish records data from a queue onto a chain,
	showing how to use queue action fields to implement a conventional map to chain concepts 
	such as append, kill, etc.

- OnBehalfOfRegistration (package com.iontrading.samples.advanced.onBehalfOfRegistrationRequest)
(ADVANCED)
	This sample shows how to register a virtual component component on the platform.

- AdvTickDataApi (package com.iontrading.samples.advanced.tickDataApi)
(ADVANCED)
	This sample shows an example of usage of the Percentile API.

- AdvUltraLowLatency (package com.iontrading.samples.advanced.ultraLowLatency)
(ADVANCED)
	These samples show the configuration of Publisher and Subscriber to use 
	Ultra Low Latency message passing on same-host connections.

- AdvCNE (package com.iontrading.samples.advanced.cne)
(ADVANCED)
	This sample shows how to subscribe R4Q records from a CMI gateway 
	and process them according to a custom business logics.

- AdvBulkEntitlements (package com.iontrading.mkv.bulkentitlements)
(ADVANCED)
	This sample shows how to perform a bulk entitlement query to the ION Entitlement Server.
	Bulk query can be use to query multiple resource/feature all at once,
	in a single evaluation batch.

- AdvByFeatureReport (package com.iontrading.mkv.bulkentitlements)
(ADVANCED)
	This sample shows how to query the ION Entitlement Server to produce a comprehensive report
	containing all the rules and permissions for a give user.

- AdvChainSnapshot (package com.iontrading.samples.advanced.chainSnapshot)
(ADVANCED)
	This sample shows how to subscribe to a chain and wait for all the records to be downloaded.

- PasswordServerServiceCredentials (package com.iontrading.samples.advanced.passwordServerServiceCredentials)
(ADVANCED)
	This smple shows how to use the Daemon Password Server to retrieve service credentials.

- Benchmarks (package com.iontrading.samples.benchmarks)
	This sample includes a Publisher and a Subscriber for supply throughput benchmarking.	
	
	NOTES
	-----
	Optimal middleware performance can be obtained as a combination of 
	hardware, operating system settings, components programming and components configurations.
	For instance, after developing and deploying components, factors like CPU, memory and network performance, 
	binding of processes to cores, processes real time prioritization, setting of host power options 
	and general host load, will have a significant impact on middleware performance.
	
	The sample benchmarking components are not meant to demonstrate optimal performance, 
	but just to provide a basic set of metrics that can be compared 
	across different hardware and OS configurations.
	
	Please contact ION support if you are interested in discussing how to optimise 
	the performance of your ION components.
