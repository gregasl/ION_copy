@echo off
set SAMPLE_NAME=
set WORKINGDIR=
set MAINCLASS=
set MAINCLASS2=
set MAINCLASS3=
set SRC=

:1
if "%1"=="PlatformListener" goto PlatformListener
if "%1"=="all" goto PlatformListener
goto 2
:PlatformListener
	set SAMPLE_NAME=PlatformListener
	set WORKINGDIR=PlatformListener
	set MAINCLASS=com.iontrading.samples.platformListener.PlatformListener
	set SRC=src\com\iontrading\samples\platformListener
	call :sample_build

:2
if "%1"=="SimpleLog" goto SimpleLog
if "%1"=="all" goto SimpleLog
goto 3
:SimpleLog
	set SAMPLE_NAME=SimpleLog
	set WORKINGDIR=SimpleLog
	set MAINCLASS=com.iontrading.samples.simpleLog.SimpleLog
	set SRC=src\com\iontrading\samples\simpleLog
	call :sample_build

:3
if "%1"=="SimplePublisher" goto SimplePublisher
if "%1"=="all" goto SimplePublisher
goto 4
:SimplePublisher
	set SAMPLE_NAME=SimplePublisher
	set WORKINGDIR=SimplePublisher
	set MAINCLASS=com.iontrading.samples.simplePublisher.Publisher
	set SRC=src\com\iontrading\samples\simplePublisher
	call :sample_build

:4
if "%1"=="SimplePublisher2" goto SimplePublisher2
if "%1"=="all" goto SimplePublisher2
goto 5
:SimplePublisher2
	set SAMPLE_NAME=SimplePublisher2
	set WORKINGDIR=SimplePublisher2
	set MAINCLASS=com.iontrading.samples.simplePublisher2.Publisher
	set SRC=src\com\iontrading\samples\simplePublisher2
	call :sample_build

:5
if "%1"=="SimplePublisher3" goto SimplePublisher3
if "%1"=="all" goto SimplePublisher3
goto 6
:SimplePublisher3
	set SAMPLE_NAME=SimplePublisher3
	set WORKINGDIR=SimplePublisher3
	set MAINCLASS=com.iontrading.samples.simplePublisher3.Publisher
	set SRC=src\com\iontrading\samples\simplePublisher3
	call :sample_build

:6
if "%1"=="SimpleTimerSupplier" goto SimpleTimerSupplier
if "%1"=="all" goto SimpleTimerSupplier
goto 7
:SimpleTimerSupplier
	set SAMPLE_NAME=SimpleTimerSupplier
	set WORKINGDIR=SimpleTimerSupplier
	set MAINCLASS=com.iontrading.samples.simpleTimerSupplier.TimerPublisher
	set SRC=src\com\iontrading\samples\simpleTimerSupplier
	call :sample_build

:7
if "%1"=="SimpleChainPublisher" goto SimpleChainPublisher
if "%1"=="all" goto SimpleChainPublisher
goto 8
:SimpleChainPublisher
	set SAMPLE_NAME=SimpleChainPublisher
	set WORKINGDIR=SimpleChainPublisher
	set MAINCLASS=com.iontrading.samples.simpleChainPublisher.ChainPublisher
	set SRC=src\com\iontrading\samples\simpleChainPublisher
	call :sample_build

:8
if "%1"=="SimplePatternPublisher" goto SimplePatternPublisher
if "%1"=="all" goto SimplePatternPublisher
goto 9
:SimplePatternPublisher
	set SAMPLE_NAME=SimplePatternPublisher
	set WORKINGDIR=SimplePatternPublisher
	set MAINCLASS=com.iontrading.samples.simplePatternPublisher.Publisher
	set SRC=src\com\iontrading\samples\simplePatternPublisher
	call :sample_build

:9
if "%1"=="SimpleSubscriber" goto SimpleSubscriber
if "%1"=="all" goto SimpleSubscriber
goto 10
:SimpleSubscriber
	set SAMPLE_NAME=SimpleSubscriber
	set WORKINGDIR=SimpleSubscriber
	set MAINCLASS=com.iontrading.samples.simpleSubscriber.Subscriber
	set SRC=src\com\iontrading\samples\simpleSubscriber
	call :sample_build

:10
if "%1"=="SimpleSubscriber2" goto SimpleSubscriber2
if "%1"=="all" goto SimpleSubscriber2
goto 11
:SimpleSubscriber2
	set SAMPLE_NAME=SimpleSubscriber2
	set WORKINGDIR=SimpleSubscriber2
	set MAINCLASS=com.iontrading.samples.simpleSubscriber2.Subscriber
	set SRC=src\com\iontrading\samples\simpleSubscriber2
	call :sample_build

:11
if "%1"=="SimpleChainSubscriber" goto SimpleChainSubscriber
if "%1"=="all" goto SimpleChainSubscriber
goto 12
:SimpleChainSubscriber
	set SAMPLE_NAME=SimpleChainSubscriber
	set WORKINGDIR=SimpleChainSubscriber
	set MAINCLASS=com.iontrading.samples.simpleChainSubscriber.ChainSubscriber
	set SRC=src\com\iontrading\samples\simpleChainSubscriber
	call :sample_build

:12
if "%1"=="SimplePatternSubscriber" goto SimplePatternSubscriber
if "%1"=="all" goto SimplePatternSubscriber
goto 13
:SimplePatternSubscriber
	set SAMPLE_NAME=SimplePatternSubscriber
	set WORKINGDIR=SimplePatternSubscriber
	set MAINCLASS=com.iontrading.samples.simplePatternSubscriber.Subscriber
	set SRC=src\com\iontrading\samples\simplePatternSubscriber
	call :sample_build

:13
if "%1"=="SimplePermChainSubscriber" goto SimplePermChainSubscriber
if "%1"=="all" goto SimplePermChainSubscriber
goto 14
:SimplePermChainSubscriber
	set SAMPLE_NAME=SimplePermChainSubscriber
	set WORKINGDIR=SimplePermChainSubscriber
	set MAINCLASS=com.iontrading.samples.simplePermChainSubscriber.ChainSubscriber
	set SRC=src\com\iontrading\samples\simplePermChainSubscriber
	call :sample_build

:14
if "%1"=="SimpleFunctionHandler" goto SimpleFunctionHandler
if "%1"=="all" goto SimpleFunctionHandler
goto 15
:SimpleFunctionHandler
	set SAMPLE_NAME=SimpleFunctionHandler
	set WORKINGDIR=SimpleFunctionHandler
	set MAINCLASS=com.iontrading.samples.simpleFunctionHandler.FunctionHandler
	set SRC=src\com\iontrading\samples\simpleFunctionHandler
	call :sample_build

:15
if "%1"=="SimpleTransactionHandler" goto SimpleTransactionHandler
if "%1"=="all" goto SimpleTransactionHandler
goto 16
:SimpleTransactionHandler
	set SAMPLE_NAME=SimpleTransactionHandler
	set WORKINGDIR=SimpleTransactionHandler
	set MAINCLASS=com.iontrading.samples.simpleTransactionHandler.TransactionHandler
	set SRC=src\com\iontrading\samples\simpleTransactionHandler
	call :sample_build

:16
if "%1"=="SimpleFunctionCaller" goto SimpleFunctionCaller
if "%1"=="all" goto SimpleFunctionCaller
goto 17
:SimpleFunctionCaller
	set SAMPLE_NAME=SimpleFunctionCaller
	set WORKINGDIR=SimpleFunctionCaller
	set MAINCLASS=com.iontrading.samples.simpleFunctionCaller.FunctionCaller
	set SRC=src\com\iontrading\samples\simpleFunctionCaller
	call :sample_build

:17
if "%1"=="SimpleTransactionCaller" goto SimpleTransactionCaller
if "%1"=="all" goto SimpleTransactionCaller
goto 18
:SimpleTransactionCaller
	set SAMPLE_NAME=SimpleTransactionCaller
	set WORKINGDIR=SimpleTransactionCaller
	set MAINCLASS=com.iontrading.samples.simpleTransactionCaller.TransactionCaller
	set SRC=src\com\iontrading\samples\simpleTransactionCaller
	call :sample_build

:18
if "%1"=="MessageQueuePublisher" goto MessageQueuePublisher
if "%1"=="all" goto MessageQueuePublisher
goto 19
:MessageQueuePublisher
	set SAMPLE_NAME=MessageQueuePublisher
	set WORKINGDIR=MessageQueuePublisher
	set MAINCLASS=com.iontrading.samples.messageQueuePublisher.MessageQueuePublisher
	set SRC=src\com\iontrading\samples\messageQueuePublisher
	call :sample_build

:19
if "%1"=="MessageQueueSubscriber" goto MessageQueueSubscriber
if "%1"=="all" goto MessageQueueSubscriber
goto 20
:MessageQueueSubscriber
	set SAMPLE_NAME=MessageQueueSubscriber
	set WORKINGDIR=MessageQueueSubscriber
	set MAINCLASS=com.iontrading.samples.messageQueueSubscriber.MessageQueueSubscriber
	set SRC=src\com\iontrading\samples\messageQueueSubscriber
	call :sample_build

:20
if "%1"=="SimpleCustomStats" goto SimpleCustomStats
if "%1"=="all" goto SimpleCustomStats
goto 21
:SimpleCustomStats
	set SAMPLE_NAME=SimpleCustomStats
	set WORKINGDIR=SimpleCustomStats
	set MAINCLASS=com.iontrading.samples.simpleCustomStats.CustomStats
	set SRC=src\com\iontrading\samples\simpleCustomStats
	call :sample_build

:21
if "%1"=="SimpleCMStats" goto SimpleCMStats
if "%1"=="all" goto SimpleCMStats
goto 22
:SimpleCMStats
	set SAMPLE_NAME=SimpleCMStats
	set WORKINGDIR=SimpleCMStats
	set MAINCLASS=com.iontrading.samples.simpleCMStats.CustomStats
	set SRC=src\com\iontrading\samples\simpleCMStats
	call :sample_build

:22
if "%1"=="SimpleCMDrillDown" goto SimpleCMDrillDown
if "%1"=="all" goto SimpleCMDrillDown
goto 23
:SimpleCMDrillDown
	set SAMPLE_NAME=SimpleCMDrillDown
	set WORKINGDIR=SimpleCMDrillDown
	set MAINCLASS=com.iontrading.samples.simpleCMDrillDown.CustomStats
	set SRC=src\com\iontrading\samples\simpleCMDrillDown
	call :sample_build

:23
if "%1"=="SimpleCMProxy" goto SimpleCMProxy
if "%1"=="all" goto SimpleCMProxy
goto 24
:SimpleCMProxy
	set SAMPLE_NAME=SimpleCMProxy
	set WORKINGDIR=SimpleCMProxy
	set MAINCLASS=com.iontrading.samples.simpleCMProxy.CustomStats
	set SRC=src\com\iontrading\samples\simpleCMProxy
	call :sample_build

:24
if "%1"=="SimpleVirtualHost" goto SimpleVirtualHost
if "%1"=="all" goto SimpleVirtualHost
goto 25
:SimpleVirtualHost
	set SAMPLE_NAME=SimpleVirtualHost
	set WORKINGDIR=SimpleVirtualHost
	set MAINCLASS=com.iontrading.samples.simpleVirtualHost.CustomStats
	set SRC=src\com\iontrading\samples\simpleVirtualHost
	call :sample_build

:25
if "%1"=="CommonMarketCustomStats" goto CommonMarketCustomStats
if "%1"=="all" goto CommonMarketCustomStats
goto 26
:CommonMarketCustomStats
	set SAMPLE_NAME=CommonMarketCustomStats
	set WORKINGDIR=CommonMarketCustomStats
	set MAINCLASS=com.iontrading.samples.commonMarketCustomStats.CustomStats
	set SRC=src\com\iontrading\samples\commonMarketCustomStats
	call :sample_build

:26
if "%1"=="SimpleExternalAuthenticator" goto SimpleExternalAuthenticator
if "%1"=="all" goto SimpleExternalAuthenticator
goto 27
:SimpleExternalAuthenticator
	set SAMPLE_NAME=SimpleExternalAuthenticator
	set WORKINGDIR=SimpleExternalAuthenticator
	set MAINCLASS=com.iontrading.samples.simpleExternalAuthenticator.ExternalAuthenticator
	set SRC=src\com\iontrading\samples\simpleExternalAuthenticator
	call :sample_build

:27
if "%1"=="SimpleAsyncShutdownComponent" goto SimpleAsyncShutdownComponent
if "%1"=="all" goto SimpleAsyncShutdownComponent
goto 28
:SimpleAsyncShutdownComponent
	set SAMPLE_NAME=SimpleAsyncShutdownComponent
	set WORKINGDIR=SimpleAsyncShutdownComponent
	set MAINCLASS=com.iontrading.samples.simpleAsyncShutdownComponent.AsyncShutdownComponent
	set SRC=src\com\iontrading\samples\simpleAsyncShutdownComponent
	call :sample_build

:28
if "%1"=="SimpleQueryUserEntitlements" goto SimpleQueryUserEntitlements
if "%1"=="all" goto SimpleQueryUserEntitlements
goto 29
:SimpleQueryUserEntitlements
	set SAMPLE_NAME=SimpleQueryUserEntitlements
	set WORKINGDIR=SimpleQueryUserEntitlements
	set MAINCLASS=com.iontrading.samples.simpleQueryUserEntitlements.QueryUserEntitlements
	set SRC=src\com\iontrading\samples\simpleQueryUserEntitlements
	call :sample_build

:29
if "%1"=="SimpleGenericAuditClient" goto SimpleGenericAuditClient
if "%1"=="all" goto SimpleGenericAuditClient
goto 30
:SimpleGenericAuditClient
	set SAMPLE_NAME=SimpleGenericAuditClient
	set WORKINGDIR=SimpleGenericAuditClient
	set MAINCLASS=com.iontrading.samples.simpleGenericAuditClient.SimpleGenericAuditClient
	set SRC=src\com\iontrading\samples\simpleGenericAuditClient
	call :sample_build

:30
if "%1"=="PersistentSubscription" goto PersistentSubscription
if "%1"=="all" goto PersistentSubscription
goto 31
:PersistentSubscription
	set SAMPLE_NAME=PersistentSubscription
	set WORKINGDIR=PersistentSubscription
	set MAINCLASS=com.iontrading.samples.persistentSubscription.PersistentSubscription
	set SRC=src\com\iontrading\samples\persistentSubscription
	call :sample_build

:31
if "%1"=="Recordset" goto Recordset
if "%1"=="all" goto Recordset
goto 32
:Recordset
	set SAMPLE_NAME=Recordset
	set WORKINGDIR=Recordset
	set MAINCLASS=com.iontrading.samples.recordset.Recordset
	set SRC=src\com\iontrading\samples\recordset
	call :sample_build

:32
if "%1"=="EntitlementConfiguration" goto EntitlementConfiguration
if "%1"=="all" goto EntitlementConfiguration
goto 33
:EntitlementConfiguration
	set SAMPLE_NAME=EntitlementConfiguration
	set WORKINGDIR=EntitlementConfiguration
	set MAINCLASS=com.iontrading.samples.entitlementConfiguration.EntitlementConfiguration
	set MAINCLASS2=com.iontrading.samples.entitlementConfiguration.EntitlementGenConfiguration
	set SRC=src\com\iontrading\samples\entitlementConfiguration
	call :sample_build

:33
if "%1"=="SimplePriceContributionCustomE2EDelay" goto SimplePriceContributionCustomE2EDelay
if "%1"=="all" goto SimplePriceContributionCustomE2EDelay
goto 34
:SimplePriceContributionCustomE2EDelay
	set SAMPLE_NAME=SimplePriceContributionCustomE2EDelay
	set WORKINGDIR=SimplePriceContributionCustomE2EDelay
	set MAINCLASS=com.iontrading.samples.simplePriceContributionCustomE2EDelay.PriceGenerator
	set MAINCLASS2=com.iontrading.samples.simplePriceContributionCustomE2EDelay.PriceProducer
	set MAINCLASS3=com.iontrading.samples.simplePriceContributionCustomE2EDelay.PriceConsumer
	set SRC=src\com\iontrading\samples\simplePriceContributionCustomE2EDelay
	call :sample_build

:34
if "%1"=="SimpleOnBehalfOfLicense" goto SimpleOnBehalfOfLicense
if "%1"=="all" goto SimpleOnBehalfOfLicense
goto 35
:SimpleOnBehalfOfLicense
	set SAMPLE_NAME=SimpleOnBehalfOfLicense
	set WORKINGDIR=SimpleOnBehalfOfLicense
	set MAINCLASS=com.iontrading.samples.onbehalfoflicense.OnBehalfOfLicense
	set SRC=src\com\iontrading\samples\onbehalfoflicense
	call :sample_build

:35
if "%1"=="XRSClient" goto XRSClient
if "%1"=="all" goto XRSClient
goto 36
:XRSClient
	set SAMPLE_NAME=XRSClient
	set WORKINGDIR=XRSClient
	set MAINCLASS=com.iontrading.samples.xrsClient.XRSClient
	set SRC=src\com\iontrading\samples\xrsClient
	call :sample_build

:36
if "%1"=="AdvConfigHandling" goto AdvConfigHandling
if "%1"=="all" goto AdvConfigHandling
goto 37
:AdvConfigHandling
	set SAMPLE_NAME=AdvConfigHandling
	set WORKINGDIR=AdvConfigHandling
	set MAINCLASS=com.iontrading.samples.advanced.configuration.PropertyRegistration
	set SRC=src\com\iontrading\samples\advanced\configuration
	call :sample_build

:37
if "%1"=="AdvOrderManagement" goto AdvOrderManagement
if "%1"=="all" goto AdvOrderManagement
goto 38
:AdvOrderManagement
	set SAMPLE_NAME=AdvOrderManagement
	set WORKINGDIR=AdvOrderManagement
	set MAINCLASS=com.iontrading.samples.advanced.orderManagement.OrderManagement
	set SRC=src\com\iontrading\samples\advanced\orderManagement
	call :sample_build

:38
if "%1"=="AdvSTP" goto AdvSTP
if "%1"=="all" goto AdvSTP
goto 39
:AdvSTP
	set SAMPLE_NAME=AdvSTP
	set WORKINGDIR=AdvSTP
	set MAINCLASS=com.iontrading.samples.advanced.stp.TradeChainSubscriber
	set SRC=src\com\iontrading\samples\advanced\stp
	call :sample_build

:39
if "%1"=="AdvMQToChain" goto AdvMQToChain
if "%1"=="all" goto AdvMQToChain
goto 40
:AdvMQToChain
	set SAMPLE_NAME=AdvMQToChain
	set WORKINGDIR=AdvMQToChain
	set MAINCLASS=com.iontrading.samples.advanced.mqtochain.MQToChain
	set SRC=src\com\iontrading\samples\advanced\mqtochain
	call :sample_build

:40
if "%1"=="AdvOnBehalfOfRegistration" goto AdvOnBehalfOfRegistration
if "%1"=="all" goto AdvOnBehalfOfRegistration
goto 41
:AdvOnBehalfOfRegistration
	set SAMPLE_NAME=AdvOnBehalfOfRegistration
	set WORKINGDIR=AdvOnBehalfOfRegistration
	set MAINCLASS=com.iontrading.samples.advanced.onBehalfOfRegistration.OnBehalfOfRegistration
	set SRC=src\com\iontrading\samples\advanced\onBehalfOfRegistration
	call :sample_build

:41
if "%1"=="AdvTickDataApi" goto AdvTickDataApi
if "%1"=="all" goto AdvTickDataApi
goto 42
:AdvTickDataApi
	set SAMPLE_NAME=AdvTickDataApi
	set WORKINGDIR=AdvTickDataApi
	set MAINCLASS=com.iontrading.samples.advanced.tickDataApi.MkvTickDataApi
	set SRC=src\com\iontrading\samples\advanced\tickDataApi
	call :sample_build

:42
if "%1"=="AdvUltraLowLatency" goto AdvUltraLowLatency
if "%1"=="all" goto AdvUltraLowLatency
goto 43
:AdvUltraLowLatency
	set SAMPLE_NAME=AdvUltraLowLatency
	set WORKINGDIR=AdvUltraLowLatency
	set MAINCLASS=com.iontrading.samples.advanced.ultraLowLatency.PubSubPublisher
	set MAINCLASS2=com.iontrading.samples.advanced.ultraLowLatency.PubSubSubscriber
	set SRC=src\com\iontrading\samples\advanced\ultraLowLatency
	call :sample_build

:43
if "%1"=="AdvCNE" goto AdvCNE
if "%1"=="all" goto AdvCNE
goto 44
:AdvCNE
	set SAMPLE_NAME=AdvCNE
	set WORKINGDIR=AdvCNE
	set MAINCLASS=com.iontrading.samples.advanced.cne.CNESample
	set SRC=src\com\iontrading\samples\advanced\cne
	call :sample_build

:44
if "%1"=="AdvBulkEntitlements" goto AdvBulkEntitlements
if "%1"=="all" goto AdvBulkEntitlements
goto 45
:AdvBulkEntitlements
	set SAMPLE_NAME=AdvBulkEntitlements
	set WORKINGDIR=AdvBulkEntitlements
	set MAINCLASS=com.iontrading.samples.advanced.bulkentitlements.BulkEntitlements
	set SRC=src\com\iontrading\samples\advanced\bulkentitlements
	call :sample_build

:45
if "%1"=="AdvByFeatureReport" goto AdvByFeatureReport
if "%1"=="all" goto AdvByFeatureReport
goto 46
:AdvByFeatureReport
	set SAMPLE_NAME=AdvByFeatureReport
	set WORKINGDIR=AdvByFeatureReport
	set MAINCLASS=com.iontrading.samples.advanced.byfeaturereport.ByFeatureReport
	set SRC=src\com\iontrading\samples\advanced\byfeaturereport
	call :sample_build

:46
if "%1"=="AdvChainSnapshot" goto AdvChainSnapshot
if "%1"=="all" goto AdvChainSnapshot
goto 47
:AdvChainSnapshot
	set SAMPLE_NAME=AdvChainSnapshot
	set WORKINGDIR=AdvChainSnapshot
	set MAINCLASS=com.iontrading.samples.advanced.chainSnapshot.WaitChainSnapshot
	set SRC=src\com\iontrading\samples\advanced\chainSnapshot
	call :sample_build
	
:47
if "%1"=="Benchmarks" goto Benchmarks
if "%1"=="all" goto Benchmarks
goto 48
:Benchmarks
	set SAMPLE_NAME=Benchmarks
	set WORKINGDIR=Benchmarks
	set MAINCLASS=com.iontrading.samples.benchmarks.PubSubPublisher
	set MAINCLASS2=com.iontrading.samples.benchmarks.PubSubSubscriber
	set SRC=src\com\iontrading\samples\benchmarks
	call :sample_build
	
:48
if "%1"=="SimpleOnBehalfOfRegistration" goto SimpleOnBehalfOfRegistration
if "%1"=="all" goto SimpleOnBehalfOfRegistration
goto 49
:SimpleOnBehalfOfRegistration
	set SAMPLE_NAME=SimpleOnBehalfOfRegistration
	set WORKINGDIR=SimpleOnBehalfOfRegistration
	set MAINCLASS=com.iontrading.samples.simpleOnBehalfOfRegistration.SimpleOnBehalfOfRegistration
	set SRC=src\com\iontrading\samples\simpleOnBehalfOfRegistration
	call :sample_build

:49
if "%1"=="PasswordServerServiceCredentials" goto PasswordServerServiceCredentials
if "%1"=="all" goto PasswordServerServiceCredentials
goto 50
:PasswordServerServiceCredentials
	set SAMPLE_NAME=PasswordServerServiceCredentials
	set WORKINGDIR=PasswordServerServiceCredentials
	set MAINCLASS=ccom.iontrading.samples.advanced.passwordServerServiceCredentials.PasswordServerServiceCredentials
	set SRC=src\com\iontrading\samples\advanced\passwordServerServiceCredentials
	call :sample_build

:50

if "%WORKINGDIR%"=="" (
	goto sample_help)

if NOT "%1"=="all" (
	call :sample_run)

goto END

:sample_build
    @echo Building [%SAMPLE_NAME%] sample
    @echo     ^> WORKINGDIR=RUN_%WORKINGDIR%
    @echo     ^> MAINCLASS=%MAINCLASS%

    if not exist build mkdir build
    if not exist build\classes mkdir build\classes

    echo javac -cp "lib/jmkv*.jar;lib/*;src" -d build\classes %SRC%\*.java
    javac -cp "lib/jmkv*.jar;lib/*;src" -d build\classes %SRC%\*.java

    @echo.
    @echo.
    goto :EOF

:sample_run
    @echo Running [%SAMPLE_NAME%] sample

    if not exist "RUN_%WORKINGDIR%" (
        echo Error: Directory RUN_%WORKINGDIR% does not exist.
        exit /b 1
    )

    cd RUN_%WORKINGDIR%

    echo java -cp "lib/jmkv*.jar;lib/*" %MAINCLASS%
    start cmd /k java -cp "lib/jmkv*.jar;lib/*" %MAINCLASS%

    if NOT "%MAINCLASS2%"=="" (
        echo java -cp "lib/jmkv*.jar;lib/*" %MAINCLASS2% -init mkv2.jinit
        start cmd /k java -cp "lib/jmkv*.jar;lib/*" %MAINCLASS2% -init mkv2.jinit
    )

    if NOT "%MAINCLASS3%"=="" (
        echo java -cp "lib/jmkv*.jar;lib/*" %MAINCLASS3% -init mkv3.jinit
        start cmd /k java -cp "lib/jmkv*.jar;lib/*" %MAINCLASS3% -init mkv3.jinit
    )

    cd ..
    goto :EOF

:sample_help
	@echo.
	@echo ---------------------------------------------------------------------------------------------
	@echo ION Java API Samples
	@echo ---------------------------------------------------------------------------------------------
	@echo PlatformListener:                      Example of MkvPlatformListener implementation
	@echo. 
	@echo SimpleLog:                             Example of MkvPlatformListener implementation tracing events in a custom log file
	@echo. 
	@echo SimplePublisher:                       The SimplePublisher sample publishes a type, a record and fill the record with values
	@echo. 
	@echo SimplePublisher2:                      Same as SimplePublisher but using the new MkvSupplyProxy mechanism
	@echo. 
	@echo SimplePublisher3:                      Same as SimplePublisher, excepts it uses a custom extended MkvRecord implementing the Subscribeable interface
	@echo.
	@echo SimpleTimerSupplier:                   The SimpleTimerSupplier is similar to the SimplePublisher sample but implements a mechanism based on the MkvTimer
	@echo. 
	@echo SimpleChainPublisher:                  The SimpleChainPublisher sample publishes a type, a set record and a chain
	@echo. 
	@echo SimplePatternPublisher:                The SimplePatternPublisher sample publishes a type, a set record and a pattern
	@echo. 
	@echo SimpleSubscriber:                      The SimpleSubscriber listens for publications and when it receives a record with a given name it susbcribes to it
	@echo. 
	@echo SimpleSubscriber2:                     Same as SimpleSubscriber but using the new MkvSupplyProxy mechanism
	@echo. 
	@echo SimpleChainSubscriber:                 The SimpleChainSubscriber listens for publications and when it receives a chain with a given name it subscribe to it
	@echo.
	@echo SimplePatternSubscriber:               The SimplePatternSubscriber listens for publications and when it receives a pattern with a given name it subscribe to it
	@echo. 
	@echo SimplePermChainSubscriber:             The subscription to the chain is performed registering a permanent subscription at startup
	@echo. 
	@echo SimpleFunctionHandler:                 The SimpleFunctionHandler publishes two functions: Avg and Min
	@echo. 
	@echo SimpleTransactionHandler:              The SimpleTransactionHandler sample publishes a record and listen for transactions on the fields
	@echo. 
	@echo SimpleFunctionCaller:                  The SimpleFunctionCaller exploits the function exposed by the SimpleFunctionHandler
	@echo. 
	@echo SimpleTransactionCaller:               This sample starts a timer with period 500ms that request for a transaction on a field
	@echo. 
	@echo MessageQueuePublisher:                 The MessageQueuePublisher publishes a function; when this function is invoked, it publishes some records on a dynamically created message queue
	@echo. 
	@echo MessageQueueSubscriber:                It invokes the function published by the MessageQueuePublisher, then proceeds to subscribe the data from the message queue
	@echo. 
	@echo SimpleCustomStats:                     This sample shows how to use the Custom Stats interfaces to report custom statistics
	@echo.
	@echo SimpleCMStats:                         This sample shows how to use the Custom Stats interfaces to report Common Market Statistics
	@echo.
	@echo SimpleCMDrillDown:                     This sample shows how to use the Custom Stats interfaces to report a per-trader drill down of Common Market Statistics
	@echo.
	@echo SimpleCMProxy:                         This sample shows how to use the Custom Stats interfaces to report Common Market Statistics on behalf of MOCKs
	@echo.
	@echo SimpleVirtualHost:                     This sample shows how to use the Custom Stats interfaces to report standard statistics for a Virtual Host
	@echo.
	@echo CommonMarketCustomStats:               This sample shows how to use the Custom Stats interfaces to report custom statistics for Common Market
	@echo.
	@echo SimpleExternalAuthenticator:           This sample shows how to implement a component behaving as an external authenticator for the platform users
	@echo.
	@echo SimpleAsyncShutdownComponent:          This sample shows how to implement a component able to request an asynchronous shutdown to the daemon
	@echo.
	@echo SimpleQueryUserEntitlements:           This sample shows how to use the advanced query interface of the Entitlement Server
	@echo.
	@echo SimpleGenericAuditClient:              This sample shows how to implement a component able to log Audit Events to a Generic Audit Server
	@echo.
	@echo PersistentSubscription:                This sample shows how to use the Persistent Subscription interface and its lifecycle events
	@echo.
	@echo Recordset:                             This sample shows how to publish a Recordset object
	@echo.
	@echo EntitlementConfiguration:              This sample shows how to configure the ION Entitlement Server, programmatically
	@echo.
	@echo SimplePriceContributionCustomE2EDelay: This sample shows how to use the Custom End-To-End Delay API, to report custom performance statitics to the ION Daemon
	@echo.
	@echo SimpleOnBehalfOfLicense:               This sample shows how to use the on-behalf-of API for license management
	@echo.
	@echo XRSClient:                             This sample shows how to interact with an XRS publisher
	@echo.
	@echo AdvConfigHandling:                     This sample exploit the component configuration features offered by the API
	@echo. 
	@echo AdvOrderManagement:                    The sample works together with a CM compliant gateway
	@echo. 
	@echo AdvSTP:                                This sample subscribes to the Common Market TRADE chain using a permanent subscription
	@echo. 
	@echo AdvMQToChain:                          This sample illustrates how to republish records data from a queue onto a chain
	@echo. 
	@echo AdvOnBehalfOfRegistration:             This sample shows how to register a virtual component component on the platform
	@echo. 
	@echo AdvTickDataApi:                        This sample shows an example of usage of the Percentile API
	@echo.
	@echo AdvUltraLowLatency:                    These samples show the configuration of Publisher and Subscriber to use Ultra Low Latency message passing on same-host connections
	@echo.
	@echo AdvCNE:                                This sample shows how to subscribe R4Q records from a CMI gateway
	@echo. 
	@echo AdvBulkEntitlements:                   This sample shows how to perform a bulk entitlement query to the ION Entitlement Server
	@echo.
	@echo AdvByFeatureReport:                    This sample shows how to query the ION Entitlement Server to produce a comprehensive report
	@echo.
	@echo AdvChainSnapshot:                      This sample shows how to subscribe to a chain and wait for all the records to be downloaded
	@echo. 
	@echo PasswordServerServiceCredentials:      This smple shows how to use the Daemon Password Server to retrieve service credentials
	@echo. 
	@echo Benchmarks:                            This sample includes a Publisher and a Subscriber for supply throughput benchmarking
	@echo. 
	@echo SimpleOnBehalfOfRegistration:          This sample shows how to register a virtual component on the platform using the on-behalf-of registration
	@echo. 
	@echo ---------------------------------------------------------------------------------------------
	@echo. 
	@echo. 
	@echo Usage: build_sample.bat [all ^| sample_name]
	@echo. 
	@echo. 

:END
