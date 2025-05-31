@echo off
set WORKINGDIR=
if "%1"=="" goto END_LIST

if NOT "%1"=="SimplePublisher" goto a
	set WORKINGDIR=RUN_SimplePublisher
	set MAINCLASS=com.iontrading.samples.simplePublisher.Publisher
	set SRC=src\com\iontrading\samples\simplePublisher
goto END_LIST

:a
if NOT "%1"=="SimpleTimerSupplier" goto b
	set WORKINGDIR=RUN_SimpleTimerSupplier
	set MAINCLASS=com.iontrading.samples.simpleTimerSupplier.TimerPublisher
	set SRC=src\com\iontrading\samples\simpleTimerSupplier
goto END_LIST

:b
if NOT "%1"=="SimpleChainPublisher" goto c
	set WORKINGDIR=RUN_SimpleTimerSupplier
	set MAINCLASS=com.iontrading.samples.simpleChainPublisher.ChainPublisher
	set SRC=src\com\iontrading\samples\simpleChainPublisher
goto END_LIST

:c
if NOT "%1"=="SimpleFunctionHandler" goto d
	set WORKINGDIR=RUN_SimpleFunctionHandler
	set MAINCLASS=com.iontrading.samples.simpleFunctionHandler.FunctionHandler
	set SRC=src\com\iontrading\samples\simpleFunctionHandler
goto END_LIST

:d
if NOT "%1"=="SimpleTransactionHandler" goto e
	set WORKINGDIR=RUN_SimpleTransactionHandler
	set MAINCLASS=com.iontrading.samples.simpleTransactionHandler.TransactionHandler
	set SRC=src\com\iontrading\samples\simpleTransactionHandler
goto END_LIST

:e
if NOT "%1"=="SimpleSubscriber" goto f
	set WORKINGDIR=RUN_SimpleSubscriber
	set MAINCLASS=com.iontrading.samples.simpleSubscriber.Subscriber
	set SRC=src\com\iontrading\samples\simpleSubscriber
goto END_LIST

:f
if NOT "%1"=="SimplePermChainSubscriber" goto g
	set WORKINGDIR=RUN_SimplePermChainSubscriber
	set MAINCLASS=com.iontrading.samples.simplePermChainSubscriber.ChainSubscriber
	set SRC=src\com\iontrading\samples\simplePermChainSubscriber
goto END_LIST

:g
if NOT "%1"=="SimpleChainSubscriber" goto h
	set WORKINGDIR=RUN_SimpleChainSubscriber
	set MAINCLASS=com.iontrading.samples.simpleChainSubscriber.ChainSubscriber
	set SRC=src\com\iontrading\samples\simpleChainSubscriber
goto END_LIST

:h
if NOT "%1"=="SimpleTransactionCaller" goto i
	set WORKINGDIR=RUN_SimpleTransactionCaller
	set MAINCLASS=com.iontrading.samples.simpleTransactionCaller.TransactionCaller
	set SRC=src\com\iontrading\samples\simpleTransactionCaller
goto END_LIST

:i
if NOT "%1"=="SimpleFunctionCaller" goto l
	set WORKINGDIR=RUN_SimpleFunctionCaller
	set MAINCLASS=com.iontrading.samples.simpleFunctionCaller.FunctionCaller
	set SRC=src\com\iontrading\samples\simpleFunctionCaller
goto END_LIST

:l
if NOT "%1"=="AdvConfigHandling" goto m
	set WORKINGDIR=RUN_AdvancedConfHandler
	set MAINCLASS=com.iontrading.samples.advanced.configuration.PropertyRegistration
	set SRC=src\com\iontrading\samples\advanced\configuration
goto END_LIST

:m
if NOT "%1"=="AdvOrderManagement" goto n
	set WORKINGDIR=RUN_OrderManagement
	set MAINCLASS=com.iontrading.samples.advanced.orderManagement.OrderManagement
	set SRC=src\com\iontrading\samples\advanced\orderManagement
goto END_LIST

:n
if NOT "%1"=="SimplePublisher2" goto o
	set WORKINGDIR=RUN_SimplePublisher2
	set MAINCLASS=com.iontrading.samples.simplePublisher2.Publisher
	set SRC=src\com\iontrading\samples\simplePublisher2
goto END_LIST

:o
if NOT "%1"=="SimpleSubscriber2" goto p
	set WORKINGDIR=RUN_SimpleSubscriber2
	set MAINCLASS=com.iontrading.samples.simpleSubscriber2.Subscriber
	set SRC=src\com\iontrading\samples\simpleSubscriber2
goto END_LIST

:p
if NOT "%1"=="PlatformListener" goto q
	set WORKINGDIR=RUN_PlatformListener
	set MAINCLASS=com.iontrading.samples.platformListener.PlatformListener
	set SRC=src\com\iontrading\samples\platformListener
goto END_LIST

:q
if NOT "%1"=="CNE" goto r
	set WORKINGDIR=RUN_CNE
	set MAINCLASS=com.iontrading.samples.advanced.cne.CNESample
	set SRC=src\com\iontrading\samples\advanced\cne
goto END_LIST

:r
if NOT "%1"=="SimplePatternSubscriber" goto s
	set WORKINGDIR=RUN_SimplePatternSubscriber
	set MAINCLASS=com.iontrading.samples.simplePatternSubscriber.Subscriber
	set SRC=src\com\iontrading\samples\simplePatternSubscriber
goto END_LIST

:s
if NOT "%1"=="SimplePatternPublisher" goto t
	set WORKINGDIR=RUN_SimplePatternPublisher
	set MAINCLASS=com.iontrading.samples.simplePatternPublisher.Publisher
	set SRC=src\com\iontrading\samples\simplePatternPublisher
goto END_LIST

:t
if NOT "%1"=="MessageQueuePublisher" goto u
	set WORKINGDIR=RUN_MessageQueuePublisher
	set MAINCLASS=com.iontrading.samples.messageQueuePublisher.MessageQueuePublisher
	set SRC=src\com\iontrading\samples\messageQueuePublisher
goto END_LIST

:u
if NOT "%1"=="MessageQueueSubscriber" goto v
	set WORKINGDIR=RUN_MessageQueueSubscriber
	set MAINCLASS=com.iontrading.samples.messageQueueSubscriber.MessageQueueSubscriber
	set SRC=src\com\iontrading\samples\messageQueueSubscriber
goto END_LIST

:v
if NOT "%1"=="SimplePublisher3" goto w
	set WORKINGDIR=RUN_SimplePublisher3
	set MAINCLASS=com.iontrading.samples.simplePublisher3.Publisher
	set SRC=src\com\iontrading\samples\simplePublisher3
goto END_LIST

:w
if NOT "%1"=="SimpleQueryUserEntitlements" goto j
	set WORKINGDIR=RUN_QueryUserEntitlements
	set MAINCLASS=com.iontrading.samples.simpleQueryUserEntitlements.QueryUserEntitlements
	set SRC=src\com\iontrading\samples\simpleQueryUserEntitlements
goto END_LIST

:j
if NOT "%1"=="SimpleGenericByFeatureEntitlements" goto x
	set WORKINGDIR=RUN_simpleGenericByFeatureEntitlements
	set MAINCLASS=com.iontrading.samples.simpleGenericByFeatureEntitlements.GenericByFeatureEntitlements
	set SRC=src\com\iontrading\samples\simpleGenericByFeatureEntitlements
goto END_LIST

:x
if NOT "%1"=="MQToChain" goto y
	set WORKINGDIR=RUN_AdvancedMQToChain
	set MAINCLASS=com.iontrading.samples.advanced.mqtochain.MQToChain
	set SRC=src\com\iontrading\samples\advanced\mqtochain
goto END_LIST

:y
if NOT "%1"=="OnBehalfOfRegistration" goto w
	set WORKINGDIR=RUN_AdvancedOnBehalfOfRegistration
	set MAINCLASS=com.iontrading.samples.advanced.onBehalfOfRegistration.OnBehalfOfRegistration
	set SRC=src\com\iontrading\samples\advanced\onBehalfOfRegistration
goto END_LIST

:w
if NOT "%1"=="SimpleCustomStats" goto aa
	set WORKINGDIR=RUN_SimpleCustomStats
	set MAINCLASS=com.iontrading.samples.simpleCustomStats.CustomStats
	set SRC=src\com\iontrading\samples\simpleCustomStats
goto END_LIST

:aa
if NOT "%1"=="SimpleCMStats" goto ab
	set WORKINGDIR=RUN_SimpleCMStats
	set MAINCLASS=com.iontrading.samples.simpleCMStats.CustomStats
	set SRC=src\com\iontrading\samples\simpleCMStats
goto END_LIST

:ab
if NOT "%1"=="SimpleCMDrillDown" goto ac
	set WORKINGDIR=RUN_SimpleCMDrillDown
	set MAINCLASS=com.iontrading.samples.simpleCMDrillDown.CustomStats
	set SRC=src\com\iontrading\samples\simpleCMDrillDown
goto END_LIST

:ac
if NOT "%1"=="SimpleCMProxy" goto ad
	set WORKINGDIR=RUN_SimpleCMProxy
	set MAINCLASS=com.iontrading.samples.simpleCMProxy.CustomStats
	set SRC=src\com\iontrading\samples\simpleCMProxy
goto END_LIST

:ad
if NOT "%1"=="SimpleVirtualHost" goto ae
	set WORKINGDIR=RUN_SimpleVirtualHost
	set MAINCLASS=com.iontrading.samples.simpleVirtualHost.CustomStats
	set SRC=src\com\iontrading\samples\simpleVirtualHost
goto END_LIST

:ae
if NOT "%1"=="CommonMarketCustomStats" goto af
	set WORKINGDIR=RUN_CommonMarketCustomStats
	set MAINCLASS=com.iontrading.samples.commonMarketCustomStats.CustomStats
	set SRC=src\com\iontrading\samples\commonMarketCustomStats
goto END_LIST

:af
if NOT "%1"=="SimpleExternalAuthenticator" goto ag
	set WORKINGDIR=RUN_SimpleExternalAuthenticator
	set MAINCLASS=com.iontrading.samples.simpleExternalAuthenticator.ExternalAuthenticator
	set SRC=src\com\iontrading\samples\simpleExternalAuthenticator
goto END_LIST

:ag
if NOT "%1"=="SimpleAsyncShutdownComponent" goto ah
	set WORKINGDIR=RUN_SimpleAsyncShutdownComponent
	set MAINCLASS=com.iontrading.samples.simpleAsyncShutdownComponent.AsyncShutdownComponent
	set SRC=src\com\iontrading\samples\simpleAsyncShutdownComponent
goto END_LIST

:ah
if NOT "%1"=="SimpleByFeatureService" goto ai
	set WORKINGDIR=RUN_SimpleByFeatureService
	set MAINCLASS=com.iontrading.samples.simpleByFeatureService.SimpleByFeatureService
	set SRC=src\com\iontrading\samples\simpleByFeatureService
goto END_LIST

:ai
if NOT "%1"=="SimpleGenericAuditClient" goto al
	set WORKINGDIR=RUN_SimpleGenericAuditClient
	set MAINCLASS=com.iontrading.samples.simpleGenericAuditClient.SimpleGenericAuditClient
	set SRC=src\com\iontrading\samples\simpleGenericAuditClient
goto END_LIST

:al
if NOT "%1"=="TickDataApi" goto am
	set WORKINGDIR=RUN_TickDataApi
	set MAINCLASS=com.iontrading.samples.advanced.tickDataApi.MkvTickDataApi
	set SRC=src\com\iontrading\samples\advanced\tickDataApi
goto END_LIST

:am
if NOT "%1"=="EntitlementConfiguration" goto an
	set WORKINGDIR=RUN_EntitlementConfiguration
	set MAINCLASS=com.iontrading.samples.entitlementConfiguration.EntitlementGenConfiguration
	set SRC=src\com\iontrading\samples\entitlementConfiguration
goto END_LIST

:an
if NOT "%1"=="SimplePriceContributionCustomE2EDelay" goto ao
	set WORKINGDIR=RUN_SimplePriceContributionCustomE2EDelay
	set MAINCLASS=com.iontrading.samples.simplePriceContributionCustomE2EDelay.PriceConsumer
	set SRC=src\com\iontrading\samples\simplePriceContributionCustomE2EDelay
goto END_LIST

:ao
if NOT "%1"=="PersistentSubscription" goto ap
	set WORKINGDIR=RUN_PersistentSubscription
	set MAINCLASS=com.iontrading.samples.persistentSubscription.PersistentSubscription
	set SRC=src\com\iontrading\samples\persistentSubscription
goto END_LIST

:ap
if NOT "%1"=="Recordset" goto aq
	set WORKINGDIR=RUN_Recordset
	set MAINCLASS=com.iontrading.samples.recordset.Recordset
	set SRC=src\com\iontrading\samples\recordset
goto END_LIST

:aq
if NOT "%1"=="SimpleOnBehalfOfLicense" goto ar
	set WORKINGDIR=RUN_SimpleOnBehalfOfLicense
	set MAINCLASS=com.iontrading.samples.onbehalfoflicense.OnBehalfOfLicense
	set SRC=src\com\iontrading\samples\onbehalfoflicense
goto END_LIST

:ar
if NOT "%1"=="XRSClient" goto as
	set WORKINGDIR=RUN_XRSClient
	set MAINCLASS=com.iontrading.samples.xrsClient.XRSClient
	set SRC=src\com\iontrading\samples\xrsClient
goto END_LIST

:as
if NOT "%1"=="PasswordServerServiceCredentials" goto at
	set WORKINGDIR=RUN_PasswordServerServiceCredentials
	set MAINCLASS=com.iontrading.samples.advanced.passwordServerServiceCredentials.PasswordServerServiceCredentials
	set SRC=src\com\iontrading\samples\advanced\passwordServerServiceCredentials
goto END_LIST

:at
if NOT "%1"=="AdvOrderManagement" goto au
	set SAMPLE_NAME=AdvOrderManagement
	set WORKINGDIR=AdvOrderManagement
	set MAINCLASS=com.iontrading.samples.advanced.orderManagement.OrderManagement
	set SRC=src\com\iontrading\samples\advanced\orderManagement
goto END_LIST

:au
if NOT "%1"=="marketDataSubscriberUAT" goto END_LIST
	set SAMPLE_NAME=marketDataSubscriberUAT
	set WORKINGDIR=RUN_marketDataSubscriberUAT
	set MAINCLASS=com.iontrading.samples.marketDataSubscriberUAT.marketDataSubscriberUAT
	set SRC=src\com\iontrading\samples\marketDataSubscriberUAT
	set INITFILE=mkv.jinit
goto END_LIST

:END_LIST

if "%WORKINGDIR%"=="" goto HELP
@echo WORKINGDIR=%WORKINGDIR%
@echo MAINCLASS=%MAINCLASS%
if not exist build mkdir build
if not exist build\classes mkdir build\classes

echo Compiling Java files...
REM if need JAVA 8
REM echo javac -source 1.8 -target 1.8 -cp "lib/*" -Xlint:unchecked -d build\classes %SRC%\*.java
REM javac -source 1.8 -target 1.8 -cp "lib/*" -Xlint:unchecked -d build\classes %SRC%\*.java

echo javac -cp "lib/*" -Xlint:unchecked -d build\classes %SRC%\*.java
javac -cp "lib/*" -Xlint:unchecked -d build\classes %SRC%\*.java

REM Check if JAR creation is requested
if "%2"=="jar" goto CREATE_JAR

if not exist "%WORKINGDIR%" (
    echo Error: Directory %WORKINGDIR% does not exist.
    exit /b 1
)

cd "%WORKINGDIR%"
echo Running Java class: %MAINCLASS%
echo Classpath: lib\jmkv*.jar;lib\*;..\build\classes
echo java -cp "lib/*;..\build\classes" %MAINCLASS%
start cmd /k java -cp "../lib/*;..\build\classes" %MAINCLASS%
cd..
goto :EOF

:CREATE_JAR
echo Creating Fat JAR file for %SAMPLE_NAME%...

REM Create manifest
echo Manifest-Version: 1.0 > build\manifest.txt
echo Main-Class: %MAINCLASS% >> build\manifest.txt
echo Class-Path: . >> build\manifest.txt

@echo off

rem Convert dot notation to path notation
set CLASSPATH_DIR=%MAINCLASS:.=\%

echo Original Class: %MAINCLASS%
echo Converted Path: %CLASSPATH_DIR%

rem Remove the last segment (class name)
for /f "tokens=1-3 delims=\" %%a in ("%CLASSPATH_DIR%") do set "PACKAGE_PATH=%%a\%%b\%%c"
echo Package Path: %PACKAGE_PATH%
echo Directory build\classes\%PACKAGE_PATH%\%SAMPLE_NAME%\

REM Ensure the directory structure exists
if not exist build\classes\%PACKAGE_PATH%\%SAMPLE_NAME%\ (
    echo Directory build\classes\%PACKAGE_PATH%\%SAMPLE_NAME%\ does not exist.
    exit /b 1
)

set "FULL_PATH=%PACKAGE_PATH%\%SAMPLE_NAME%\"

REM Create JAR with classes and all libraries
echo jar cvfm "build\%SAMPLE_NAME%.jar" build\manifest.txt -C build\classes %FULL_PATH%*.class
jar cvfm "build\%SAMPLE_NAME%.jar" build\manifest.txt -C build\classes %FULL_PATH%*.class

REM Add all libraries to JAR
for %%i in (lib\*.jar) do jar uvf "build\%SAMPLE_NAME%.jar" -C lib %%~nxi

REM Add init file if exists
if not "%INITFILE%"=="" (
    if exist "%WORKINGDIR%\%INITFILE%" (
        jar uvf "build\%SAMPLE_NAME%.jar" -C "%WORKINGDIR%" %INITFILE%
    )
)

REM Verify JAR contents
echo JAR Contents:
jar tf "build\%SAMPLE_NAME%.jar"

goto :EOF


:HELP
@echo.
@echo ION Java API Samples
@echo ---------------------------------------------------------------------------------------------
@echo PlatformListener:                  Listen for platform events
@echo. 
@echo SimplePublisher:                   Publish a type, a record and supply values for the record.
@echo. 
@echo SimplePublisher2:                  Same as SimplePublisher + MkvSupplyProxy mechanism.
@echo. 
@echo SimplePublisher3:                  SimplePublisher3: Same as SimplePublisher, it publishes an MkvRecord implementing the Subscribeable interface.
@echo.
@echo SimpleChainPublisher:              Publish a type, some records and a chain.
@echo. 
@echo SimplePatternPublisher:            Publish a type, some records and a pattern.
@echo. 
@echo SimpleTimerSupplier:               Publish a type, a record and supply values for the record every 500ms.
@echo. 
@echo SimpleTransactionHandler:          As SimplePublisher but support for transaction.
@echo. 
@echo SimpleFunctionHandler:             Publish and manage functions.
@echo. 
@echo SimpleSubscriber:                  Subscribe to the record published by SimplePublisher or SimpleTimerSupplier and listen for supplies.
@echo. 
@echo SimpleSubscriber2:                 Same as SimpleSubscriber + MkvSupplyProxy mechanism.
@echo. 
@echo SimpleChainSubscriber:             Subscribe to the chain published by SimpleChainPublisher.
@echo. 
@echo SimplePatternSubscriber:           Subscribe to the pattern published by SimplePatternPublisher.
@echo. 
@echo SimplePermChainSubscriber:         Subscribe to the chain published by SimpleChainPublisher using a permanent subscription.
@echo. 
@echo SimpleFunctionCaller:              Calls the functions published by SimpleFunctionhandler.
@echo. 
@echo SimpleTransactionCaller:           As SimpleSubscriber but request a transaction at timer.
@echo. 
@echo AdvConfigHandling:                 Management of component configuration.
@echo. 
@echo AdvOrderManagement:                An example of order management. Work together with a CM compliant gateway.
@echo. 
@echo CNE:                               An example CNE. Work together with a CM4D2C compliant gateway.
@echo. 
@echo MessageQueuePublisher:             Publish a function, return result on a dynamically created message queue.
@echo. 
@echo MessageQueueSubscriber:            Request MessageQueuePublisher to dynamically create a message queue, the retrieve data from it.
@echo. 
@echo SimpleQueryUserEntitlements:       Show how to use the advanced query interface of the Entitlement Server.
@echo. 
@echo MQToChain:                         Show how to implement a protocol to deliver data from a message queue to a chain.
@echo. 
@echo OnBehalfOfRegistration:            Show how to register a virtual component component on the platform.
@echo. 
@echo SimpleQueryUserEntitlements        Show how to use the Entitlement Query interface, for batch check requests to the Entitlement Server (104 and higher).
@echo.
@echo SimpleGenericByFeatureEntitlements Show how to use the Generic By-Feature Entitlement interface, to do generic requests to the Entitlement Server (105 and higher).
@echo.
@echo SimpleCustomStats                  Show how to use the Custom Stats interfaces to report custom statistics.
@echo.
@echo SimpleCMStats                      Show how to use the Custom Stats interfaces to report Common Market Statistics.
@echo.
@echo SimpleCMDrillDown                  Show how to use the Custom Stats interfaces to report a per-trader drill down of Common Market Statistics.
@echo.
@echo SimpleCMProxy                      Show how to use the Custom Stats interfaces to report Common Market Statistics on behalf of MOCKs.
@echo.
@echo SimpleVirtualHost                  Show how to use the Custom Stats interfaces to report standard statistics for a Virtual Host.
@echo.
@echo CommonMarketCustomStats            Show how to use the Custom Stats interfaces to report custom statistics for Common Market.
@echo.
@echo SimpleExternalAuthenticator        Show how to implement a component behaving as an external authenticator for the platform users.
@echo.
@echo SimpleAsyncShutdownComponent       Show how to implement a component able to request an asynchronous shutdown to the daemon.
@echo.
@echo SimpleByFeatureService             Show how to implement a custom ByFeature Service to be used with Generic ByFeature Entitlements.
@echo.
@echo SimpleGenericAuditClient           Show how to implement a component able to log Audit Events to a Generic Audit Server.
@echo.
@echo TickDataApi                        Show an example of usage of the TickData API.
@echo.
@echo EntitlementConfiguration           Show how to configure the ION Entitlement Server.
@echo.
@echo SimplePriceContributionCustomE2EDelay Show how to use the Custom End-To-End Delay API.
@echo.
@echo PersistentSubscription             Show how to use the Persistent Subscription API.
@echo.
@echo Recordset                          Show how to use the Recordset API.
@echo.
@echo SimpleOnBehalfOfLicense            Show how to use the on-behalf-of API for license management.
@echo.
@echo UltraLowLatency		         Show how to leverage Ultra Low Latency message passing for same-host connections.
@echo.
@echo XRSClient			         Show how to interact with a typical XRS publisher.
@echo.
@echo PasswordServerServiceCredentials   Show how to subscribe to a chain and wait for all the records to be downloaded.
@echo.
@echo ---------------------------------------------------------------------------------------------
@echo. 
@echo. 
@echo Usage: go.bat [sample_name]

echo Usage: %0 ^<project^> [jar]
echo Available Projects:
echo   - AdvOrderManagement
echo   - marketDataSubscriber_UAT
echo.
echo Options:
echo   - No second argument: Compile and Run
echo   - 'jar': Create JAR file only
exit /b 1

:END

