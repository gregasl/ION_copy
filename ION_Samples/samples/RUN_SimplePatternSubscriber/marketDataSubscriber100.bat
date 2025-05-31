@echo off
SETLOCAL ENABLEDELAYEDEXPANSION

REM Key Variables
set JAVACMD=java
set JARFILE=marketDataSubscriber.jar
set COMPONENTBINDIR=%BINDIR%\marketDataSubscriber100

REM Argument Parsing Logic
set host=
set port=
set hosts=
set ports=
set CleanArgs=

REM Iterate through arguments
for %%i in (%*) do (
    set Q=%%i
    set COD=!Q:~0,1!
    
    IF !Q!==-cshost (
        set host=1
        set port=0
    ) ELSE (
        IF !Q!==-csport (
            set host=0
            set port=1
        ) ELSE (
            IF !COD!==- (
                set host=0
                set port=0
            ) ELSE (
                IF !host!==1 (
                    set hosts=!hosts! !Q!
                ) ELSE (
                    IF !port!==1 (
                        set ports=!ports! !Q!
                    ) ELSE (
                        REM Capture non-host/port arguments, excluding dashed options
                        IF !COD! NEQ - set CleanArgs=!CleanArgs! !Q!
                    )
                )
            )
        )
    )
)

REM Prepare extended variables for hosts and ports
set ExtVar=
if not "1%hosts%"=="1" set ExtVar=-cshost "%hosts%"
if not "1%ports%"=="1" set ExtVar=%ExtVar% -csport "%ports%"

REM Final command to launch Java application
%JAVACMD% -classpath %COMPONENTBINDIR%\%JARFILE% com.iontrading.samples.simplePatternSubscriber.Subscriber %ExtVar% %CleanArgs%
ENDLOCAL