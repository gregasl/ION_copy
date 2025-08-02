@echo off
set WORKINGDIR=
if "%1"=="" goto END_LIST

:au
if NOT "%1"=="marketDataSubscriber" goto END_LIST
	set SAMPLE_NAME=marketDataSubscriber
	set WORKINGDIR=RUN_marketDataSubscriber
	set MAINCLASS=com.iontrading.samples.marketDataSubscriber.marketDataSubscriber
	set SRC=src\com\iontrading\samples\marketDataSubscriber
	REM set INITFILE=mkv.jinit
goto END_LIST

:END_LIST

if "%WORKINGDIR%"=="" goto HELP
@echo WORKINGDIR=%WORKINGDIR%
@echo MAINCLASS=%MAINCLASS%
if not exist build mkdir build
if not exist build\classes mkdir build\classes

echo Compiling Java files...
REM if need JAVA 8
echo javac -source 1.8 -target 1.8 -cp "lib/*" -Xlint:unchecked -d build\classes %SRC%\*.java
javac -source 1.8 -target 1.8 -cp "lib/*" -Xlint:unchecked -d build\classes %SRC%\*.java

REM echo javac -cp "lib/*" -Xlint:unchecked -d build\classes %SRC%\*.java
REM javac -cp "lib/*" -Xlint:unchecked -d build\classes %SRC%\*.java

REM Check if JAR creation is requested
if "%2"=="jar" goto CREATE_JAR

if not exist "%WORKINGDIR%" (
    echo Error: Directory %WORKINGDIR% does not exist.
    exit /b 1
)

@echo off
cd "%WORKINGDIR%"

echo Running Java class: %MAINCLASS%
echo Classpath: lib\jmkv*.jar;lib\*;..\build\classes
echo java -cp "lib/*;..\build\classes" %MAINCLASS%
start cmd /k java -cp "../lib/*;..\build\classes" %MAINCLASS%
cd..
goto :EOF


:CREATE_JAR
echo Creating Fat JAR file for %SAMPLE_NAME%...

REM Convert dot notation to path notation for package structure
set CLASSPATH_DIR=%MAINCLASS:.=\%
for /f "tokens=1-3 delims=\" %%a in ("%CLASSPATH_DIR%") do set "PACKAGE_PATH=%%a\%%b\%%c"

REM Verify directory structure
if not exist build\classes\%PACKAGE_PATH%\%SAMPLE_NAME%\ (
    echo Error: Directory build\classes\%PACKAGE_PATH%\%SAMPLE_NAME%\ does not exist.
    exit /b 1
)

REM Create manifest
echo Manifest-Version: 1.0 > build\manifest.txt
echo Main-Class: %MAINCLASS% >> build\manifest.txt
echo Class-Path: . >> build\manifest.txt

REM Create temporary directory for library extraction
if exist temp-jar-extract rmdir /s /q temp-jar-extract
mkdir temp-jar-extract

REM Extract and add libraries
for %%i in (lib\*.jar) do (
    echo Processing library: %%i
    mkdir "temp-jar-extract\%%~ni"
    tar -xf "%%i" -C "temp-jar-extract\%%~ni"
)

REM Create initial JAR with main classes
jar cvfm "build\%SAMPLE_NAME%.jar" build\manifest.txt -C build\classes .

REM Add extracted library contents
cd temp-jar-extract
for /d %%D in (*) do (
    jar uvf "..\build\%SAMPLE_NAME%.jar" -C "%%D" .
)
cd ..

REM Add init file if specified
if not "%INITFILE%"=="" (
    if exist "%WORKINGDIR%\%INITFILE%" (
        echo Adding init file: %INITFILE%
        jar uvf "build\%SAMPLE_NAME%.jar" -C "%WORKINGDIR%" %INITFILE%
    )
)

REM Clean up
rmdir /s /q temp-jar-extract

REM Display JAR contents and running instructions
echo JAR Contents:
jar tf "build\%SAMPLE_NAME%.jar"
echo.
echo To run the JAR:
echo java -jar build\%SAMPLE_NAME%.jar

goto :EOF

:HELP
@echo.
echo Usage: %0 ^<project^> [jar]
echo Available Projects:
echo   - AdvOrderManagement
echo   - marketDataSubscriber
echo.
echo Options:
echo   - No second argument: Compile and Run
echo   - 'jar': Create JAR file only
exit /b 1

:END

