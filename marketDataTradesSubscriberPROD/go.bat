@echo off
set WORKINGDIR=
if "%1"=="" goto END_LIST

:au
if NOT "%1"=="marketDataSubscriberPROD" goto END_LIST
	set SAMPLE_NAME=marketDataSubscriberPROD
	set WORKINGDIR=RUN_marketDataSubscriberPROD
	set MAINCLASS=com.iontrading.samples.marketDataSubscriberPROD.marketDataSubscriberPROD
	set SRC=src\com\iontrading\samples\marketDataSubscriberPROD
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

cd "%WORKINGDIR%"
echo Running Java class: %MAINCLASS%
echo Classpath: lib\jmkv*.jar;lib\*;..\build\classes
echo java -cp "lib/*;..\build\classes" %MAINCLASS%
start cmd /k java -cp "../lib/*;..\build\classes" %MAINCLASS%
cd..
goto :EOF

@echo off
:CREATE_JAR
echo Creating Fat JAR file for %SAMPLE_NAME%...

REM Create a temporary directory for unpacking libraries
if exist temp-jar-extract rmdir /s /q temp-jar-extract
mkdir temp-jar-extract

REM Unpack all library JARs
for %%i in (lib\*.jar) do (
    echo Extracting %%i...
    mkdir "temp-jar-extract\%%~ni"
    tar -xf "%%i" -C "temp-jar-extract\%%~ni"
)

REM Create manifest with proper classpath
echo Manifest-Version: 1.0 > build\manifest.txt
echo Main-Class: %MAINCLASS% >> build\manifest.txt
echo Class-Path: . >> build\manifest.txt

REM Create JAR with all compiled classes and extracted library classes
echo Creating comprehensive JAR...
jar cvfm "build\%SAMPLE_NAME%.jar" build\manifest.txt -C build\classes .

REM Add all extracted library contents to the JAR
echo Adding library contents...
cd temp-jar-extract
for /d %%D in (*) do (
    jar uvf "..\build\%SAMPLE_NAME%.jar" -C "%%D" .
)
cd ..

REM Add init file if exists
if not "%INITFILE%"=="" (
    if exist "%WORKINGDIR%\%INITFILE%" (
        echo Adding init file to JAR...
        jar uvf "build\%SAMPLE_NAME%.jar" -C "%WORKINGDIR%" %INITFILE%
    )
)

REM Clean up temporary extraction directory
rmdir /s /q temp-jar-extract

REM Verify JAR contents
echo JAR Contents:
jar tf "build\%SAMPLE_NAME%.jar"

REM Suggest running command
echo To run the JAR, use:
echo java -jar build\%SAMPLE_NAME%.jar

goto :EOF

:HELP
@echo.
echo Usage: %0 ^<project^> [jar]
echo Available Projects:
echo   - marketDataSubscriberPROD
echo.
echo Options:
echo   - No second argument: Compile and Run
echo   - 'jar': Create JAR file only
exit /b 1

:END

