@echo off
REM Build script for REDIS-ION Bridge on Windows

REM Set variables
set APP_NAME=redisbridge
set SRC_DIR=src
set BUILD_DIR=build
set LIB_DIR=lib
set MANIFEST_FILE=manifest.txt

REM Create build directory if it doesn't exist
if not exist %BUILD_DIR% mkdir %BUILD_DIR%

REM Clean previous build
echo Cleaning previous build...
if exist %BUILD_DIR%\* del /Q /S %BUILD_DIR%\*

REM Compile Java files
echo Compiling Java files...
javac -cp "%LIB_DIR%\*" -d %BUILD_DIR% %SRC_DIR%\com\iontrading\redisbridge\*.java %SRC_DIR%\com\iontrading\redisbridge\admin\*.java %SRC_DIR%\com\iontrading\redisbridge\config\*.java %SRC_DIR%\com\iontrading\redisbridge\heartbeat\*.java %SRC_DIR%\com\iontrading\redisbridge\instrument\*.java %SRC_DIR%\com\iontrading\redisbridge\ion\*.java %SRC_DIR%\com\iontrading\redisbridge\order\*.java %SRC_DIR%\com\iontrading\redisbridge\redis\*.java %SRC_DIR%\com\iontrading\redisbridge\trader\*.java %SRC_DIR%\com\iontrading\redisbridge\util\*.java

REM Check if compilation was successful
if %ERRORLEVEL% neq 0 (
    echo Compilation failed!
    exit /b 1
)

REM Create JAR file
echo Creating JAR file...
jar cfm %BUILD_DIR%\%APP_NAME%.jar %MANIFEST_FILE% -C %BUILD_DIR% .

REM Check if JAR creation was successful
if %ERRORLEVEL% neq 0 (
    echo JAR creation failed!
    exit /b 1
)

REM Copy configuration file
echo Copying configuration file...
if not exist %BUILD_DIR%\config mkdir %BUILD_DIR%\config
copy %SRC_DIR%\com\iontrading\redisbridge\config\config.properties %BUILD_DIR%\config\

REM Copy library files
echo Copying library files...
if not exist %BUILD_DIR%\lib mkdir %BUILD_DIR%\lib
copy %LIB_DIR%\*.jar %BUILD_DIR%\lib\

REM Create logs directory
echo Creating logs directory...
if not exist %BUILD_DIR%\logs mkdir %BUILD_DIR%\logs

REM Create run script
echo Creating run script...
echo @echo off > %BUILD_DIR%\run.bat
echo java -jar %APP_NAME%.jar --config config\config.properties %%* >> %BUILD_DIR%\run.bat

echo Build completed successfully!
echo To run the application, execute: cd %BUILD_DIR% ^&^& run.bat
