#!/bin/bash

# Build script for REDIS-ION Bridge

# Set variables
APP_NAME="redisbridge"
SRC_DIR="src"
BUILD_DIR="build"
LIB_DIR="lib"
MANIFEST_FILE="manifest.txt"

# Create build directory if it doesn't exist
mkdir -p $BUILD_DIR

# Clean previous build
echo "Cleaning previous build..."
rm -rf $BUILD_DIR/*

# Compile Java files
echo "Compiling Java files..."
javac -cp "$LIB_DIR/*" -d $BUILD_DIR $SRC_DIR/com/iontrading/redisbridge/*.java $SRC_DIR/com/iontrading/redisbridge/admin/*.java $SRC_DIR/com/iontrading/redisbridge/config/*.java $SRC_DIR/com/iontrading/redisbridge/heartbeat/*.java $SRC_DIR/com/iontrading/redisbridge/instrument/*.java $SRC_DIR/com/iontrading/redisbridge/ion/*.java $SRC_DIR/com/iontrading/redisbridge/order/*.java $SRC_DIR/com/iontrading/redisbridge/redis/*.java $SRC_DIR/com/iontrading/redisbridge/trader/*.java $SRC_DIR/com/iontrading/redisbridge/util/*.java

# Check if compilation was successful
if [ $? -ne 0 ]; then
    echo "Compilation failed!"
    exit 1
fi

# Create JAR file
echo "Creating JAR file..."
jar cfm $BUILD_DIR/$APP_NAME.jar $MANIFEST_FILE -C $BUILD_DIR .

# Check if JAR creation was successful
if [ $? -ne 0 ]; then
    echo "JAR creation failed!"
    exit 1
fi

# Copy configuration file
echo "Copying configuration file..."
mkdir -p $BUILD_DIR/config
cp $SRC_DIR/com/iontrading/redisbridge/config/config.properties $BUILD_DIR/config/

# Copy library files
echo "Copying library files..."
mkdir -p $BUILD_DIR/lib
cp $LIB_DIR/*.jar $BUILD_DIR/lib/

# Create logs directory
echo "Creating logs directory..."
mkdir -p $BUILD_DIR/logs

# Create run script
echo "Creating run script..."
cat > $BUILD_DIR/run.sh << EOF
#!/bin/bash
java -jar $APP_NAME.jar --config config/config.properties \$@
EOF

# Make run script executable
chmod +x $BUILD_DIR/run.sh

echo "Build completed successfully!"
echo "To run the application, execute: cd $BUILD_DIR && ./run.sh"
