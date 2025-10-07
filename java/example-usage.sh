#!/bin/bash

# Example usage of the new Flight Client Configuration System
# This script demonstrates the priority order: env vars > properties file > command line > defaults

echo "=== Flight Client Configuration Examples ==="
echo

# Build the project first
echo "Building the project..."
mvn clean package -DskipTests -q

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build successful!"
echo

# Example 1: Using defaults only
echo "Example 1: Using defaults only"
echo "Command: java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT.jar --help"
java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT.jar --help
echo

# Example 2: Using command line arguments
echo "Example 2: Using command line arguments"
echo "Command: java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT.jar --hostname example.com --port 9999 --verbose --help"
java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT.jar --hostname example.com --port 9999 --verbose --help
echo

# Example 3: Using environment variables (highest priority)
echo "Example 3: Using environment variables (highest priority)"
echo "Setting: FLIGHT_HOST=env-server.com FLIGHT_PORT=8888 FLIGHT_VERBOSE=true"
FLIGHT_HOST=env-server.com FLIGHT_PORT=8888 FLIGHT_VERBOSE=true \
java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT.jar --hostname cmd-server.com --port 7777 --help
echo

# Example 4: Using properties file
echo "Example 4: Using properties file"
cat > demo.properties << EOF
# Demo configuration file
host=props-server.com
port=6666
verbose=true
user=props_user
EOF

echo "Created demo.properties with:"
cat demo.properties
echo
echo "Command: java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT.jar --configFile demo.properties --hostname cmd-server.com --help"
java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT.jar --configFile demo.properties --hostname cmd-server.com --help
echo

# Example 5: Mixed configuration (demonstrating priority order)
echo "Example 5: Mixed configuration (demonstrating priority order)"
echo "Environment: FLIGHT_HOST=env-priority.com"
echo "Properties file: host=props-priority.com, port=5555"
echo "Command line: --port 4444"
echo "Expected result: host=env-priority.com (env wins), port=4444 (cmd line wins over props)"

cat > priority-demo.properties << EOF
host=props-priority.com
port=5555
user=props_user
EOF

FLIGHT_HOST=env-priority.com \
java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT.jar \
--configFile priority-demo.properties \
--port 4444 \
--verbose \
--help

echo

# Cleanup
rm -f demo.properties priority-demo.properties

echo "=== Configuration Priority Order ==="
echo "1. Environment variables (FLIGHT_*) - HIGHEST PRIORITY"
echo "2. Properties file (--configFile or default)"
echo "3. Command line arguments"
echo "4. Default values - LOWEST PRIORITY"
echo
echo "=== Available Environment Variables ==="
echo "FLIGHT_HOST, FLIGHT_PORT, FLIGHT_USER, FLIGHT_PASS, FLIGHT_PAT,"
echo "FLIGHT_QUERY, FLIGHT_BINPATH, FLIGHT_TLS, FLIGHT_DSV,"
echo "FLIGHT_KEYSTOREPATH, FLIGHT_KEYSTOREPASS, FLIGHT_DEMO,"
echo "FLIGHT_ENGINE, FLIGHT_PROJECTID, FLIGHT_VERBOSE"
