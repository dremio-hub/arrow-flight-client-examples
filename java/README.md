# Java Arrow Flight Client Application Example
![Build Status](https://github.com/dremio-hub/arrow-flight-client-examples/workflows/java-build/badge.svg)

### Pre-requisites
- Java 8 
- Maven 3.5 or above

### Steps to run this sample application
- Clone this repository.
- Navigate to arrow-flight-client-examples/java.
- Build the sample application on the command line with:
  - `mvn clean install -DskipTests` 
- Run the sample application on the command line with:
  - `jar -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar` 
- The application has a demo mode that runs an end-to-end demonstration without any arguments required, use the `-demo` flag to run the demo:
  - `jar -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -demo`
  - Note that it uses `localhost` and port `32010` by default. If you wish to use a different host and port, please use the `-host` and `-port` flags to specify the values.
  - To ru the demo, you must have a running Dremio instance at the specified host and port.
  - The Dremio instance must also have `services.flight.auth.mode: "arrow.flight.auth2"` set in the dremio.conf file. 
- Without the `-demo` flag, the regular mode requires the `-query` flag and uses `localhost` and `32010` by default:
  - `jar -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -query "select * from table"` 
- Learn more about different configurations with the help menu:
  - `jar -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -help` 




