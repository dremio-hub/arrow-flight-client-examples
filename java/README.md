# Java Arrow Flight Client Application Example
![Build Status](https://github.com/dremio-hub/arrow-flight-client-examples/workflows/java-build/badge.svg)

This lightweight Java client application connects to the Dremio Arrow Flight server endpoint. It requires the username and password for authentication. Developers can use admin or regular user credentials for authentication. Any datasets in Dremio that are accessible by the provided Dremio user can be queried. By default, the hostname is `localhost` and the port is `32010`. Developers can change these default settings by providing the hostname and port as arguments when running the client. Moreover, the tls option can be provided to establish an encrypted connection. 
> Note that: Trusted certificates must be provided when the tls option is enabled.

### Prerequisites
- Java 8 
- Maven 3.5 or above
- Dremio version 12.0+

### Build the Java sample application
- Clone this repository.
- Navigate to arrow-flight-client-examples/java.
- Build the sample application on the command line with:
  - `mvn clean install -DskipTests` 

### Instructions on using this Java sample application
- By default, the hostname is `localhost` and the port is `32010`.
- Run the Java sample application:
  - `java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -query <QUERY> -host <DREMIO_HOSTNAME> -user <DREMIO_USER> -pass <DREMIO_PASSWORD>`
- The application has a demo mode that runs an end-to-end demonstration without any arguments required, use the `-demo` flag to run the demo:
  - `java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -demo`
  - To run the demo, you must have a running Dremio instance at the specified host and port.
  - The Dremio instance must also have `services.flight.auth.mode: "arrow.flight.auth2"` set in the dremio.conf file. 
- Learn more about different command line options with the help menu:
  - `java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -h` 

### Usage
```
usage: java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -query <QUERY> -host <DREMIO_HOSTNAME> -port <DREMIO_PORT> -user <DREMIO_USER> -pass <DREMIO_PASSWORD>

optional arguments:
  -h, --help            
    show this help message and exit
  -port, --flightport
    Dremio flight server port
    Default: 32010
  -host, --hostname
    Dremio co-ordinator hostname
    Default: localhost
  -kstpass, --keyStorePassword
    The jks keystore password
  -kstpath, --keyStorePath
    Path to the jks keystore
  -pass, --password
    Dremio password
    Default: dremio123
  -demo, --runDemo
    A flag to to run a demo of querying the Dremio Flight Server Endpoint.
    Default: false
  -query, --sqlQuery
    SQL query to test
  -tls, --tls
    Enable encrypted connection
    Default: false
  -user, --username
    Dremio username
    Default: dremio
```

