# Java Arrow Flight Client Application Example
![Build Status](https://github.com/dremio-hub/arrow-flight-client-examples/workflows/java-build/badge.svg)

This lightweight Java client application connects to the Dremio Arrow Flight server endpoint. Developers can use admin or regular user credentials for authentication. Any datasets in Dremio that are accessible by the provided Dremio user can be queried. By default, the hostname is `localhost` and the port is `32010`. Developers can change these default settings by providing the hostname and port as arguments when running the client. Moreover, the tls option can be provided to establish an encrypted connection.
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
- The application has a demo mode that runs an end-to-end demonstration without any arguments required, use the `-demo` flag to run the demo:
  - `java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -demo`
  - To run the demo, you must have a running Dremio instance at the specified host and port.
  - The Dremio instance must also have `services.flight.auth.mode: "arrow.flight.auth2"` set in the dremio.conf file.
- Only one of the following parameters must be provided for authentication: [password, pat/authToken].
- Run the Java sample application:
  - `java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -query <QUERY> -host <DREMIO_HOSTNAME> -port <DREMIO_PORT> -user <DREMIO_USER> -pass <DREMIO_PASSWORD>`
- Learn more about different command line options with the help menu:
  - `java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -h`

### Usage
```
usage: java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar <ARGUMENTS>

Arguments:
    -dsv, --disableServerVerification
      Disable TLS server verification.
      Defaults to false.
    -engine, --engine
      The specific engine to run query against.
    -port, --flightport
      Dremio flight server port.
      Defaults to 32010.
    -h, --help
      Show usage.
    -host, --hostname
      Dremio co-ordinator hostname.
      Defaults to "localhost".
    -kstpass, --keyStorePassword
      The jks keystore password.
    -kstpath, --keyStorePath
      Path to the jks keystore.
    -pass, --password
      Dremio password.
      Defaults to "dremio123".
    -pat, --personalAccessToken, -authToken, --authToken
      Either a Personal Access Token or an OAuth2 Token.
    -demo, --runDemo
      A flag to to run a demo of querying the Dremio Flight Server Endpoint.
      Defaults to false.
    -binpath, --saveBinaryPath
      Path to save the SQL result binary to.
    -sessionProperties, --sessionProperties
      Key value pairs of SessionProperty to be sent.
      Example: --sessionProperties key1:value1 key2:value2
    -query, --sqlQuery
      SQL query to test.
    -tls, --tls
      Enable encrypted connection.
      Defaults to false.
    -user, --username
      Dremio username.
      Defaults to "dremio".
    -projectId, --projectId
      Dremio Cloud project to connect to.
      Default: default project for organization
```
