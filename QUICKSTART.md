
# Dremio and Arrow Flight Quickstart

## Documentation

- Documentation is available [here](https://docs.dremio.com).

- QUICKSTART at Dremio [Here](https://docs.dremio.com/quickstart/).

---

## 1 Query data using Flight clients
This process is the same if you launched the Dremio locally or via docker.

### 1.1 Query your datasets with arrow flight client in python

This lightweight Python client application connects to the Dremio Arrow Flight server endpoint. It requires the username and password for authentication. Developers can use admin or regular user credentials for authentication. Any datasets in Dremio that are accessible by the provided Dremio user can be queried. By default, the hostname is `localhost` and the port is `32010`. Developers can change these default settings by providing the hostname and port as arguments when running the client. Moreover, the tls option can be provided to establish an encrypted connection.

> Note: Trusted certificates must be provided when the tls option is enabled.
> 
#### 1.1.1 Prerequisites

-   Python 3

#### 1.1.2 Instructions on using this Python sample application

-   This application also requires `pyarrow` and `pandas`. Consider one of the dependency installation methods below. We recommend using `conda` for its ease of use.
	-   Install dependencies using `conda`
	    -   `conda install -c conda-forge --file requirements.txt`
	-   Alternatively, install dependencies using `pip`
	    -   `pip3 install -r requirements.txt`
-   Run the Python sample application:
    -   `python3 example.py -host '<DREMIO_HOST>' -user '<DREMIO_USERNAME>' -pass '<DREMIO_PASSWORD>'`

#### 1.1.3 Usage
```
example.py [-h] [-host HOSTNAME] [-port FLIGHTPORT] -user USERNAME -pass PASSWORD [-query SQLQUERY] [-tls] [-certs TRUSTEDCERTIFICATES]

optional arguments:
  -h, --help            show this help message and exit
  -host HOSTNAME, --hostname HOSTNAME Dremio co-ordinator hostname
  -port FLIGHTPORT, --flightPort FLIGHTPORT Dremio flight server port
  -user USERNAME, --username USERNAME Dremio username
  -pass PASSWORD, --password PASSWORD Dremio password
  -query SQLQUERY, --sqlquery SQLQUERY SQL query to test
  -tls, --tls Enable encrypted connection
  -certs TRUSTEDCERTIFICATES, --trustedCertificates TRUSTEDCERTIFICATES Path to trusted certificates for encrypted connection
```
---

### 1.2 Query your dataset with arrow flight client in java
This lightweight Java client application connects to the Dremio Arrow Flight server endpoint. It requires the username and password for authentication. Developers can use admin or regular user credentials for authentication. Any datasets in Dremio that are accessible by the provided Dremio user can be queried. By default, the hostname is `localhost` and the port is `32010`. Developers can change these default settings by providing the hostname and port as arguments when running the client. Moreover, the tls option can be provided to establish an encrypted connection.

#### 1.2.1 Prerequisites
-   Java 8
-   Maven 3.5 or above

#### 1.2.2 Build the Java sample application

-   Clone this [repository](https://github.com/dremio-hub/arrow-flight-client-examples).
-   Navigate to arrow-flight-client-examples/java.
-   Build the sample application on the command line with:
    -   `mvn clean install -DskipTests`

#### 1.2.3 Instructions on using this Java sample application

-   By default, the hostname is `localhost` and the port is `32010`.
-   Run the Java sample application:
    -   `java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -query <QUERY> -host <DREMIO_HOSTNAME> -user <DREMIO_USER> -pass <DREMIO_PASSWORD>`
-   The application has a demo mode that runs an end-to-end demonstration without any arguments required, use the `-demo` flag to run the demo:
    -   `java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -demo`
    -   To run the demo, you must have a running Dremio instance at the specified host and port.
    -   The Dremio instance must also have `services.flight.auth.mode: "arrow.flight.auth2"` set in the dremio.conf file.
-   Learn more about different command line options with the help menu:
    -   `java -jar target/java-flight-sample-client-application-1.0-SNAPSHOT-shaded.jar -h`
    -
#### 1.2.4  Usage

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

## Forums
If you have any questions, click [here](https://community.dremio.com/) to join our forums.
