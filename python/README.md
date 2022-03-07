# Python Arrow Flight Client Application Example
![Build Status](https://github.com/dremio-hub/arrow-flight-client-examples/workflows/python-build/badge.svg)

This lightweight Python client application connects to the Dremio Arrow Flight server endpoint. Developers can use token based or regular user credentials (username/password) for authentication. Please note username/password is not supported for DCS. Any datasets in Dremio that are accessible by the provided Dremio user can be queried. By default, the hostname is `localhost` and the port is `32010`. Developers can change these default settings by providing the hostname and port as arguments when running the client.
Moreover, the tls option can be provided to establish an encrypted connection.

### Instructions on using this Python sample application
- Install and setup Python3 as `pyarrow` requires Python3
- This application also requires `pyarrow` and `pandas`. Consider one of the dependency installation methods below. We recommend using `conda` for its ease of use.
- Install dependencies using `conda`
  - `conda install -c conda-forge --file requirements.txt`
- Alternatively, install dependencies using `pip` 
  - `pip3 install -r requirements.txt`
- Run the Python sample application with a local instance of Dremio (with default parameters):
  - `python3 example.py -query 'SELECT 1'`

```
usage: example.py [-h] [-host HOSTNAME] [-port PORT] [-user USERNAME] [-pass PASSWORD]
                  [-pat, -authToken PAT_OR_AUTH_TOKEN] [-query QUERY] [-tls] [-dsv DISABLE_SERVER_VERIFICATION]
                  [-certs TRUSTED_CERTIFICATES] [-sessionProperties [SESSION_PROPERTIES ...]] [-engine ENGINE]

optional arguments:
  -h, --help            show this help message and exit
  -host HOSTNAME, --hostname HOSTNAME
                        Dremio co-ordinator hostname. Defaults to "localhost".
  -port PORT, --flightport PORT
                        Dremio flight server port. Defaults to 32010.
  -user USERNAME, --username USERNAME
                        Dremio username. Defaults to "dremio".
  -pass PASSWORD, --password PASSWORD
                        Dremio password. Defaults to "dremio123".
  -pat PAT_OR_AUTH_TOKEN, --personalAccessToken PAT_OR_AUTH_TOKEN, -authToken PAT_OR_AUTH_TOKEN, --authToken PAT_OR_AUTH_TOKEN
                        Either a Personal Access Token or an OAuth2 Token.
  -query QUERY, --sqlQuery QUERY
                        SQL query to test.
  -tls, --tls           Enable encrypted connection. Defaults to False.
  -dsv DISABLE_SERVER_VERIFICATION, --disableServerVerification DISABLE_SERVER_VERIFICATION
                        Disable TLS server verification. Defaults to False.
  -certs TRUSTED_CERTIFICATES, --trustedCertificates TRUSTED_CERTIFICATES
                        Path to trusted certificates for encrypted connection. Defaults to system certificates.
  -sessionProperties [SESSION_PROPERTIES ...], --sessionProperties [SESSION_PROPERTIES ...]
                        Key value pairs of SessionProperty, example: -sessionProperties schema='Samples."samples.dremio.com"'
  -engine ENGINE, --engine ENGINE
                        The specific engine to run against.
```

### Getting Started

Get started with your first query to Dremio Cloud.

* The following example requires you to create a [Personal Access Token](https://docs.dremio.com/software/security/personal-access-tokens/) in Dremio. Replace ```<INSERT PAT HERE>``` in the example below with your actual PAT token.
* You may need to wait for a Dremio engine to start up if no Dremio engine for your Organization is running.

This example queries the Dremio Sample dataset ```NYC-taxi-trips``` and returns the first 10 values.

```python3 example.py -host data.dremio.cloud -port 443 -pat '<INSERT PAT HERE>' -tls -query 'SELECT * FROM Samples."samples.dremio.com"."NYC-taxi-trips" limit 10'```

Running the command will return the following.

``` [INFO] Enabling TLS connection
[INFO] Trusted certificates provided
[INFO] Authentication skipped until first request
[INFO] Query:  SELECT * FROM Samples."samples.dremio.com"."NYC-taxi-trips" limit 10
[INFO] GetSchema was successful
[INFO] Schema:  <pyarrow._flight.SchemaResult object at 0x7febe2944610>
[INFO] GetFlightInfo was successful
[INFO] Ticket:  <Ticket b'\nDSELECT * FROM Samples."samples.dremio.com"."NYC-taxi-trips" limit 10\x12^\n\\\nDSELECT * FROM Samples."samples.dremio.com"."NYC-taxi-trips" limit 10\x10(\x1a\x12\t\x8a\x883#\x12\xd1\xd9\x1d\x11\x00\xb3\xbbC\xdb\xd9J\t'>
[INFO] Reading query results from Dremio
      pickup_datetime  passenger_count  trip_distance_mi  fare_amount  tip_amount  total_amount
0 2013-05-27 19:15:00                1              1.26          7.5        0.00          8.00
1 2013-05-31 16:40:00                1              0.73          5.0        1.20          7.70
2 2013-05-27 19:03:00                2              9.23         27.5        5.00         38.33
3 2013-05-31 16:24:00                1              2.27         12.0        0.00         13.50
4 2013-05-27 19:17:00                1              0.71          5.0        0.00          5.50
5 2013-05-27 19:11:00                1              2.52         10.5        3.15         14.15
6 2013-05-31 16:41:00                5              1.01          6.0        1.10          8.60
7 2013-05-31 16:37:00                1              1.25          8.5        0.00         10.00
8 2013-05-31 16:39:00                1              2.04         10.0        1.50         13.00
9 2013-05-27 19:02:00                1             11.73         32.5        8.12         41.12
```

You have now ran your first Flight query on Dremio Cloud!
