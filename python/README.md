# Python Arrow Flight Client Application Example
![Build Status](https://github.com/dremio-hub/arrow-flight-client-examples/workflows/python-build/badge.svg)

This lightweight Python client application connects to the Dremio Arrow Flight server endpoint. Developers can use token based or regular user credentials (username/password) for authentication. Please note username/password is not supported for Dremio Cloud. Any datasets in Dremio that are accessible by the provided Dremio user can be queried. By default, the hostname is `localhost` and the port is `32010`. Developers can change these default settings by providing the hostname and port as arguments when running the client.
Moreover, the tls option can be provided to establish an encrypted connection.

### Instructions on using this Python sample application
- Install and setup Python3 as `pyarrow` requires Python3
- This application also requires `pyarrow` and `pandas`. 
- Install dependencies using `pip` 
  - `pip3 install -r requirements.txt`
- Run the Python sample application with a local instance of Dremio (with default parameters):
  - `python3 example.py -username <USER> -password <password> -query 'SELECT 1'`

```
usage: example.py [-h] [-host HOSTNAME] [-port PORT] -user USERNAME -pass PASSWORD -pat TOKEN -query QUERY [-tls]
                  [-dcv DISABLE_CERTIFICATE_VERIFICATION] [-path_to_certs PATH_TO_CERTS] [-sp [SESSION_PROPERTIES ...]]
                  [-engine ENGINE]

optional arguments:
  -h, --help            show this help message and exit
  -host HOSTNAME, --hostname HOSTNAME
                        Dremio co-ordinator hostname. Defaults to "localhost".
  -port PORT, --flightport PORT
                        Dremio flight server port. Defaults to 32010.
  -user USERNAME, --username USERNAME
                        Dremio username.
  -pass PASSWORD, --password PASSWORD
                        Dremio password.
  -pat TOKEN, --token TOKEN
                        Either a Personal Access Token or an OAuth2 Token.
  -query QUERY, --sqlQuery QUERY
                        SQL query to test
  -tls, --tls           Enable encrypted connection. Defaults to False.
  -dcv DISABLE_CERTIFICATE_VERIFICATION, --disableCertificateVerification DISABLE_CERTIFICATE_VERIFICATION
                        Disables TLS server verification. Defaults to False.
  -path_to_certs PATH_TO_CERTS, --trustedCertificates PATH_TO_CERTS
                        Path to trusted certificates for encrypted connection. Defaults to system certificates.
  -sp [SESSION_PROPERTIES ...], --sessionProperty [SESSION_PROPERTIES ...]
                        Key value pairs of SessionProperty, example: -sp schema='Samples."samples.dremio.com"' -sp key=value
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

``` INFO:root:Trusted certificates provided
INFO:root:Authentication skipped until first request
INFO:root:GetFlightInfo was successful
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

You have now run your first Flight query on Dremio Cloud!
