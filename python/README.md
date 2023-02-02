# Python Arrow Flight Client Application Example

## Getting Started
1. Install [Python 3](https://www.python.org/downloads/)
2. Download and install the [dremio-flight-endpoint whl file](https://github.com/dremio-hub/arrow-flight-client-examples/releases)
    - `python -m pip install <PATH TO WHEEL>` 
3. Copy the contents of arrow-flight-client-examples/python/example.py into your own python file. 
4. Run your python file with a local instance of Dremio:
    - `python3 example.py -username <USER> -password <password> -query 'SELECT 1'`

## How to connect to Dremio Cloud

Get started with your first query to Dremio Cloud.

* The following example requires you to create a [Personal Access Token](https://docs.dremio.com/cloud/security/authentication/personal-access-token/) in Dremio. Replace ```<INSERT PAT HERE>``` in the example below with your actual PAT token.
* You may need to wait for a Dremio engine to start up or start it manually if no Dremio engine for your Organization is running.

This example queries the Dremio Sample dataset ```NYC-taxi-trips``` and returns the first 10 values.

```python3 example.py -host data.dremio.cloud -port 443 -pat '<INSERT PAT HERE>' -tls -query 'SELECT * FROM Samples."samples.dremio.com"."NYC-taxi-trips" limit 10'```

You have now run your first Flight query on Dremio Cloud!

## Configuration Options

```
usage: example.py [-h] [-host HOSTNAME] [-port PORT] -user USERNAME -pass PASSWORD -pat TOKEN -query QUERY [-tls] [-dcv DISABLE_CERTIFICATE_VERIFICATION]
                  [-path_to_certs PATH_TO_CERTS] [-sp [SESSION_PROPERTIES ...]] [-engine ENGINE]

optional arguments:
  -h, --help            show this help message and exit
  -host HOSTNAME, --hostname HOSTNAME
                        Dremio co-ordinator hostname. Defaults to "localhost".
  -port PORT, --flightport PORT
                        Dremio flight server port. Defaults to 32010.
  -user USERNAME, --username USERNAME
                        Dremio username. Not required when connecting to Dremio Cloud
  -pass PASSWORD, --password PASSWORD
                        Dremio password. Not required when connecting to Dremio Cloud
  -pat TOKEN, --token TOKEN
                        Either a Personal Access Token or an OAuth2 Token.
  -query QUERY, --sqlQuery QUERY
                        SQL query to test. Must be enclosed in single quotes. If single quotes are already present within the query, change those to double quotes and
                        enclose entire query in single quotes.
  -tls, --tls           Enable encrypted connection. Defaults to False.
  -dcv DISABLE_CERTIFICATE_VERIFICATION, --disableCertificateVerification DISABLE_CERTIFICATE_VERIFICATION
                        Disables TLS server verification. Defaults to False.
  -path_to_certs PATH_TO_CERTS, --trustedCertificates PATH_TO_CERTS
                        Path to trusted certificates for encrypted connection. Defaults to system certificates.
  -sp [SESSION_PROPERTIES ...], --sessionProperty [SESSION_PROPERTIES ...]
                        Key value pairs of SessionProperty, example: -sp schema='Samples."samples.dremio.com"' -sp key=value
  -engine ENGINE, --engine ENGINE
                        The specific engine to run against. Only applicable to Dremio Cloud.

```

## Description
![Build Status](https://github.com/dremio-hub/arrow-flight-client-examples/workflows/python-build/badge.svg)
This lightweight Python client application connects to the Dremio Arrow Flight server endpoint. Developers can use token based or regular user credentials (username/password) for authentication. Please note username/password is not supported for Dremio Cloud. Any datasets in Dremio that are accessible by the provided Dremio user can be queried. By default, the hostname is `localhost` and the port is `32010`. Developers can change these default settings by providing the hostname and port as arguments when running the client.
Moreover, the tls option can be provided to establish an encrypted connection.