# Python Arrow Flight Client Application Example
![Build Status](https://github.com/dremio-hub/arrow-flight-client-examples/workflows/python-build/badge.svg)

This lightweight Python client application connects to the Dremio Arrow Flight server endpoint. Developers can use token based or regular user credentials (username/password) for authentication. Please not username/password is not supported for DCS. Any datasets in Dremio that are accessible by the provided Dremio user can be queried. By default, the hostname is `localhost` and the port is `32010`. Developers can change these default settings by providing the hostname and port as arguments when running the client.
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
