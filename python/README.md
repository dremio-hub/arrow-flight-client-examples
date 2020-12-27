# Python Arrow Flight Client Application Example
![Build Status](https://github.com/dremio-hub/arrow-flight-client-examples/workflows/python-build/badge.svg)

This lightweight Python client application connects to the Dremio Arrow Flight server endpoint. It requires the username and password for authentication. Developers can use admin or regular user credentials for authentication. Any datasets in Dremio that are accessible by the provided Dremio user can be queried. By default, the hostname is `localhost` and the port is `32010`. Developers can change these default settings by providing the hostname and port as arguments when running the client. Moreover, the tls option can be provided to establish an encrypted connection. 
> Note: Trusted certificates must be provided when the tls option is enabled.

### Instructions on using this Python sample application
- Install and setup Python3 as `pyarrow` requires Python3
- This application also requires `pyarrow` and `pandas`. Consider one of the dependency installation methods below. We recommend using `conda` for its ease of use.
- Install dependencies using `conda`
  - `conda install -c conda-forge --file requirements.txt`
- Alternatively, install dependencies using `pip` 
  - `pip3 install -r requirements.txt`
- Run the Python sample application:
  - `python3 example.py -host '<DREMIO_HOST>' -user '<DREMIO_USERNAME>' -pass '<DREMIO_PASSWORD>'`

```
usage: example.py [-h] [-host HOSTNAME] [-port FLIGHTPORT] -user USERNAME -pass PASSWORD [-query SQLQUERY] [-tls] [-certs TRUSTEDCERTIFICATES]

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