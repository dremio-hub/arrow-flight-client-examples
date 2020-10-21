# Python Arrow Flight Client Exmaple
This lightweight Python client connects to the Dremio Arrow Flight service endpoint. This client requires the username and password for authentication. The hostname and port used is default to `localhost` and `32010`, these settings can be changed by providing the hostname and port as arguments when running the client. Moreover, the tls option can be provided to establish an encrypted connection. Please note that trusted certificates must be provided when the tls option is enabled. This client can query any datasets in Dremio that are accessible by the Dremio user provided.

## Instructions on using this Python client
- Install and setup Python3 as `pyarrow` requires Python3
- Install `pyarrow` package using pip
  - `pip3 install pyarrow`
- Install `pandas` package using pip
  - `pip3 install pandas`
- Run the Python client:
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