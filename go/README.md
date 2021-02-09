# Golang Arrow Flight Client Application Example

This lightweight Golang client app connects to the Dremio Arrow Flight server endpoint. It requires a username
and password for authentication. Developers can use admin or regular user credentials for authentication. Any
datasets in Dremio that are accessible by the provided Dremio user can be queried. By default, the hostname is
`localhost` and the port is `32010`. Developers can change these default settings by providing the hostname and
port as arguments when running the client. Moreover, the tls option can be provided to establish an encrypted
connection.

> Note: If the tls option is enabled and trusted certs are not provided, the app will attempt to use
  the host's default root CA bundle.

Instructions on using this Golang Sample application

* Install and setup Golang in your preferred way
  * Using your local package manager
  * Downloading from https://golang.org/dl/
* Build the app
  * `go build`
  * go will automatically download the dependencies
* Alternatively, you can just run the application directly
  * `go run .`
  * go will still automatically download the dependencies


```
$ go run . -h
Dremio Client Example.

Usage:
  example -h | --help
  example [--host=<hostname>] [--port=<port>] --user=<username> --pass=<password>
          [--query=<query>] [--tls] [--certs=<path>]

Options:
  -h --help           Show this help.
  --host=<hostname>   Dremio coordinator hostname [default: localhost]
  --port=<port>       Dremio flight server port [default: 32010]
  --user=<username>   Dremio username
  --pass=<password>   Dremio password
  --query=<query>     SQL Query to test.
  --tls               Enable encrypted connection.
  --certs=<path>      Path to trusted certificates for encrypted connection.
```
