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
  example [--host=<hostname>] [--port=<port>] (--user=<username> --pass=<password> | --pat=<pat>)
          [--tls] [--certs=<path>] [--query <query>] [--project_id=<project_id>]

Options:
  -h --help           Show this help.
  --host=<hostname>   Dremio coordinator hostname [default: localhost]
  --port=<port>       Dremio flight server port [default: 32010]
  --user=<username>   Dremio username
  --pass=<password>   Dremio password
  --pat=<pat>         Dremio personal access token
  --query <query>     SQL Query to test.
  --tls               Enable encrypted connection.
  --certs=<path>      Path to trusted certificates for encrypted connection.
  --project_id=<project_id>   Dremio project ID
```

## Example

If you have a local instance of Dremio running on `localhost:32010` with a user `dremio` and password `dremio123`, 
you can run the following command to connect and do a simple query:
```
go run . --user=dremio --pass=dremio123 --query="SELECT * FROM (VALUES(1,2,3))" 
```
To connect to a Dremio Cloud instance with data organized in different folders, you must use TLS and generate a Personal 
Access Token (PAT) for authentication. If your data resides in a specific project, you can specify the project by 
providing its project_id. If the project_id is not provided, the connection will use the default project.

You can run a command similar to the following::
```
go run . --host=<cloud.hostname> --port=443 --query="SELECT * FROM \"Samples\".\"samples.dremio.com\".\"NYC-taxi-trips\"" --tls --pat=<mypat> --project_id=<myprojectid>
```
Here we're querying for a dataset called `NYC-taxi-trips`, in a source called `Samples`, in the `samples.dremio.com` folder.

## Tests

To run the tests, you'll need a flight client mock class. This class is generated using mockgen. To aid in this process,
we created a script that generates the mock class and runs all tests. You can run the script using the following command:
```bash
./run_tests.sh 
```

If the mock class is already generated you can alternatively use the following command:
```go
go test -v
```
