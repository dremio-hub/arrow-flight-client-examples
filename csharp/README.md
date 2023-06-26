# C\# Arrow Flight Client Application Example

This simple example C-sharp client application connects to the Dremio Arrow Flight server endpoint. Developers can use admin or regular user credentials for authentication. Any datasets in Dremio that are accessible by the provided Dremio user can be queried. By default, the hostname is `localhost` and the port is `32010`. Developers can change these default settings by providing the hostname and port as arguments when running the client.

Note: This uses Microsoft.Data.Analysis as an example library for working with the data -- this is similar to python pandas.  However, the python pandas DataFrame is more mature and supports more data types.  There are some basic checks and examples of exceptions to look out for due to this issue.

### Prerequisites
- dotnet 7 [sdk](https://dotnet.microsoft.com/en-us/download/dotnet/7.0)
- Dremio 21 or later 

NOTE: This code was tested using MacOS x64 with localhost running on Docker
  - `docker run -p 9047:9047 -p 31010:31010 -p 32010:32010 dremio/dremio-oss:latest`
  - For quick setup, login to your local docker instance using http://localhost:9047 in a browser to add the 'dremio/dremio123' user as ADMIN

### Build the C\# sample application
- Clone this repository.
  - `git clone https://github.com/dremio-hub/arrow-flight-client-examples`
- Navigate to arrow-flight-client-examples/csharp/example.
  - `cd csharp/example`
- Build the sample application on the command line with:
  - `dotnet build`

### Instructions on using this C\# sample application
- By default, the hostname is `localhost` and the port is `32010` with user `dremio` and password `dremio123`.  There is also a default query on Samples datasource
  - `dotnet run`
  - NOTE: To use the default query you will need to first add the Samples datasource in Dremio.  "Format" the zips.json file in the Dremio.
- Run the dotnet sample application with command line args:
  - `dotnet run -query <QUERY> -host <DREMIO_HOSTNAME> -port <DREMIO_PORT> -user <DREMIO_USER> -pass <DREMIO_PASSWORD>`
  - `dotnet run -host localhost -user dremio -pass dremio123 -port 32010 -query "SELECT job_id, status, query from sys.jobs"`

### Usage
```
usage: dotnet run <ARGUMENTS>

Arguments:
    -port
      Dremio flight server port.
      Defaults to 32010. Use 443 for Dremio Cloud.
    -host
      Dremio coordinator hostname.
      Defaults to "localhost".  Use data.dremio.cloud for Dremio Cloud.
    -pass
      Dremio password.
      Defaults to "dremio123".
    -pat
      Personal access token -- use instead of password when authenticating.  This is required for Dremio Cloud.
    -protocol
      The protocol to use when connecting -- use 'https' for production or Dremio Cloud
    -query
      SQL query to test.
    -user
      Dremio username.
      Defaults to "dremio".
```

### Usage with Dremio Cloud

The following example is how to run a Dremio Cloud query.  Dremio Cloud has the following differences from previous examples:

-  Host name is data.dremio.cloud or data.eu.dremio.cloud
-  Port is 443
-  Protocol is https
-  Only Personal access token authentication supported
- `dotnet run -protocol https -host data.dremio.cloud -port 443 -pat abc123abc123abc123abc123 -query "SELECT job_id, status, query from sys.project.jobs"`


