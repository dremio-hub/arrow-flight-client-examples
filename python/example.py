"""
  Copyright (C) 2017-2018 Dremio Corporation
 
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""

from pyarrow import flight
import argparse
import traceback

class DremioBasicServerAuthHandler(flight.ClientAuthHandler):
  def __init__(self, username, password):
    self.username = username
    self.password = password
    super(flight.ClientAuthHandler, self).__init__()
  
  def authenticate(self, outgoing, incoming):
    # Authenticate with Dremio user credentials.
    basicAuth = flight.BasicAuth(self.username, self.password)
    outgoing.write(basicAuth.serialize())
    self.token = incoming.read()

  def get_token(self):
    return self.token

def parseArguments():
  """
  Parses the command-line arguments supplied to the script.
  """
  # Process command line arguments.
  parser = argparse.ArgumentParser()
  parser.add_argument('-host', '--hostname', type=str, help='Dremio co-ordinator hostname', default='localhost')
  parser.add_argument('-port', '--flightPort', type=str, help='Dremio flight server port', default='32010')
  parser.add_argument('-user', '--username', type=str, help='Dremio username', required=True)
  parser.add_argument('-pass', '--password', type=str, help='Dremio password', required=True)
  parser.add_argument('-query', '--sqlquery', type=str, help='SQL query to test', required=False)
  parser.add_argument('-tls', '--tls', dest='tls', help='Enable encrypted connection', required=False, default=False, action='store_true')
  parser.add_argument('-certs', '--trustedCertificates', type=str, help='Path to trusted certificates for encrypted connection', required=False)
  return parser.parse_args()

def connectToDremioFlightServerEndpoint(hostname, flightPort, username, password, sqlquery, tls, certs):
  """
   Connects to the Dremio Server and executes the specified Flight API calls.

  Args:
      hostname : Hostname to connect to.
      port : Flight server port.
      username : Dremio username to use for authentication.
      password : Dremio password to use for authentication.
      sqlquery : SQL query to test.
      tls : Enable encrypted connection.
      certs : Path to trusted certificates for a TLS connection.
  """

  try:
    # Default to use an unencrypted TCP connection.
    scheme = "grpc+tcp"
    connection_args = {}
    
    if tls:
      # Connect to the server endpoint with an encrypted TLS connection.
      print('[INFO] Enabling TLS connection')
      scheme = "grpc+tls"
      if certs:
        print('[INFO] Trusted certificates provided')
        # TLS certificates are provided in a list of connection arguments.
        with open(certs, "rb") as root_certs:
                connection_args["tls_root_certs"] = root_certs.read()
      else:
        print('[ERROR] Trusted certificates must be provided to establish a TLS connection')
        exit()

    client = flight.FlightClient(f"{scheme}://{hostname}:{flightPort}",**connection_args)
    
    # Authenticate with the server endpoint.
    client.authenticate(DremioBasicServerAuthHandler(username, password))
    print('[INFO] Authentication was successful')

    if sqlquery:
      # Construct FlightDescriptor for the query result set.
      flight_desc = flight.FlightDescriptor.for_command(sqlquery)
      print('[INFO] Query: ', sqlquery)
      
      # Retrieve the schema of the result set.
      schema = client.get_schema(flight_desc)
      print('[INFO] GetSchema was successful')
      print('[INFO] Schema: ', schema)
      
      # Get the FlightInfo message to retrieve the Ticket corresponding to the query result set.
      flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(sqlquery))
      print('[INFO] GetFlightInfo was successful')
      print('[INFO] Ticket: ', flight_info.endpoints[0].ticket)
      
      # Retrieve the result set as a stream of Arrow record batches.
      reader = client.do_get(flight_info.endpoints[0].ticket)
      print('[INFO] Reading query results from Dremio')
      print(reader.read_pandas())

  except Exception as e:
    print(f'[ERROR] Exception: {repr(e)}')
    # Uncomment the line below to enable stack trace printing.
    # traceback.print_exc()

if __name__ == "__main__":
  # Parse the command line arguments.
  args = parseArguments()
  # Connect to Dremio Arrow Flight server endpoint.
  connectToDremioFlightServerEndpoint(args.hostname, args.flightPort, args.username, args.password, args.sqlquery, args.tls, args.trustedCertificates)
