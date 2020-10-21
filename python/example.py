from pyarrow import flight
import argparse
import traceback

# DremioServerClientAuthHandler extends flight.ClientAuthHandler
class DremioBasicServerAuthHandler(flight.ClientAuthHandler):
  # Constructor
  def __init__(self, username, password):
    self.username = username
    self.password = password
    super(flight.ClientAuthHandler, self).__init__()

  def authenticate(self, outgoing, incoming):
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

def connectToFlightServer(hostname, flightPort, username, password, sqlquery, tls, certs):
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
    scheme = "grpc+tcp"
    connection_args = {}
    if tls:
      print('[INFO] Enabling TLS connection')
      scheme = "grpc+tls"
      if certs:
        print('[INFO] Trusted certificates provided')
        with open(certs, "rb") as root_certs:
                connection_args["tls_root_certs"] = root_certs.read()
      else:
        print('[ERROR] Trusted certificates must be provided to establish a TLS connection')
        exit()

    client = flight.FlightClient(f"{scheme}://{hostname}:{flightPort}",**connection_args)
    client.authenticate(DremioBasicServerAuthHandler(username, password))
    print('[INFO] Authentication was successful')

    if sqlquery:
      flight_desc = flight.FlightDescriptor.for_command(sqlquery)
      schema = client.get_schema(flight_desc)
      print('[INFO] GetSchema was successful')
      print('[INFO] Schema: ', schema)
      flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(sqlquery))
      print('[INFO] GetFlightInfo was successful')
      print('[INFO] Ticket: ', flight_info.endpoints[0].ticket)
      reader = client.do_get(flight_info.endpoints[0].ticket)
      print('[INFO] Reading query results from Dremio')
      print(reader.read_pandas())
      print('[INFO] GetStream was successful')

  except Exception as e:
    # Print the exception along with the stack trace.
    print(f'[ERROR] Exception: {repr(e)}')
    traceback.print_exc()

if __name__ == "__main__":
  # Parse the command line arguments.
  args = parseArguments()
  # Connecto to Dremio Arrow Flight server.
  connectToFlightServer(args.hostname, args.flightPort, args.username, args.password, args.sqlquery, args.tls, args.trustedCertificates)
