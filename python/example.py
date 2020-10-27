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
import argparse
import sys

from pyarrow import flight


class DremioBasicServerAuthHandler(flight.ClientAuthHandler):
    """
    ClientAuthHandler for connections to Dremio server endpoint.
    """
    def __init__(self, username, password):
        self.username = username
        self.password = password
        super(flight.ClientAuthHandler, self).__init__()

    def authenticate(self, outgoing, incoming):
        """
        Authenticate with Dremio user credentials.
        """
        basic_auth = flight.BasicAuth(self.username, self.password)
        outgoing.write(basic_auth.serialize())
        self.token = incoming.read()

    def get_token(self):
        """
        Get the token from this AuthHandler.
        """
        return self.token


def parse_arguments():
    """
    Parses the command-line arguments supplied to the script.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-host', '--hostname', type=str, help='Dremio co-ordinator hostname',
      default='localhost')
    parser.add_argument('-port', '--flightport', type=str, help='Dremio flight server port',
      default='32010')
    parser.add_argument('-user', '--username', type=str, help='Dremio username',
      required=True)
    parser.add_argument('-pass', '--password', type=str, help='Dremio password',
      required=True)
    parser.add_argument('-query', '--sqlquery', type=str, help='SQL query to test',
      required=False)
    parser.add_argument('-tls', '--tls', dest='tls', help='Enable encrypted connection',
      required=False, default=False, action='store_true')
    parser.add_argument('-certs', '--trustedCertificates', type=str,
      help='Path to trusted certificates for encrypted connection', required=False)
    return parser.parse_args()


def connect_to_dremio_flight_server_endpoint(hostname, flightport, username, password, sqlquery,
  tls, certs):
    """
    Connects to Dremio Flight server endpoint with the provided credentials.
    It also runs the query and retrieves the result set.
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
                sys.exit()

        client = flight.FlightClient("{}://{}:{}".format(scheme, hostname, flightport),
          **connection_args)

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

            # Get the FlightInfo message to retrieve the Ticket corresponding
            # to the query result set.
            flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(sqlquery))
            print('[INFO] GetFlightInfo was successful')
            print('[INFO] Ticket: ', flight_info.endpoints[0].ticket)

            # Retrieve the result set as a stream of Arrow record batches.
            reader = client.do_get(flight_info.endpoints[0].ticket)
            print('[INFO] Reading query results from Dremio')
            print(reader.read_pandas())

    except Exception as exception:
        print("[ERROR] Exception: {}".format(repr(exception)))
        raise


if __name__ == "__main__":
    # Parse the command line arguments.
    args = parse_arguments()
    # Connect to Dremio Arrow Flight server endpoint.
    connect_to_dremio_flight_server_endpoint(args.hostname, args.flightport, args.username,
      args.password, args.sqlquery, args.tls, args.trustedCertificates)
