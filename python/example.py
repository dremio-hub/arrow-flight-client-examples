"""
  Copyright (C) 2017-2021 Dremio Corporation

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
import certifi
import sys

from http.cookies import SimpleCookie
from pyarrow import flight


class DremioClientAuthMiddlewareFactory(flight.ClientMiddlewareFactory):
    """A factory that creates DremioClientAuthMiddleware(s)."""

    def __init__(self):
        self.call_credential = []

    def start_call(self, info):
        return DremioClientAuthMiddleware(self)

    def set_call_credential(self, call_credential):
        self.call_credential = call_credential


class DremioClientAuthMiddleware(flight.ClientMiddleware):
    """
    A ClientMiddleware that extracts the bearer token from 
    the authorization header returned by the Dremio 
    Flight Server Endpoint.

    Parameters
    ----------
    factory : ClientHeaderAuthMiddlewareFactory
        The factory to set call credentials if an
        authorization header with bearer token is
        returned by the Dremio server.
    """

    def __init__(self, factory):
        self.factory = factory

    def received_headers(self, headers):
        auth_header_key = 'authorization'
        authorization_header = []
        for key in headers:
            if key.lower() == auth_header_key:
                authorization_header = headers.get(auth_header_key)
        if not authorization_header:
            raise Exception('Did not receive authorization header back from server.')
        self.factory.set_call_credential([
            b'authorization', authorization_header[0].encode('utf-8')])


class CookieMiddlewareFactory(flight.ClientMiddlewareFactory):
    """A factory that creates CookieMiddleware(s)."""

    def __init__(self):
        self.cookies = {}

    def start_call(self, info):
        return CookieMiddleware(self)


class CookieMiddleware(flight.ClientMiddleware):
    """
    A ClientMiddleware that receives and retransmits cookies.
    For simplicity, this does not auto-expire cookies.

    Parameters
    ----------
    factory : CookieMiddlewareFactory
        The factory containing the currently cached cookies.
    """

    def __init__(self, factory):
        self.factory = factory

    def received_headers(self, headers):
        for key in headers:
            if key.lower() == 'set-cookie':
                cookie = SimpleCookie()
                for item in headers.get(key):
                    cookie.load(item)

                self.factory.cookies.update(cookie.items())

    def sending_headers(self):
        if self.factory.cookies:
            cookie_string = '; '.join("{!s}={!s}".format(key, val.value) for (key, val) in self.factory.cookies.items())
            return {b'cookie': cookie_string.encode('utf-8')}
        return {}


class KVParser(argparse.Action):
    def __call__(self, parser, namespace,
                 values, option_string=None):
        setattr(namespace, self.dest, list())
          
        for value in values:
            # split it into key and value
            key, value = value.split('=')
            # insert into list as key-value tuples
            getattr(namespace, self.dest).append((key.encode('utf-8'), value.encode('utf-8')))


def parse_arguments():
    """
    Parses the command-line arguments supplied to the script.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-host', '--hostname', type=str,
                        help='Dremio co-ordinator hostname. Defaults to \"localhost\".',
                        default='localhost')
    parser.add_argument('-port', '--flightport', dest='port', type=int,
                        help='Dremio flight server port. Defaults to 32010.',
                        default=32010)
    parser.add_argument('-user', '--username', type=str, help='Dremio username. Defaults to \"dremio\".',
                        default="dremio")
    parser.add_argument('-pass', '--password', type=str, help='Dremio password. Defaults to \"dremio123\".',
                        default="dremio123")
    parser.add_argument('-pat', '--personalAccessToken', '-authToken', '--authToken', dest='pat_or_auth_token', type=str,
                        help="Either a Personal Access Token or an OAuth2 Token.",
                        required=False)
    parser.add_argument('-query', '--sqlQuery', dest="query", type=str,
                        help='SQL query to test',
                        required=True)
    parser.add_argument('-tls', '--tls', dest='tls', help='Enable encrypted connection. Defaults to False.',
                        default=False, action='store_true')
    parser.add_argument('-dsv', '--disableServerVerification', dest='disable_server_verification', type=bool,
                        help='Disable TLS server verification. Defaults to False.',
                        default=False)
    parser.add_argument('-certs', '--trustedCertificates', dest='trusted_certificates', type=str,
                        help='Path to trusted certificates for encrypted connection. Defaults to system certificates.',
                        default=certifi.where())
    parser.add_argument('-sp', '--sessionProperty', dest='session_properties',
                        help="Key value pairs of SessionProperty, example: -sp schema=\'Samples.\"samples.dremio.com\"' -sp key=value",
                        required=False, nargs='*', action=KVParser)
    parser.add_argument('-engine', '--engine', type=str, help='The specific engine to run against.',
                        required=False)

    return parser.parse_args()


def connect_to_dremio_flight_server_endpoint(host, port, username, password, query,
                                             tls, certs, disable_server_verification, pat_or_auth_token,
                                             engine, session_properties):
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
            elif disable_server_verification:
                # Connect to the server endpoint with server verification disabled.
                print('[INFO] Disable TLS server verification.')
                connection_args['disable_server_verification'] = disable_server_verification
            else:
                print('[ERROR] Trusted certificates must be provided to establish a TLS connection')
                sys.exit()

        headers = session_properties
        if not headers:
            headers = []

        if engine:
            headers.append((b'routing_engine', engine.encode('utf-8')))

        # Two WLM settings can be provided upon initial authentication with the Dremio Server Flight Endpoint:
        # routing_tag
        # routing_queue
        headers.append((b'routing_tag', b'test-routing-tag'))
        headers.append((b'routing_queue', b'Low Cost User Queries'))

        client_cookie_middleware = CookieMiddlewareFactory()

        if pat_or_auth_token:
            client = flight.FlightClient("{}://{}:{}".format(scheme, host, port),
                                         middleware=[client_cookie_middleware], **connection_args)

            headers.append((b'authorization', "Bearer {}".format(pat_or_auth_token).encode('utf-8')))
            print('[INFO] Authentication skipped until first request')

        elif username and password:
            client_auth_middleware = DremioClientAuthMiddlewareFactory()
            client = flight.FlightClient("{}://{}:{}".format(scheme, host, port),
                                         middleware=[client_auth_middleware, client_cookie_middleware],
                                         **connection_args)

            # Authenticate with the server endpoint.
            bearer_token = client.authenticate_basic_token(username, password,
                                                           flight.FlightCallOptions(headers=headers))
            print('[INFO] Authentication was successful')
            headers.append(bearer_token)
        else:
            print('[ERROR] Username/password or PAT/Auth token must be supplied.')
            sys.exit()

        if query:
            # Construct FlightDescriptor for the query result set.
            flight_desc = flight.FlightDescriptor.for_command(query)
            print('[INFO] Query: ', query)

            # In addition to the bearer token, a query context can also
            # be provided as an entry of FlightCallOptions.
            # options = flight.FlightCallOptions(headers=[
            #     bearer_token,
            #     (b'schema', b'test.schema')
            # ])

            # Retrieve the schema of the result set.
            options = flight.FlightCallOptions(headers=headers)
            schema = client.get_schema(flight_desc, options)
            print('[INFO] GetSchema was successful')
            print('[INFO] Schema: ', schema)

            # Get the FlightInfo message to retrieve the Ticket corresponding
            # to the query result set.
            flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
            print('[INFO] GetFlightInfo was successful')
            print('[INFO] Ticket: ', flight_info.endpoints[0].ticket)

            # Retrieve the result set as a stream of Arrow record batches.
            reader = client.do_get(flight_info.endpoints[0].ticket, options)
            print('[INFO] Reading query results from Dremio')
            print(reader.read_pandas())

    except Exception as exception:
        print("[ERROR] Exception: {}".format(repr(exception)))
        raise


if __name__ == "__main__":
    # Parse the command line arguments.
    args = parse_arguments()
    # Connect to Dremio Arrow Flight server endpoint.
    connect_to_dremio_flight_server_endpoint(args.hostname, args.port, args.username, args.password,
                                             args.query, args.tls, args.trusted_certificates,
                                             args.disable_server_verification, args.pat_or_auth_token,
                                             args.engine, args.session_properties)

