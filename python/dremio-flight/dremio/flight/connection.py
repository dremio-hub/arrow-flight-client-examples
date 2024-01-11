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
import logging
from pyarrow import flight
from dremio.middleware.auth import DremioClientAuthMiddlewareFactory
from dremio.middleware.cookie import CookieMiddlewareFactory


logging.basicConfig(level=logging.INFO)


class DremioFlightEndpointConnection:
    def __init__(self, connection_args: dict) -> None:
        self.hostname = connection_args.get("hostname")
        self.port = connection_args.get("port")
        self.username = connection_args.get("username")
        self.password = connection_args.get("password")
        self.token = connection_args.get("token")
        self.tls = connection_args.get("tls")
        self.disable_certificate_verification = connection_args.get(
            "disable_certificate_verification"
        )
        self.path_to_certs = connection_args.get("path_to_certs")
        self.session_properties = connection_args.get("session_properties")
        self.engine = connection_args.get("engine")
        self._set_headers()

    def connect(self) -> flight.FlightClient:
        """Connects to Dremio Flight server endpoint with the
        provided credentials."""
        try:
            # Default to use an unencrypted TCP connection.
            scheme = "grpc+tcp"
            client_cookie_middleware = CookieMiddlewareFactory()
            tls_args = {}

            if self.tls:
                tls_args = self._set_tls_connection_args()
                scheme = "grpc+tls"

            if self.username and (self.password or self.token):
                return self._connect_to_software(
                    tls_args, client_cookie_middleware, scheme
                )

            elif self.token:
                return self._connect_to_cloud(
                    tls_args, client_cookie_middleware, scheme
                )

            raise ConnectionError(
                "username+password or username+token or token must be supplied."
            )

        except Exception:
            logging.exception(
                "There was an error trying to connect to the Dremio Flight Endpoint"
            )
            raise

    def _connect_to_cloud(
        self,
        tls_args: dict,
        client_cookie_middleware: CookieMiddlewareFactory,
        scheme: str,
    ) -> flight.FlightClient:
        client = flight.FlightClient(
            f"{scheme}://{self.hostname}:{self.port}",
            middleware=[client_cookie_middleware],
            **tls_args,
        )

        self.headers.append((b"authorization", f"Bearer {self.token}".encode("utf-8")))
        logging.info("Authentication skipped until first request")
        return client

    def _connect_to_software(
        self,
        tls_args: dict,
        client_cookie_middleware: CookieMiddlewareFactory,
        scheme: str,
    ) -> flight.FlightClient:
        client_auth_middleware = DremioClientAuthMiddlewareFactory()
        client = flight.FlightClient(
            f"{scheme}://{self.hostname}:{self.port}",
            middleware=[client_auth_middleware, client_cookie_middleware],
            **tls_args,
        )
        # Authenticate with the server endpoint.
        password_or_token = self.password if self.password else self.token
        bearer_token = client.authenticate_basic_token(
            self.username,
            password_or_token,
            flight.FlightCallOptions(headers=self.headers),
        )
        logging.info("Authentication was successful")
        self.headers.append(bearer_token)
        return client

    def _set_tls_connection_args(self) -> dict:
        # Connect to the server endpoint with an encrypted TLS connection.
        logging.debug(" Enabling TLS connection")
        tls_args = {}

        if self.disable_certificate_verification:
            # Connect to the server endpoint with server verification disabled.
            logging.info("Disable TLS server verification.")
            tls_args[
                "disable_server_verification"
            ] = self.disable_certificate_verification

        elif self.path_to_certs:
            logging.info("Trusted certificates provided")
            # TLS certificates are provided in a list of connection arguments.
            with open(self.path_to_certs, "rb") as root_certs:
                tls_args["tls_root_certs"] = root_certs.read()
        else:
            raise Exception(
                "Trusted certificates must be provided to establish a TLS connection"
            )

        return tls_args

    def _set_headers(self) -> list:
        self.headers = self.session_properties
        if not self.headers:
            self.headers = []

        if self.engine:
            self.headers.append((b"routing_engine", self.engine.encode("utf-8")))

        # Two WLM settings can be provided upon initial authentication with the Dremio Server Flight Endpoint:
        # routing_tag
        # routing_queue
        self.headers.append((b"routing_tag", b"test-routing-tag"))
        self.headers.append((b"routing_queue", b"Low Cost User Queries"))
