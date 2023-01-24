from pyarrow import flight
from argparse import Namespace
from dremio_middleware import DremioClientAuthMiddlewareFactory
from cookie_middleware import CookieMiddlewareFactory
import logging

logging.basicConfig(level=logging.INFO)


class DremioFlightEndpointConnection:
    def __init__(self, connection_args: Namespace) -> None:
        self.hostname = connection_args.hostname
        self.port = connection_args.port
        self.username = connection_args.username
        self.password = connection_args.password
        self.token = connection_args.token
        self.tls = connection_args.tls
        self.disable_certificate_verification = (
            connection_args.disable_certificate_verification
        )
        self.path_to_certs = connection_args.path_to_certs
        self.session_properties = connection_args.session_properties
        self.engine = connection_args.engine
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

            if self.token:
                return self._connect_with_pat(
                    tls_args, client_cookie_middleware, scheme
                )

            elif self.username and self.password:
                return self._connect_with_password(
                    tls_args, client_cookie_middleware, scheme
                )

            else:
                raise ConnectionError(
                    "Username/password or PAT/Auth token must be supplied."
                )

        except Exception:
            logging.exception(
                "There was an error trying to connect to the Dremio Flight Endpoint"
            )
            raise

    def _connect_with_pat(
        self,
        tls_args: dict,
        client_cookie_middleware: CookieMiddlewareFactory,
        scheme: str,
    ) -> flight.FlightClient:
        client = flight.FlightClient(
            "{}://{}:{}".format(scheme, self.hostname, self.port),
            middleware=[client_cookie_middleware],
            **tls_args
        )

        self.headers.append(
            (b"authorization", "Bearer {}".format(self.token).encode("utf-8"))
        )
        logging.info("Authentication skipped until first request")
        return client

    def _connect_with_password(
        self,
        tls_args: dict,
        client_cookie_middleware: CookieMiddlewareFactory,
        scheme: str,
    ) -> flight.FlightClient:
        client_auth_middleware = DremioClientAuthMiddlewareFactory()
        client = flight.FlightClient(
            "{}://{}:{}".format(scheme, self.hostname, self.port),
            middleware=[client_auth_middleware, client_cookie_middleware],
            **tls_args
        )

        # Authenticate with the server endpoint.
        bearer_token = client.authenticate_basic_token(
            self.username, self.password, flight.FlightCallOptions(headers=self.headers)
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
