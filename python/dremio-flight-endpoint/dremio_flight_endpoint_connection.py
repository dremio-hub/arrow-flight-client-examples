import certifi
from pyarrow import flight
import argparse
import dremio_middleware


class DremioFlightEndpointClient:
    def __init__(self, args: argparse.Namespace) -> None:
        pass

    def connect(self, connection_args: dict, headers: list) -> flight.FlightClient:
        """Connects to Dremio Flight server endpoint with the
        provided credentials."""

    def _connect_with_pat(
        self, connection_args: dict, headers: list
    ) -> flight.FlightClient:
        pass

    def _connect_with_password(
        self, connection_args: dict, headers: list
    ) -> flight.FlightClient:
        pass

    def _set_tls_connection_args(self, connection_args: dict) -> dict:
        pass

    def _set_headers(self) -> list:
        pass
