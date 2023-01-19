from pyarrow import flight
from pyarrow import csv


class DremioFlightEndpointQuery:
    def __init__(self) -> None:
        pass

    def execute_query(
        self, client: flight.FlightClient, headers: list
    ) -> flight.FlightStreamReader:
        """Runs the query and retrieves the result set."""

    def _get_result_schema(self, query: str, headers: list) -> None:
        pass

    def _get_result(self, query: str) -> flight.FlightStreamReader:
        pass
