from dremio.flight.connection import DremioFlightEndpointConnection
from dremio.flight.query import DremioFlightEndpointQuery
from pyarrow import flight
from pandas import DataFrame


class DremioFlightEndpoint:
    def __init__(self, connection_args: dict) -> None:
        self.connection_args = connection_args
        self.dremio_flight_conn = DremioFlightEndpointConnection(self.connection_args)

    def connect(self) -> flight.FlightClient:
        return self.dremio_flight_conn.connect()

    def execute_query(self, flight_client: flight.FlightClient) -> DataFrame:
        dremio_flight_query = DremioFlightEndpointQuery(
            self.connection_args.get("query"), flight_client, self.dremio_flight_conn
        )
        return dremio_flight_query.execute_query()
