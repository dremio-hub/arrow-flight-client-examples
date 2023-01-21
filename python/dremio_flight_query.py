from pyarrow import flight
from dremio_flight_connection import DremioFlightEndpointConnection
from pandas import DataFrame, concat


class DremioFlightEndpointQuery:
    def __init__(
        self,
        query: str,
        client: flight.FlightClient,
        connection: DremioFlightEndpointConnection,
    ) -> None:
        self.query = query
        self.client = client
        self.headers = getattr(connection, "headers")

    def execute_query(self) -> DataFrame:
        options = flight.FlightCallOptions(headers=self.headers)
        # Get the FlightInfo message to retrieve the Ticket corresponding
        # to the query result set.
        flight_info = self.client.get_flight_info(
            flight.FlightDescriptor.for_command(self.query), options
        )
        print("[INFO] GetFlightInfo was successful")
        print("[INFO] Ticket: ", flight_info.endpoints[0].ticket)

        # Retrieve the result set as pandas DataFrame
        # For further optimization, read the data in chunks using read_chunks
        reader = self.client.do_get(flight_info.endpoints[0].ticket, options)
        return self._get_chunks(reader)

    def _get_chunks(self, reader: flight.FlightStreamReader) -> DataFrame:
        dataframe = DataFrame()
        while True:
            try:
                flight_batch = reader.read_chunk()
                record_batch = flight_batch.data
                data_to_pandas = record_batch.to_pandas()
                dataframe = concat([dataframe, data_to_pandas])
            except StopIteration:
                break

        return dataframe
