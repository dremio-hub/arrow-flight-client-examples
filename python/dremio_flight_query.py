from pyarrow import flight
from pyarrow import csv
from dremio_flight_connection import DremioFlightEndpointConnection


class DremioFlightEndpointQuery:
    def __init__(
        self,
        query: str,
        client: flight.FlightClient,
        output_file: str,
        connection: DremioFlightEndpointConnection,
    ) -> None:
        self.query = query
        self.output_file = output_file
        self.client = client
        self.headers = getattr(connection, "headers")

    def execute_query(self) -> flight.FlightStreamReader:
        """Runs the query and retrieves the result set."""
        reader = self._get_result()
        return reader

    def _get_result(self) -> flight.FlightStreamReader:
        options = flight.FlightCallOptions(headers=self.headers)
        # Get the FlightInfo message to retrieve the Ticket corresponding
        # to the query result set.
        flight_info = self.client.get_flight_info(
            flight.FlightDescriptor.for_command(self.query), options
        )
        print("[INFO] GetFlightInfo was successful")
        print("[INFO] Ticket: ", flight_info.endpoints[0].ticket)

        # Retrieve the result set as a stream of Arrow record batches.
        reader = self.client.do_get(flight_info.endpoints[0].ticket, options)
        print("[INFO] Reading query results from Dremio")
        # Output to a file if selected otherwise just echo to stdout
        if self.output_file is not None:
            res_table = reader.read_all()
            with csv.CSVWriter(self.output_file, res_table.schema) as writer:
                writer.write_table(res_table)
            print("[INFO] Output results to " + str(self.output_file))
        else:
            print(reader.read_pandas())
        return reader
