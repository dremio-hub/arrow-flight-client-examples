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

from parse_arguments import parse_arguments
from dremio_flight_connection import DremioFlightEndpointConnection
from dremio_flight_query import DremioFlightEndpointQuery

if __name__ == "__main__":
    # Parse the command line arguments.
    args = parse_arguments()

    # Connect to Dremio Arrow Flight server endpoint.
    dremio_flight_conn = DremioFlightEndpointConnection(args)
    flight_client = dremio_flight_conn.connect()

    # Execute query and get reader
    dremio_flight_query = DremioFlightEndpointQuery(
        args.query, flight_client, dremio_flight_conn
    )
    dataframe = dremio_flight_query.execute_query()

    # Print out the data
    print(dataframe)
