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
from dremio.arguments.parse import get_config
from dremio.flight.endpoint import DremioFlightEndpoint

if __name__ == "__main__":
    # Parse the config file.
    args = get_config()

    # Instantiate DremioFlightEndpoint object
    dremio_flight_endpoint = DremioFlightEndpoint(args)

    # Connect to Dremio Arrow Flight server endpoint.
    flight_client = dremio_flight_endpoint.connect()

    # Get reader
    reader = dremio_flight_endpoint.get_reader(flight_client)

    # OPTION 1: Read all data at once (suitable for smaller datasets)
    # Uncomment this line if working with manageable data sizes.
    # print(reader.read_pandas())

    # OPTION 2: Read data in chunks using RecordBatchReader (recommended for large datasets)
    # Uncomment the following block if working with large datasets

    """
    # Convert the reader to a RecordBatchReader to read data in chunks
    record_batch_reader = reader.to_reader()

    # Iterate through each RecordBatch and convert to pandas DataFrame
    try:
        for record_batch in record_batch_reader:
            # Convert the current RecordBatch to a pandas DataFrame
            df = record_batch.to_pandas()
            # Process or accumulate the DataFrame as needed
            print(df)  # For demonstration, replace with the desired processing
    except StopIteration:
        # Raised when all data has been read
        print("All data successfully loaded.")
    """
