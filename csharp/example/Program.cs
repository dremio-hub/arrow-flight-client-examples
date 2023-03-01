/*
 * Copyright (C) 2023 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Microsoft.Data.Analysis;
using Grpc.Net.Client;
using Grpc.Core;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight;
using Apache.Arrow;

namespace FlightClientExample
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            string host = "localhost";
            string port = "32010";
            string user = "dremio";
            string pass = "dremio123";
            // For default query, add the Samples source and Format the zips.json in Dremio UI
            string query = "SELECT city, loc[0] AS longitude, loc[1] AS latitude, pop, state, _id FROM Samples.\"samples.dremio.com\".\"zips.json\" LIMIT 100";
            string protocol = "http";

            // Parse command-line arguments to override the defaults when set
            for (int i = 0; i < args.Length-1; i++)
            {
                if (i % 2 == 0 && args[i].StartsWith("-")) 
                {
                    var key = args[i];
                    var value = args[i+1];
                    if (key.EndsWith("host")) host = value;
                    if (key.EndsWith("port")) port = value;
                    if (key.EndsWith("user")) user = value;
                    if (key.EndsWith("pass")) pass = value;
                    if (key.EndsWith("query")) query = value;
                    if (key.EndsWith("protocol")) protocol = value;
                }
            }

            // Basic auth using username and password
            string encoded = System.Convert.ToBase64String(System.Text.Encoding.GetEncoding("ISO-8859-1").GetBytes(user + ":" + pass));
            Console.WriteLine($"The encoded credentials: {encoded}");

            // Create client
            var address = $"{protocol}://{host}:{port}";
            Console.WriteLine($"Connecting to: {address}");

            var handler = new HttpClientHandler();
            // For localhost https (TLS) endpoint testing, uncomment the following to avoid a cert error
            // handler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;

            var httpClient = new HttpClient(handler);
            httpClient.DefaultRequestHeaders.Add("Authorization", "Basic " + encoded);

            // An example header for token authentication instead of Basic auth
            // httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + "X4NxSDN5...H11kUqYU/vWmzA==");
           
            var channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions
            {
                HttpClient = httpClient
            });

            FlightClient client = new FlightClient(channel);

            // Pass the query text as the Command Descriptor
            Console.WriteLine($"Query: \n {query}");
            var descriptor = FlightDescriptor.CreateCommandDescriptor(query);
            var schema = await client.GetSchema(descriptor).ResponseAsync;

            foreach(var schema_item in schema.Fields)
            {
                Console.WriteLine($"Schema Item: {schema_item.Key} - {schema_item.Value.DataType.Name}");
                // The following is advance warning of an upcoming Exception due to specific data types not supported by Microsoft.Data.Analysis
                // TODO: There may be a better alternative to Microsoft.Data.Analysis library
                if (schema_item.Value.DataType.Name == "list" || schema_item.Value.DataType.Name == "timestamp")
                {
                    // The fix would be to create a VDS that converts this column to a string instead (or a VDS that does not include this column)
                    Console.WriteLine($"ERROR: Found column of type '{schema_item.Value.DataType.Name}'.  This is not supported by Microsoft.Data.Analysis DataFrame conversion");
                }
            }
            
            var info = await client.GetInfo(descriptor).ResponseAsync;

            Console.WriteLine("-----BEGIN-----");
            // Download data using existing channel
            await foreach (var batch in StreamRecordBatches(info, channel))
            {
                // Microsoft.Data.Analysis library behaves similar to python pandas, but limited support for DataTypes
                var df = DataFrame.FromArrowRecordBatch(batch);
 
                for (long index = 0; index < df.Rows.Count; index++)
                {
                    DataFrameRow row = df.Rows[index];
                    Console.WriteLine(row);
                }
            }
            Console.WriteLine("-----END-----");
        }

        public static async IAsyncEnumerable<RecordBatch> StreamRecordBatches(
            FlightInfo info,
            GrpcChannel channel
        )
        {
            // Assuming one endpoint for example
            var endpoint = info.Endpoints[0];
            // Console.WriteLine($"endpoint.Ticket.GetHashCode: {endpoint.Ticket.GetHashCode()}");
            // Console.WriteLine($"endpoint locations uri: \n {endpoint.Locations.First().Uri}");

            var download_client = new FlightClient(channel);
            var stream = download_client.GetStream(endpoint.Ticket);
  
            while (await stream.ResponseStream.MoveNext())
            {
                yield return stream.ResponseStream.Current;
            }
        }
    }
}

