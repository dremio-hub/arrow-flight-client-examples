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

namespace SimpleFlightClientExample
{
    public class Program
    {
        private const string HOST = "host";
        private const string PORT = "port";
        private const string USER = "user";
        private const string PASS = "pass";
        private const string PAT = "pat";
        private const string QUERY = "query";
        private const string PROTOCOL = "protocol";

        public static class DefaultValues
        {
            public static readonly Dictionary<string, string> args = new Dictionary<string, string>{
                { HOST, "localhost" },
                { PORT, "32010" },
                { USER, "dremio" },
                { PASS, "dremio123" },
                { QUERY, "SELECT city, loc[0] AS longitude, loc[1] AS latitude, pop, state, _id FROM Samples.\"samples.dremio.com\".\"zips.json\" LIMIT 100" },
                { PROTOCOL, "http" },
                { PAT, "" }
            };
        }

        public static async Task Main(string[] args)
        {
            var arguments = BuildArgumentsDictionary(args);
            var address = $"{arguments[PROTOCOL]}://{arguments[HOST]}:{arguments[PORT]}";
            var authHeader = BuildAuthorizationHeader(arguments);
            var channel = BuildGrpcChannel(address, authHeader);

            FlightClient client = new FlightClient(channel);

            Console.WriteLine("-----Address-----");
            Console.WriteLine(address);
            Console.WriteLine();

            Console.WriteLine("-----Query-----");
            Console.WriteLine(arguments[QUERY]);
            Console.WriteLine();

            // Pass the query text as the Command Descriptor
            var descriptor = FlightDescriptor.CreateCommandDescriptor(arguments[QUERY]);
            var schema = await client.GetSchema(descriptor).ResponseAsync;

            Console.WriteLine("-----Schema Items-----");
            Console.WriteLine(ConvertSchemaToJsonString(schema));

            // Get Flight Info
            var info = await client.GetInfo(descriptor).ResponseAsync;

            Console.WriteLine("-----DATA-----");
            await foreach (var batch in StreamRecordBatches(info, channel))
            {
                // Microsoft.Data.Analysis library has DataFrame which behaves similar to python pandas, but has limited support for DataTypes at time of writing
                var df = DataFrame.FromArrowRecordBatch(batch);

                for (long index = 0; index < df.Rows.Count; index++)
                {
                    DataFrameRow row = df.Rows[index];
                    Console.WriteLine(row);
                }
            }
        }

        /*
         *  The following "ParseCommandLineArgs" function is close to a generic commmand-line parser
         *
         *  Params:
         *    args = arguments from Main command line
         *    supportedArgs = List of the expected command-line params to look for (ignore everything else)
         *    argPrefix = Default to "-" but could also work if someone wants to use something else instead to indicate argument (e.g. "--")
        */
        private static Dictionary<string, string> ParseCommandLineArgs(string[] args, string[] supportedArgs, string argPrefix = "-")
        {
            Dictionary<string, string> dictionaryOfArgs = new Dictionary<string, string>();
            
            for (int i = 0; i < args.Length - 1; i++)
            {
                if (i % 2 == 0 && args[i].StartsWith(argPrefix)) 
                {
                    var key = args[i].Substring(argPrefix.Length);
                    var value = args[i+1];
                   
                    if (supportedArgs.Contains(key))
                    {
                        // Console.WriteLine($"Argument: {key} = {value}");
                        dictionaryOfArgs.Add(key, value);
                    } else {
                        Console.WriteLine($"Value '{key}' is not a supported argument, this is ignored.");
                    }
                }
            }

            return dictionaryOfArgs;
        }

        // Uses default values for arguments (DefaultValues.args) unless we have command-line override for these defaults
        private static Dictionary<string, string> BuildArgumentsDictionary(string[] args) 
        {
            Dictionary<string,string> arguments = new Dictionary<string, string>(DefaultValues.args);

            // Override arguments: Replace default settings with values from command line such as -host
            foreach (var item in ParseCommandLineArgs(args, DefaultValues.args.Keys.ToArray()))
            {
                arguments[item.Key] = item.Value;
            }

            return arguments;
        }

        private static string BuildAuthorizationHeader(Dictionary<string, string> arguments) 
        {
            var authorizationHeader = "";

            // If -pat (Personal Access Token) set, use that instead for auth
            if (arguments.ContainsKey(PAT) && arguments[PAT] != "")
            {
                Console.WriteLine("Using personal access token for authorization");
                authorizationHeader = "Bearer " + arguments[PAT];
            } else {
                Console.WriteLine("Using Basic auth with user and pass for authorization");
                authorizationHeader = "Basic " + System.Convert.ToBase64String(System.Text.Encoding.GetEncoding("ISO-8859-1").GetBytes(arguments[USER] + ":" + arguments[PASS]));
            }

            return authorizationHeader;
        }

        private static GrpcChannel BuildGrpcChannel(string address, string authHeader)
        {
            // Console.WriteLine($"Creating Grpc Channel for address: {address}");

            var handler = new HttpClientHandler();

            // For localhost https (TLS) endpoint testing with a self-signed cert, uncomment the following to avoid a cert error.  Not for production.
            //handler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
 
            var httpClient = new HttpClient(handler);
            httpClient.DefaultRequestHeaders.Add("Authorization", authHeader);
 
            var channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions
            {
                HttpClient = httpClient
            });
 
            return channel;
        }

        // This function used to clean up the console output to make it more readable
        private static string ConvertSchemaToJsonString (Schema schema) {
            var schemaMessage = "";

            // ** NOTE **
            // The Microsoft.Data.Analysis library did not work well with item.Value.DataType.Name == "list" || item.Value.DataType.Name == "timestamp"
            // The fix would be to create a VDS that converts this column to a string instead (or a VDS that does not include this column)

            foreach(var item in schema.Fields)
            {
                schemaMessage += "  \"" + item.Key + "\": \"" + item.Value.DataType.Name + "\",\n";
            }

            return "{\n" + schemaMessage + "}\n";
        }

        public static async IAsyncEnumerable<RecordBatch> StreamRecordBatches(FlightInfo info, GrpcChannel channel)
        {
            // Assuming one endpoint for example
            var endpoint = info.Endpoints[0];
            //Console.WriteLine($"endpoint.Ticket.GetHashCode: {endpoint.Ticket.GetHashCode()}");
            //Console.WriteLine($"endpoint locations uri: \n {endpoint.Locations.First().Uri}");

            var download_client = new FlightClient(channel);
            var stream = download_client.GetStream(endpoint.Ticket);
 
            // TODO: Potential RPC Exception? https://groups.google.com/g/grpc-io/c/MS7uCIabkO4
            while (await stream.ResponseStream.MoveNext())
            {
                yield return stream.ResponseStream.Current;
            }
        }
    }
}
