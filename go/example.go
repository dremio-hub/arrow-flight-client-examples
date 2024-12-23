// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"

	"arrow-flight-client-example/implementations"
	"arrow-flight-client-example/interfaces"
	"github.com/apache/arrow-go/v18/arrow/flight"
	flightgen "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/docopt/docopt-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const usage = `Dremio Client Example.

Usage:
  example -h | --help
  example [--host=<hostname>] [--port=<port>] (--user=<username> --pass=<password> | --pat=<pat>)
          [--tls] [--certs=<path>] [--query <query>] [--project_id=<project_id>]

Options:
  -h --help           Show this help.
  --host=<hostname>   Dremio coordinator hostname [default: localhost]
  --port=<port>       Dremio flight server port [default: 32010]
  --user=<username>   Dremio username
  --pass=<password>   Dremio password
  --pat=<pat>         Dremio personal access token
  --query <query>     SQL Query to test.
  --tls               Enable encrypted connection.
  --certs=<path>      Path to trusted certificates for encrypted connection.
  --project_id=<project_id>   Dremio project ID`

func main() {
	args, err := docopt.ParseDoc(usage)
	if err != nil {
		log.Fatalf("error parsing arguments: %v", err)
	}

	var config interfaces.FlightConfig
	if err := args.Bind(&config); err != nil {
		log.Fatalf("error binding arguments: %v", err)
	}

	var creds credentials.TransportCredentials
	if config.TLS {
		log.Println("[INFO] Enabling TLS Connection.")
		// if we want to use TLS let's set up our credentials
		if config.Certs == "" {
			log.Println("[INFO] Using default host root certificate.")
			// default will use local root certificates
			// if you want to disable verifying the server's certificate you can add `InsecureSkipVerify: true`
			// to the &tls.Config here
			// if you need to specify more options than just the path to the root certificate, then you can also
			// use this and then specify a full complete TLS config
			creds = credentials.NewTLS(&tls.Config{})
		} else {
			log.Println("[INFO] Trusted certificates provided.")
			var err error
			// this is just the easiest route to construct creds from a file, use the above credentials.NewTLS
			// to specify a more complete tls config if needed.
			if creds, err = credentials.NewClientTLSFromFile(config.Certs, ""); err != nil {
				log.Fatal(err)
			}
		}
	} else {
		// default is to use unencrypted connection
		creds = insecure.NewCredentials()
	}

	rawClient, err := flight.NewClientWithMiddleware(
		net.JoinHostPort(config.Host, config.Port),
		nil,
		[]flight.ClientMiddleware{flight.NewClientCookieMiddleware()},
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer rawClient.Close()

	// GoFlightClient and the TestableClient interface are simple wrappers for flight.Client to provide testing and modularity.
	abstractClient := &implementations.GoFlightClient{Client: rawClient}
	if err := run(config, abstractClient, interfaces.WrapRecordReader); err != nil {
		log.Fatal(err)
	}

}

func run(config interfaces.FlightConfig, abstractClient interfaces.TestableClient,
	readerCreator func(flight.FlightService_DoGetClient) (interfaces.RecordReader, error),
) error {

	// Two WLM settings can be provided upon initial authentication with the dremio
	// server flight endpoint:
	//  - routing-tag
	//  - routing-queue
	ctx := metadata.NewOutgoingContext(context.TODO(),
		metadata.Pairs("routing-tag", "test-routing-tag", "routing-queue", "Low Cost User Queries"))

	var err error
	if config.Pat != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", fmt.Sprintf("Bearer %s", config.Pat))
		log.Println("[INFO] Using PAT.")

		if config.ProjectID != "" {
			log.Println("[INFO] Project ID added to sessions options.")
			err = setSessionOptions(ctx, abstractClient, config.ProjectID)
			if err != nil {
				return fmt.Errorf("failed to set session options: %v", err)
			}
			// Close the session once the query is done
			defer abstractClient.CloseSession(ctx, &flight.CloseSessionRequest{})
		}
	} else {
		if ctx, err = abstractClient.AuthenticateBasicToken(ctx, config.User, config.Pass); err != nil {
			return fmt.Errorf("failed to authenticate user: %v", err)
		}
		log.Println("[INFO] Authentication was successful.")
	}

	if config.Query == "" {
		return nil
	}

	// Once successful, the context object now contains the credentials, use it for subsequent calls.
	desc := &flight.FlightDescriptor{
		Type: flightgen.FlightDescriptor_CMD,
		Cmd:  []byte(config.Query),
	}
	log.Println("[INFO] Query:", config.Query)

	// In addition to the auth credentials, a context can also be provided via schema header
	// ctx = metadata.AppendToOutgoingContext(ctx, "schema", "test.schema")

	// Retrieve the schema of the result set
	sc, err := abstractClient.GetSchema(ctx, desc)
	if err != nil {
		return fmt.Errorf("failed to get schema: %v", err)
	}
	log.Println("[INFO] GetSchema was successful.")

	schema, err := flight.DeserializeSchema(sc.GetSchema(), memory.DefaultAllocator)
	if err != nil {
		return fmt.Errorf("failed to deserialize schema: %v", err)
	}
	log.Println("[INFO] Schema:", schema)

	// Get the FlightInfo message to retrieve the ticket corresponding to the query result set
	info, err := abstractClient.GetFlightInfo(ctx, desc)
	if err != nil {
		return fmt.Errorf("failed to get flight info: %v", err)
	}
	log.Println("[INFO] GetFlightInfo was successful.")

	// retrieve the result set as a stream of Arrow record batches.
	stream, err := abstractClient.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return fmt.Errorf("failed to get flight stream: %v", err)
	}

	rdr, err := readerCreator(stream)
	if err != nil {
		return fmt.Errorf("failed to create record reader: %v", err)
	}
	defer rdr.Release()

	log.Println("[INFO] Reading query results from dremio.")
	for rdr.Next() {
		rec := rdr.Record()
		defer rec.Release()
		log.Println(rec)
	}
	return nil
}

func setSessionOptions(ctx context.Context, client interfaces.TestableClient, projectID string) error {
	projectIdSessionOption, err := flight.NewSessionOptionValue(projectID)
	if err != nil {
		return fmt.Errorf("failed to create session option: %v", err)
	}
	sessionOptionsRequest := flight.SetSessionOptionsRequest{
		SessionOptions: map[string]*flight.SessionOptionValue{
			"project_id": &projectIdSessionOption,
		},
	}
	_, err = client.SetSessionOptions(ctx, &sessionOptionsRequest)
	if err != nil {
		return fmt.Errorf("set session options: %v", err)
	}
	log.Printf("[INFO] Session options set with project_id: %s", projectID)
	return nil
}
