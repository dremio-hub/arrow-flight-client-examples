package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"

	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/docopt/docopt-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const usage = `Dremio Client Example.

Usage:
  example -h | --help
  example [--host=<hostname>] [--port=<port>] --user=<username> --pass=<password>
          [--tls] [--certs=<path>] [--query <query>]

Options:
  -h --help           Show this help.
  --host=<hostname>   Dremio coordinator hostname [default: localhost]
  --port=<port>       Dremio flight server port [default: 32010]
  --user=<username>   Dremio username
  --pass=<password>   Dremio password
  --query <query>     SQL Query to test.
  --tls               Enable encrypted connection.
  --certs=<path>      Path to trusted certificates for encrypted connection.`

func main() {
	args, _ := docopt.ParseDoc(usage)
	var config struct {
		Host  string
		Port  string
		User  string
		Pass  string
		Query string
		TLS   bool `docopt:"--tls"`
		Certs string
	}
	args.Bind(&config)

	opts := make([]grpc.DialOption, 0)
	if config.TLS {
		log.Println("[INFO] Enabling TLS Connection.")
		// if we want to use TLS let's set up our credentials
		var creds credentials.TransportCredentials
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
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		// default is to use unencrypted connection
		opts = append(opts, grpc.WithInsecure())
	}

	client, err := flight.NewFlightClient(net.JoinHostPort(config.Host, config.Port), nil, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Two WLM settings can be provided upon initial authentication with the dremio
	// server flight endpoint:
	//  - routing-tag
	//  - routing-queue
	ctx := metadata.NewOutgoingContext(context.TODO(),
		metadata.Pairs("routing-tag", "test-routing-tag", "routing-queue", "Low Cost User Queries"))

	if ctx, err = client.AuthenticateBasicToken(ctx, config.User, config.Pass); err != nil {
		log.Fatal(err)
	}
	log.Println("[INFO] Authentication was successful.")

	if config.Query == "" {
		return
	}

	// Once successful, the context object now contains the credentials, use it for subsequent calls.
	desc := &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_CMD,
		Cmd:  []byte(config.Query),
	}
	log.Println("[INFO] Query:", config.Query)

	// In addition to the auth credentials, a context can also be provided via schema header
	// ctx = metadata.AppendToOutgoingContext(ctx, "schema", "test.schema")

	// Retrieve the schema of the result set
	sc, err := client.GetSchema(ctx, desc)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("[INFO] GetSchema was successful.")

	schema, err := flight.DeserializeSchema(sc.GetSchema(), memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("[INFO] Schema:", schema)

	// Get the FlightInfo message to retireve the ticket corresponding to the query result set
	info, err := client.GetFlightInfo(ctx, desc)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("[INFO] GetFlightInfo was successful.")
	log.Println("[INFO] Ticket:", info.Endpoint[0].Ticket)

	// retrieve the result set as a stream of Arrow record batches.
	stream, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		log.Fatal(err)
	}

	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatal(err)
	}
	defer rdr.Release()

	log.Println("[INFO] Reading query results from dremio.")
	for rdr.Next() {
		rec := rdr.Record()
		defer rec.Release()
		fmt.Println(rec)
	}
}
