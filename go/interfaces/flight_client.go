package interfaces

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/flight"
	flightgen "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
)

// FlightClient abstracts the flight.Client functionality for testing and modularity.
type FlightClient interface {
	AuthenticateBasicToken(ctx context.Context, user, pass string) (context.Context, error)
	GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flightgen.SchemaResult, error)
	GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flightgen.FlightInfo, error)
	DoGet(ctx context.Context, ticket *flightgen.Ticket) (flight.FlightService_DoGetClient, error)
	Close() error
	SetSessionOptions(ctx context.Context, req *flight.SetSessionOptionsRequest) (*flight.SetSessionOptionsResult, error)
	CloseSession(ctx context.Context, req *flight.CloseSessionRequest) (*flight.CloseSessionResult, error)
}
