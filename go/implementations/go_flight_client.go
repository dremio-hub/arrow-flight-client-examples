package implementations

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/flight"
	flightgen "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"

	"arrow-flight-client-example/interfaces"
)

var _ interfaces.TestableClient = &GoFlightClient{}

type GoFlightClient struct {
	Client flight.Client
}

func (r *GoFlightClient) AuthenticateBasicToken(ctx context.Context, user, pass string) (context.Context, error) {
	return r.Client.AuthenticateBasicToken(ctx, user, pass)
}

func (r *GoFlightClient) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flightgen.SchemaResult, error) {
	return r.Client.GetSchema(ctx, desc)
}

func (r *GoFlightClient) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flightgen.FlightInfo, error) {
	return r.Client.GetFlightInfo(ctx, desc)
}

func (r *GoFlightClient) DoGet(ctx context.Context, ticket *flightgen.Ticket) (flight.FlightService_DoGetClient, error) {
	return r.Client.DoGet(ctx, ticket)
}

func (r *GoFlightClient) Close() error {
	return r.Client.Close()
}

func (r *GoFlightClient) SetSessionOptions(ctx context.Context, req *flight.SetSessionOptionsRequest) (*flight.SetSessionOptionsResult, error) {
	return r.Client.SetSessionOptions(ctx, req)
}

func (r *GoFlightClient) CloseSession(ctx context.Context, req *flight.CloseSessionRequest) (*flight.CloseSessionResult, error) {
	return r.Client.CloseSession(ctx, req)
}
