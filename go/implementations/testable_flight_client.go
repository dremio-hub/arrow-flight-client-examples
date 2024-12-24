package implementations

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
)

// TestableFlightClient implements the Client interface by wrapping a flight.Client
type TestableFlightClient struct {
	flight.Client
}

// Authenticate uses the ClientAuthHandler that was used when creating the client
// to use the Handshake endpoints of the service.
func (r *TestableFlightClient) Authenticate(ctx context.Context, opts ...grpc.CallOption) error {
	return r.Client.Authenticate(ctx, opts...)
}

// AuthenticateBasicToken authenticates the client using a basic token
func (r *TestableFlightClient) AuthenticateBasicToken(ctx context.Context, username string, password string, opts ...grpc.CallOption) (context.Context, error) {
	return r.Client.AuthenticateBasicToken(ctx, username, password, opts...)
}

// CancelFlightInfo cancels a flight info request
func (r *TestableFlightClient) CancelFlightInfo(ctx context.Context, request *flight.CancelFlightInfoRequest, opts ...grpc.CallOption) (*flight.CancelFlightInfoResult, error) {
	return r.Client.CancelFlightInfo(ctx, request, opts...)
}

// Close closes the flight client connection
func (r *TestableFlightClient) Close() error {
	return r.Client.Close()
}

// RenewFlightEndpoint renews a flight endpoint
func (r *TestableFlightClient) RenewFlightEndpoint(ctx context.Context, request *flight.RenewFlightEndpointRequest, opts ...grpc.CallOption) (*flight.FlightEndpoint, error) {
	return r.Client.RenewFlightEndpoint(ctx, request, opts...)
}

// SetSessionOptions sets session options for the flight client
func (r *TestableFlightClient) SetSessionOptions(ctx context.Context, request *flight.SetSessionOptionsRequest, opts ...grpc.CallOption) (*flight.SetSessionOptionsResult, error) {
	return r.Client.SetSessionOptions(ctx, request, opts...)
}

// GetSessionOptions retrieves the session options for the flight client
func (r *TestableFlightClient) GetSessionOptions(ctx context.Context, request *flight.GetSessionOptionsRequest, opts ...grpc.CallOption) (*flight.GetSessionOptionsResult, error) {
	return r.Client.GetSessionOptions(ctx, request, opts...)
}

// CloseSession closes the client session
func (r *TestableFlightClient) CloseSession(ctx context.Context, request *flight.CloseSessionRequest, opts ...grpc.CallOption) (*flight.CloseSessionResult, error) {
	return r.Client.CloseSession(ctx, request, opts...)
}

// FlightServiceClient methods are inherited from the embedded flight.Client
