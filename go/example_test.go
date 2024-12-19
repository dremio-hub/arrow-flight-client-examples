package main

import (
	"bytes"
	"context"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/golang/mock/gomock"

	"arrow-flight-client-example/implementations"
	"arrow-flight-client-example/interfaces"
)

func TestUsernamePassAuth(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockFlightClient(ctrl)

	mockClient.EXPECT().
		AuthenticateBasicToken(gomock.Any(), "testuser", "testpass").
		Return(context.Background(), nil).
		Times(1)

	config := interfaces.FlightConfig{
		User:  "testuser",
		Pass:  "testpass",
		Query: "",
	}

	err := run(config, mockClient, nil)
	if err != nil {
		t.Errorf("Expected successful authentication with no error, got: %v", err)
	}
}

func TestPATAuth(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockFlightClient(ctrl)

	mockClient.EXPECT().
		SetSessionOptions(gomock.Any(), gomock.Any()).
		Return(nil, nil).
		Times(1)

	mockClient.EXPECT().
		CloseSession(gomock.Any(), gomock.Any()).
		Return(nil, nil).
		Times(1)

	config := interfaces.FlightConfig{
		Pat:       "testpat",
		ProjectID: "testproject",
	}

	err := run(config, mockClient, nil)
	if err != nil {
		t.Errorf("Expected successful PAT authentication with no error, got: %v", err)
	}
}

func TestRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	allocator := memory.NewGoAllocator()

	// Create a schema with a single Int32 field
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "field1", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	// Create a record batch
	bldr := array.NewInt32Builder(allocator)
	defer bldr.Release()

	bldr.AppendValues([]int32{1, 2, 3}, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	// Create a record batch
	record := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer record.Release()

	// Serialize the schema and record batch to IPC format
	var schemaBuf, recordBuf bytes.Buffer

	schemaWriter := ipc.NewWriter(&schemaBuf, ipc.WithSchema(schema))
	if err := schemaWriter.Close(); err != nil {
		t.Fatalf("Failed to write schema: %v", err)
	}

	recordWriter := ipc.NewWriter(&recordBuf, ipc.WithSchema(schema))
	if err := recordWriter.Write(record); err != nil {
		t.Fatalf("Failed to write record batch: %v", err)
	}
	if err := recordWriter.Close(); err != nil {
		t.Fatalf("Failed to close record writer: %v", err)
	}

	mockStream := implementations.NewMockFlightService_DoGetClient(ctrl)

	mockClient := NewMockFlightClient(ctrl)

	mockClient.EXPECT().
		AuthenticateBasicToken(gomock.Any(), "testuser", "testpass").
		Return(context.Background(), nil).
		Times(1)

	mockClient.EXPECT().
		GetSchema(gomock.Any(), gomock.Any()).
		Return(&flight.SchemaResult{
			Schema: schemaBuf.Bytes(),
		}, nil).
		Times(1)

	mockClient.EXPECT().
		GetFlightInfo(gomock.Any(), gomock.Any()).
		Return(&flight.FlightInfo{
			Endpoint: []*flight.FlightEndpoint{
				{Ticket: &flight.Ticket{Ticket: []byte("mock_ticket")}},
			},
		}, nil).
		Times(1)

	mockClient.EXPECT().
		DoGet(gomock.Any(), gomock.Any()).
		Return(mockStream, nil).
		Times(1)

	config := interfaces.FlightConfig{
		User:  "testuser",
		Pass:  "testpass",
		Query: "SELECT * FROM test",
	}

	mockReaderCreator := func(stream flight.FlightService_DoGetClient) (interfaces.RecordReader, error) {
		return implementations.NewMockRecordReader([]arrow.Record{record}), nil
	}

	run(config, mockClient, mockReaderCreator)
}

func TestRunWithPAT(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	allocator := memory.NewGoAllocator()

	// Create a schema with a single Int32 field
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "field1", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	// Create a record batch
	bldr := array.NewInt32Builder(allocator)
	defer bldr.Release()

	bldr.AppendValues([]int32{1, 2, 3}, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	// Create a record batch
	record := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer record.Release()

	// Serialize the schema and record batch to IPC format
	var schemaBuf, recordBuf bytes.Buffer

	schemaWriter := ipc.NewWriter(&schemaBuf, ipc.WithSchema(schema))
	if err := schemaWriter.Close(); err != nil {
		t.Fatalf("Failed to write schema: %v", err)
	}

	recordWriter := ipc.NewWriter(&recordBuf, ipc.WithSchema(schema))
	if err := recordWriter.Write(record); err != nil {
		t.Fatalf("Failed to write record batch: %v", err)
	}
	if err := recordWriter.Close(); err != nil {
		t.Fatalf("Failed to close record writer: %v", err)
	}

	mockStream := implementations.NewMockFlightService_DoGetClient(ctrl)

	mockClient := NewMockFlightClient(ctrl)

	mockClient.EXPECT().
		GetSchema(gomock.Any(), gomock.Any()).
		Return(&flight.SchemaResult{
			Schema: schemaBuf.Bytes(),
		}, nil).
		Times(1)

	mockClient.EXPECT().
		GetFlightInfo(gomock.Any(), gomock.Any()).
		Return(&flight.FlightInfo{
			Endpoint: []*flight.FlightEndpoint{
				{Ticket: &flight.Ticket{Ticket: []byte("mock_ticket")}},
			},
		}, nil).
		Times(1)

	mockClient.EXPECT().
		DoGet(gomock.Any(), gomock.Any()).
		Return(mockStream, nil).
		Times(1)

	mockClient.EXPECT().
		SetSessionOptions(gomock.Any(), gomock.Any()).
		Return(nil, nil).
		Times(1)

	mockClient.EXPECT().
		CloseSession(gomock.Any(), gomock.Any()).
		Return(nil, nil).
		Times(1)

	config := interfaces.FlightConfig{
		Pat:       "test_pat_token",
		Query:     "SELECT * FROM test",
		ProjectID: "test_project_id",
	}

	mockReaderCreator := func(stream flight.FlightService_DoGetClient) (interfaces.RecordReader, error) {
		return implementations.NewMockRecordReader([]arrow.Record{record}), nil
	}

	run(config, mockClient, mockReaderCreator)
}

func TestRunWithPATNoProjectID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	allocator := memory.NewGoAllocator()

	// Create a schema with a single Int32 field
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "field1", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	// Create a record batch
	bldr := array.NewInt32Builder(allocator)
	defer bldr.Release()

	bldr.AppendValues([]int32{1, 2, 3}, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	// Create a record batch
	record := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer record.Release()

	// Serialize the schema and record batch to IPC format
	var schemaBuf, recordBuf bytes.Buffer

	schemaWriter := ipc.NewWriter(&schemaBuf, ipc.WithSchema(schema))
	if err := schemaWriter.Close(); err != nil {
		t.Fatalf("Failed to write schema: %v", err)
	}

	recordWriter := ipc.NewWriter(&recordBuf, ipc.WithSchema(schema))
	if err := recordWriter.Write(record); err != nil {
		t.Fatalf("Failed to write record batch: %v", err)
	}
	if err := recordWriter.Close(); err != nil {
		t.Fatalf("Failed to close record writer: %v", err)
	}

	mockStream := implementations.NewMockFlightService_DoGetClient(ctrl)

	mockClient := NewMockFlightClient(ctrl)

	mockClient.EXPECT().
		GetSchema(gomock.Any(), gomock.Any()).
		Return(&flight.SchemaResult{
			Schema: schemaBuf.Bytes(),
		}, nil).
		Times(1)

	mockClient.EXPECT().
		GetFlightInfo(gomock.Any(), gomock.Any()).
		Return(&flight.FlightInfo{
			Endpoint: []*flight.FlightEndpoint{
				{Ticket: &flight.Ticket{Ticket: []byte("mock_ticket")}},
			},
		}, nil).
		Times(1)

	mockClient.EXPECT().
		DoGet(gomock.Any(), gomock.Any()).
		Return(mockStream, nil).
		Times(1)

	config := interfaces.FlightConfig{
		Pat:   "test_pat_token",
		Query: "SELECT * FROM test",
	}

	mockReaderCreator := func(stream flight.FlightService_DoGetClient) (interfaces.RecordReader, error) {
		return implementations.NewMockRecordReader([]arrow.Record{record}), nil
	}

	run(config, mockClient, mockReaderCreator)
}

func TestInvalidCredentials(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockFlightClient(ctrl)

	expectedErr := status.Error(codes.Unauthenticated, "failed to authenticate user: rpc error: code = "+
		"Unauthenticated desc = Unable to authenticate user dremio, exception: Login failed: Invalid username or "+
		"password, user dremio")

	mockClient.EXPECT().
		AuthenticateBasicToken(gomock.Any(), "dremio", "dremio12").
		Return(context.Background(), expectedErr).
		Times(1)

	config := interfaces.FlightConfig{
		User:  "dremio",
		Pass:  "dremio12",
		Query: "SELECT 1",
	}

	mockReaderCreator := func(stream flight.FlightService_DoGetClient) (interfaces.RecordReader, error) {
		t.Fatal("Reader creator should not be called due to authentication failure")
		return nil, nil
	}

	err := run(config, mockClient, mockReaderCreator)
	if err == nil {
		t.Fatal("Expected an error, but got nil")
	}

	expectedErrStr := "failed to authenticate user: rpc error: code = Unauthenticated desc = Unable to authenticate " +
		"user dremio, exception: Login failed: Invalid username or password, user dremio"

	if !strings.Contains(err.Error(), expectedErrStr) {
		t.Errorf("Expected error message to contain %q, got %q", expectedErrStr, err.Error())
	}
}

func TestInvalidHost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockFlightClient(ctrl)

	expectedErr := status.Error(codes.Unauthenticated, "failed to authenticate user: rpc error: code = "+
		"Unavailable desc = name resolver error: produced zero addresses")

	mockClient.EXPECT().
		AuthenticateBasicToken(gomock.Any(), "dremio", "dremio12").
		Return(context.Background(), expectedErr).
		Times(1)

	config := interfaces.FlightConfig{
		User:  "dremio",
		Pass:  "dremio12",
		Query: "SELECT 1",
	}

	mockReaderCreator := func(stream flight.FlightService_DoGetClient) (interfaces.RecordReader, error) {
		t.Fatal("Reader creator should not be called due to authentication failure")
		return nil, nil
	}

	err := run(config, mockClient, mockReaderCreator)
	if err == nil {
		t.Fatal("Expected an error, but got nil")
	}

	expectedErrStr := "failed to authenticate user: rpc error: code = Unavailable desc = name resolver error: " +
		"produced zero addresses"

	if !strings.Contains(err.Error(), expectedErrStr) {
		t.Errorf("Expected error message to contain %q, got %q", expectedErrStr, err.Error())
	}
}

func TestInvalidPort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockFlightClient(ctrl)

	expectedErr := status.Error(codes.Unauthenticated, "failed to authenticate user: rpc error: code = "+
		"Unavailable desc = connection error: desc = \"transport: Error while dialing: dial tcp: lookup tcp/320o: unknown port\"")

	mockClient.EXPECT().
		AuthenticateBasicToken(gomock.Any(), "dremio", "dremio12").
		Return(context.Background(), expectedErr).
		Times(1)

	config := interfaces.FlightConfig{
		User:  "dremio",
		Pass:  "dremio12",
		Query: "SELECT 1",
	}

	mockReaderCreator := func(stream flight.FlightService_DoGetClient) (interfaces.RecordReader, error) {
		t.Fatal("Reader creator should not be called due to authentication failure")
		return nil, nil
	}

	err := run(config, mockClient, mockReaderCreator)
	if err == nil {
		t.Fatal("Expected an error, but got nil")
	}

	expectedErrStr := "failed to authenticate user: rpc error: code = Unavailable desc = connection error: desc = " +
		"\"transport: Error while dialing: dial tcp: lookup tcp/320o: unknown port\""

	if !strings.Contains(err.Error(), expectedErrStr) {
		t.Errorf("Expected error message to contain %q, got %q", expectedErrStr, err.Error())
	}
}
