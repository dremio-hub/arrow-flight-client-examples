package main

import (
	"bytes"
	"context"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/golang/mock/gomock"

	"arrow-flight-client-example/implementations"
	"arrow-flight-client-example/interfaces"
)

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

	config := struct {
		Host      string
		Port      string
		Pat       string
		User      string
		Pass      string
		Query     string
		TLS       bool `docopt:"--tls"`
		Certs     string
		ProjectID string `docopt:"--project_id"`
	}{
		User:  "testuser",
		Pass:  "testpass",
		Query: "SELECT * FROM test",
	}

	mockReaderCreator := func(stream flight.FlightService_DoGetClient) (interfaces.RecordReader, error) {
		return implementations.NewMockRecordReader([]arrow.Record{record}), nil
	}

	run(config, mockClient, mockReaderCreator)
}
