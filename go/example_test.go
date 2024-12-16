package main

import (
	"bytes"
	"context"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"io"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/golang/mock/gomock"
	"google.golang.org/grpc"
)

type MockFlightService_DoGetClient struct {
	ctrl     *gomock.Controller
	recorder *MockFlightService_DoGetClientMockRecorder
	grpc.ClientStream
}

type MockFlightService_DoGetClientMockRecorder struct {
	mock *MockFlightService_DoGetClient
}

func NewMockFlightService_DoGetClient(ctrl *gomock.Controller) *MockFlightService_DoGetClient {
	mock := &MockFlightService_DoGetClient{ctrl: ctrl}
	mock.recorder = &MockFlightService_DoGetClientMockRecorder{mock}
	return mock
}

func (m *MockFlightService_DoGetClient) EXPECT() *MockFlightService_DoGetClientMockRecorder {
	return m.recorder
}

func (m *MockFlightService_DoGetClient) Recv() (*flight.FlightData, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Recv")
	ret0, _ := ret[0].(*flight.FlightData)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockFlightService_DoGetClientMockRecorder) Recv() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Recv", reflect.TypeOf((*MockFlightService_DoGetClient)(nil).Recv))
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

	// Write schema
	schemaWriter := ipc.NewWriter(&schemaBuf, ipc.WithSchema(schema))
	if err := schemaWriter.Close(); err != nil {
		t.Fatalf("Failed to write schema: %v", err)
	}

	// Write record batch
	recordWriter := ipc.NewWriter(&recordBuf, ipc.WithSchema(schema))
	if err := recordWriter.Write(record); err != nil {
		t.Fatalf("Failed to write record batch: %v", err)
	}
	if err := recordWriter.Close(); err != nil {
		t.Fatalf("Failed to close record writer: %v", err)
	}

	mockStream := NewMockFlightService_DoGetClient(ctrl)

	mockStream.EXPECT().Recv().Return(&flight.FlightData{
		DataHeader: schemaBuf.Bytes(),
		DataBody:   recordBuf.Bytes(),
	}, nil).Times(1)

	mockStream.EXPECT().Recv().Return(nil, io.EOF).Times(1)

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

	run(config, mockClient)
}
