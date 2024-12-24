package implementations

import (
	"reflect"

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
