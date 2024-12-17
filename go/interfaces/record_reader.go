package interfaces

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

// RecordReader interface to abstract record reading functionality
type RecordReader interface {
	Next() bool
	Record() arrow.Record
	Err() error
	Release()
}

func WrapRecordReader(stream flight.FlightService_DoGetClient) (RecordReader, error) {
	return flight.NewRecordReader(stream)
}
