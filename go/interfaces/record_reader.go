package interfaces

import "github.com/apache/arrow-go/v18/arrow"

type RecordReader interface {
	Next() bool
	Record() arrow.Record
	Err() error
	Release()
}
