package implementations

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// MockRecordReader for testing purposes
type MockRecordReader struct {
	records      []arrow.Record
	currentIndex int
	err          error
}

func NewMockRecordReader(records []arrow.Record) *MockRecordReader {
	return &MockRecordReader{
		records:      records,
		currentIndex: -1,
	}
}

func (m *MockRecordReader) Next() bool {
	m.currentIndex++
	return m.currentIndex < len(m.records)
}

func (m *MockRecordReader) Record() arrow.Record {
	if m.currentIndex < 0 || m.currentIndex >= len(m.records) {
		return nil
	}
	return m.records[m.currentIndex]
}

func (m *MockRecordReader) Err() error {
	return m.err
}

func (m *MockRecordReader) Release() {
	// In a mock, we don't need to do anything for release
}
