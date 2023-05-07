package es

import (
	"io"

	jsoniter "github.com/json-iterator/go"
)

// Iterator is a io.Reader like object with read functions for revent custom format.
type DataReader struct {
	iter *jsoniter.Iterator
}

// Create a DataReader object
func NewDataReader(reader io.Reader) *DataReader {
	iter := jsoniter.Parse(jsoniter.ConfigFastest, reader, _buflen)
	return &DataReader{
		iter,
	}
}

// Report error to underlying stream
func (r *DataReader) ReportError(op string, error string) bool {
	r.iter.ReportError(op, error)
	return false
}

// Underlying stream error
func (r *DataReader) Error() error {
	return r.iter.Error
}

// Name of the index
func (r *DataReader) ReadIndexName() string {
	r.iter.ReadObject()
	return r.iter.ReadString()
}

// Metadata of the index
func (r *DataReader) ReadIndexData() []byte {
	r.iter.ReadObject()
	return r.iter.SkipAndReturnBytes()
}

// Maximum number of fetched events
func (r *DataReader) ReadEventsFetchLimit() int {
	r.iter.ReadObject()
	return r.iter.ReadInt()
}

// Actual number of events in the index
func (r *DataReader) ReadEventsNb() int {
	r.iter.ReadObject()
	return r.iter.ReadInt()
}

// Give events one by one as data to the callback
func (r *DataReader) ReadEvents(callback func(data []byte) bool) {
	r.iter.ReadObject()
	r.iter.ReadArrayCB(func(_ *jsoniter.Iterator) bool {
		data := r.iter.SkipAndReturnBytes()
		return callback(data)
	})
}
