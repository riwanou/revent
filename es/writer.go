package es

import (
	"io"

	jsoniter "github.com/json-iterator/go"
)

const (
	// Index name
	INDEX_NAME_FIELD = "indexName"

	// Index metadata used for re-creation
	INDEX_DATA_FIELD = "indexData"

	// Number of events fetched
	EVENTS_FETCH_NB = "eventsFetchNb"

	// Actual number of events in index
	EVENTS_NB = "eventsNb"

	// array of the events
	EVENTS_ARRAY = "events"
)

// DataWriter is an io.Writer like object with write functions for revent custom format.
type DataWriter struct {
	s *jsoniter.Stream
}

// Create a DataWriter object
func NewDataWriter(writer io.Writer) *DataWriter {
	s := jsoniter.NewStream(jsoniter.ConfigFastest, writer, _buflen)
	s.WriteObjectStart()
	return &DataWriter{
		s,
	}
}

// Underlying stream error
func (w *DataWriter) Error() error {
	return w.s.Error
}

// Writes any buffered data to the underlying io.Writer and close object
func (w *DataWriter) WriteEnd() {
	w.s.WriteObjectEnd()
	w.s.Flush()
}

// Name and metadata of the index
func (w *DataWriter) WriteIndex(indexName string, indexData []byte) {
	w.s.WriteObjectField(INDEX_NAME_FIELD)
	w.s.WriteString(indexName)
	w.s.WriteMore()
	w.s.WriteObjectField(INDEX_DATA_FIELD)
	w.s.Write(indexData)
}

// Maximum number of fetched events
func (w *DataWriter) WriteEventsFetchNb(eventsMaxFetchNb int) {
	w.s.WriteMore()
	w.s.WriteObjectField(EVENTS_FETCH_NB)
	w.s.WriteInt(eventsMaxFetchNb)
}

// Actual number of events in the index
func (w *DataWriter) WriteEventsNb(eventsNb int) {
	w.s.WriteMore()
	w.s.WriteObjectField(EVENTS_NB)
	w.s.WriteInt(eventsNb)
}

// Beginning of the events array
func (w *DataWriter) WriteEventsArrayBegin() {
	w.s.WriteMore()
	w.s.WriteObjectField(EVENTS_ARRAY)
	w.s.WriteArrayStart()
}

// Event data, WriteEventArrayBegin shall be call before
// eventAfter determine if another event will be written afterward
func (w *DataWriter) WriteEvent(event []byte, eventAfter bool) {
	w.s.Write(event)
	if eventAfter {
		w.s.WriteMore()
	}
}

// End of the events array
func (w *DataWriter) WriteEventsArrayEnd() {
	w.s.WriteArrayEnd()
}
