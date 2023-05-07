package es

import (
	"bytes"
	"io"
	"testing"
)

const DUMMY_FETCH_DATA = `{` +
	`"indexName":"foo",` +
	`"indexData":{},` +
	`"eventsFetchLimit":10,` +
	`"eventsNb":100,` +
	`"events":[` +
	`{},{},{}` +
	`]` +
	`}`

func TestDummyWrite(t *testing.T) {
	var b bytes.Buffer
	func() {
		w := NewDataWriter(io.Writer(&b))
		defer w.WriteEnd()
		w.WriteIndex("foo", []byte("{}"))
		w.WriteEventsFetchLimit(10)
		w.WriteEventsNb(100)
		w.WriteEventsArrayBegin()
		w.WriteEvent([]byte("{}"), true)
		w.WriteEvent([]byte("{}"), true)
		w.WriteEvent([]byte("{}"), false)
		w.WriteEventsArrayEnd()
	}()

	if b.String() != DUMMY_FETCH_DATA {
		t.Error("String not matching. \nleft:", b.String(), "\nright:", DUMMY_FETCH_DATA)
	}
}
