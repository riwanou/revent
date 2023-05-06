package es

import (
	"bytes"
	"io"
	"testing"
)

func TestDummyFetch(t *testing.T) {
	const expectedData = `{` +
		`"indexName":"foo",` +
		`"indexData":{},` +
		`"eventsFetchNb":10,` +
		`"eventsNb":100,` +
		`"events":[` +
		`{},{},{}` +
		`]` +
		`}`

	var b bytes.Buffer
	func() {
		w := NewDataWriter(io.Writer(&b))
		defer w.WriteEnd()
		w.WriteIndex("foo", []byte("{}"))
		w.WriteEventsFetchNb(10)
		w.WriteEventsNb(100)
		w.WriteEventsArrayBegin()
		w.WriteEvent([]byte("{}"), true)
		w.WriteEvent([]byte("{}"), true)
		w.WriteEvent([]byte("{}"), false)
		w.WriteEventsArrayEnd()
	}()

	if b.String() != expectedData {
		t.Error("String not matching. \nleft:", b.String(), "\nright:", expectedData)
	}
}
