package es

import (
	"fmt"
	"strings"
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestDummyRead(t *testing.T) {
	r := NewDataReader(strings.NewReader(DUMMY_FETCH_DATA))
	assert.Equal(t, r.ReadIndexName(), "foo", "unexpected index name")
	assert.Equal(t, string(r.ReadIndexData()), "{}", "unexpected index data")
	assert.Equal(t, r.ReadEventsFetchLimit(), 10, "unexpected fetch limit")
	assert.Equal(t, r.ReadEventsNb(), 100, "unexpected events nb")

	i := 0
	expectedData := []string{"{}", "{}", "{}"}
	r.ReadEvents(func(data []byte) bool {
		assert.Equal(t, string(data), expectedData[i],
			fmt.Sprint("unexpected data at", i))
		i += 1
		return true
	})
}
