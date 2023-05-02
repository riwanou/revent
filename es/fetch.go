package es

import (
	"io"
	"time"

	jsoniter "github.com/json-iterator/go"
	mpb "github.com/vbauerster/mpb/v8"
)

type fetchState struct {
	iter    *jsoniter.Iterator
	onMeta  func(fetchMeta)
	onEvent func([]byte)
}

type fetchMeta struct {
	nbDocs   int
	scrollID string
}

// min between two number
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// fetch from elasticsearch indices data and populate client
func (c *EsClient) FetchIndices() error {
	res, err := c.es.Indices.Get([]string{"*"})
	if err != nil {
		return err
	}
	defer res.Body.Close()

	iter := jsoniter.Parse(jsoniter.ConfigFastest, res.Body, _buflen)
	c.Indices = make(map[string][]byte)

	iter.ReadMapCB(func(_ *jsoniter.Iterator, index string) bool {
		c.Indices[index] = iter.SkipAndReturnBytes()
		return true
	})
	if iter.Error != nil {
		return iter.Error
	}

	return nil
}

// write beginning of custom json and index data
func (c *EsClient) writeIndexInit(s *jsoniter.Stream, index string) {
	s.WriteObjectStart()

	s.WriteObjectField("name")
	s.WriteString(index)

	s.WriteMore()
	s.WriteObjectField("index")
	s.Write(c.Indices[index])
}

func (c *EsClient) writeMetadata(s *jsoniter.Stream, meta fetchMeta) {
	s.WriteMore()
	s.WriteObjectField("nb_events")
	s.WriteInt(meta.nbDocs)
}

func (c *EsClient) writeEventsInit(s *jsoniter.Stream) {
	s.WriteMore()
	s.WriteObjectField("events")
	s.WriteArrayStart()
}

// write single event, more indicate if `,` is needed
func (c *EsClient) writeEvent(s *jsoniter.Stream, event []byte, more bool) {
	s.Write(event)
	if more {
		s.WriteMore()
	}
}

// write end of custom json format
func (c *EsClient) writeEnd(s *jsoniter.Stream) {
	s.WriteArrayEnd()
	s.WriteObjectEnd()
}

// fetch all events from an index and write all needed data
// to reconstruct it
func (c *EsClient) FetchEvents(writer io.Writer,
	bar *mpb.Bar, index string, limit int) error {

	// write beginning of file and index data
	s := jsoniter.NewStream(jsoniter.ConfigFastest, writer, _buflen)
	defer s.Flush()
	c.writeIndexInit(s, index)

	var scrollID string
	var nbEvents int
	fetched := 0
	onEvent := func(data []byte) {
		fetched += 1
		c.writeEvent(s, data, fetched < nbEvents)
		bar.Increment()
	}

	// fetch initial events
	err := c.fetchFirstEvents(writer, index, min(limit, 10000), fetchState{
		onMeta: func(meta fetchMeta) {
			nbEvents = min(meta.nbDocs, limit)
			bar.SetTotal(int64(nbEvents), false)
			bar.EnableTriggerComplete()
			scrollID = meta.scrollID

			// write nb docs, beginning of events array
			c.writeMetadata(s, meta)
			c.writeEventsInit(s)
		},
		onEvent: onEvent,
	})
	if err != nil {
		return err
	}
	limit -= fetched

	// scroll fetch till no events left
	for fetched < nbEvents {
		err := c.fetchScrollEvents(writer, scrollID, fetchState{
			onMeta: func(meta fetchMeta) {
				scrollID = meta.scrollID
			},
			onEvent: onEvent,
		})
		if err != nil {
			return err
		}
		limit -= fetched
	}

	// finish json object
	c.writeEnd(s)

	return s.Error
}

// original query to fetch events
func (c *EsClient) fetchFirstEvents(writer io.Writer, index string,
	limit int, state fetchState) error {

	query := ""
	sort := []string{""}

	res, err := c.es.Search(
		c.es.Search.WithIndex(index),
		c.es.Search.WithQuery(query),
		c.es.Search.WithSort(sort...),
		c.es.Search.WithScroll(time.Minute),
		c.es.Search.WithSize(limit),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	state.iter = jsoniter.Parse(jsoniter.ConfigFastest, res.Body, _buflen)
	return state.consumeResponse()
}

// from scrollID, fetch next events
func (c *EsClient) fetchScrollEvents(writer io.Writer, scrollID string,
	state fetchState) error {

	res, err := c.es.Scroll(
		c.es.Scroll.WithScroll(time.Minute),
		c.es.Scroll.WithScrollID(scrollID),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	state.iter = jsoniter.Parse(jsoniter.ConfigFastest, res.Body, _buflen)
	return state.consumeResponse()
}

// push error to jsoniter iterator
func (s *fetchState) error(context string, msg string) bool {
	s.iter.ReportError(context, msg)
	return false
}

// consume response body stream, extract metadata and events
func (s *fetchState) consumeResponse() error {
	var meta fetchMeta

	s.iter.ReadMapCB(func(_ *jsoniter.Iterator, field string) bool {
		switch field {
		case "_scroll_id":
			meta.scrollID = s.iter.ReadString()
		case "hits":
			return s.iter.ReadMapCB(func(_ *jsoniter.Iterator, field string) bool {
				switch field {
				case "total":
					meta.nbDocs = s.consumeTotal()
					if s.iter.Error != nil {
						return s.error("response", "could not parse total")
					}
				case "hits":
					s.onMeta(meta)
					s.consumeEvents()
					if s.iter.Error != nil {
						return s.error("response", "could not parse event")
					}
				default:
					s.iter.Skip()
				}
				return true
			})
		default:
			s.iter.Skip()
		}
		return true
	})

	return s.iter.Error
}

// consume total number of events
func (s *fetchState) consumeTotal() int {
	var total int
	s.iter.ReadMapCB(func(_ *jsoniter.Iterator, field string) bool {
		switch field {
		case "value":
			total = s.iter.ReadInt()
		default:
			s.iter.Skip()
		}
		return true
	})
	return total
}

// consume all events in response
func (s *fetchState) consumeEvents() {
	s.iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
		return s.iter.ReadMapCB(func(_ *jsoniter.Iterator, field string) bool {
			switch field {
			case "_source":
				s.onEvent(iter.SkipAndReturnBytes())
			default:
				s.iter.Skip()
			}
			return true
		})
	})
}
