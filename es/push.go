package es

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	jsoniter "github.com/json-iterator/go"
	"github.com/vbauerster/mpb/v8"
)

type pushState struct {
	index      string
	iter       *jsoniter.Iterator
	onEvent    func(nb int)
	onFinished func()
}

// push error to jsoniter iterator
func (s *pushState) error(context string, msg string) bool {
	s.iter.ReportError(context, msg)
	return false
}

// create an es index and push all events into it
func (c *EsClient) CreatePushIndex(reader io.Reader, bar *mpb.Bar, batch int) error {

	s := &pushState{
		onEvent: func(nb int) {
			bar.SetCurrent(int64(nb))
		},
		onFinished: func() {
			bar.EnableTriggerComplete()
		},
	}
	s.iter = jsoniter.Parse(jsoniter.ConfigFastest, reader, _buflen)

	bar.SetTotal(100000, false)

	s.iter.ReadMapCB(func(_ *jsoniter.Iterator, field string) bool {
		switch field {
		case "name":
			s.index = s.iter.ReadString()
			return true
		case "index":
			s.iter.Skip()
			// return c.createIndex(s)
		case "nb_events":
			bar.SetTotal(int64(s.iter.ReadInt()), false)
		case "events":
			return c.pushEvents(s, batch)
		default:
			s.iter.Skip()
		}
		return true
	})

	if c.Error != nil {
		return c.Error
	}

	return s.iter.Error
}

// remove private field in index definition
func filterIndexDefinition(iter *jsoniter.Iterator) ([]byte, error) {

	var def map[string]interface{}
	jsoniter.Unmarshal(iter.SkipAndReturnBytes(), &def)

	settings, ok := def["settings"]
	if !ok {
		return nil, errors.New("settings not found in index def")
	}

	index, ok := settings.(map[string]interface{})["index"]
	if !ok {
		return nil, errors.New("index not found in settings")
	}

	obj, ok := index.(map[string]interface{})
	if !ok {
		return nil, errors.New("unexpected index struct in settings")
	}

	delete(obj, "uuid")
	delete(obj, "version")
	delete(obj, "provided_name")
	delete(obj, "creation_date")

	data, err := jsoniter.Marshal(&def)
	if err != nil {
		return nil, err
	}

	return data, iter.Error
}

// wipe old indices
func (c *EsClient) WipeIndices(indices []string) error {

	res, err := c.es.Indices.Delete(indices,
		c.es.Indices.Delete.WithIgnoreUnavailable(true),
	)
	if err != nil {
		return err
	}
	if res.IsError() {
		if data, err := io.ReadAll(res.Body); err == nil {
			return errors.New(string(data))
		}
		return errors.New("Could not remove old indices")
	}

	return nil
}

// create an es index
func (c *EsClient) createIndex(s *pushState) bool {
	def, err := filterIndexDefinition(s.iter)
	if err != nil {
		c.error(err.Error())
	}

	res, err := c.es.Indices.Create(
		s.index,
		c.es.Indices.Create.WithBody(bytes.NewReader(def)),
	)
	if err != nil {
		c.error(err.Error())
		return false
	}

	if res.IsError() {
		if data, err := io.ReadAll(res.Body); err == nil {
			c.error(string(data))
		}
		return false
	}

	return true
}

// push all events to es index
func (c *EsClient) pushEvents(s *pushState, batch int) bool {

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:  s.index,
		Client: c.es,
		// NumWorkers: runtime.NumCPU(),
		FlushBytes: int(1024 * 1024 * 20),
	})
	if err != nil {
		c.error(err.Error())
		return false
	}

	// start := time.Now().UTC()

	s.iter.ReadArrayCB(func(_ *jsoniter.Iterator) bool {
		data := s.iter.SkipAndReturnBytes()
		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action: "create",
				Body:   bytes.NewReader(data),
			},
		)
		if err != nil {
			c.error(err.Error())
			return false
		}

		s.onEvent(int(bi.Stats().NumAdded))
		return true
	})

	if err := bi.Close(context.Background()); err != nil {
		c.error(err.Error())
		return false
	}

	// dur := time.Since(start)
	// log.Println(bi.Stats(), dur)

	s.onFinished()

	if bi.Stats().NumFailed > 0 {
		log.Println("ERROR")
		c.error(fmt.Sprintf("Bulk index failed for %d events",
			bi.Stats().NumFailed))
		return false
	}

	return true
}
