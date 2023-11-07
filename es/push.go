package es

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	jsoniter "github.com/json-iterator/go"
	"github.com/vbauerster/mpb/v8"
)

const _flushlen = 1024 * 1024 * 8

type pushState struct {
	dr         *DataReader
	indexName  string
	onEvent    func(nb int)
	onFinished func()
}

// create an es index and push all events into it
func (c *EsClient) CreatePushIndex(reader io.Reader, bar *mpb.Bar) error {

	s := &pushState{
		dr: NewDataReader(reader),
		onEvent: func(nb int) {
			bar.SetCurrent(int64(nb))
		},
		onFinished: func() {
			bar.EnableTriggerComplete()
		},
	}

	s.indexName = s.dr.ReadIndexName()
	indexData := s.dr.ReadIndexData()
	c.createIndex(s.indexName, indexData)

	limit := s.dr.ReadEventsFetchLimit()
	nbEvents := s.dr.ReadEventsNb()
	bar.SetTotal(int64(min(nbEvents, limit)), false)

	c.pushEvents(s)

	if c.Error != nil {
		return c.Error
	}
	return s.dr.Error()
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

// remove private field in index definition
func filterIndexDefinition(indexData []byte) ([]byte, error) {

	var def map[string]interface{}
	jsoniter.Unmarshal(indexData, &def)

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

	return data, nil
}

// create an es index
func (c *EsClient) createIndex(indexName string, indexData []byte) bool {
	def, err := filterIndexDefinition(indexData)
	if err != nil {
		c.error(err.Error())
	}

	res, err := c.es.Indices.Create(
		indexName,
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
func (c *EsClient) pushEvents(s *pushState) bool {

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      s.indexName,
		Client:     c.es,
		FlushBytes: _flushlen,
	})
	if err != nil {
		c.error(err.Error())
		return false
	}

	s.dr.ReadEvents(func(data []byte) bool {
		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action: "create",
				Body:   bytes.NewReader(data),
				OnSuccess: func(context.Context, esutil.BulkIndexerItem, esutil.BulkIndexerResponseItem) {
					s.onEvent(int(bi.Stats().NumFlushed))
				},
			},
		)
		if err != nil {
			c.error(err.Error())
			return false
		}

		return true
	})

	if err := bi.Close(context.Background()); err != nil {
		c.error(err.Error())
		return false
	}

	s.onFinished()

	if bi.Stats().NumFailed > 0 {
		c.error(fmt.Sprintf("Bulk index failed for %d events",
			bi.Stats().NumFailed))
		return false
	}

	return true
}
