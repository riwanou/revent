package es

import (
	"errors"

	"github.com/elastic/go-elasticsearch/v8"
)

const _buflen = 1024

type EsClient struct {
	es      *elasticsearch.Client
	Indices map[string][]byte
	Error   error
}

func (c *EsClient) error(msg string) {
	c.Error = errors.New(msg)
}

// connect to elasticsearch database
func NewEsClient(addr string) (*EsClient, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			addr,
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &EsClient{es: es}, nil
}
