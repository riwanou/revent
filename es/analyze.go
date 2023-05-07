package es

import (
	"bytes"
	"encoding/json"
	"io"
	"sort"
	"strings"

	mpb "github.com/vbauerster/mpb/v8"
)

type Analyzer struct {
	indexName      string
	nbEvents       int
	analyzedEvents int
	uniqueEvents   map[string]EventAnalyzeData
}

func NewAnalyzer() *Analyzer {
	return &Analyzer{
		nbEvents:       0,
		analyzedEvents: 0,
		uniqueEvents:   make(map[string]EventAnalyzeData),
	}
}

type EventAnalyzeData struct {
	IndexName string
	Data      []byte
}

type AnalyzeData struct {
	UniqueEvents map[string]EventAnalyzeData
	NbEvents     int
}

func (a *Analyzer) Analyze(reader io.Reader, bar *mpb.Bar) (AnalyzeData, error) {
	rd := NewDataReader(reader)
	a.indexName = rd.ReadIndexName()
	rd.ReadIndexData()
	fetchEventsLimit := rd.ReadEventsFetchLimit()
	fetchEventsNb := rd.ReadEventsNb()
	a.nbEvents += min(fetchEventsLimit, fetchEventsNb)
	if bar != nil {
		bar.SetTotal(int64(a.nbEvents), false)
	}

	rd.ReadEvents(func(data []byte) bool {
		res := a.analyzeEvent(data)
		if bar != nil {
			bar.SetCurrent(int64(a.analyzedEvents))
		}
		return res
	})

	if bar != nil {
		bar.EnableTriggerComplete()
	}
	data := AnalyzeData{
		UniqueEvents: a.uniqueEvents,
		NbEvents:     a.nbEvents,
	}
	return data, rd.Error()
}

func getKeys(obj map[string]interface{}) []string {
	keys := make([]string, 0, len(obj))
	for k, v := range obj {
		keys = append(keys, k)
		if subObj, ok := v.(map[string]interface{}); ok {
			subKeys := getKeys(subObj)
			keys = append(keys, subKeys...)
		}
	}
	return keys
}

func (a *Analyzer) analyzeEvent(data []byte) bool {
	var obj map[string]interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return false
	}

	keys := getKeys(obj)
	sort.Strings(keys)
	eventID := strings.Join(keys, ",")

	eventData := EventAnalyzeData{IndexName: a.indexName, Data: data}
	a.uniqueEvents[eventID] = eventData
	a.analyzedEvents += 1

	return true
}

func WriteOutput(writer io.Writer, indexName string, events [][]byte) {
	rw := NewDataWriter(writer)
	defer rw.WriteEnd()

	rw.WriteIndexName(indexName)

	rw.WriteEventsArrayBegin()
	defer rw.WriteEventsArrayEnd()

	total := len(events)
	for i, v := range events {
		i += 1
		var pretty bytes.Buffer
		json.Indent(&pretty, v, "", "  ")
		rw.WriteEvent(pretty.Bytes(), i < total)
	}
}
