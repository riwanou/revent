package es

import (
	"os"
	"sort"
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestDummyAnalyze(t *testing.T) {
	f, err := os.Open("../testdata/status-2023.05.01")
	if err != nil {
		t.Error(err)
	}

	var expectedUniqueKeys = []string{
		"@timestamp,@version,CallAttempts,CallEnd,attrs,host," +
			"message,sum_reg_del_and_expired,tags,type,type2,type_translate",
	}
	sort.Strings(expectedUniqueKeys)

	analyzer := NewAnalyzer()
	data, err := analyzer.Analyze(f, nil)
	if err != nil {
		t.Error(err)
	}

	keys := make([]string, 0, len(data.UniqueEvents))
	for k := range data.UniqueEvents {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i, v := range keys {
		assert.Equal(t, v, expectedUniqueKeys[i], "unexpected event keys")
	}
}
