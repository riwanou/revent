package cmd

import (
	"fmt"
	"log"
	"os"
	"path"
	"sync"

	"frafos.com/revent/es"
	"github.com/spf13/cobra"
	mpb "github.com/vbauerster/mpb/v8"
)

var (
	AnalyzeInputPath  string
	AnalyzeOutputPath string
	OnlyKeys          bool
	analyzeCmd        = &cobra.Command{
		Use:   "analyze",
		Short: "Analyze events",
		RunE:  runAnalyze,
	}
)

func init() {
	analyzeCmd.Flags().StringVarP(&AnalyzeInputPath, "input", "i",
		"", "input directory path")
	analyzeCmd.Flags().BoolVarP(&OnlyKeys, "only-keys", "k", false,
		"only show the unique event keys")
	analyzeCmd.Flags().StringVarP(&AnalyzeOutputPath, "output", "o",
		"", "output directory path")
	analyzeCmd.MarkFlagRequired("input")
}

func showReport(data es.AnalyzeData) {
	fmt.Printf("\nFound %d different event types in %d analyzed events. \n\n",
		len(data.UniqueEvents), data.NbEvents)
	i := 0
	for k := range data.UniqueEvents {
		i += 1
		fmt.Printf("%d :[%s]\n", i, k)
	}
}

func runAnalyze(cmd *cobra.Command, args []string) error {
	indices, err := ReadIndices(AnalyzeInputPath)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	p := mpb.New(
		mpb.WithWaitGroup(&wg),
		mpb.WithWidth(30),
	)
	wg.Add(len(indices))
	analyzeDataChan := make(chan es.AnalyzeData)

	for _, index := range indices {
		f, err := os.Open(path.Join(AnalyzeInputPath, index))
		if err != nil {
			log.Println(err)
		}

		bar := newBar(p, "\033[38;2;120;180;255m"+index+"\033[39m  ",
			"error while analyzing")

		go func(f *os.File, index string) {
			defer wg.Done()
			defer f.Close()

			analyzer := es.NewAnalyzer()
			data, err := analyzer.Analyze(f, bar)
			if err != nil {
				bar.Abort(false)
			}

			analyzeDataChan <- data
		}(f, index)
	}

	indicesData := es.AnalyzeData{
		NbEvents:     0,
		UniqueEvents: make(map[string]es.EventAnalyzeData),
	}

	go func() {
		wg.Wait()
		close(analyzeDataChan)
	}()

	for data := range analyzeDataChan {
		for k, v := range data.UniqueEvents {
			indicesData.UniqueEvents[k] = v
		}
		indicesData.NbEvents += data.NbEvents
	}

	// output file
	output := len(AnalyzeOutputPath) > 0
	if output {
		indexEvents := make(map[string][][]byte)
		for _, v := range indicesData.UniqueEvents {
			list, ok := indexEvents[v.IndexName]
			if !ok {
				indexEvents[v.IndexName] = [][]byte{v.Data}
			} else {
				indexEvents[v.IndexName] = append(list, v.Data)
			}
		}
		for key, v := range indexEvents {
			fw, err := os.Create(path.Join(AnalyzeOutputPath, key))
			if err != nil {
				log.Println(err)
				continue
			}
			es.WriteOutput(fw, key, v)
		}
	}

	showReport(indicesData)

	return nil
}
