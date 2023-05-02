package cmd

import (
	"os"
	"sync"

	"frafos.com/revent/es"
	"github.com/spf13/cobra"
	mpb "github.com/vbauerster/mpb/v8"
	decor "github.com/vbauerster/mpb/v8/decor"
)

var (
	OutputPath string
	Limit      int
	fetchCmd   = &cobra.Command{
		Use:   "fetch",
		Short: "Fetch indices and events from Elasticsearch",
		RunE:  runFetch,
	}
)

func init() {
	fetchCmd.Flags().StringVarP(&OutputPath, "output", "o",
		"", "output directory path")
	fetchCmd.Flags().IntVarP(&Limit, "limit", "l", 1000, "max number of events")
	fetchCmd.MarkFlagRequired("output")
}

func newBar(p *mpb.Progress, name string, errMsg string) *mpb.Bar {
	return p.AddBar(0,
		mpb.BarFillerClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name(name, decor.WC{W: len(name) + 1, C: decor.DSyncSpaceR}),
			decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.OnAbort(
				decor.OnComplete(decor.AverageSpeed(0, " %.f ev/s"), "done"),
				errMsg,
			),
		),
	)
}

func runFetch(cmd *cobra.Command, args []string) error {

	client, err := es.NewEsClient(URL)
	if err != nil {
		return err
	}

	if err := client.FetchIndices(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	p := mpb.New(
		mpb.WithWaitGroup(&wg),
		mpb.WithWidth(30),
	)
	wg.Add(len(client.Indices))

	// fetch events for all indices
	for index := range client.Indices {

		f, err := os.Create(OutputPath + "/" + index)
		if err != nil {
			return err
		}

		bar := newBar(p, "\033[38;2;120;180;255m"+index+"\033[39m  ",
			"error while fetching")

		go func(f *os.File, index string) {
			defer wg.Done()
			defer f.Close()
			if err := client.FetchEvents(f, bar, index, Limit); err != nil {
				bar.Abort(false)
			}
		}(f, index)

	}

	p.Wait()

	return nil
}
