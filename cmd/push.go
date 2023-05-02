package cmd

import (
	"log"
	"os"
	"path"
	"sync"

	"frafos.com/revent/es"
	"github.com/spf13/cobra"
	mpb "github.com/vbauerster/mpb/v8"
)

var (
	InputPath string
	Wipe      bool
	pushCmd   = &cobra.Command{
		Use:   "push",
		Short: "Push an event to logstash",
		RunE:  runPush,
	}
)

func init() {
	pushCmd.Flags().StringVarP(&InputPath, "input", "i",
		"", "input directory path")
	pushCmd.Flags().BoolVarP(&Wipe, "wipe", "w",
		true, "wipe old index with same name")
	pushCmd.MarkFlagRequired("input")
}

func runPush(cmd *cobra.Command, args []string) error {

	client, err := es.NewEsClient(URL)
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(InputPath)
	if err != nil {
		return err
	}

	var indices []string
	for _, entry := range entries {
		if !entry.IsDir() {
			indices = append(indices, entry.Name())
		}
	}
	if err := client.WipeIndices(indices); err != nil {
		return err
	}

	// return nil

	var wg sync.WaitGroup
	p := mpb.New(
		mpb.WithWaitGroup(&wg),
		mpb.WithWidth(30),
	)
	wg.Add(len(indices))

	// push events for all indices
	for _, index := range indices {

		f, err := os.Open(path.Join(InputPath, index))
		if err != nil {
			return err
		}

		bar := newBar(p, "\033[38;2;120;180;255m"+index+"\033[39m  ",
			"error while pushing")

		go func(f *os.File, index string) {
			defer wg.Done()
			defer f.Close()
			if err := client.CreatePushIndex(f, bar, 2); err != nil {
				log.Println(err)
				bar.Abort(false)
			}
		}(f, index)
	}

	p.Wait()

	return nil
}
