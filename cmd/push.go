package cmd

import (
	"os"
	"path"
	"sync"

	"frafos.com/revent/es"
	"github.com/spf13/cobra"
	mpb "github.com/vbauerster/mpb/v8"
)

var (
	PushInputPath  string
	keepOldIndices bool
	pushCmd        = &cobra.Command{
		Use:   "push",
		Short: "Push events to Elasticsearch",
		RunE:  runPush,
	}
)

func init() {
	pushCmd.Flags().StringVarP(&PushInputPath, "input", "i",
		"", "input directory path")
	pushCmd.Flags().BoolVarP(&keepOldIndices, "keep", "k",
		false, "do not wipe old indices with the same names")
	pushCmd.MarkFlagRequired("input")
}

// get indices from filenames
func ReadIndices(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var indices []string
	for _, entry := range entries {
		if entry.IsDir() || entry.Name()[0] == '.' {
			continue
		}
		indices = append(indices, entry.Name())
	}
	return indices, nil
}

func runPush(cmd *cobra.Command, args []string) error {

	indices, err := ReadIndices(PushInputPath)
	if err != nil {
		return err
	}

	// create an es client and wipe old indices
	client, err := es.NewEsClient(URL)
	if err != nil {
		return err
	}

	if !keepOldIndices {
		if err := client.WipeIndices(indices); err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	p := mpb.New(
		mpb.WithWaitGroup(&wg),
		mpb.WithWidth(30),
	)
	wg.Add(len(indices))

	// push events for all indices
	for _, index := range indices {

		f, err := os.Open(path.Join(PushInputPath, index))
		if err != nil {
			return err
		}

		bar := newBar(p, "\033[38;2;120;180;255m"+index+"\033[39m  ",
			"error while pushing")

		go func(f *os.File) {
			defer wg.Done()
			defer f.Close()
			if err := client.CreatePushIndex(f, bar); err != nil {
				bar.Abort(false)
			}
		}(f)
	}

	p.Wait()

	return nil
}
