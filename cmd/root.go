package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	URL     string
	rootCmd = &cobra.Command{
		Use:   "revent",
		Short: "Events generator for logstash",
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&URL, "url", "u",
		"http://localhost:9200", "URL")

	rootCmd.AddCommand(fetchCmd)
	rootCmd.AddCommand(pushCmd)
	rootCmd.AddCommand(analyzeCmd)

	rootCmd.AddCommand(templateCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
