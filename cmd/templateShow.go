package cmd

import "github.com/spf13/cobra"

var (
	templateShowCmd = &cobra.Command{
		Use:   "show",
		Short: "Show generated events from template input",
		RunE:  runTemplateShow,
	}
)

func init() {

}

func runTemplateShow(cmd *cobra.Command, args []string) error {
	return nil
}
