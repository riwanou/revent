package cmd

import "github.com/spf13/cobra"

var (
	TemplateInputPath string
	templateCmd       = &cobra.Command{
		Use:   "template",
		Short: "Events generation with template",
	}
)

func init() {
	templateCmd.PersistentFlags().StringVarP(&TemplateInputPath, "input",
		"i", "", "input template file")
	templateCmd.MarkPersistentFlagRequired("input")

	templateCmd.AddCommand(templateShowCmd)
}
