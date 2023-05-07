package tmpl

import (
	"os"
	"testing"
	"text/template"
)

type Todo struct {
	Name string
}

func TestShow(t *testing.T) {
	td := Todo{"Hello"}
	tp, err := template.New("todos").Parse("Hello \"{{ .Name }}\"")
	if err != nil {
		t.Error(err)
	}
	err = tp.Execute(os.Stdout, td)
	if err != nil {
		panic(err)
	}
}
