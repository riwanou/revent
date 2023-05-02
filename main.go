package main

import (
	"log"
	"os"

	"frafos.com/revent/cmd"
)

func main() {
	log.SetOutput(os.Stderr)
	cmd.Execute()
}