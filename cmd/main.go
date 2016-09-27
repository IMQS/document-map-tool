package main

import (
	"fmt"

	"github.com/IMQS/cli"
	documentGeom "github.com/dbferreira/mongo2postgres"
)

func main() {
	app := cli.App{}
	app.Description = "The document geometry tool reads document records from MongoDB, translates\nthe data into valid SQL and inserts the result into Postgres.\n\nUSAGE:\ndocgeom -c=configfile [options] command"
	app.DefaultExec = exec
	app.AddCommand("run", "Run the docgeom tool")
	app.AddValueOption("c", "configfile", "Configuration file. This option is mandatory")
	app.Run()
}

func exec(cmdName string, args []string, options cli.OptionSet) int {
	configFile := options["c"]
	if configFile == "" {
		fmt.Printf("You must specify a config file\n")
		return 1
	}

	server := documentGeom.Server{}
	err := server.Config.NewConfig(configFile)
	if err != nil {
		fmt.Printf("Error loading docgeom config: %v\n", err)
		return 1
	}

	if err := server.Initialize(); err != nil {
		fmt.Printf("Error initializing docgeom server: %v\n", err)
		return 1
	}

	run := func() {
		err := server.TranslateDocumentGeometry()
		if err != nil {
			server.Log.Errorf("%v\n", err)
			return
		}
	}

	switch cmdName {
	case "run":
		run()
	default:
		fmt.Printf("Unknown command %v\n", cmdName)
	}

	return 0
}
