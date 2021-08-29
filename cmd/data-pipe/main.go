package main

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

type cli struct {
	cfg cfg
}

type cfg struct {
}

func (c *cli) initConfig(cmd *cobra.Command, args []string) error {
	return nil
}

func (c *cli) run(cmd *cobra.Command, args []string) error {

	fmt.Print("Running")

	return nil
}

func main() {
	cli := &cli{}
	cmd := &cobra.Command{
		Use:     "data-pipe",
		PreRunE: cli.initConfig,
		RunE:    cli.run,
	}

	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
