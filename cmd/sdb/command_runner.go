package main

import (
	"errors"
	"fmt"
	"os"
)

type Runner interface {
	Name() string
	Init([]string) error
	Run() error
}

func run(args []string) error {
	if len(args) < 1 {
		return errors.New("at least one subcommand is needed")
	}

	cmds := []Runner{
		NewDebugCommand(),
	}

	subcommand := os.Args[1]

	for _, cmd := range cmds {
		if cmd.Name() == subcommand {
			cmd.Init(os.Args[2:])
			return cmd.Run()
		}
	}

	return fmt.Errorf("Unknown subcommand: %s", subcommand)
}
