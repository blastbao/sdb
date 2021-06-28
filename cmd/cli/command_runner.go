package cli

import (
	"os"
)

type Runner interface {
	Name() string
	Init([]string) error
	Run() error
}

func Run(args []string) error {
	cmds := []Runner{
		NewDebugCommand(),
		NewServerCommand(),
	}

	if len(os.Args) < 2 {
		return runCli()
	}

	subcommand := args[0]

	for _, cmd := range cmds {
		if cmd.Name() == subcommand {
			cmd.Init(args[1:])
			return cmd.Run()
		}
	}

	return runCli()
}

