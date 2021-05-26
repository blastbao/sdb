package cli

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/dty1er/sdb/sdb"
)

type ServerCommand struct {
	fs *flag.FlagSet
}

func NewServerCommand() *ServerCommand {
	return &ServerCommand{fs: flag.NewFlagSet("server", flag.ExitOnError)}
}

func (sc *ServerCommand) Name() string {
	return sc.fs.Name()
}

func (sc *ServerCommand) Init(args []string) error {
	return sc.fs.Parse(args)
}

func (sc *ServerCommand) Run() error {
	ctx := context.Background()

	db, err := sdb.New()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	go func() {
		<-ctx.Done()
		if err := db.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return
		}

		fmt.Fprintf(os.Stdout, "sdb server successfully stopped\n")
	}()

	err = db.Run()
	if err == http.ErrServerClosed {
		err = nil
	}

	return err
}
