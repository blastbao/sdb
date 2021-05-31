package cli

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/dty1er/sdb/config"
	"github.com/dty1er/sdb/engine"
	"github.com/dty1er/sdb/sdb"
	"github.com/dty1er/sdb/server"
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

	conf, err := config.Process()
	if err != nil {
		return fmt.Errorf("process configuration: %w", err)
	}

	// TODO: init parser, planner, catalog, executor, engine
	engine, err := engine.New(conf.Server)
	if err != nil {
		return fmt.Errorf("initialize storage engine: %w", err)
	}

	sdb := sdb.New(engine)

	svr := server.New(sdb, conf.Server.Port)

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	go func() {
		<-ctx.Done()
		if err := svr.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return
		}

		fmt.Fprintf(os.Stdout, "sdb server successfully stopped\n")
	}()

	err = svr.Run()
	if err == http.ErrServerClosed {
		err = nil
	}

	return err
}
