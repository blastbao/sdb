package cli

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/dty1er/sdb/catalog"
	"github.com/dty1er/sdb/config"
	"github.com/dty1er/sdb/diskmanager"
	"github.com/dty1er/sdb/engine"
	"github.com/dty1er/sdb/executor"
	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/planner"
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

	diskManager := diskmanager.New(conf.Server.DBFilesDirectory)

	catalog, err := catalog.New(diskManager)
	if err != nil {
		return fmt.Errorf("initialize catalog: %w", err)
	}

	parser := parser.New(catalog)

	engine, err := engine.New(conf.Server, catalog, diskManager)
	if err != nil {
		return fmt.Errorf("initialize storage engine: %w", err)
	}

	executor := executor.New(engine, catalog)

	planner := planner.New(catalog)

	sdb := sdb.New(parser, planner, catalog, executor, engine, diskManager)

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
