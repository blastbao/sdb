package main

import (
	"context"
	"flag"
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

	server, err := sdb.New()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	go func() {
		<-ctx.Done()
		server.Shutdown(ctx)
	}()

	err = server.Run()
	if err == http.ErrServerClosed {
		err = nil
	}

	return err
}
