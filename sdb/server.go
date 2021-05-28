package sdb

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/dty1er/sdb/config"
	"github.com/dty1er/sdb/engine"
)

type SDB struct {
	engine *engine.Engine

	server *http.Server
}

func New() (*SDB, error) {
	conf, err := config.Process()
	if err != nil {
		return nil, fmt.Errorf("process configuration: %w", err)
	}

	e, err := engine.New(conf.Server)
	if err != nil {
		return nil, fmt.Errorf("initialize storage engine: %w", err)
	}

	sdb := &SDB{}

	server := &http.Server{}
	server.Addr = fmt.Sprintf(":%d", conf.Server.Port)

	mux := http.NewServeMux()
	mux.Handle("/execute", sdb.mainHandler())

	server.Handler = mux

	sdb.server = server
	sdb.engine = e

	return sdb, nil
}

func (s *SDB) Run() error {
	fmt.Fprintf(os.Stdout, "sdb server started running\n")
	return s.server.ListenAndServe()
}

func (s *SDB) Shutdown(ctx context.Context) error {
	if err := s.engine.Shutdown(); err != nil {
		return nil
	}

	if err := s.server.Shutdown(ctx); err != nil {
		return nil
	}

	return nil
}
