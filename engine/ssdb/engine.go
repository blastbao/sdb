package ssdb

import "fmt"

// Engine is ssdb core storage engine.
// It implements sdb/engine.Engine interface.
type Engine struct {
	bufferPool *BufferPool
}

// config is the configuration of ssdb storage engine.
type config struct {
	BufferPoolEntryCount int
}

func New() (*Engine, error) {
	config := &config{BufferPoolEntryCount: 1000} // TODO: use config file

	bufferPool, err := NewBufferPool(config.BufferPoolEntryCount)
	if err != nil {
		return nil, fmt.Errorf("initialize engine: %w", err)
	}

	return &Engine{bufferPool: bufferPool}, nil
}
}
