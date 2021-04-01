package ssdb

// Engine is ssdb core storage engine.
// It implements sdb/engine.Engine interface.
type Engine struct {
	bufferPool *BufferPool
}

// config is the configuration of ssdb storage engine.
type config struct {
	BufferPoolEntryCount int
}

func New() *Engine {
	config := &config{BufferPoolEntryCount: 1000} // TODO: use config file

	return &Engine{bufferPool: NewBufferPool(config.BufferPoolEntryCount)}
}
