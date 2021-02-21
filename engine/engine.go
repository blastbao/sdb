// engine is a storage engine of sdb.
// It works as a "backend" of sdb,
// which is responsible to store and load data, transactions,
// cache, log and checkpoint etc.
package engine

// Engine is storage engine interface.
// Because of this, storage engine is interchangeable in sdb like MySQL.
type Engine interface {
	// TODO: define interfaces
}
