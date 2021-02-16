package ssdb

import (
	"encoding/gob"
	"fmt"
	"io"
)

// Tuple represents a row in a table. The length varies.
type Tuple struct {
	Data []TupleData
}

type TupleData struct {
	Typ       Type
	Int32Val  int32
	Char64Val [64]byte
}

type Type int

const (
	// TODO: support more types
	Int32 Type = iota + 1
	Char64
)

// Serialize encodes t into w.
func (t *Tuple) Serialize(w io.Writer) error {
	if err := gob.NewEncoder(w).Encode(t); err != nil {
		return fmt.Errorf("encode tuple by encoding/gob: %w", err)
	}

	return nil
}

// Deserialize decodes r into t.
// Note that t is overwritten.
func (t *Tuple) Deserialize(r io.Reader) error {
	if err := gob.NewDecoder(r).Decode(t); err != nil {
		return fmt.Errorf("decode tuple by encoding/gob: %w", err)
	}

	return nil
}
