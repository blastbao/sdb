package ssdb

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// Tuple represents a row in a table. The size varies.
type Tuple struct {
	Data []TupleData
}

// TupleData represents a column in a row.
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

// Serialize encodes given t into byte slice.
func SerializeTuple(t Tuple) ([]byte, error) {
	buff := bytes.Buffer{}
	if err := gob.NewEncoder(&buff).Encode(t); err != nil {
		return nil, fmt.Errorf("encode tuple by encoding/gob: %w", err)
	}

	return buff.Bytes(), nil
}

// Deserialize decodes given byte slice to a tuple.
func DeserializeTuple(b []byte) (Tuple, error) {
	t := Tuple{}
	br := bytes.NewReader(b)
	if err := gob.NewDecoder(br).Decode(&t); err != nil {
		return t, fmt.Errorf("decode tuple by encoding/gob: %w", err)
	}

	return t, nil
}
