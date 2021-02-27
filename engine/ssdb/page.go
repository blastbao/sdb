package ssdb

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// Page contains multiple tuples in one sequential bytes on the disk.
type Page struct {
	Header *PageHeader
	Tuples []Tuple
}

type PageHeader struct {
	TupleCount     uint8
	TupleLocations []uint16
	FreeSpaceEnd   uint16
}

func SerializePage(p *Page) ([4096]byte, error) {
	var bs [4096]byte

	if p.Header == nil {
		p.Header = &PageHeader{}
	}

	tupleBytes := make([][]byte, len(p.Tuples))
	for i := 0; i < len(p.Tuples); i++ {
		b, err := SerializeTuple(p.Tuples[i])
		if err != nil {
			return bs, fmt.Errorf("serialize tuple: %w", err)
		}
		tupleBytes[i] = b
	}

	header := &PageHeader{TupleCount: uint8(len(p.Tuples)), FreeSpaceEnd: 4096, TupleLocations: make([]uint16, len(p.Tuples))}

	for i := 0; i < len(tupleBytes); i++ {
		header.TupleLocations[i] = uint16(int(header.FreeSpaceEnd) - len(tupleBytes[i]))
		copy(bs[header.TupleLocations[i]:header.FreeSpaceEnd], tupleBytes[i])
		header.FreeSpaceEnd -= uint16(len(tupleBytes[i]))
	}

	buff := bytes.Buffer{}
	if err := gob.NewEncoder(&buff).Encode(header); err != nil {
		return bs, fmt.Errorf("encode page by encoding/gob: %w", err)
	}

	headerLength := buff.Len()

	copy(bs[1:1+buff.Len()], buff.Bytes())
	bs[0] = byte(headerLength)

	return bs, nil
}

func DeserializePage(bs [4096]byte) (*Page, error) {
	headerLength := int(bs[0])

	var header PageHeader
	buff := bytes.NewReader(bs[1 : 1+headerLength])
	if err := gob.NewDecoder(buff).Decode(&header); err != nil {
		return nil, fmt.Errorf("decode page by encoding/gob: %w", err)
	}

	last := 4096
	tuples := make([]Tuple, header.TupleCount)
	for i := 0; i < len(header.TupleLocations); i++ {
		tb := bs[header.TupleLocations[i]:last]
		t, err := DeserializeTuple(tb)
		if err != nil {
			return nil, fmt.Errorf("deserialize tuple: %w", err)
		}

		tuples[i] = t
		last -= len(tb)
	}

	return &Page{Header: &header, Tuples: tuples}, nil
}
