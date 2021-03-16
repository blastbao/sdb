package ssdb

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

const PageSize = 16 * 1024 // 16KB

// Page manages multiple tuples as slotted page.
// The layout looks like below:
// -----------------------------
// |xxxx|xx|xxxx|xxxx|xxxx|xxxx| // header starts from the head
// |xxxx|xxxx|... ->           |
// |        [free space]       |
// |                 <- ...|xxx|
// |xxxxxx|xxxxxxx|xxxxxxxxxxxx| // tuples starts from the bottom
// -----------------------------
//
// header layout:
// |page_id(4byte)|tuples_count(2byte)|slot1(2byte)|slot2(2byte)|slot3(2byte)|...|slotN(2byte)|
// note: N is the same as tuples_count
//
// slot layout:
// |offset(2byte)|length(2byte)|
//
// The first slot represents of the first tuple. Because the tuples are placed from bottom to head,
// the first slot's offset is the starting point of the last section of the byte stream.
//
// This layout cannot avoid a few empty bytes between the tail of header and the head of tuples.
//
// tuple layout: see tuple.go
type Page struct {
	bs [PageSize]byte
}

type slot struct {
	offset uint16 // [2]byte
	length uint16 // [2]byte
}

type pageHeader struct {
	id          uint32 // [4]byte
	tuplesCount uint16 // [2]byte
	slots       []slot
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
		return bs, fmt.Errorf("encode page header by encoding/gob: %w", err)
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
		return nil, fmt.Errorf("decode page header by encoding/gob: %w", err)
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
