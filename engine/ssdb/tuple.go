package ssdb

import (
	"bytes"
	"fmt"
	"os"
)

// Tuple represents a row in a table. The size varies.
type Tuple struct {
	Data []TupleData
}

// TupleData represents a column in a row.
type TupleData struct {
	Typ       Type
	Int32Val  int32
	Byte64Val [64]byte
}

func NewTuple(values []interface{}) *Tuple {
	t := &Tuple{Data: make([]TupleData, len(values))}
	for i, v := range values {
		switch actual := v.(type) {
		case int32:
			t.Data[i] = TupleData{Typ: Int32, Int32Val: actual}
		case [64]byte:
			t.Data[i] = TupleData{Typ: Byte64, Byte64Val: actual}
		default:
			fmt.Fprintf(os.Stdout, "[WARN] unexpected type in init tuple")
		}
	}

	return t
}

type Type uint32

const (
	// TODO: support more types
	Int32 Type = iota + 1
	Byte64
)

// Serialize encodes given t into byte slice.
func SerializeTuple(t Tuple) []byte {
	var buf bytes.Buffer
	for _, d := range t.Data {
		var result []byte
		switch d.Typ {
		case Int32: // 4byte
			result = make([]byte, 4+4)
			val := make([]byte, 4)
			putUint32OnBytes(val, uint32(d.Int32Val))
			copy(result[4:], val)
		case Byte64:
			result = make([]byte, 4+64)
			copy(result[4:], d.Byte64Val[:])
		}

		putUint32OnBytes(result[0:], uint32(d.Typ))

		buf.Write(result)
	}

	return buf.Bytes()
}

// Deserialize decodes given byte slice to a tuple.
func DeserializeTuple(bs []byte) Tuple {
	t := Tuple{}
	offset := 0
	for {
		if len(bs) <= offset { // return once finished reading bs
			break
		}

		typ := Type(bytesToUint32(bs[offset : offset+4]))
		switch typ {
		case Int32: // 4byte
			d := TupleData{Typ: Int32}
			d.Int32Val = int32(bytesToUint32(bs[offset+4 : offset+4+4]))
			offset += 4 + 4
			t.Data = append(t.Data, d)
		case Byte64:
			d := TupleData{Typ: Byte64}
			var buff [64]byte
			copy(buff[:], bs[offset+4:offset+4+64])
			d.Byte64Val = buff
			offset += 4 + 64
			t.Data = append(t.Data, d)
		}
	}

	return t
}
