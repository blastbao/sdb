package ssdb

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/dty1er/sdb/btree"
)

// Tuple represents a row in a table. The size varies.
// The tuple layout looks like below:
// |Type(4byte)|value(Nbyte)|Type(4byte)|value(Nbyte)|...|Type(4byte)|value(Nbyte)|
// The N depends on the type.
// e.g. When the type is int32, the length is 4byte (=32bit).
//      When the type is [64]byte, the length is 64 byte.
type Tuple struct {
	Data []*TupleData
}

// TupleData represents a column in a row.
type TupleData struct {
	Key       bool
	Typ       Type
	Int32Val  int32
	Int64Val  int64
	Byte64Val [64]byte
}

// NewTuple returns a tuple which represents a row in a table.
// values are supposed to be the multiple column value of a column.
func NewTuple(values []interface{}) *Tuple {
	t := &Tuple{Data: make([]*TupleData, len(values))}
	for i, v := range values {
		switch actual := v.(type) {
		case int32:
			t.Data[i] = &TupleData{Typ: Int32, Int32Val: actual}
		case int64:
			t.Data[i] = &TupleData{Typ: Int64, Int64Val: actual}
		case [64]byte:
			t.Data[i] = &TupleData{Typ: Byte64, Byte64Val: actual}
		default:
			fmt.Fprintf(os.Stdout, "[WARN] unexpected type in init tuple")
		}
	}

	return t
}

// TODO? the name "Type" might be too generic
type Type uint32

const (
	// TODO: support more types
	Int32 Type = iota + 1
	Int64
	Byte64
)

// Serialize encodes given t into byte slice. The size is not fixed.
func SerializeTuple(t *Tuple) []byte {
	var buf bytes.Buffer
	for _, d := range t.Data {
		var result []byte
		switch d.Typ {
		case Int32: // 4byte
			result = make([]byte, 4+4)
			val := make([]byte, 4)
			putUint32OnBytes(val, uint32(d.Int32Val))
			copy(result[4:], val)
		case Int64:
			result = make([]byte, 4+8)
			val := make([]byte, 8)
			putUint64OnBytes(val, uint64(d.Int64Val))
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
func DeserializeTuple(bs []byte) *Tuple {
	t := &Tuple{}
	offset := 0
	for {
		if len(bs) <= offset { // return once finished reading bs
			break
		}

		typ := Type(bytesToUint32(bs[offset : offset+4]))
		switch typ {
		case Int32: // 4byte
			d := &TupleData{Typ: Int32}
			d.Int32Val = int32(bytesToUint32(bs[offset+4 : offset+4+4]))
			offset += 4 + 4
			t.Data = append(t.Data, d)
		case Byte64:
			d := &TupleData{Typ: Byte64}
			var buff [64]byte
			copy(buff[:], bs[offset+4:offset+4+64])
			d.Byte64Val = buff
			offset += 4 + 64
			t.Data = append(t.Data, d)
		}
	}

	return t
}

func (t *Tuple) String() string {
	sb := strings.Builder{}
	// put spaces at the head to print as an element of page. See page.String()
	sb.WriteString("    Tuple{\n")
	for _, d := range t.Data {
		if d.Key {
			sb.WriteString("      (Key)\n")
		}
		switch d.Typ {
		case Int32:
			sb.WriteString(fmt.Sprintf("      (int32) %v,\n", d.Int32Val))
		case Int64:
			sb.WriteString(fmt.Sprintf("      (int64) %v,\n", d.Int64Val))
		case Byte64:
			sb.WriteString(fmt.Sprintf("      (byte64) %v,\n", string(d.Byte64Val[:])))
		}
	}
	sb.WriteString("    },")

	return sb.String()
}

// Less satisfies btree.Item interface
func (t *Tuple) Less(than btree.Item) bool {
	thanT := than.(*Tuple)
	for _, data := range t.Data {
		if data.Key {
			for _, thanD := range thanT.Data {
				if thanD.Key {
					switch data.Typ {
					case Int32:
						return data.Int32Val < thanD.Int32Val
					case Int64:
						return data.Int64Val < thanD.Int64Val
					case Byte64:
						// bytes.Compare(a, b) returns negative if a < b
						return bytes.Compare(data.Byte64Val[:], thanD.Byte64Val[:]) < 0
					}
				}
			}
		}
	}

	return false
}
