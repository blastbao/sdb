package engine

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/dty1er/sdb/sdb"
)

// Tuple represents a row in a table. The size varies.
// The tuple layout looks like below:
// |Type(2byte)|Length(2byte)|IsKey(1byte)|value(Nbyte)|Type(4byte)|Length(2byte)|IsKey(1byte)|value(Nbyte)|...|Type(4byte)|Length(2byte)|IsKey(1byte)|value(Nbyte)|
// The N depends on the type.
// e.g. When the type is int64, the length is 8byte (=64bit).
//      When the type is []byte and the length is 100, the length is 100 byte.
type Tuple struct {
	Data []*TupleData
}

type Type uint16

const (
	Bool Type = iota + 1
	Int64
	Float64
	Bytes
	String
	Timestamp
)

// TupleData represents a column in a row.
type TupleData struct {
	Key bool

	Typ          Type
	Length       uint16 // n byte
	BoolVal      bool
	Int64Val     int64
	Float64Val   float64
	BytesVal     []byte // length-variable
	StringVal    string // length-variable
	TimestampVal int64
}

func (t Type) String() string {
	switch t {
	case Bool:
		return "Bool"
	case Int64:
		return "Int64"
	case Float64:
		return "Float64"
	case Bytes:
		return "Bytes"
	case String:
		return "String"
	case Timestamp:
		return "Timestamp"
	}

	return ""
}

func TypeFromString(s string) Type {
	switch s {
	case "Bool":
		return Bool
	case "Int64":
		return Int64
	case "Float64":
		return Float64
	case "Bytes":
		return Bytes
	case "String":
		return String
	case "Timestamp":
		return Timestamp
	}

	return 0
}

// NewTuple returns a tuple which represents a row in a table.
// values are supposed to be the multiple column value of a column.
func NewTuple(values []interface{}, keyIndex int) *Tuple {
	t := &Tuple{Data: make([]*TupleData, len(values))}
	for i, v := range values {
		switch actual := v.(type) {
		case bool:
			t.Data[i] = &TupleData{Typ: Bool, Length: 1, BoolVal: actual}
		case int64:
			t.Data[i] = &TupleData{Typ: Int64, Length: 8, Int64Val: actual}
		case float64:
			t.Data[i] = &TupleData{Typ: Float64, Length: 8, Float64Val: actual}
		case []byte:
			t.Data[i] = &TupleData{Typ: Bytes, Length: uint16(len(actual)), BytesVal: actual}
		case string:
			length := uint16(len([]byte(actual)))
			t.Data[i] = &TupleData{Typ: String, Length: length, StringVal: actual}
		case time.Time:
			t.Data[i] = &TupleData{Typ: Timestamp, Length: 8, TimestampVal: actual.Unix()}
		default:
			fmt.Fprintf(os.Stdout, "[WARN] unexpected type in init tuple\n")
		}

		if i == keyIndex {
			t.Data[i].Key = true
		}
	}

	return t
}

// Serialize encodes given t into byte slice. The size is not fixed.
func (t *Tuple) Serialize() ([]byte, error) {
	// type + length + is_key + spare
	metadataLen := 2 + 2 + 1 + 3
	var buf bytes.Buffer
	for _, d := range t.Data {
		var result []byte

		switch d.Typ {
		case Bool:
			result = make([]byte, metadataLen+int(d.Length))
			if d.BoolVal {
				copy(result[metadataLen:], []byte{1}) // true
			} else {
				copy(result[metadataLen:], []byte{0}) // false
			}
		case Int64:
			result = make([]byte, metadataLen+int(d.Length))
			val := make([]byte, d.Length)
			putUint64OnBytes(val, uint64(d.Int64Val))
			copy(result[metadataLen:], val)
		case Float64: // 8 byte
			result = make([]byte, metadataLen+int(d.Length))
			val := make([]byte, d.Length)
			putUint64OnBytes(val, math.Float64bits(d.Float64Val))
			copy(result[metadataLen:], val)
		case Bytes:
			result = make([]byte, metadataLen+int(d.Length))
			copy(result[metadataLen:], d.BytesVal)
		case String:
			bs := []byte(d.StringVal)
			result = make([]byte, metadataLen+int(d.Length))
			copy(result[metadataLen:], bs)
		case Timestamp:
			result = make([]byte, metadataLen+int(d.Length))
			val := make([]byte, d.Length)
			putUint64OnBytes(val, uint64(d.TimestampVal))
			copy(result[metadataLen:], val)
		}

		// write metadata
		putUint16OnBytes(result[0:], uint16(d.Typ))
		putUint16OnBytes(result[2:], d.Length)
		if d.Key {
			copy(result[4:], []byte{1})
		} else {
			copy(result[4:], []byte{0})
		}

		buf.Write(result)
	}

	return buf.Bytes(), nil
}

// Deserialize decodes given byte slice to a tuple.
func (t *Tuple) Deserialize(r io.Reader) error {
	bs, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	offset := 0
	for {
		if len(bs) <= offset { // return once finished reading bs
			break
		}

		typ := Type(bytesToUint16(bs[offset : offset+2]))
		offset += 2
		length := bytesToUint16(bs[offset : offset+2])
		offset += 2
		isKey := bs[offset : offset+1]
		offset += 4
		d := &TupleData{Key: isKey[0] == 1, Length: length}
		switch typ {
		case Bool:
			d.Typ = Bool
			b := bs[offset : offset+int(length)]
			if b[0] == 0 {
				d.BoolVal = false
			} else {
				d.BoolVal = true
			}
			t.Data = append(t.Data, d)
		case Int64:
			d.Typ = Int64
			d.Int64Val = int64(bytesToUint64(bs[offset : offset+int(length)]))
			t.Data = append(t.Data, d)
		case Float64:
			d.Typ = Float64
			d.Float64Val = math.Float64frombits(bytesToUint64(bs[offset : offset+int(length)]))
			t.Data = append(t.Data, d)
		case Bytes:
			d.Typ = Bytes
			d.BytesVal = bs[offset : offset+int(length)]
			t.Data = append(t.Data, d)
		case String:
			d.Typ = String
			d.StringVal = string(bs[offset : offset+int(length)])
			t.Data = append(t.Data, d)
		case Timestamp:
			d.Typ = Timestamp
			d.TimestampVal = int64(bytesToUint64(bs[offset : offset+int(length)]))
			t.Data = append(t.Data, d)
		}

		offset += int(length)
	}

	return nil
}

func (t *Tuple) String() string {
	sb := strings.Builder{}
	// put spaces at the head to print as an element of page. See page.String()
	sb.WriteString("    Tuple{\n")
	for _, d := range t.Data {
		switch d.Typ {
		case Bool:
			sb.WriteString(fmt.Sprintf("      (key: %v) (bool) %v,\n", d.Key, d.BoolVal))
		case Int64:
			sb.WriteString(fmt.Sprintf("      (key: %v) (int64) %v,\n", d.Key, d.Int64Val))
		case Float64:
			sb.WriteString(fmt.Sprintf("      (key: %v) (float64) %v,\n", d.Key, d.Float64Val))
		case Bytes:
			sb.WriteString(fmt.Sprintf("      (key: %v) (bytes) %v (%v),\n", d.Key, d.BytesVal, string(d.BytesVal)))
		case String:
			sb.WriteString(fmt.Sprintf("      (key: %v) (string) %v,\n", d.Key, d.StringVal))
		case Timestamp:
			sb.WriteString(fmt.Sprintf("      (key: %v) (timestamp) %v,\n", d.Key, time.Unix(d.TimestampVal, 0).Format(time.RFC3339)))
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
					case Bool:
						if !data.BoolVal && thanD.BoolVal {
							// 0 vs 1
							return true
						} else if data.BoolVal && !thanD.BoolVal {
							// 1 vs 0
							return false
						} else {
							// 1 vs 1 or 0 vs 0
							return false
						}
					case Int64:
						return data.Int64Val < thanD.Int64Val
					case Float64:
						return data.Float64Val < thanD.Float64Val
					case Bytes:
						return bytes.Compare(data.BytesVal[:], thanD.BytesVal[:]) < 0
					case String:
						return data.StringVal < thanD.StringVal
					case Timestamp:
						dt := time.Unix(0, data.TimestampVal)
						tt := time.Unix(0, thanD.TimestampVal)
						return dt.Before(tt)
					}
				}
			}
		}
	}

	return false
}
