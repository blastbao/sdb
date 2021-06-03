package engine

import (
	"bytes"
	"testing"
	"time"

	"github.com/dty1er/sdb/testutil"
)

func Test_Serialize_Deserialize_Tuple(t *testing.T) {
	tim := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	tuple := NewTuple([]interface{}{true, int64(99), 3.14, []byte{'a', 'b', 'c'}, "sdb is a simple database", tim}, 1)

	s, err := tuple.Serialize()
	testutil.MustBeNil(t, err)
	expected := []byte{
		0, 1, // Type bool
		0, 1, // Length 1
		0,       // Key: false
		0, 0, 0, // spare bytes (always 0)
		1, // value: true

		0, 2, // Type Int64
		0, 8, // Length 8
		1, // Key: true
		0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 99, // value: 99

		0, 3, // Type Float64
		0, 8, // Length 8
		0, // Key: false
		0, 0, 0,
		64, 9, 30, 184, 81, 235, 133, 31, // value: 3.14

		0, 4, // Type Bytes
		0, 3, // Length 3
		0, // Key: false
		0, 0, 0,
		97, 98, 99, // value: 'a', 'b', 'c'

		0, 5, // Type String
		0, 24, // Length 8
		0, // Key: false
		0, 0, 0,
		// value: "sdb is a simple database"
		115, 100, 98, 32, 105, 115, 32, 97, 32, 115, 105, 109, 112, 108, 101, 32, 100, 97, 116, 97, 98, 97, 115, 101,

		0, 6, // Type Timestamp
		0, 8, // Length 8
		0, // Key: false
		0, 0, 0,
		13, 35, 76, 207, 82, 67, 0, 0, // value: Unixnano timestamp of tim
	}

	testutil.MustEqual(t, s, expected)

	var nt Tuple
	err = nt.Deserialize(bytes.NewReader(s))
	testutil.MustBeNil(t, err)
	testutil.MustEqual(t, len(nt.Data), 6)
	testutil.MustEqual(t, nt.Data[0], &TupleData{Typ: Bool, Length: 1, BoolVal: true})
	testutil.MustEqual(t, nt.Data[1], &TupleData{Typ: Int64, Length: 8, Int64Val: 99, Key: true})
	testutil.MustEqual(t, nt.Data[2], &TupleData{Typ: Float64, Length: 8, Float64Val: 3.14})
	testutil.MustEqual(t, nt.Data[3], &TupleData{Typ: Bytes, Length: 3, BytesVal: []byte{'a', 'b', 'c'}})
	testutil.MustEqual(t, nt.Data[4], &TupleData{Typ: String, Length: 24, StringVal: "sdb is a simple database"})
	testutil.MustEqual(t, nt.Data[5], &TupleData{Typ: Timestamp, Length: 8, TimestampVal: tim.UnixNano()})
}
