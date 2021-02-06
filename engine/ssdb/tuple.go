package ssdb

type Tuple struct {
	Data []TupleData
}

type TupleData struct {
	typ         Type
	int32Val    int32
	char1024Val [1024]byte // TODO: support multi-byte character
}

type Type int

const (
	Int32 Type = iota + 1
	Char1024
)
