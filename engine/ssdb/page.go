package ssdb

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
