package ssdb

type PageHeader struct {
	TupleCount     uint8
	TupleLocations []uint16
	FreeSpaceEnd   uint16
}
