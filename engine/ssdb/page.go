package ssdb

import (
	"fmt"
	"strings"
)

type PageID uint32

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
// tuple layout: see engine/ssdb/tuple.go
type Page struct {
	bs [PageSize]byte
}

func NewPage(bs [PageSize]byte) *Page {
	return &Page{bs: bs}
}

type slot struct {
	offset uint16 // [2]byte
	length uint16 // [2]byte
}

type pageHeader struct {
	id          PageID // [4]byte
	tuplesCount uint16 // [2]byte
	slots       []*slot
}

func InitPage(id uint32) *Page {
	bs := [PageSize]byte{}
	putUint32OnBytes(bs[0:], id)
	putUint16OnBytes(bs[4:], 0) // tuple count is initially 0
	return &Page{bs: bs}
}

func (h *pageHeader) encode() []byte {
	length := 4 + 2 + len(h.slots)*4
	bs := make([]byte, length)
	putUint32OnBytes(bs[0:], uint32(h.id))
	putUint16OnBytes(bs[4:], h.tuplesCount)
	for i := 0; i < len(h.slots); i++ {
		putUint16OnBytes(bs[6+i*4:], h.slots[i].offset)
		putUint16OnBytes(bs[8+i*4:], h.slots[i].length)
	}

	return bs
}

func (p *Page) decodeHeader() pageHeader {
	h := pageHeader{}
	h.id = PageID(bytesToUint32(p.bs[0:]))
	h.tuplesCount = bytesToUint16(p.bs[4:])
	h.slots = make([]*slot, h.tuplesCount)

	for i := 0; i < int(h.tuplesCount); i++ {
		s := &slot{}
		o := i * 4 // offset
		s.offset = bytesToUint16(p.bs[6+o:])
		s.length = bytesToUint16(p.bs[8+o:])
		h.slots[i] = s
	}

	return h
}

func (p *Page) AppendTuple(t *Tuple) error {
	tb := SerializeTuple(t)

	header := p.decodeHeader()
	headerLength := 4 + 2 + len(header.slots)*4

	last := uint16(PageSize)
	if header.tuplesCount != 0 {
		last = header.slots[header.tuplesCount-1].offset
	}

	availableSpace := last - uint16(headerLength) - 4 // make sure tuple and its slot can be placed
	if int(availableSpace) < len(tb) {
		return fmt.Errorf("no enough space on the page")
	}

	// place tuple
	start := int(last) - len(tb)
	copy(p.bs[start:last], tb)

	header.tuplesCount++
	header.slots = append(header.slots, &slot{offset: uint16(start), length: uint16(len(tb))})
	copy(p.bs[0:], header.encode())

	return nil
}

func (p *Page) GetID() PageID {
	return p.decodeHeader().id
}

func (p *Page) String() string {
	header := p.decodeHeader()

	sb := strings.Builder{}
	sb.WriteString("Page{\n")

	sb.WriteString("  Header{\n")
	sb.WriteString(fmt.Sprintf("    ID: %v,\n", uint32(header.id)))
	sb.WriteString(fmt.Sprintf("    tuplesCount: %v,\n", header.tuplesCount))
	for _, slot := range header.slots {
		sb.WriteString(fmt.Sprintf("    {offset: %v, length: %v},\n", slot.offset, slot.length))
	}
	sb.WriteString("  },\n")

	sb.WriteString("  Tuples{\n")
	for _, slot := range header.slots {
		sb.WriteString(fmt.Sprintf("%v\n", DeserializeTuple(p.bs[slot.offset:slot.offset+slot.length])))
	}
	sb.WriteString("  },\n")
	sb.WriteString("}\n")

	return sb.String()
}
