package lru

import (
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func Test_DoublyLinkedList(t *testing.T) {
	l := newDoublyLinkedList()

	// add e1, currently e1 is head
	e1 := newElement("k1", "v1")
	l.add(e1)

	// add e2, currently e2 -> e1
	e2 := newElement("k2", "v2")
	l.add(e2)

	// make sure the head is e2
	testutil.MustEqual(t, l.head.next.value.(string), "v2")

	// make sure the next to e2 is e1
	testutil.MustEqual(t, l.head.next.next.value.(string), "v1")

	// make sure the prev of e1 is e2
	testutil.MustEqual(t, l.head.next.next.prev.value.(string), "v2")

	// move e1 to the head, now e1 -> e2
	l.moveToHead(e1)
	testutil.MustEqual(t, l.head.next.value.(string), "v1")

	// make sure the next to e1 is e2
	testutil.MustEqual(t, l.head.next.next.value.(string), "v2")

	// make sure the prev of e2 is e1
	testutil.MustEqual(t, l.head.next.next.prev.value.(string), "v1")

	// remove e1, now e2 is head
	l.remove(e1)

	testutil.MustEqual(t, l.head.next.value.(string), "v2")
}
