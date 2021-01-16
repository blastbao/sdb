package lru

type element struct {
	next  *element
	prev  *element
	key   string
	value interface{}
}

func newElement(k string, v interface{}) *element {
	return &element{key: k, value: v}
}

type doublyLinkedList struct {
	head *element
	tail *element
}

func newDoublyLinkedList() *doublyLinkedList {
	l := &doublyLinkedList{}
	l.head = &element{} // dummy
	l.tail = &element{} // dummy
	l.head.next = l.tail
	l.tail.prev = l.head
	return l
}

// add adds an e at the next to the head (because head is dummy, e will be true head)
func (l *doublyLinkedList) add(e *element) {
	e.prev = l.head
	e.next = l.head.next
	l.head.next = e
	e.next.prev = e
}

// removes given node from the list
func (l *doublyLinkedList) remove(e *element) {
	e.prev.next = e.next
	e.next.prev = e.prev
}

// moveToHead moves the given node to the head
func (l *doublyLinkedList) moveToHead(e *element) {
	l.remove(e)
	l.add(e)
}
