// btree package provides B-tree data structure.
// It is highly inspired by emirpasic/gods/trees/btree/btree.go
// https://github.com/emirpasic/gods/blob/master/trees/btree/btree.go
package btree

import (
	"sync"
)

type Entry struct {
	key   interface{}
	value interface{}
}

type Node struct {
	Parent   *Node
	Entries  []*Entry
	Children []*Node
}

// KeyType defines the type of key.
type KeyType uint8

const (
	Int KeyType = iota + 1
	String
)

type BTree struct {
	root    *Node // root node
	size    int   // count of keys in the tree
	m       int   // maximum number of children
	compare func(a, b interface{}) int
	latch   sync.RWMutex
}

// NewIntKeyTree returns BTree whose key is integer type.
func NewIntKeyTree() *BTree {
	return &BTree{m: 3, compare: func(a, b interface{}) int {
		ai := a.(int)
		bi := b.(int)
		if ai > bi {
			return 1
		} else if ai == bi {
			return 0
		} else {
			return -1
		}
	}}
}

// NewStringKeyTree returns BTree whose key is string type.
func NewStringKeyTree() *BTree {
	return &BTree{m: 3, compare: func(a, b interface{}) int {
		as := a.(string)
		bs := b.(string)
		if as > bs {
			return 1
		} else if as == bs {
			return 0
		} else {
			return -1
		}
	}}
}

/*
 * -----------------
 * Public interfaces
 * -----------------
 */

// Put puts the given value by the given key to the BTree.
func (bt *BTree) Put(key, value interface{}) {
	e := &Entry{key: key, value: value}

	if bt.root == nil {
		bt.root = &Node{Entries: []*Entry{e}, Children: []*Node{}}
		bt.size++
		return
	}

	if bt.insert(bt.root, e) {
		bt.size++
	}
}

// Get retrieves a value by the given key.
func (bt *BTree) Get(key interface{}) (interface{}, bool) {
	node, index, found := bt.searchRecursively(bt.root, key)
	if found {
		return node.Entries[index].value, true
	}

	return nil, false
}

// TODO: change to Remove
func (bt *BTree) remove(key interface{}) {
	// TODO: support deletion
}

func (bt *BTree) Empty() bool {
	return bt.size == 0
}

/*
 * -------------------
 * READ helper methods
 * -------------------
 */

// search searches the key from the given node by binary search.
func (bt *BTree) search(node *Node, key interface{}) (int, bool) {
	low, high := 0, len(node.Entries)-1
	var mid int
	for low <= high {
		mid = (high + low) / 2
		result := bt.compare(key, node.Entries[mid].key)
		switch {
		case result > 0:
			low = mid + 1
		case result < 0:
			high = mid - 1
		default:
			return mid, true
		}
	}
	return low, false
}

func (bt *BTree) searchRecursively(node *Node, key interface{}) (*Node, int, bool) {
	if bt.Empty() {
		return nil, -1, false
	}

	for {
		index, found := bt.search(node, key)
		if found {
			return node, index, true
		}

		if bt.isLeaf(node) {
			return nil, -1, false
		}

		node = node.Children[index]
	}
}

func (bt *BTree) isLeaf(node *Node) bool {
	return len(node.Children) == 0
}

func (bt *BTree) shouldSplit(node *Node) bool {
	return len(node.Entries) > bt.maxEntries()
}

func (bt *BTree) maxEntries() int {
	return bt.m - 1
}

func (bt *BTree) middle() int {
	return (bt.m - 1) / 2
}

/*
 * --------------------
 * WRITE helper methods
 * --------------------
 */

func (bt *BTree) insert(node *Node, entry *Entry) bool {
	if bt.isLeaf(node) {
		return bt.insertIntoLeaf(node, entry)
	}
	return bt.insertIntoInternal(node, entry)
}

// insertIntoLeaf inserts the entry to the node. It assumes that the node is leaf.
func (bt *BTree) insertIntoLeaf(node *Node, entry *Entry) bool {
	insertPos, found := bt.search(node, entry.key)
	// when the key is already in the tree, update it
	if found {
		node.Entries[insertPos] = entry
		return false
	}

	// insert the entry in the middle of the entries of the node
	node.Entries = append(node.Entries, nil)
	copy(node.Entries[insertPos+1:], node.Entries[insertPos:])
	node.Entries[insertPos] = entry
	bt.split(node) // split the node if needed
	return true
}

// insertIntoInternal inserts the entry to the node.
func (bt *BTree) insertIntoInternal(node *Node, entry *Entry) bool {
	insertPos, found := bt.search(node, entry.key)
	// when the key is already in the tree, update it
	if found {
		node.Entries[insertPos] = entry
		return false
	}

	return bt.insert(node.Children[insertPos], entry)
}

// split splits the node if rebalancing needed.
func (bt *BTree) split(node *Node) {
	if !bt.shouldSplit(node) {
		return
	}

	if node == bt.root {
		bt.splitRoot()
		return
	}

	bt.splitNonRoot(node)
}

// splitRoot splits root node.
func (bt *BTree) splitRoot() {
	middle := bt.middle()

	// split root into left and right
	left := &Node{Entries: append([]*Entry(nil), bt.root.Entries[:middle]...)}
	right := &Node{Entries: append([]*Entry(nil), bt.root.Entries[middle+1:]...)}

	if !bt.isLeaf(bt.root) {
		// split root children into left.Children and right.Children
		left.Children = append([]*Node(nil), bt.root.Children[:middle+1]...)
		right.Children = append([]*Node(nil), bt.root.Children[middle+1:]...)
		setParent(left.Children, left)
		setParent(right.Children, right)
	}

	newRoot := &Node{
		Entries:  []*Entry{bt.root.Entries[middle]},
		Children: []*Node{left, right},
	}
	left.Parent = newRoot
	right.Parent = newRoot
	bt.root = newRoot
}

func (bt *BTree) splitNonRoot(node *Node) {
	middle := bt.middle()
	parent := node.Parent

	// split node into left and right
	left := &Node{Entries: append([]*Entry(nil), node.Entries[:middle]...), Parent: parent}
	right := &Node{Entries: append([]*Entry(nil), node.Entries[middle+1:]...), Parent: parent}

	if !bt.isLeaf(node) {
		// split node children into left.Children and right.Children
		left.Children = append([]*Node(nil), node.Children[:middle+1]...)
		right.Children = append([]*Node(nil), node.Children[middle+1:]...)
		setParent(left.Children, left)
		setParent(right.Children, right)
	}

	insertPos, _ := bt.search(parent, node.Entries[middle].key)

	// insert middle entry into parent
	parent.Entries = append(parent.Entries, nil)
	copy(parent.Entries[insertPos+1:], parent.Entries[insertPos:])
	parent.Entries[insertPos] = node.Entries[middle]

	// set inserted entry's child left to the created left
	parent.Children[insertPos] = left

	// set inserted entry's child right to the created right
	parent.Children = append(parent.Children, nil)
	copy(parent.Children[insertPos+2:], parent.Children[insertPos+1:])
	parent.Children[insertPos+1] = right

	// split parent if needed
	bt.split(parent)
}

/*
 * -------
 * helpers
 * -------
 */

func setParent(nodes []*Node, parent *Node) {
	for _, node := range nodes {
		node.Parent = parent
	}
}
