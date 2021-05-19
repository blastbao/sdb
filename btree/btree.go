// btree package provides B-tree data structure.
// It is highly inspired by emirpasic/gods/trees/btree/btree.go
// https://github.com/emirpasic/gods/blob/master/trees/btree/btree.go
package btree

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"sync"
)

type Entry struct {
	Key   interface{}
	Value interface{}
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
	Root    *Node // root node
	Size    int   // count of keys in the tree
	M       int   // maximum number of children
	KeyType KeyType
	latch   sync.RWMutex
}

var (
	CompareInt = func(a, b interface{}) int {
		ai := a.(int)
		bi := b.(int)
		if ai > bi {
			return 1
		} else if ai == bi {
			return 0
		} else {
			return -1
		}
	}

	CompareString = func(a, b interface{}) int {
		as := a.(string)
		bs := b.(string)
		if as > bs {
			return 1
		} else if as == bs {
			return 0
		} else {
			return -1
		}
	}
)

// NewIntKeyTree returns BTree whose key is integer type.
func NewIntKeyTree() *BTree {
	return &BTree{
		Root:    nil,
		Size:    0,
		M:       3,
		KeyType: Int,
		latch:   sync.RWMutex{},
	}
}

// NewStringKeyTree returns BTree whose key is string type.
func NewStringKeyTree() *BTree {
	return &BTree{
		Root:    nil,
		Size:    0,
		M:       3,
		KeyType: String,
		latch:   sync.RWMutex{},
	}
}

/*
 * -----------------
 * Public interfaces
 * -----------------
 */

// Put puts the given value by the given key to the BTree.
func (bt *BTree) Put(key, value interface{}) {
	e := &Entry{Key: key, Value: value}

	if bt.Root == nil {
		bt.Root = &Node{Entries: []*Entry{e}, Children: nil}
		bt.Size++
		return
	}

	if bt.insert(bt.Root, e) {
		bt.Size++
	}
}

// Get retrieves a value by the given key.
func (bt *BTree) Get(key interface{}) (interface{}, bool) {
	node, index, found := bt.searchRecursively(bt.Root, key)
	if found {
		return node.Entries[index].Value, true
	}

	return nil, false
}

// TODO: change to Remove
func (bt *BTree) remove(key interface{}) {
	// TODO: support deletion
}

func (bt *BTree) Empty() bool {
	return bt.Size == 0
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
		result := 0
		if bt.KeyType == Int {
			result = CompareInt(key, node.Entries[mid].Key)
		} else {
			result = CompareString(key, node.Entries[mid].Key)
		}
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
	return bt.M - 1
}

func (bt *BTree) middle() int {
	return (bt.M - 1) / 2
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
	insertPos, found := bt.search(node, entry.Key)
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
	insertPos, found := bt.search(node, entry.Key)
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

	if node == bt.Root {
		bt.splitRoot()
		return
	}

	bt.splitNonRoot(node)
}

// splitRoot splits root node.
func (bt *BTree) splitRoot() {
	middle := bt.middle()

	// split root into left and right
	left := &Node{Entries: append([]*Entry(nil), bt.Root.Entries[:middle]...)}
	right := &Node{Entries: append([]*Entry(nil), bt.Root.Entries[middle+1:]...)}

	if !bt.isLeaf(bt.Root) {
		// split root children into left.Children and right.Children
		left.Children = append([]*Node(nil), bt.Root.Children[:middle+1]...)
		right.Children = append([]*Node(nil), bt.Root.Children[middle+1:]...)
		setParent(left.Children, left)
		setParent(right.Children, right)
	}

	newRoot := &Node{
		Entries:  []*Entry{bt.Root.Entries[middle]},
		Children: []*Node{left, right},
	}
	left.Parent = newRoot
	right.Parent = newRoot
	bt.Root = newRoot
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

	insertPos, _ := bt.search(parent, node.Entries[middle].Key)

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

// Serialize serializes the btree using encoding/gob
func (bt *BTree) Serialize() ([]byte, error) {
	buff := new(bytes.Buffer)
	if err := gob.NewEncoder(buff).Encode(&bt); err != nil {
		return nil, fmt.Errorf("serialize btree: %w", err)
	}

	return buff.Bytes(), nil
}

func Deserialize(bs []byte) (*BTree, error) {
	var bt BTree
	buff := bytes.NewBuffer(bs)
	if err := gob.NewDecoder(buff).Decode(&bt); err != nil {
		return nil, fmt.Errorf("deserialize btree: %w", err)
	}

	bt.latch = sync.RWMutex{}
	return &bt, nil
}

// String returns a string representation of container (for debugging purposes)
func (bt *BTree) String() string {
	var buffer bytes.Buffer
	if _, err := buffer.WriteString("BTree\n"); err != nil {
	}
	if !bt.Empty() {
		bt.output(&buffer, bt.Root, 0, true)
	}
	return buffer.String()
}

func (bt *BTree) output(buffer *bytes.Buffer, node *Node, level int, isTail bool) {
	for e := 0; e < len(node.Entries)+1; e++ {
		if e < len(node.Children) {
			bt.output(buffer, node.Children[e], level+1, true)
		}
		if e < len(node.Entries) {
			if _, err := buffer.WriteString(strings.Repeat("    ", level)); err != nil {
			}
			if _, err := buffer.WriteString(fmt.Sprintf("%v", node.Entries[e].Key) + "\n"); err != nil {
			}
		}
	}
}

func (entry *Entry) String() string {
	return fmt.Sprintf("%v", entry.Key)
}

// RegisterSerializationTarget registers the target value to the serialization.
// This must be called before serializint the btree which contains the value type element.
func RegisterSerializationTarget(value interface{}) {
	gob.Register(value)
}
