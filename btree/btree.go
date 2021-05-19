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

type Item interface {
	Less(than Item) bool
}

type Node struct {
	Parent   *Node
	Items    []Item
	Children []*Node
}

type BTree struct {
	Root  *Node // root node
	Size  int   // count of keys in the tree
	M     int   // maximum number of children
	latch sync.RWMutex
}

func New() *BTree {
	return &BTree{
		Root:  nil,
		Size:  0,
		M:     3,
		latch: sync.RWMutex{},
	}
}

/*
 * -----------------
 * Public interfaces
 * -----------------
 */

// Put puts the given value by the given key to the BTree.
func (bt *BTree) Put(i Item) {
	if bt.Root == nil {
		bt.Root = &Node{Items: []Item{i}, Children: nil}
		bt.Size++
		return
	}

	if bt.insert(bt.Root, i) {
		bt.Size++
	}
}

// Get retrieves a value by the given key.
func (bt *BTree) Get(key Item) (Item, bool) {
	node, index, found := bt.searchRecursively(bt.Root, key)
	if found {
		return node.Items[index], true
	}

	return nil, false
}

// TODO: change to Remove
func (bt *BTree) remove(key Item) {
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
func (bt *BTree) search(node *Node, key Item) (int, bool) {
	low, high := 0, len(node.Items)-1
	var mid int
	for low <= high {
		mid = (high + low) / 2
		if key.Less(node.Items[mid]) {
			high = mid - 1
		} else if node.Items[mid].Less(key) {
			low = mid + 1
		} else {
			return mid, true
		}
	}
	return low, false
}

func (bt *BTree) searchRecursively(node *Node, key Item) (*Node, int, bool) {
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
	return len(node.Items) > bt.maxEntries()
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

func (bt *BTree) insert(node *Node, item Item) bool {
	if bt.isLeaf(node) {
		return bt.insertIntoLeaf(node, item)
	}
	return bt.insertIntoInternal(node, item)
}

// insertIntoLeaf inserts the entry to the node. It assumes that the node is leaf.
func (bt *BTree) insertIntoLeaf(node *Node, item Item) bool {
	insertPos, found := bt.search(node, item)
	// when the key is already in the tree, update it
	if found {
		node.Items[insertPos] = item
		return false
	}

	// insert the entry in the middle of the entries of the node
	node.Items = append(node.Items, nil)
	copy(node.Items[insertPos+1:], node.Items[insertPos:])
	node.Items[insertPos] = item
	bt.split(node) // split the node if needed
	return true
}

// insertIntoInternal inserts the entry to the node.
func (bt *BTree) insertIntoInternal(node *Node, item Item) bool {
	insertPos, found := bt.search(node, item)
	// when the key is already in the tree, update it
	if found {
		node.Items[insertPos] = item
		return false
	}

	return bt.insert(node.Children[insertPos], item)
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
	left := &Node{Items: append([]Item(nil), bt.Root.Items[:middle]...)}
	right := &Node{Items: append([]Item(nil), bt.Root.Items[middle+1:]...)}

	if !bt.isLeaf(bt.Root) {
		// split root children into left.Children and right.Children
		left.Children = append([]*Node(nil), bt.Root.Children[:middle+1]...)
		right.Children = append([]*Node(nil), bt.Root.Children[middle+1:]...)
		setParent(left.Children, left)
		setParent(right.Children, right)
	}

	newRoot := &Node{
		Items:    []Item{bt.Root.Items[middle]},
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
	left := &Node{Items: append([]Item(nil), node.Items[:middle]...), Parent: parent}
	right := &Node{Items: append([]Item(nil), node.Items[middle+1:]...), Parent: parent}

	if !bt.isLeaf(node) {
		// split node children into left.Children and right.Children
		left.Children = append([]*Node(nil), node.Children[:middle+1]...)
		right.Children = append([]*Node(nil), node.Children[middle+1:]...)
		setParent(left.Children, left)
		setParent(right.Children, right)
	}

	insertPos, _ := bt.search(parent, node.Items[middle])

	// insert middle entry into parent
	parent.Items = append(parent.Items, nil)
	copy(parent.Items[insertPos+1:], parent.Items[insertPos:])
	parent.Items[insertPos] = node.Items[middle]

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
	for e := 0; e < len(node.Items)+1; e++ {
		if e < len(node.Children) {
			bt.output(buffer, node.Children[e], level+1, true)
		}
		if e < len(node.Items) {
			if _, err := buffer.WriteString(strings.Repeat("    ", level)); err != nil {
			}
			if _, err := buffer.WriteString(fmt.Sprintf("%v", node.Items[e]) + "\n"); err != nil {
			}
		}
	}
}

// RegisterSerializationTarget registers the target value to the serialization.
// This must be called before serializint the btree which contains the value type element.
func RegisterSerializationTarget(value interface{}) {
	gob.Register(value)
}

type IntItem int

func (i IntItem) Less(than Item) bool {
	t := than.(IntItem)
	return i < t
}
