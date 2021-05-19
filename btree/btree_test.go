package btree

import (
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func assertValidTree(t *testing.T, tree *BTree, expectedSize int) {
	t.Helper()
	testutil.MustEqual(t, tree.Size, expectedSize)
}

func assertValidTreeNode(t *testing.T, node *Node, expectedEntries int, expectedChildren int, keys []int, hasParent bool) {
	t.Helper()
	testutil.MustEqual(t, hasParent, node.Parent != nil)
	testutil.MustEqual(t, len(node.Items), expectedEntries)
	testutil.MustEqual(t, len(node.Children), expectedChildren)
	for i, key := range keys {
		testutil.MustEqual(t, node.Items[i], IntItem(key))
	}
}

func TestBTree_Get(t *testing.T) {
	RegisterSerializationTarget(IntItem(0))
	tree := New()
	tree.Put(IntItem(7))
	tree.Put(IntItem(9))
	tree.Put(IntItem(10))
	tree.Put(IntItem(6))
	tree.Put(IntItem(3))
	tree.Put(IntItem(4))
	tree.Put(IntItem(5))
	tree.Put(IntItem(8))
	tree.Put(IntItem(2))
	tree.Put(IntItem(1))

	tests := []struct {
		item  Item
		found bool
	}{
		{item: IntItem(0), found: false},
		{item: IntItem(1), found: true},
		{item: IntItem(2), found: true},
		{item: IntItem(3), found: true},
		{item: IntItem(4), found: true},
		{item: IntItem(5), found: true},
		{item: IntItem(6), found: true},
		{item: IntItem(7), found: true},
		{item: IntItem(8), found: true},
		{item: IntItem(9), found: true},
		{item: IntItem(10), found: true},
		{item: IntItem(11), found: false},
	}

	for _, test := range tests {
		_, found := tree.Get(test.item)
		testutil.MustEqual(t, found, test.found)
	}
}

func TestBTree_Int_Put(t *testing.T) {
	RegisterSerializationTarget(IntItem(0))
	tree := New()
	assertValidTree(t, tree, 0)

	tree.Put(IntItem(1))
	assertValidTree(t, tree, 1)
	assertValidTreeNode(t, tree.Root, 1, 0, []int{1}, false)

	tree.Put(IntItem(2))
	assertValidTree(t, tree, 2)
	assertValidTreeNode(t, tree.Root, 2, 0, []int{1, 2}, false)

	tree.Put(IntItem(3))
	assertValidTree(t, tree, 3)
	assertValidTreeNode(t, tree.Root, 1, 2, []int{2}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 1, 0, []int{1}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 0, []int{3}, true)

	tree.Put(IntItem(4))
	assertValidTree(t, tree, 4)
	assertValidTreeNode(t, tree.Root, 1, 2, []int{2}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 1, 0, []int{1}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 2, 0, []int{3, 4}, true)

	tree.Put(IntItem(5))
	assertValidTree(t, tree, 5)
	assertValidTreeNode(t, tree.Root, 2, 3, []int{2, 4}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 1, 0, []int{1}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 0, []int{3}, true)
	assertValidTreeNode(t, tree.Root.Children[2], 1, 0, []int{5}, true)

	tree.Put(IntItem(6))
	assertValidTree(t, tree, 6)
	assertValidTreeNode(t, tree.Root, 2, 3, []int{2, 4}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 1, 0, []int{1}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 0, []int{3}, true)
	assertValidTreeNode(t, tree.Root.Children[2], 2, 0, []int{5, 6}, true)

	tree.Put(IntItem(7))
	assertValidTree(t, tree, 7)
	assertValidTreeNode(t, tree.Root, 1, 2, []int{4}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 1, 2, []int{2}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 2, []int{6}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[0], 1, 0, []int{1}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[1], 1, 0, []int{3}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[0], 1, 0, []int{5}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[1], 1, 0, []int{7}, true)
}

func TestBTreePut4(t *testing.T) {
	RegisterSerializationTarget(IntItem(0))
	tree := New()
	assertValidTree(t, tree, 0)

	tree.Put(IntItem(6))
	assertValidTree(t, tree, 1)
	assertValidTreeNode(t, tree.Root, 1, 0, []int{6}, false)

	tree.Put(IntItem(5))
	assertValidTree(t, tree, 2)
	assertValidTreeNode(t, tree.Root, 2, 0, []int{5, 6}, false)

	tree.Put(IntItem(4))
	assertValidTree(t, tree, 3)
	assertValidTreeNode(t, tree.Root, 1, 2, []int{5}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 1, 0, []int{4}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 0, []int{6}, true)

	tree.Put(IntItem(3))
	assertValidTree(t, tree, 4)
	assertValidTreeNode(t, tree.Root, 1, 2, []int{5}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 2, 0, []int{3, 4}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 0, []int{6}, true)

	tree.Put(IntItem(2))
	assertValidTree(t, tree, 5)
	assertValidTreeNode(t, tree.Root, 2, 3, []int{3, 5}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 1, 0, []int{2}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 0, []int{4}, true)
	assertValidTreeNode(t, tree.Root.Children[2], 1, 0, []int{6}, true)

	tree.Put(IntItem(1))
	assertValidTree(t, tree, 6)
	assertValidTreeNode(t, tree.Root, 2, 3, []int{3, 5}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 2, 0, []int{1, 2}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 0, []int{4}, true)
	assertValidTreeNode(t, tree.Root.Children[2], 1, 0, []int{6}, true)

	tree.Put(IntItem(0))
	assertValidTree(t, tree, 7)
	assertValidTreeNode(t, tree.Root, 1, 2, []int{3}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 1, 2, []int{1}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 2, []int{5}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[0], 1, 0, []int{0}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[1], 1, 0, []int{2}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[0], 1, 0, []int{4}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[1], 1, 0, []int{6}, true)

	tree.Put(IntItem(-1))
	assertValidTree(t, tree, 8)
	assertValidTreeNode(t, tree.Root, 1, 2, []int{3}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 1, 2, []int{1}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 2, []int{5}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[0], 2, 0, []int{-1, 0}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[1], 1, 0, []int{2}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[0], 1, 0, []int{4}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[1], 1, 0, []int{6}, true)

	tree.Put(IntItem(-2))
	assertValidTree(t, tree, 9)
	assertValidTreeNode(t, tree.Root, 1, 2, []int{3}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 2, 3, []int{-1, 1}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 2, []int{5}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[0], 1, 0, []int{-2}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[1], 1, 0, []int{0}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[2], 1, 0, []int{2}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[0], 1, 0, []int{4}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[1], 1, 0, []int{6}, true)

	tree.Put(IntItem(-3))
	assertValidTree(t, tree, 10)
	assertValidTreeNode(t, tree.Root, 1, 2, []int{3}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 2, 3, []int{-1, 1}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 2, []int{5}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[0], 2, 0, []int{-3, -2}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[1], 1, 0, []int{0}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[2], 1, 0, []int{2}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[0], 1, 0, []int{4}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[1], 1, 0, []int{6}, true)

	tree.Put(IntItem(-4))
	assertValidTree(t, tree, 11)
	assertValidTreeNode(t, tree.Root, 2, 3, []int{-1, 3}, false)
	assertValidTreeNode(t, tree.Root.Children[0], 1, 2, []int{-3}, true)
	assertValidTreeNode(t, tree.Root.Children[1], 1, 2, []int{1}, true)
	assertValidTreeNode(t, tree.Root.Children[2], 1, 2, []int{5}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[0], 1, 0, []int{-4}, true)
	assertValidTreeNode(t, tree.Root.Children[0].Children[1], 1, 0, []int{-2}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[0], 1, 0, []int{0}, true)
	assertValidTreeNode(t, tree.Root.Children[1].Children[1], 1, 0, []int{2}, true)
	assertValidTreeNode(t, tree.Root.Children[2].Children[0], 1, 0, []int{4}, true)
	assertValidTreeNode(t, tree.Root.Children[2].Children[1], 1, 0, []int{6}, true)
}
