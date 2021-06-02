package testutil

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func MustEqual(t *testing.T, a, b interface{}) {
	t.Helper()
	if diff := deepEqual(a, b); diff != nil {
		t.Fatal(diff)
	}
}

func MustBeNil(t *testing.T, a interface{}) {
	t.Helper()
	if a != nil {
		t.Fatalf("not nil: %v", a)
	}
}

func deepEqual(a, b interface{}) []string {
	c := &cmp{diff: []string{}}

	if a == nil && b == nil {
		return nil
	}

	if a == nil && b != nil {
		c.saveDiff("<nil>", b)
		return c.diff
	}

	if a != nil && b == nil {
		c.saveDiff(a, "<nil>")
		return c.diff
	}

	c.equals(reflect.ValueOf(a), reflect.ValueOf(b))

	if len(c.diff) > 0 {
		return c.diff // diffs
	}
	return nil // no diffs
}

const maxDiff = 10

type cmp struct {
	diff []string
	buff []string
}

func (c *cmp) saveDiff(aval, bval interface{}) {
	if len(c.buff) > 0 {
		varName := strings.Join(c.buff, ".")
		c.diff = append(c.diff, fmt.Sprintf("%s: %v != %v", varName, aval, bval))
	} else {
		c.diff = append(c.diff, fmt.Sprintf("%v != %v", aval, bval))
	}
}

func (c *cmp) equals(a, b reflect.Value) {
	// check nil
	if !a.IsValid() || !b.IsValid() {
		if a.IsValid() && !b.IsValid() {
			c.saveDiff(a.Type(), "<nil>")
		} else if !a.IsValid() && b.IsValid() {
			c.saveDiff("<nil>", b.Type())
		}
		return
	}

	// If different types, they can't be equal
	aType := a.Type()
	bType := b.Type()
	if aType != bType {
		// Built-in types don't have a name
		if aType.Name() == "" || aType.Name() != bType.Name() {
			c.saveDiff(aType, bType)
		} else {
			// coming here means the type is actually different but the type names are the same.
			// It can happen when comparing e.g. pkg/v1.Error and pkg/v2.Error.
			// So should include the full path in the diff.
			aFullType := aType.PkgPath() + "." + aType.Name()
			bFullType := bType.PkgPath() + "." + bType.Name()
			c.saveDiff(aFullType, bFullType)
		}
		return
	}

	aKind := a.Kind()
	bKind := b.Kind()

	aHasElem := aKind == reflect.Ptr || aKind == reflect.Interface
	bHasElem := bKind == reflect.Ptr || bKind == reflect.Interface

	// Dereference pointers and interface{} then compare again
	if aHasElem || bHasElem {
		if aHasElem {
			a = a.Elem()
		}
		if bHasElem {
			b = b.Elem()
		}
		c.equals(a, b)
		return
	}

	// If coming here, the types are the same

	switch aKind {
	case reflect.Float32, reflect.Float64:
		if a.Float() != b.Float() {
			c.saveDiff(a.Float(), b.Float())
		}
	case reflect.Bool:
		if a.Bool() != b.Bool() {
			c.saveDiff(a.Bool(), b.Bool())
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if a.Int() != b.Int() {
			c.saveDiff(a.Int(), b.Int())
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if a.Uint() != b.Uint() {
			c.saveDiff(a.Uint(), b.Uint())
		}
	case reflect.String:
		if a.String() != b.String() {
			c.saveDiff(a.String(), b.String())
		}

	case reflect.Slice:
		if a.IsNil() && !b.IsNil() {
			c.saveDiff("<nil slice>", b)
			return
		} else if !a.IsNil() && b.IsNil() {
			c.saveDiff(a, "<nil slice>")
			return
		}

		aLen := a.Len()
		bLen := b.Len()

		if a.Pointer() == b.Pointer() && aLen == bLen {
			return
		}

		longer := aLen
		if bLen > aLen {
			longer = bLen
		}
		// compare elements in slice one by one
		for i := 0; i < longer; i++ {
			c.push(fmt.Sprintf("slice[%d]", i))
			if i < aLen && i < bLen {
				c.equals(a.Index(i), b.Index(i))
			} else if i < aLen {
				c.saveDiff(a.Index(i), "<no value>")
			} else {
				c.saveDiff("<no value>", b.Index(i))
			}
			c.pop()
			if len(c.diff) >= maxDiff {
				break
			}
		}

	case reflect.Array:
		n := a.Len()
		for i := 0; i < n; i++ {
			c.push(fmt.Sprintf("array[%d]", i))
			c.equals(a.Index(i), b.Index(i))
			c.pop()
			if len(c.diff) >= maxDiff {
				break
			}
		}

	case reflect.Map:
		if a.IsNil() || b.IsNil() {
			if a.IsNil() && !b.IsNil() {
				c.saveDiff("<nil map>", b)
			} else if !a.IsNil() && b.IsNil() {
				c.saveDiff(a, "<nil map>")
			}
			return
		}

		if a.Pointer() == b.Pointer() {
			return
		}

		for _, key := range a.MapKeys() {
			c.push(fmt.Sprintf("map[%v]", key))

			aVal := a.MapIndex(key)
			bVal := b.MapIndex(key)
			if bVal.IsValid() {
				c.equals(aVal, bVal)
			} else {
				c.saveDiff(aVal, "<does not have key>")
			}

			c.pop()

			if len(c.diff) >= maxDiff {
				return
			}
		}

		for _, key := range b.MapKeys() {
			if aVal := a.MapIndex(key); aVal.IsValid() {
				continue
			}

			c.push(fmt.Sprintf("map[%v]", key))
			c.saveDiff("<does not have key>", b.MapIndex(key))
			c.pop()
			if len(c.diff) >= maxDiff {
				return
			}
		}

	case reflect.Struct:
		// If the type has `Equal` method, it should be used to compare the equality
		if eqFunc := a.MethodByName("Equal"); eqFunc.IsValid() && eqFunc.CanInterface() {
			funcType := eqFunc.Type()
			if funcType.NumIn() == 1 && funcType.In(0) == bType {
				retVals := eqFunc.Call([]reflect.Value{b})
				if !retVals[0].Bool() {
					c.saveDiff(a, b)
				}
				return
			}
		}

		for i := 0; i < a.NumField(); i++ {
			if aType.Field(i).PkgPath != "" {
				continue // skip unexported field, e.g. s in type T struct {s string}
			}

			c.push(aType.Field(i).Name)

			af := a.Field(i)
			bf := b.Field(i)

			c.equals(af, bf)

			c.pop()

			if len(c.diff) >= maxDiff {
				break
			}
		}
	}
}

func (c *cmp) push(name string) {
	c.buff = append(c.buff, name)
}

func (c *cmp) pop() {
	if len(c.buff) > 0 {
		c.buff = c.buff[0 : len(c.buff)-1]
	}
}
