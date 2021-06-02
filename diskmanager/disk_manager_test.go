package diskmanager

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/dty1er/sdb/testutil"
)

// for test
type KeyValue struct {
	KV map[string]string
}

func (kv *KeyValue) Serialize() ([]byte, error) {
	var buff bytes.Buffer
	if err := json.NewEncoder(&buff).Encode(kv); err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func (kv *KeyValue) Deserialize(r io.Reader) error {
	if err := json.NewDecoder(r).Decode(kv); err != nil {
		return err
	}

	return nil
}

func TestDiskManager_Load_Persist(t *testing.T) {
	kv := &KeyValue{map[string]string{"A": "a", "B": "b"}}

	tempDir := t.TempDir()

	dm := New(tempDir)
	err := dm.Persist("test_kv", 0, kv)
	testutil.MustBeNil(t, err)

	newKV := &KeyValue{}
	dm.Load("test_kv", 0, newKV)
	testutil.MustEqual(t, kv, newKV)
}
