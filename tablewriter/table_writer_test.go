package tablewriter

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func TestWriter_Render(t *testing.T) {
	expected := `+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 1  | x     | 5   |
| 2  | xx    | 10  |
| 3  | xxx   | 15  |
| 4  | xxxx  | 20  |
| 5  | xxxxx | 25  |
+----+-------+-----+
`
	buff := new(bytes.Buffer)
	tw := New(buff)
	tw.SetHeader([]string{"id", "name", "age"})
	for i := 1; i <= 5; i++ {
		row := []string{}
		row = append(row, fmt.Sprintf("%d", i))
		row = append(row, strings.Repeat("x", i))
		row = append(row, fmt.Sprintf("%d", i*5))

		tw.Append(row)
	}

	tw.Render()

	testutil.MustEqual(t, buff.String(), expected)
}
