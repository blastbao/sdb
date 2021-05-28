package tablewriter

import (
	"fmt"
	"io"
	"strings"
)

type Writer struct {
	w      io.Writer
	rows   [][]string
	header []string
	maxes  []int
}

const (
	CENTER = "+"
	ROW    = "-"
	COLUMN = "|"
	SPACE  = " "
)

func New(w io.Writer) *Writer {
	return &Writer{
		w: w,
	}
}

func (w *Writer) SetHeader(hdr []string) {
	w.maxes = make([]int, len(hdr))
	for i := range hdr {
		w.maxes[i] = len(hdr[i]) + 2
	}
	w.header = hdr
}

func (w *Writer) Append(row []string) {
	for i := range row {
		if w.maxes[i] < len(row[i])+2 {
			w.maxes[i] = len(row[i]) + 2
		}
	}
	w.rows = append(w.rows, row)
}

func (w *Writer) Render() {
	w.printLine()
	w.print(w.header)
	w.printLine()
	for _, row := range w.rows {
		w.print(row)
	}
	w.printLine()
}

func (w *Writer) printLine() {
	fmt.Fprintf(w.w, CENTER)
	for _, max := range w.maxes {
		fmt.Fprintf(w.w, "%s", strings.Repeat(ROW, max))
		fmt.Fprintf(w.w, "%s", CENTER)
	}
	fmt.Fprintf(w.w, "\n")
}

func (w *Writer) print(row []string) {
	fmt.Fprintf(w.w, COLUMN)
	for i, val := range row {
		spacesCnt := w.maxes[i] - len(val) - 1
		fmt.Fprintf(w.w, " %s%s", val, strings.Repeat(" ", spacesCnt))
		fmt.Fprintf(w.w, "%s", COLUMN)
	}
	fmt.Fprintf(w.w, "\n")
}
