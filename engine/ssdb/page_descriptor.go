package ssdb

// pageDescriptor is a management unit of page from buffer pool point of view.
type pageDescriptor struct {
	page *Page

	// when dirty flag is true, the page on the buffer pool
	dirty bool
}

func (pd *pageDescriptor) appendTuple(t Tuple) bool {
	if err := pd.page.AppendTuple(t); err != nil {
		// no available space. new page should be created
		return false
	}

	pd.dirty = true // when new tuple is appended to the page, it is marked dirty
	return true
}
