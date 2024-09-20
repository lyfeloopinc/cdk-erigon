package slice_manager

import (
	"sync"
)

type SliceManager struct {
	items []interface{}
	iMu   sync.RWMutex
}

func NewSliceManager() *SliceManager {
	return &SliceManager{
		items: make([]interface{}, 0),
		iMu:   sync.RWMutex{},
	}
}

func (s *SliceManager) AddItem(item interface{}) {
	s.iMu.Lock()
	defer s.iMu.Unlock()
	s.items = append(s.items, item)
}

func (s *SliceManager) AddItems(items []interface{}) {
	s.iMu.Lock()
	defer s.iMu.Unlock()
	s.items = append(s.items, items...)
}

func (s *SliceManager) ClearItems() {
	s.iMu.Lock()
	defer s.iMu.Unlock()
	s.items = nil
}

func (s *SliceManager) ConsumeCurrentItems() []interface{} {
	s.iMu.Lock()
	defer s.iMu.Unlock()

	dCopy := make([]interface{}, len(s.items))
	copy(dCopy, s.items)

	s.items = s.items[:0]

	return dCopy
}

func (s *SliceManager) ReadCurrentItems() []interface{} {
	s.iMu.RLock()
	defer s.iMu.RUnlock()

	dCopy := make([]interface{}, len(s.items))
	copy(dCopy, s.items)

	return dCopy
}

func (s *SliceManager) ReadLastItem() interface{} {
	s.iMu.RLock()
	defer s.iMu.RUnlock()

	if len(s.items) == 0 {
		return nil
	}

	return s.items[len(s.items)-1]
}

func (s *SliceManager) ReadCurrentItemsWithOffset(offset int) []interface{} {
	s.iMu.RLock()
	defer s.iMu.RUnlock()

	if offset >= len(s.items) {
		return []interface{}{}
	}

	dCopy := make([]interface{}, len(s.items)-offset)
	copy(dCopy, s.items[offset:])

	return dCopy
}

func (s *SliceManager) RemoveRange(offset, length int) {
	s.iMu.Lock()
	defer s.iMu.Unlock()

	if offset >= len(s.items) {
		return
	}

	if offset+length > len(s.items) {
		length = len(s.items) - offset
	}

	copy(s.items[offset:], s.items[offset+length:])
	s.items = s.items[:len(s.items)-length]
}

func (s *SliceManager) RemoveUntilOffset(offset int) {
	s.iMu.Lock()
	defer s.iMu.Unlock()

	if offset >= len(s.items) {
		s.items = s.items[:0]
		return
	}

	copy(s.items, s.items[offset:])
	s.items = s.items[:len(s.items)-offset]
}
