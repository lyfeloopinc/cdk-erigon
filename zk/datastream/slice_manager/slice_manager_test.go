package slice_manager

import (
	"sync"
	"testing"
	"time"
	"sync/atomic"
)

func TestSliceManager_AddItem(t *testing.T) {
	sm := &SliceManager{}

	// Test adding a single item
	sm.AddItem("item1")

	if len(sm.ConsumeCurrentItems()) != 1 {
		t.Errorf("Expected 1 item, got %d", len(sm.ConsumeCurrentItems()))
	}
}

func TestSliceManager_AddItems(t *testing.T) {
	sm := &SliceManager{}

	// multiple items
	items := []interface{}{"item1", "item2", "item3"}
	sm.AddItems(items)

	if len(sm.ConsumeCurrentItems()) != 3 {
		t.Errorf("Expected 3 items, got %d", len(sm.ConsumeCurrentItems()))
	}
}

func TestSliceManager_ClearItems(t *testing.T) {
	sm := &SliceManager{}

	// add/clear
	items := []interface{}{"item1", "item2"}
	sm.AddItems(items)
	sm.ClearItems()

	if len(sm.ConsumeCurrentItems()) != 0 {
		t.Errorf("Expected 0 items after clear, got %d", len(sm.ConsumeCurrentItems()))
	}
}

func TestSliceManager_ConsumeCurrentItems(t *testing.T) {
	sm := &SliceManager{}

	// consume
	items := []interface{}{"item1", "item2"}
	sm.AddItems(items)
	consumed := sm.ConsumeCurrentItems()

	if len(consumed) != 2 {
		t.Errorf("Expected 2 items consumed, got %d", len(consumed))
	}

	if len(sm.ConsumeCurrentItems()) != 0 {
		t.Errorf("Expected slice to be empty after consumption")
	}
}

func TestSliceManager_ConcurrentAccess(t *testing.T) {
	sm := &SliceManager{}
	var wg sync.WaitGroup
	const numAdds = 1000

	var added int32

	// add items (goroutine1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numAdds; i++ {
			sm.AddItem(i)
			atomic.AddInt32(&added, 1)
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// consume items until no more (goroutine2)
	wg.Add(1)
	go func() {
		seenItems := false
		defer wg.Done()
		for {
			rI := sm.ConsumeCurrentItems()
			if len(rI) == 0 && seenItems {
				break
			}
			if len(rI) > 0 {
				seenItems = true
			}
			atomic.AddInt32(&added, -int32(len(rI)))
			time.Sleep(100 * time.Millisecond)
		}
	}()

	wg.Wait()

	// ensure all added items are consumed
	if atomic.LoadInt32(&added) != 0 {
		t.Errorf("Expected all items to be consumed, but %d items remain", atomic.LoadInt32(&added))
	}
}

func TestSliceManager_NoDeadlock(t *testing.T) {
	sm := &SliceManager{}
	var wg sync.WaitGroup

	// add items, check for deadlock
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			sm.AddItem(i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			sm.ConsumeCurrentItems()
		}
	}()

	wg.Wait()
}

func TestSliceManager_ReadCurrentItems(t *testing.T) {
	sm := &SliceManager{}

	items := []interface{}{"item1", "item2", "item3"}
	sm.AddItems(items)

	// read
	readItems := sm.ReadCurrentItems()
	if len(readItems) != 3 {
		t.Errorf("Expected 3 items, got %d", len(readItems))
	}

	// readonly test
	if len(sm.ReadCurrentItems()) != 3 {
		t.Errorf("Expected 3 items after reading, but got %d", len(sm.ReadCurrentItems()))
	}
}

func TestSliceManager_RemoveRange(t *testing.T) {
	sm := &SliceManager{}

	items := []interface{}{"item1", "item2", "item3", "item4", "item5"}
	sm.AddItems(items)

	// normal
	sm.RemoveRange(1, 2)

	remainingItems := sm.ReadCurrentItems()
	if len(remainingItems) != 3 {
		t.Errorf("Expected 3 items after removal, got %d", len(remainingItems))
	}

	if remainingItems[0] != "item1" || remainingItems[1] != "item4" || remainingItems[2] != "item5" {
		t.Errorf("Items not as expected after removal: %+v", remainingItems)
	}

	// length too long
	sm.RemoveRange(1, 10)
	if len(sm.ReadCurrentItems()) != 1 {
		t.Errorf("Expected 1 item after removing an out-of-bounds range, got %d", len(sm.ReadCurrentItems()))
	}

	// offset too long
	sm.RemoveRange(10, 1)
	if len(sm.ReadCurrentItems()) != 1 {
		t.Errorf("Expected 1 item after removing with offset beyond slice length, got %d", len(sm.ReadCurrentItems()))
	}
}

func TestSliceManager_ReadCurrentItemsWithOffset(t *testing.T) {
	sm := &SliceManager{}

	// add items
	items := []interface{}{"item1", "item2", "item3", "item4", "item5"}
	sm.AddItems(items)

	// set offset and read from offset 2
	readItems := sm.ReadCurrentItemsWithOffset(2)
	if len(readItems) != 3 {
		t.Errorf("Expected 3 items, got %d", len(readItems))
	}

	expected := []interface{}{"item3", "item4", "item5"}
	for i, item := range expected {
		if readItems[i] != item {
			t.Errorf("Expected %v at index %d, but got %v", item, i, readItems[i])
		}
	}

	// set offset too long
	readItems = sm.ReadCurrentItemsWithOffset(10)
	if len(readItems) != 0 {
		t.Errorf("Expected 0 items with out-of-bounds offset, got %d", len(readItems))
	}
}

func TestSliceManager_RemoveUntilOffset(t *testing.T) {
	sm := &SliceManager{}

	// add items
	items := []interface{}{"item1", "item2", "item3", "item4", "item5"}
	sm.AddItems(items)

	// remove until offset 2
	sm.RemoveUntilOffset(2)
	remainingItems := sm.ReadCurrentItems()
	if len(remainingItems) != 3 {
		t.Errorf("Expected 3 items after removing until offset 2, got %d", len(remainingItems))
	}

	expected := []interface{}{"item3", "item4", "item5"}
	for i, item := range expected {
		if remainingItems[i] != item {
			t.Errorf("Expected %v at index %d, but got %v", item, i, remainingItems[i])
		}
	}

	// remove until offset too long
	sm.RemoveUntilOffset(10)
	remainingItems = sm.ReadCurrentItems()
	if len(remainingItems) != 0 {
		t.Errorf("Expected 0 items after removing with offset exceeding slice length, got %d", len(remainingItems))
	}

	// remove with offset equal to the slice length
	sm.AddItems(items)
	sm.RemoveUntilOffset(len(items))
	remainingItems = sm.ReadCurrentItems()
	if len(remainingItems) != 0 {
		t.Errorf("Expected 0 items after removing all items, got %d", len(remainingItems))
	}
}

func TestSliceManager_ConsumeCurrentItems_ResetsOffset(t *testing.T) {
	sm := &SliceManager{}

	// add items and set an offset
	items := []interface{}{"item1", "item2", "item3"}
	sm.AddItems(items)

	// consume items
	consumed := sm.ConsumeCurrentItems()

	// check if items were consumed correctly
	if len(consumed) != 3 {
		t.Errorf("Expected 3 items consumed, got %d", len(consumed))
	}

	// check if items slice is empty after consumption
	if len(sm.ConsumeCurrentItems()) != 0 {
		t.Errorf("Expected slice to be empty after consumption")
	}
}

func TestSliceManager_ReadLastItem(t *testing.T) {
	sm := &SliceManager{}

	// add items
	items := []interface{}{"item1", "item2", "item3"}
	sm.AddItems(items)

	// read last item
	lastItem := sm.ReadLastItem()
	if lastItem != "item3" {
		t.Errorf("Expected 'item3' as last item, got %v", lastItem)
	}

	// add more items
	sm.AddItems([]interface{}{"item4", "item5"})

	// read last item again
	lastItem = sm.ReadLastItem()
	if lastItem != "item5" {
		t.Errorf("Expected 'item5' as last item, got %v", lastItem)
	}
}
