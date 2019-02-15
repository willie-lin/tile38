package collection

import (
	"sync"

	"github.com/tidwall/tile38/internal/collection/item"
)

// Fields ...
type Fields struct {
	mu       sync.Mutex
	unpacked bool       // fields have been unpacked
	values   []float64  // unpacked values
	item     *item.Item // base item
}

func (fields *Fields) unpack() {
	if fields.unpacked {
		return
	}
	fields.values = nil
	fields.item.ForEachField(-1, func(value float64) bool {
		fields.values = append(fields.values, value)
		return true
	})
	fields.unpacked = true
}

// ForEach iterates over each field. The count param is the number of
// iterations. When count is less than zero, then all fields are returns.
func (fields *Fields) ForEach(count int, iter func(value float64) bool) {
	if fields == nil || fields.item == nil {
		return
	}
	if !fields.item.Packed() {
		fields.item.ForEachField(count, iter)
		return
	}
	// packed values
	fields.mu.Lock()
	defer fields.mu.Unlock()
	if !fields.unpacked {
		fields.unpack()
	}
	var n int
	if count < 0 {
		n = len(fields.values)
	} else {
		n = count
	}
	for i := 0; i < n; i++ {
		var field float64
		if i < len(fields.values) {
			field = fields.values[i]
		}
		if !iter(field) {
			return
		}
	}
}

// Get returns the value for a field at index. If there is no field at index,
// then zero is returned.
func (fields *Fields) Get(index int) float64 {
	if fields == nil || fields.item == nil {
		return 0
	}
	if !fields.item.Packed() {
		return fields.item.GetField(index)
	}
	// packed values
	fields.mu.Lock()
	if !fields.unpacked {
		fields.unpack()
	}
	var value float64
	if index < len(fields.values) {
		value = fields.values[index]
	}
	fields.mu.Unlock()
	return value
}

func itemFields(item *item.Item) *Fields {
	if item == nil || !item.HasFields() {
		return nil
	}
	return &Fields{item: item}
}
