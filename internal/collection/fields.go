package collection

import "github.com/tidwall/tile38/internal/collection/item"

// // FieldIter ...
// type FieldIter interface {
// 	ForEachField(count int, iter func(value float64) bool)
// 	GetField(index int) float64
// 	HasFields() bool
// }

// Fields ...
type Fields struct {
	item *item.Item
}

// ForEach iterates over each field. The count param is the number of
// iterations. When count is less than zero, then all fields are returns.
func (fields *Fields) ForEach(count int, iter func(value float64) bool) {
	if fields == nil || fields.item == nil {
		return
	}
	fields.item.ForEachField(count, iter)
}

// Get returns the value for a field at index. If there is no field at index,
// then zero is returned.
func (fields *Fields) Get(index int) float64 {
	if fields == nil || fields.item == nil {
		return 0
	}
	return fields.item.GetField(index)
}

func itemFields(item *item.Item) *Fields {
	if item == nil || !item.HasFields() {
		return nil
	}
	return &Fields{item: item}
}
