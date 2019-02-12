package collection

import (
	"reflect"
	"unsafe"

	"github.com/tidwall/btree"
	"github.com/tidwall/geojson"
)

type itemT struct {
	obj       geojson.Object
	fieldsLen uint32 // fields block size in bytes, not num of fields
	idLen     uint32 // id block size in bytes
	data      unsafe.Pointer
}

func (item *itemT) id() string {
	return *(*string)((unsafe.Pointer)(&reflect.StringHeader{
		Data: uintptr(unsafe.Pointer(item.data)) + uintptr(item.fieldsLen),
		Len:  int(item.idLen),
	}))
}

func (item *itemT) fields() []float64 {
	return *(*[]float64)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(item.data)),
		Len:  int(item.fieldsLen) / 8,
		Cap:  int(item.fieldsLen) / 8,
	}))
}

func (item *itemT) dataBytes() []byte {
	return *(*[]byte)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(item.data)),
		Len:  int(item.fieldsLen) + int(item.idLen),
		Cap:  int(item.fieldsLen) + int(item.idLen),
	}))
}

func newItem(id string, obj geojson.Object) *itemT {
	item := new(itemT)
	item.obj = obj
	item.idLen = uint32(len(id))
	if len(id) > 0 {
		data := make([]byte, len(id))
		copy(data, id)
		item.data = unsafe.Pointer(&data[0])
	}
	return item
}

func (item *itemT) weightAndPoints() (weight, points int) {
	if objIsSpatial(item.obj) {
		points = item.obj.NumPoints()
		weight = points * 16
	} else {
		weight = len(item.obj.String())
	}
	weight += int(item.fieldsLen + item.idLen)
	return weight, points
}

func (item *itemT) Less(other btree.Item, ctx interface{}) bool {
	value1 := item.obj.String()
	value2 := other.(*itemT).obj.String()
	if value1 < value2 {
		return true
	}
	if value1 > value2 {
		return false
	}
	// the values match so we'll compare IDs, which are always unique.
	return item.id() < other.(*itemT).id()
}

func floatsToBytes(f []float64) []byte {
	return *(*[]byte)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: ((*reflect.SliceHeader)(unsafe.Pointer(&f))).Data,
		Len:  len(f) * 8,
		Cap:  len(f) * 8,
	}))
}

// directCopyFields copies fields, overwriting previous fields
func (item *itemT) directCopyFields(fields []float64) {
	fieldBytes := floatsToBytes(fields)
	oldData := item.dataBytes()
	newData := make([]byte, len(fieldBytes)+int(item.idLen))
	copy(newData, fieldBytes)
	copy(newData[len(fieldBytes):], oldData[item.fieldsLen:])
	item.fieldsLen = uint32(len(fieldBytes))
	if len(newData) > 0 {
		item.data = unsafe.Pointer(&newData[0])
	} else {
		item.data = nil
	}
}

func (c *Collection) setField(
	item *itemT, fieldName string, fieldValue float64, updateWeight bool,
) (updated bool) {
	idx, ok := c.fieldMap[fieldName]
	if !ok {
		idx = len(c.fieldMap)
		c.fieldMap[fieldName] = idx
	}
	itemFields := item.fields()
	if idx >= len(itemFields) {
		// make room for new field

		itemBytes := item.dataBytes()
		oldLen := len(itemFields)
		data := make([]byte, (idx+1)*8+int(item.idLen))

		copy(data, itemBytes[:item.fieldsLen])
		copy(data[(idx+1)*8:], itemBytes[item.fieldsLen:])
		item.fieldsLen = uint32((idx + 1) * 8)
		item.data = unsafe.Pointer(&data[0])

		itemFields := item.fields()
		if updateWeight {
			c.weight += (len(itemFields) - oldLen) * 8
		}
		itemFields[idx] = fieldValue
		updated = true
	} else if itemFields[idx] != fieldValue {
		// existing field needs updating
		itemFields[idx] = fieldValue
		updated = true
	}
	return updated
}
func (c *Collection) setFields(
	item *itemT, fieldNames []string, fieldValues []float64, updateWeight bool,
) (updatedCount int) {
	// TODO: optimize to predict the item data growth.
	// TODO: do all sets here, instead of calling setFields in a loop
	for i, fieldName := range fieldNames {
		var fieldValue float64
		if i < len(fieldValues) {
			fieldValue = fieldValues[i]
		}
		if c.setField(item, fieldName, fieldValue, updateWeight) {
			updatedCount++
		}
	}
	return updatedCount
}
