package collection

import (
	"reflect"
	"unsafe"

	"github.com/tidwall/btree"
	"github.com/tidwall/geojson"
)

type itemT struct {
	obj       geojson.Object
	idLen     uint32 // id block size in bytes
	fieldsLen uint32 // fields block size in bytes, not num of fields
	data      unsafe.Pointer
}

func (item *itemT) id() string {
	return *(*string)((unsafe.Pointer)(&reflect.StringHeader{
		Data: uintptr(unsafe.Pointer(item.data)),
		Len:  int(item.idLen),
	}))
}

func (item *itemT) fields() []float64 {
	return *(*[]float64)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(item.data)) + uintptr(item.idLen),
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

// directSetFields copies fields, overwriting previous fields
func (item *itemT) directSetFields(fields []float64) {
	n := int(item.idLen) + len(fields)*8
	item.fieldsLen = uint32(len(fields) * 8)
	if n > 0 {
		newData := make([]byte, int(item.idLen)+len(fields)*8)
		item.data = unsafe.Pointer(&newData[0])
		copy(newData, item.id())
		copy(item.fields(), fields)
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

		oldLen := len(itemFields)
		// print(c.weight)
		data := make([]byte, int(item.idLen)+(idx+1)*8)
		copy(data, item.dataBytes())
		item.fieldsLen = uint32((idx + 1) * 8)
		item.data = unsafe.Pointer(&data[0])
		itemFields := item.fields()
		if updateWeight {
			c.weight += (len(itemFields) - oldLen) * 8
		}
		// print(":")
		// print(c.weight)
		// println()
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
