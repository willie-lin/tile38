package item

import (
	"reflect"
	"unsafe"
)

func getFieldAt(data unsafe.Pointer, index int) float64 {
	return *(*float64)(unsafe.Pointer(uintptr(data) + uintptr(index*8)))
}

func setFieldAt(data unsafe.Pointer, index int, value float64) {
	*(*float64)(unsafe.Pointer(uintptr(data) + uintptr(index*8))) = value
}

func (item *Item) dataBytes() []byte {
	return *(*[]byte)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(item.data)),
		Len:  item.fieldsDataSize() + item.idDataSize(),
		Cap:  item.fieldsDataSize() + item.idDataSize(),
	}))
}

func bytesToFloats(f []byte) []float64 {
	return *(*[]float64)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: ((*reflect.SliceHeader)(unsafe.Pointer(&f))).Data,
		Len:  len(f) / 8,
		Cap:  len(f) / 8,
	}))
}

func (item *Item) unpackedGenerateFieldBytes(values []float64) []byte {
	return *(*[]byte)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: ((*reflect.SliceHeader)(unsafe.Pointer(&values))).Data,
		Len:  len(values) * 8,
		Cap:  len(values) * 8,
	}))
}

func (item *Item) unpackedSetField(index int, value float64) (updated bool) {
	numFields := item.fieldsDataSize() / 8
	if index < numFields {
		// field exists
		if getFieldAt(item.data, index) == value {
			return false
		}
	} else if value == 0 {
		return false
	} else {

		// make room for new field
		oldBytes := item.dataBytes()
		newData := make([]byte, (index+1)*8+item.idDataSize())
		// copy the existing fields
		copy(newData, oldBytes[:item.fieldsDataSize()])
		// copy the id
		copy(newData[(index+1)*8:], oldBytes[item.fieldsDataSize():])
		// update the fields length
		item.setFieldsDataSize((index + 1) * 8)
		// update the raw data
		item.data = unsafe.Pointer(&newData[0])
	}
	// set the new field
	setFieldAt(item.data, index, value)
	return true
}

func (item *Item) unpackedForEachField(
	count int, iter func(value float64) bool,
) {
	fields := bytesToFloats(item.fieldsBytes())
	var n int
	if count < 0 {
		n = len(fields)
	} else {
		n = count
	}
	for i := 0; i < n; i++ {
		var field float64
		if i < len(fields) {
			field = fields[i]
		}
		if !iter(field) {
			return
		}
	}
}

func (item *Item) unpackedGetField(index int) float64 {
	numFields := item.fieldsDataSize() / 8
	if index < numFields {
		return getFieldAt(item.data, index)
	}
	return 0
}
