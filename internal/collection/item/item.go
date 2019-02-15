package item

import (
	"reflect"
	"unsafe"

	"github.com/tidwall/btree"
	"github.com/tidwall/geojson"
)

// Item is a item for Tile38 collections
type Item struct {
	head [2]uint32      // (1:isPoint,1:isPacked,30:fieldsByteLen),(32:idLen)
	data unsafe.Pointer // pointer to raw block of bytes, fields+id
}
type objItem struct {
	_   [2]uint32
	_   unsafe.Pointer
	obj geojson.Object
}
type pointItem struct {
	_  [2]uint32
	_  unsafe.Pointer
	pt geojson.SimplePoint
}

func setbit(n uint32, pos uint) uint32 {
	return n | (1 << pos)
}
func unsetbit(n uint32, pos uint) uint32 {
	return n & ^(1 << pos)
}
func hasbit(n uint32, pos uint) bool {
	return (n & (1 << pos)) != 0
}

func (item *Item) isPoint() bool {
	return hasbit(item.head[0], 31)
}

func (item *Item) setIsPoint(isPoint bool) {
	if isPoint {
		item.head[0] = setbit(item.head[0], 31)
	} else {
		item.head[0] = unsetbit(item.head[0], 31)
	}
}

func (item *Item) isPacked() bool {
	return hasbit(item.head[0], 30)
}
func (item *Item) setIsPacked(isPacked bool) {
	if isPacked {
		item.head[0] = setbit(item.head[0], 30)
	} else {
		item.head[0] = unsetbit(item.head[0], 30)
	}
}

func (item *Item) fieldsLen() int {
	return int(item.head[0] & 0x3FFFFFFF)
}

func (item *Item) setFieldsLen(len int) {
	item.head[0] = item.head[0]>>30<<30 | uint32(len)
}

func (item *Item) idLen() int {
	return int(item.head[1])
}

func (item *Item) setIDLen(len int) {
	item.head[1] = uint32(len)
}

// ID returns the items ID as a string
func (item *Item) ID() string {
	return *(*string)((unsafe.Pointer)(&reflect.StringHeader{
		Data: uintptr(unsafe.Pointer(item.data)) + uintptr(item.fieldsLen()),
		Len:  item.idLen(),
	}))
}

// Fields returns the field values
func (item *Item) fields() []float64 {
	return *(*[]float64)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(item.data)),
		Len:  item.fieldsLen() / 8,
		Cap:  item.fieldsLen() / 8,
	}))
}

// Obj returns the geojson object
func (item *Item) Obj() geojson.Object {
	if item.isPoint() {
		return &(*pointItem)(unsafe.Pointer(item)).pt
	}
	return (*objItem)(unsafe.Pointer(item)).obj
}

// New returns a newly allocated Item
func New(id string, obj geojson.Object, packed bool) *Item {
	var item *Item
	if pt, ok := obj.(*geojson.SimplePoint); ok {
		pitem := new(pointItem)
		pitem.pt = *pt
		item = (*Item)(unsafe.Pointer(pitem))
		item.setIsPoint(true)
	} else {
		oitem := new(objItem)
		oitem.obj = obj
		item = (*Item)(unsafe.Pointer(oitem))
	}
	item.setIsPacked(packed)
	item.setIDLen(len(id))
	item.data = unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&id)).Data)
	return item
}

// WeightAndPoints returns the memory weight and number of points for Item.
func (item *Item) WeightAndPoints() (weight, points int) {
	_, objIsSpatial := item.Obj().(geojson.Spatial)
	if objIsSpatial {
		points = item.Obj().NumPoints()
		weight = points * 16
	} else if item.Obj() != nil {
		weight = len(item.Obj().String())
	}
	weight += item.fieldsLen() + item.idLen()
	return weight, points
}

// Less is a btree interface that compares if item is less than other item.
func (item *Item) Less(other btree.Item, ctx interface{}) bool {
	value1 := item.Obj().String()
	value2 := other.(*Item).Obj().String()
	if value1 < value2 {
		return true
	}
	if value1 > value2 {
		return false
	}
	// the values match so we'll compare IDs, which are always unique.
	return item.ID() < other.(*Item).ID()
}

// CopyOverFields overwriting previous fields. Accepts an *Item or []float64
func (item *Item) CopyOverFields(from interface{}) {
	var values []float64
	switch from := from.(type) {
	case *Item:
		values = from.fields()
	case []float64:
		values = from
	}
	fieldBytes := floatsToBytes(values)
	oldData := item.dataBytes()
	newData := make([]byte, len(fieldBytes)+item.idLen())
	copy(newData, fieldBytes)
	copy(newData[len(fieldBytes):], oldData[item.fieldsLen():])
	item.setFieldsLen(len(fieldBytes))
	if len(newData) > 0 {
		item.data = unsafe.Pointer(&newData[0])
	} else {
		item.data = nil
	}
}

func getFieldAt(data unsafe.Pointer, index int) float64 {
	return *(*float64)(unsafe.Pointer(uintptr(data) + uintptr(index*8)))
}

func setFieldAt(data unsafe.Pointer, index int, value float64) {
	*(*float64)(unsafe.Pointer(uintptr(data) + uintptr(index*8))) = value
}

// SetField set a field value at specified index.
func (item *Item) SetField(index int, value float64) (updated bool) {
	numFields := item.fieldsLen() / 8
	if index < numFields {
		// field exists
		if getFieldAt(item.data, index) == value {
			return false
		}
	} else {
		// make room for new field
		oldBytes := item.dataBytes()
		newData := make([]byte, (index+1)*8+item.idLen())
		// copy the existing fields
		copy(newData, oldBytes[:item.fieldsLen()])
		// copy the id
		copy(newData[(index+1)*8:], oldBytes[item.fieldsLen():])
		// update the fields length
		item.setFieldsLen((index + 1) * 8)
		// update the raw data
		item.data = unsafe.Pointer(&newData[0])
	}
	// set the new field
	setFieldAt(item.data, index, value)
	return true
}

func (item *Item) dataBytes() []byte {
	return *(*[]byte)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(item.data)),
		Len:  item.fieldsLen() + item.idLen(),
		Cap:  item.fieldsLen() + item.idLen(),
	}))
}

func floatsToBytes(f []float64) []byte {
	return *(*[]byte)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: ((*reflect.SliceHeader)(unsafe.Pointer(&f))).Data,
		Len:  len(f) * 8,
		Cap:  len(f) * 8,
	}))
}

// ForEachField iterates over each field. The count param is the number of
// iterations. When count is less than zero, then all fields are returns.
func (item *Item) ForEachField(count int, iter func(value float64) bool) {
	if item == nil {
		return
	}
	fields := item.fields()
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

// Packed returns true when the item's fields are packed
func (item *Item) Packed() bool {
	return item == nil || item.isPacked()
}

// GetField returns the value for a field at index.
func (item *Item) GetField(index int) float64 {
	if index < 0 {
		panic("index out of range")
	}
	if item == nil {
		return 0
	}
	if item.Packed() {
		var fvalue float64
		var idx int
		item.ForEachField(-1, func(value float64) bool {
			if idx == index {
				fvalue = value
				return false
			}
			idx++
			return true
		})
		return fvalue
	}
	numFields := item.fieldsLen() / 8
	if index < numFields {
		return getFieldAt(item.data, index)
	}
	return 0
}

// HasFields returns true when item has fields
func (item *Item) HasFields() bool {
	return item != nil && item.fieldsLen() > 0
}
