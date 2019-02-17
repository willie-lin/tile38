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

func (item *Item) fieldsDataSize() int {
	return int(item.head[0] & 0x3FFFFFFF)
}

func (item *Item) setFieldsDataSize(len int) {
	item.head[0] = item.head[0]>>30<<30 | uint32(len)
}

func (item *Item) idDataSize() int {
	return int(item.head[1])
}

func (item *Item) setIDDataSize(len int) {
	item.head[1] = uint32(len)
}

// ID returns the items ID as a string
func (item *Item) ID() string {
	return *(*string)((unsafe.Pointer)(&reflect.StringHeader{
		Data: uintptr(unsafe.Pointer(item.data)) +
			uintptr(item.fieldsDataSize()),
		Len: item.idDataSize(),
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
	item.setIDDataSize(len(id))
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
	weight += item.fieldsDataSize() + item.idDataSize()
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

// fieldBytes returns the raw fields data section
func (item *Item) fieldsBytes() []byte {
	return *(*[]byte)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(item.data)),
		Len:  item.fieldsDataSize(),
		Cap:  item.fieldsDataSize(),
	}))
}

// Packed returns true when the item's fields are packed
func (item *Item) Packed() bool {
	return item == nil || item.isPacked()
}

// CopyOverFields overwriting previous fields. Accepts an *Item or []float64
func (item *Item) CopyOverFields(from interface{}) {
	if item == nil {
		return
	}
	var values []float64
	var fieldBytes []byte
	var directCopy bool
	switch from := from.(type) {
	case *Item:
		if item.Packed() == from.Packed() {
			// direct copy the bytes
			fieldBytes = from.fieldsBytes()
			directCopy = true
		} else {
			// get the values through iteration
			item.ForEachField(-1, func(value float64) bool {
				values = append(values, value)
				return true
			})
		}
	case []float64:
		values = from
	}
	if !directCopy {
		if item.Packed() {
			fieldBytes = item.packedGenerateFieldBytes(values)
		} else {
			fieldBytes = item.unpackedGenerateFieldBytes(values)
		}
	}
	id := item.ID()
	newData := make([]byte, len(fieldBytes)+len(id))
	copy(newData, fieldBytes)
	copy(newData[len(fieldBytes):], id)
	item.setFieldsDataSize(len(fieldBytes))
	if len(newData) > 0 {
		item.data = unsafe.Pointer(&newData[0])
	} else {
		item.data = nil
	}
}

// SetField set a field value at specified index.
func (item *Item) SetField(index int, value float64) (updated bool) {
	if item == nil {
		return false
	}
	if item.Packed() {
		return item.packedSetField(index, value)
	}
	return item.unpackedSetField(index, value)
}

// ForEachField iterates over each field. The count param is the number of
// iterations. When count is less than zero, then all fields are returns.
func (item *Item) ForEachField(count int, iter func(value float64) bool) {
	if item == nil {
		return
	}
	if item.Packed() {
		item.packedForEachField(count, iter)
	} else {
		item.unpackedForEachField(count, iter)
	}
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
		return item.packedGetField(index)
	}
	return item.unpackedGetField(index)
}

// HasFields returns true when item has fields
func (item *Item) HasFields() bool {
	return item != nil && item.fieldsDataSize() > 0
}
