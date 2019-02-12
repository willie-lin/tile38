package collection

import (
	"unsafe"

	"github.com/tidwall/boxtree/d2"
	"github.com/tidwall/btree"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/tile38/internal/collection/ptrbtree"
)

// Cursor allows for quickly paging through Scan, Within, Intersects, and Nearby
type Cursor interface {
	Offset() uint64
	Step(count uint64)
}

type itemT struct {
	id     string
	obj    geojson.Object
	fields []float64
}

func (item *itemT) weightAndPoints() (weight, points int) {
	if objIsSpatial(item.obj) {
		points = item.obj.NumPoints()
		weight = points * 16
	} else {
		weight = len(item.obj.String())
	}
	weight += len(item.fields)*8 + len(item.id)
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
	return item.id < other.(*itemT).id
}

// Collection represents a collection of geojson objects.
type Collection struct {
	items    ptrbtree.BTree // items sorted by keys
	index    d2.BoxTree     // items geospatially indexed
	values   *btree.BTree   // items sorted by value+key
	fieldMap map[string]int
	weight   int
	points   int
	objects  int // geometry count
	nobjects int // non-geometry count
}

var counter uint64

// New creates an empty collection
func New() *Collection {
	col := &Collection{
		values:   btree.New(16, nil),
		fieldMap: make(map[string]int),
	}
	return col
}

// Count returns the number of objects in collection.
func (c *Collection) Count() int {
	return c.objects + c.nobjects
}

// StringCount returns the number of string values.
func (c *Collection) StringCount() int {
	return c.nobjects
}

// PointCount returns the number of points (lat/lon coordinates) in collection.
func (c *Collection) PointCount() int {
	return c.points
}

// TotalWeight calculates the in-memory cost of the collection in bytes.
func (c *Collection) TotalWeight() int {
	return c.weight
}

// Bounds returns the bounds of all the items in the collection.
func (c *Collection) Bounds() (minX, minY, maxX, maxY float64) {
	min, max := c.index.Bounds()
	if len(min) >= 2 && len(max) >= 2 {
		return min[0], min[1], max[0], max[1]
	}
	return
}

func objIsSpatial(obj geojson.Object) bool {
	_, ok := obj.(geojson.Spatial)
	return ok
}

func (c *Collection) indexDelete(item *itemT) {
	if !item.obj.Empty() {
		rect := item.obj.Rect()
		c.index.Delete(
			[]float64{rect.Min.X, rect.Min.Y},
			[]float64{rect.Max.X, rect.Max.Y},
			item)
	}
}

func (c *Collection) indexInsert(item *itemT) {
	if !item.obj.Empty() {
		rect := item.obj.Rect()
		c.index.Insert(
			[]float64{rect.Min.X, rect.Min.Y},
			[]float64{rect.Max.X, rect.Max.Y},
			item)
	}
}

func (c *Collection) addItem(item *itemT) {
	if objIsSpatial(item.obj) {
		c.indexInsert(item)
		c.objects++
	} else {
		c.values.ReplaceOrInsert(item)
		c.nobjects++
	}
	weight, points := item.weightAndPoints()
	c.weight += weight
	c.points += points
}

func (c *Collection) delItem(item *itemT) {
	if objIsSpatial(item.obj) {
		c.indexDelete(item)
		c.objects--
	} else {
		c.values.Delete(item)
		c.nobjects--
	}
	weight, points := item.weightAndPoints()
	c.weight -= weight
	c.points -= points
}

// Set adds or replaces an object in the collection and returns the fields
// array. If an item with the same id is already in the collection then the
// new item will adopt the old item's fields.
// The fields argument is optional.
// The return values are the old object, the old fields, and the new fields
func (c *Collection) Set(
	id string, obj geojson.Object, fields []string, values []float64,
) (
	oldObject geojson.Object, oldFields []float64, newFields []float64,
) {
	newItem := &itemT{id: id, obj: obj}

	// add the new item to main btree and remove the old one if needed
	oldItemV, ok := c.items.Set(unsafe.Pointer(newItem))
	if ok {
		oldItem := (*itemT)(oldItemV)

		// remove old item from indexes
		c.delItem(oldItem)

		oldObject = oldItem.obj
		if len(oldItem.fields) > 0 {
			// merge old and new fields
			oldFields = oldItem.fields
			newItem.fields = make([]float64, len(oldFields))
			copy(newItem.fields, oldFields)
		}
	}

	if fields == nil && len(values) > 0 {
		// directly set the field values, from copy
		newItem.fields = make([]float64, len(values))
		copy(newItem.fields, values)

	} else if len(fields) > 0 {
		// add new field to new item
		if len(newItem.fields) == 0 {
			// make exact room
			newItem.fields = make([]float64, 0, len(fields))
		}
		c.setFields(newItem, fields, values, false)
	}

	// add new item to indexes
	c.addItem(newItem)

	return oldObject, oldFields, newItem.fields
}

// Delete removes an object and returns it.
// If the object does not exist then the 'ok' return value will be false.
func (c *Collection) Delete(id string) (
	obj geojson.Object, fields []float64, ok bool,
) {
	oldItemV, ok := c.items.Delete(id)
	if !ok {
		return nil, nil, false
	}
	oldItem := (*itemT)(oldItemV)

	c.delItem(oldItem)

	return oldItem.obj, oldItem.fields, true
}

// Get returns an object.
// If the object does not exist then the 'ok' return value will be false.
func (c *Collection) Get(id string) (
	obj geojson.Object, fields []float64, ok bool,
) {
	itemV, ok := c.items.Get(id)
	if !ok {
		return nil, nil, false
	}
	item := (*itemT)(itemV)

	return item.obj, item.fields, true
}

// SetField set a field value for an object and returns that object.
// If the object does not exist then the 'ok' return value will be false.
func (c *Collection) SetField(id, fieldName string, fieldValue float64) (
	obj geojson.Object, fields []float64, updated bool, ok bool,
) {
	itemV, ok := c.items.Get(id)
	if !ok {
		return nil, nil, false, false
	}
	item := (*itemT)(itemV)
	updated = c.setField(item, fieldName, fieldValue, true)
	return item.obj, item.fields, updated, true
}

func (c *Collection) setField(
	item *itemT, fieldName string, fieldValue float64, updateWeight bool,
) (updated bool) {
	idx, ok := c.fieldMap[fieldName]
	if !ok {
		idx = len(c.fieldMap)
		c.fieldMap[fieldName] = idx
	}

	if idx >= len(item.fields) {
		// grow the fields slice
		oldLen := len(item.fields)
		for idx >= len(item.fields) {
			item.fields = append(item.fields, 0)
		}
		if updateWeight {
			c.weight += (len(item.fields) - oldLen) * 8
		}
		item.fields[idx] = fieldValue
		updated = true
	} else if item.fields[idx] != fieldValue {
		// existing field needs updating
		item.fields[idx] = fieldValue
		updated = true
	}
	return updated
}

// SetFields is similar to SetField, just setting multiple fields at once
func (c *Collection) SetFields(
	id string, fieldNames []string, fieldValues []float64,
) (obj geojson.Object, fields []float64, updatedCount int, ok bool) {
	itemV, ok := c.items.Get(id)
	if !ok {
		return nil, nil, 0, false
	}
	item := (*itemT)(itemV)

	updatedCount = c.setFields(item, fieldNames, fieldValues, true)

	return item.obj, item.fields, updatedCount, true
}

func (c *Collection) setFields(
	item *itemT, fieldNames []string, fieldValues []float64, updateWeight bool,
) (updatedCount int) {

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

// FieldMap return a maps of the field names.
func (c *Collection) FieldMap() map[string]int {
	return c.fieldMap
}

// FieldArr return an array representation of the field names.
func (c *Collection) FieldArr() []string {
	arr := make([]string, len(c.fieldMap))
	for field, i := range c.fieldMap {
		arr[i] = field
	}
	return arr
}

// Scan iterates though the collection ids.
func (c *Collection) Scan(desc bool, cursor Cursor,
	iterator func(id string, obj geojson.Object, fields []float64) bool,
) bool {
	var keepon = true
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	iter := func(ptr unsafe.Pointer) bool {
		count++
		if count <= offset {
			return true
		}
		if cursor != nil {
			cursor.Step(1)
		}
		iitm := (*itemT)(ptr)
		keepon = iterator(iitm.id, iitm.obj, iitm.fields)
		return keepon
	}
	if desc {
		c.items.Reverse(iter)
	} else {
		c.items.Scan(iter)
	}
	return keepon
}

// ScanRange iterates though the collection starting with specified id.
func (c *Collection) ScanRange(start, end string, desc bool, cursor Cursor,
	iterator func(id string, obj geojson.Object, fields []float64) bool,
) bool {
	var keepon = true
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	iter := func(ptr unsafe.Pointer) bool {
		count++
		if count <= offset {
			return true
		}
		if cursor != nil {
			cursor.Step(1)
		}
		iitm := (*itemT)(ptr)
		if !desc {
			if iitm.id >= end {
				return false
			}
		} else {
			if iitm.id <= end {
				return false
			}
		}
		keepon = iterator(iitm.id, iitm.obj, iitm.fields)
		return keepon
	}

	if desc {
		c.items.Descend(start, iter)
	} else {
		c.items.Ascend(start, iter)
	}
	return keepon
}

// SearchValues iterates though the collection values.
func (c *Collection) SearchValues(desc bool, cursor Cursor,
	iterator func(id string, obj geojson.Object, fields []float64) bool,
) bool {
	var keepon = true
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	iter := func(item btree.Item) bool {
		count++
		if count <= offset {
			return true
		}
		if cursor != nil {
			cursor.Step(1)
		}
		iitm := item.(*itemT)
		keepon = iterator(iitm.id, iitm.obj, iitm.fields)
		return keepon
	}
	if desc {
		c.values.Descend(iter)
	} else {
		c.values.Ascend(iter)
	}
	return keepon
}

// SearchValuesRange iterates though the collection values.
func (c *Collection) SearchValuesRange(start, end string, desc bool,
	cursor Cursor,
	iterator func(id string, obj geojson.Object, fields []float64) bool,
) bool {
	var keepon = true
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	iter := func(item btree.Item) bool {
		count++
		if count <= offset {
			return true
		}
		if cursor != nil {
			cursor.Step(1)
		}
		iitm := item.(*itemT)
		keepon = iterator(iitm.id, iitm.obj, iitm.fields)
		return keepon
	}
	if desc {
		c.values.DescendRange(&itemT{obj: String(start)},
			&itemT{obj: String(end)}, iter)
	} else {
		c.values.AscendRange(&itemT{obj: String(start)},
			&itemT{obj: String(end)}, iter)
	}
	return keepon
}

// ScanGreaterOrEqual iterates though the collection starting with specified id.
func (c *Collection) ScanGreaterOrEqual(id string, desc bool,
	cursor Cursor,
	iterator func(id string, obj geojson.Object, fields []float64) bool,
) bool {
	var keepon = true
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	iter := func(ptr unsafe.Pointer) bool {
		count++
		if count <= offset {
			return true
		}
		if cursor != nil {
			cursor.Step(1)
		}
		iitm := (*itemT)(ptr)
		keepon = iterator(iitm.id, iitm.obj, iitm.fields)
		return keepon
	}
	if desc {
		c.items.Descend(id, iter)
	} else {
		c.items.Ascend(id, iter)
	}
	return keepon
}

func (c *Collection) geoSearch(
	rect geometry.Rect,
	iter func(id string, obj geojson.Object, fields []float64) bool,
) bool {
	alive := true
	c.index.Search(
		[]float64{rect.Min.X, rect.Min.Y},
		[]float64{rect.Max.X, rect.Max.Y},
		func(_, _ []float64, itemv interface{}) bool {
			item := itemv.(*itemT)
			alive = iter(item.id, item.obj, item.fields)
			return alive
		},
	)
	return alive
}

func (c *Collection) geoSparse(
	obj geojson.Object, sparse uint8,
	iter func(id string, obj geojson.Object, fields []float64) (match, ok bool),
) bool {
	matches := make(map[string]bool)
	alive := true
	c.geoSparseInner(obj.Rect(), sparse,
		func(id string, o geojson.Object, fields []float64) (
			match, ok bool,
		) {
			ok = true
			if !matches[id] {
				match, ok = iter(id, o, fields)
				if match {
					matches[id] = true
				}
			}
			return match, ok
		},
	)
	return alive
}
func (c *Collection) geoSparseInner(
	rect geometry.Rect, sparse uint8,
	iter func(id string, obj geojson.Object, fields []float64) (match, ok bool),
) bool {
	if sparse > 0 {
		w := rect.Max.X - rect.Min.X
		h := rect.Max.Y - rect.Min.Y
		quads := [4]geometry.Rect{
			geometry.Rect{
				Min: geometry.Point{X: rect.Min.X, Y: rect.Min.Y + h/2},
				Max: geometry.Point{X: rect.Min.X + w/2, Y: rect.Max.Y},
			},
			geometry.Rect{
				Min: geometry.Point{X: rect.Min.X + w/2, Y: rect.Min.Y + h/2},
				Max: geometry.Point{X: rect.Max.X, Y: rect.Max.Y},
			},
			geometry.Rect{
				Min: geometry.Point{X: rect.Min.X, Y: rect.Min.Y},
				Max: geometry.Point{X: rect.Min.X + w/2, Y: rect.Min.Y + h/2},
			},
			geometry.Rect{
				Min: geometry.Point{X: rect.Min.X + w/2, Y: rect.Min.Y},
				Max: geometry.Point{X: rect.Max.X, Y: rect.Min.Y + h/2},
			},
		}
		for _, quad := range quads {
			if !c.geoSparseInner(quad, sparse-1, iter) {
				return false
			}
		}
		return true
	}
	alive := true
	c.geoSearch(rect,
		func(id string, obj geojson.Object, fields []float64) bool {
			match, ok := iter(id, obj, fields)
			if !ok {
				alive = false
				return false
			}
			return !match
		},
	)
	return alive
}

// Within returns all object that are fully contained within an object or
// bounding box. Set obj to nil in order to use the bounding box.
func (c *Collection) Within(
	obj geojson.Object,
	sparse uint8,
	cursor Cursor,
	iter func(id string, obj geojson.Object, fields []float64) bool,
) bool {
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	if sparse > 0 {
		return c.geoSparse(obj, sparse,
			func(id string, o geojson.Object, fields []float64) (
				match, ok bool,
			) {
				count++
				if count <= offset {
					return false, true
				}
				if cursor != nil {
					cursor.Step(1)
				}
				if match = o.Within(obj); match {
					ok = iter(id, o, fields)
				}
				return match, ok
			},
		)
	}
	return c.geoSearch(obj.Rect(),
		func(id string, o geojson.Object, fields []float64) bool {
			count++
			if count <= offset {
				return true
			}
			if cursor != nil {
				cursor.Step(1)
			}
			if o.Within(obj) {
				return iter(id, o, fields)
			}
			return true
		},
	)
}

// Intersects returns all object that are intersect an object or bounding box.
// Set obj to nil in order to use the bounding box.
func (c *Collection) Intersects(
	obj geojson.Object,
	sparse uint8,
	cursor Cursor,
	iter func(id string, obj geojson.Object, fields []float64) bool,
) bool {
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	if sparse > 0 {
		return c.geoSparse(obj, sparse,
			func(id string, o geojson.Object, fields []float64) (
				match, ok bool,
			) {
				count++
				if count <= offset {
					return false, true
				}
				if cursor != nil {
					cursor.Step(1)
				}
				if match = o.Intersects(obj); match {
					ok = iter(id, o, fields)
				}
				return match, ok
			},
		)
	}
	return c.geoSearch(obj.Rect(),
		func(id string, o geojson.Object, fields []float64) bool {
			count++
			if count <= offset {
				return true
			}
			if cursor != nil {
				cursor.Step(1)
			}
			if o.Intersects(obj) {
				return iter(id, o, fields)
			}
			return true
		},
	)
}

// Nearby returns the nearest neighbors
func (c *Collection) Nearby(
	target geojson.Object,
	cursor Cursor,
	iter func(id string, obj geojson.Object, fields []float64) bool,
) bool {
	// First look to see if there's at least one candidate in the circle's
	// outer rectangle. This is a fast-fail operation.
	if circle, ok := target.(*geojson.Circle); ok {
		meters := circle.Meters()
		if meters > 0 {
			center := circle.Center()
			minLat, minLon, maxLat, maxLon :=
				geo.RectFromCenter(center.Y, center.X, meters)
			var exists bool
			c.index.Search(
				[]float64{minLon, minLat},
				[]float64{maxLon, maxLat},
				func(_, _ []float64, itemv interface{}) bool {
					exists = true
					return false
				},
			)
			if !exists {
				// no candidates
				return true
			}
		}
	}
	// do the kNN operation
	alive := true
	center := target.Center()
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	c.index.Nearby(
		[]float64{center.X, center.Y},
		[]float64{center.X, center.Y},
		func(_, _ []float64, itemv interface{}) bool {
			count++
			if count <= offset {
				return true
			}
			if cursor != nil {
				cursor.Step(1)
			}
			item := itemv.(*itemT)
			alive = iter(item.id, item.obj, item.fields)
			return alive
		},
	)
	return alive
}
