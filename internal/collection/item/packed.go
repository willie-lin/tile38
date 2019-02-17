package item

import (
	"fmt"
	"unsafe"

	"github.com/h2so5/half"
)

// kind		bits	bytes	values			min				max
// --------------------------------------------------------------------
// 0		5		1		32				-16				15
// 1		13		2		16384			-4095			4095
// 2		21		3		2097152			-1048576		1048575
// 3		29		4		536870912		-268435456		268435455
// 4		16		3		-- standard 16-bit floating point --
// 5		32		5		-- standard 32-bit floating point --
// 6		64		9		-- standard 64-bit floating point --

const maxFieldBytes = 9

const (
	maxInt5  = 15
	maxInt13 = 4095
	maxInt21 = 1048575
	maxInt29 = 268435455
)

func appendPacked(dst []byte, f64 float64) []byte {
	if f64 == 0 {
		return append(dst, 0)
	}
	i64 := int64(f64)
	if f64 == float64(i64) {
		// whole number
		var signed byte
		if i64 < 0 {
			i64 *= -1
			signed = 16
		}
		if i64 <= maxInt5 {
			return append(dst, 0<<5|signed|
				byte(i64))
		}
		if i64 <= maxInt13 {
			return append(dst, 1<<5|signed|
				byte(i64>>8), byte(i64))
		}
		if i64 <= maxInt21 {
			return append(dst, 2<<5|signed|
				byte(i64>>16), byte(i64>>8), byte(i64))
		}
		if i64 <= maxInt29 {
			return append(dst, 3<<5|signed|
				byte(i64>>24), byte(i64>>16), byte(i64>>8), byte(i64))
		}
		// fallthrough
	}
	f32 := float32(f64)
	if f64 == float64(f32) {
		f16 := half.NewFloat16(f32)
		if f32 == f16.Float32() {
			dst = append(dst, 4<<5, 0, 0)
			*(*half.Float16)(unsafe.Pointer(&dst[len(dst)-2])) = f16
			return dst
		}
		dst = append(dst, 5<<5, 0, 0, 0, 0)
		*(*float32)(unsafe.Pointer(&dst[len(dst)-4])) = f32
		return dst
	}
	dst = append(dst, 6<<5, 0, 0, 0, 0, 0, 0, 0, 0)
	*(*float64)(unsafe.Pointer(&dst[len(dst)-8])) = f64
	return dst
}

func skipPacked(data []byte, count int) (out []byte, read int) {
	var i int
	for i < len(data) {
		if read >= count {
			return data[i:], read
		}
		kind := data[i] >> 5
		if kind < 4 {
			i += int(kind) + 1
		} else if kind == 4 {
			i += 3
		} else if kind == 5 {
			i += 5
		} else {
			i += 9
		}
		read++
	}
	return nil, read
}

func readPacked(data []byte) ([]byte, float64) {
	if len(data) == 0 {
		return nil, 0
	}
	if data[0] == 0 {
		return data[1:], 0
	}
	kind := data[0] >> 5
	switch kind {
	case 0, 1, 2, 3:
		// whole number
		var value float64
		if kind == 0 {
			value = float64(
				uint32(data[0] & 0xF),
			)
		} else if kind == 1 {
			value = float64(
				uint32(data[0]&0xF)<<8 | uint32(data[1]),
			)
		} else if kind == 2 {
			value = float64(
				uint32(data[0]&0xF)<<16 | uint32(data[1])<<8 |
					uint32(data[2]),
			)
		} else {
			value = float64(
				uint32(data[0]&0xF)<<24 | uint32(data[1])<<16 |
					uint32(data[2])<<8 | uint32(data[3]),
			)
		}
		if data[0]&0x10 != 0 {
			value *= -1
		}
		return data[kind+1:], value
	case 4:
		// 16-bit float
		return data[3:],
			float64((*half.Float16)(unsafe.Pointer(&data[1])).Float32())
	case 5:
		// 32-bit float
		return data[5:],
			float64(*(*float32)(unsafe.Pointer(&data[1])))
	case 6:
		// 64-bit float
		return data[9:], *(*float64)(unsafe.Pointer(&data[1]))
	}
	panic("invalid data")
}

func (item *Item) packedGenerateFieldBytes(values []float64) []byte {
	var dst []byte
	for i := 0; i < len(values); i++ {
		dst = appendPacked(dst, values[i])
	}
	return dst
}

func (item *Item) packedSetField(index int, value float64) (updated bool) {
	if false {
		func() {
			data := item.fieldsBytes()
			fmt.Printf("%v >> [%x]", value, data)
			defer func() {
				data := item.fieldsBytes()
				fmt.Printf(" >> [%x]\n", data)
			}()
		}()
	}
	/////////////////////////////////////////////////////////////////

	// original field bytes
	headBytes := item.fieldsBytes()

	// quickly skip over head fields.
	// returns the start of the field at index, and the number of valid
	// fields that were read.
	fieldBytes, read := skipPacked(headBytes, index)

	// number of empty/blank bytes that need to be added between the
	// head bytes and the new field bytes.
	var blankSpace int

	// data a that follows the new field bytes
	var tailBytes []byte

	if len(fieldBytes) == 0 {
		// field at index was not found.
		if value == 0 {
			// zero value is the default, so we can assume that the fields was
			// not updated.
			return false
		}
		// set the blank space
		blankSpace = index - read
		fieldBytes = nil
	} else {
		// field at index was found.

		// truncate the head bytes to reflect only the bytes up to
		// the current field.
		headBytes = headBytes[:len(headBytes)-len(fieldBytes)]

		// read the current value and get the tail data following the
		// current field.
		var cvalue float64
		tailBytes, cvalue = readPacked(fieldBytes)
		if cvalue == value {
			// no change to value
			return false
		}

		// truncate the field bytes to exactly match current field.
		fieldBytes = fieldBytes[:len(fieldBytes)-len(tailBytes)]
	}

	// create the new field bytes
	{
		var buf [maxFieldBytes]byte
		newFieldBytes := appendPacked(buf[:0], value)
		if len(newFieldBytes) == len(fieldBytes) {
			// no change in data size, update in place
			copy(fieldBytes, newFieldBytes)
			return true
		}
		// reassign the field bytes
		fieldBytes = newFieldBytes
	}

	// hang on to the item id
	id := item.ID()

	// create a new byte slice
	// head+blank+field+tail+id
	nbytes := make([]byte,
		len(headBytes)+blankSpace+len(fieldBytes)+len(tailBytes)+len(id))

	// fill the data
	copy(nbytes, headBytes)
	copy(nbytes[len(headBytes)+blankSpace:], fieldBytes)
	copy(nbytes[len(headBytes)+blankSpace+len(fieldBytes):], tailBytes)
	copy(nbytes[len(headBytes)+blankSpace+len(fieldBytes)+len(tailBytes):], id)

	// update the field size
	item.setFieldsDataSize(len(nbytes) - len(id))

	// update the data pointer
	item.data = unsafe.Pointer(&nbytes[0])

	return true
}

func (item *Item) packedForEachField(count int, iter func(value float64) bool) {
	data := item.fieldsBytes()
	if count < 0 {
		// iterate over of the known the values
		for len(data) > 0 {
			var value float64
			data, value = readPacked(data)
			if !iter(value) {
				return
			}
		}
	} else {
		for i := 0; i < count; i++ {
			var value float64
			data, value = readPacked(data)
			if !iter(value) {
				return
			}
		}
	}
}

func (item *Item) packedGetField(index int) float64 {

	var idx int
	var fvalue float64
	item.packedForEachField(-1, func(value float64) bool {
		if idx == index {
			fvalue = value
			return false
		}
		idx++
		return true
	})
	return fvalue
}
