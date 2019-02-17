package item

import (
	"math/rand"
	"testing"
	"time"
)

func TestPacked(t *testing.T) {
	start := time.Now()
	for time.Since(start) < time.Second/2 {
		testPacked(t)
	}
}
func testPacked(t *testing.T) {
	n := rand.Int() % 1024
	if n%2 == 1 {
		n++
	}
	values := make([]float64, n)
	for i := 0; i < len(values); i++ {
		switch rand.Int() % 9 {
		case 0:
			values[i] = 0
		case 1:
			values[i] = float64((rand.Int() % 32) - 32/2)
		case 2:
			values[i] = float64((rand.Int() % 128))
		case 3:
			values[i] = float64((rand.Int() % 8191) - 8191/2)
		case 4:
			values[i] = float64((rand.Int() % 2097152) - 2097152/2)
		case 5:
			values[i] = float64((rand.Int() % 536870912) - 536870912/2)
		case 6:
			values[i] = float64(rand.Int() % 500)
			switch rand.Int() % 4 {
			case 1:
				values[i] = 0.25
			case 2:
				values[i] = 0.50
			case 3:
				values[i] = 0.75
			}
		case 7:
			values[i] = float64(rand.Float32())
		case 8:
			values[i] = rand.Float64()
		}
	}
	var dst []byte
	for i := 0; i < len(values); i++ {
		dst = appendPacked(dst, values[i])
	}
	data := dst
	var pvalues []float64
	for {
		var value float64
		data, value = readPacked(data)
		if data == nil {
			break
		}
		pvalues = append(pvalues, value)
	}
	if !floatsEquals(values, pvalues) {
		if len(values) != len(pvalues) {
			t.Fatalf("sizes not equal")
		}
		for i := 0; i < len(values); i++ {
			if values[i] != pvalues[i] {
				t.Fatalf("expected '%v', got '%v'", values[i], pvalues[i])
			}
		}
	}
	data = dst
	var read int

	data, read = skipPacked(data, len(values)/2)
	if read != len(values)/2 {
		t.Fatalf("expected '%v', got '%v'", len(values)/2, read)
	}
	data, read = skipPacked(data, len(values)/2)
	if read != len(values)/2 {
		t.Fatalf("expected '%v', got '%v'", len(values)/2, read)
	}
	if len(data) != 0 {
		t.Fatalf("expected '%v', got '%v'", 0, len(data))
	}

}

// func TestPackedItem(t *testing.T) {
// 	item := New("hello", nil, true)
// 	values := []float64{0, 1, 1, 0, 0, 1, 1, 0, 1} //, 1} //, 1, 0, 1, 0, 0, 1}
// 	fmt.Println(values)
// 	for i := 0; i < len(values); i++ {
// 		item.SetField(i, values[i])
// 	}
// 	fmt.Print("[")
// 	for j := 0; j < len(values); j++ {
// 		if j > 0 {
// 			print(" ")
// 		}
// 		fmt.Print(item.GetField(j))
// 	}
// 	print("]")
// 	println(item.ID())

// 	// for i := 0; i < len(values); i++ {

// 	// 	fmt.Println(values[i], item.GetField(i))
// 	// }

// 	// fmt.Println(item.GetField(0))
// 	// println(">>", item.ID())

// }
