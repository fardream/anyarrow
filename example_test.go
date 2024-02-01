package anyarrow_test

import (
	"fmt"

	"github.com/fardream/anyarrow"

	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

func Example() {
	mem := memory.NewGoAllocator()
	ab := array.NewFloat32Builder(mem)
	defer ab.Release()

	for i := 0; i < 5; i++ {
		ab.Append(float32(i))
	}

	a := ab.NewArray()
	defer a.Release()

	u64, err := anyarrow.NewUint64(a)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 5; i++ {
		fmt.Println(u64.Value(i))
	}

	// Output: 0
	// 1
	// 2
	// 3
	// 4
}
