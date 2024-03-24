package anyarrow_test

import (
	"fmt"

	"github.com/fardream/anyarrow"

	"github.com/apache/arrow/go/v15/arrow"
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

	f32, err := anyarrow.NewFloat32(a)
	if !f32.IsDirect() {
		panic("f32 is not direct")
	}

	for i := 0; i < 5; i++ {
		fmt.Println(f32.Value(i))
	}

	// Output: 0
	// 1
	// 2
	// 3
	// 4
	// 0
	// 1
	// 2
	// 3
	// 4
}

func Example_dictionary() {
	mem := memory.NewGoAllocator()

	dicttype := arrow.DictionaryType{
		ValueType: &arrow.Int64Type{},
		IndexType: &arrow.Int8Type{},
	}

	ab := array.NewDictionaryBuilder(mem, &dicttype)
	defer ab.Release()

	abb, ok := ab.(*array.Int64DictionaryBuilder)
	if !ok {
		panic("not correct dictionary builder type")
	}

	abb.Append(1024)
	abb.Append(1023)
	abb.Append(1024)
	abb.Append(1024)
	abb.Append(1023)

	dictarray := abb.NewArray()
	defer dictarray.Release()

	int64array, err := anyarrow.NewInt64(dictarray)
	if err != nil {
		panic(err)
	}

	fmt.Printf("len: %d\n", int64array.Len())
	for i := 0; i < 5; i++ {
		fmt.Println(int64array.Value(i))
	}

	dict, ok := dictarray.(*array.Dictionary)
	if !ok {
		panic("not a dictionary")
	}

	fmt.Printf("dict len: %d", dict.Dictionary().Len())

	// Output: len: 5
	// 1024
	// 1023
	// 1024
	// 1024
	// 1023
	// dict len: 2
}

func Example_string() {
	mem := memory.NewGoAllocator()

	dicttype := arrow.DictionaryType{
		ValueType: &arrow.StringType{},
		IndexType: &arrow.Int8Type{},
	}

	ab := array.NewDictionaryBuilder(mem, &dicttype)
	defer ab.Release()

	abb, ok := ab.(*array.BinaryDictionaryBuilder)
	if !ok {
		panic("not correct dictionary builder type")
	}

	abb.AppendString("abc")
	abb.AppendString("def")
	abb.AppendString("abc")
	abb.AppendString("abc")
	abb.AppendString("def")

	dictarray := abb.NewArray()
	defer dictarray.Release()

	stringarray, err := anyarrow.NewString(dictarray)
	if err != nil {
		panic(err)
	}

	fmt.Printf("len: %d\n", stringarray.Len())
	for i := 0; i < 5; i++ {
		fmt.Println(stringarray.Value(i))
	}

	dict, ok := dictarray.(*array.Dictionary)
	if !ok {
		panic("not a dictionary")
	}

	fmt.Printf("dict len: %d", dict.Dictionary().Len())

	// Output: len: 5
	// abc
	// def
	// abc
	// abc
	// def
	// dict len: 2
}
