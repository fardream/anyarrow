package anyarrow

import (
	"fmt"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
)

type Byte struct {
	direct *array.Uint8
	// Embed the array
	arrow.Array
	getFunc func(int) byte
}

func (a *Byte) IsDirect() bool {
	return a.direct != nil
}

func (a *Byte) Value(i int) byte {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type byte")
	}
}

func NewByte(a arrow.Array) (*Byte, error) {
	if direct, ok := a.(*array.Uint8); ok {
		return &Byte{direct: direct, Array: a}, nil
	}

	r := &Byte{}

	switch v := a.(type) {
	case *array.Int8:
		r.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	default:
		return nil, fmt.Errorf("cannot use %s for gotype byte", a.String())
	}
}

type Int8 struct {
	direct *array.Int8
	// Embed the array
	arrow.Array
	getFunc func(int) int8
}

func (a *Int8) IsDirect() bool {
	return a.direct != nil
}

func (a *Int8) Value(i int) int8 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type int8")
	}
}

func NewInt8(a arrow.Array) (*Int8, error) {
	if direct, ok := a.(*array.Int8); ok {
		return &Int8{direct: direct, Array: a}, nil
	}

	r := &Int8{}

	switch v := a.(type) {
	case *array.Int16:
		r.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	default:
		return nil, fmt.Errorf("cannot use %s for gotype int8", a.String())
	}
}

type Int16 struct {
	direct *array.Int16
	// Embed the array
	arrow.Array
	getFunc func(int) int16
}

func (a *Int16) IsDirect() bool {
	return a.direct != nil
}

func (a *Int16) Value(i int) int16 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type int16")
	}
}

func NewInt16(a arrow.Array) (*Int16, error) {
	if direct, ok := a.(*array.Int16); ok {
		return &Int16{direct: direct, Array: a}, nil
	}

	r := &Int16{}

	switch v := a.(type) {
	case *array.Int8:
		r.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	default:
		return nil, fmt.Errorf("cannot use %s for gotype int16", a.String())
	}
}

type Int32 struct {
	direct *array.Int32
	// Embed the array
	arrow.Array
	getFunc func(int) int32
}

func (a *Int32) IsDirect() bool {
	return a.direct != nil
}

func (a *Int32) Value(i int) int32 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type int32")
	}
}

func NewInt32(a arrow.Array) (*Int32, error) {
	if direct, ok := a.(*array.Int32); ok {
		return &Int32{direct: direct, Array: a}, nil
	}

	r := &Int32{}

	switch v := a.(type) {
	case *array.Int8:
		r.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	default:
		return nil, fmt.Errorf("cannot use %s for gotype int32", a.String())
	}
}

type Int64 struct {
	direct *array.Int64
	// Embed the array
	arrow.Array
	getFunc func(int) int64
}

func (a *Int64) IsDirect() bool {
	return a.direct != nil
}

func (a *Int64) Value(i int) int64 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type int64")
	}
}

func NewInt64(a arrow.Array) (*Int64, error) {
	if direct, ok := a.(*array.Int64); ok {
		return &Int64{direct: direct, Array: a}, nil
	}

	r := &Int64{}

	switch v := a.(type) {
	case *array.Int8:
		r.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	default:
		return nil, fmt.Errorf("cannot use %s for gotype int64", a.String())
	}
}

type Uint8 struct {
	direct *array.Uint8
	// Embed the array
	arrow.Array
	getFunc func(int) uint8
}

func (a *Uint8) IsDirect() bool {
	return a.direct != nil
}

func (a *Uint8) Value(i int) uint8 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type uint8")
	}
}

func NewUint8(a arrow.Array) (*Uint8, error) {
	if direct, ok := a.(*array.Uint8); ok {
		return &Uint8{direct: direct, Array: a}, nil
	}

	r := &Uint8{}

	switch v := a.(type) {
	case *array.Int8:
		r.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	default:
		return nil, fmt.Errorf("cannot use %s for gotype uint8", a.String())
	}
}

type Uint16 struct {
	direct *array.Uint16
	// Embed the array
	arrow.Array
	getFunc func(int) uint16
}

func (a *Uint16) IsDirect() bool {
	return a.direct != nil
}

func (a *Uint16) Value(i int) uint16 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type uint16")
	}
}

func NewUint16(a arrow.Array) (*Uint16, error) {
	if direct, ok := a.(*array.Uint16); ok {
		return &Uint16{direct: direct, Array: a}, nil
	}

	r := &Uint16{}

	switch v := a.(type) {
	case *array.Int8:
		r.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	default:
		return nil, fmt.Errorf("cannot use %s for gotype uint16", a.String())
	}
}

type Uint32 struct {
	direct *array.Uint32
	// Embed the array
	arrow.Array
	getFunc func(int) uint32
}

func (a *Uint32) IsDirect() bool {
	return a.direct != nil
}

func (a *Uint32) Value(i int) uint32 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type uint32")
	}
}

func NewUint32(a arrow.Array) (*Uint32, error) {
	if direct, ok := a.(*array.Uint32); ok {
		return &Uint32{direct: direct, Array: a}, nil
	}

	r := &Uint32{}

	switch v := a.(type) {
	case *array.Int8:
		r.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	default:
		return nil, fmt.Errorf("cannot use %s for gotype uint32", a.String())
	}
}

type Uint64 struct {
	direct *array.Uint64
	// Embed the array
	arrow.Array
	getFunc func(int) uint64
}

func (a *Uint64) IsDirect() bool {
	return a.direct != nil
}

func (a *Uint64) Value(i int) uint64 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type uint64")
	}
}

func NewUint64(a arrow.Array) (*Uint64, error) {
	if direct, ok := a.(*array.Uint64); ok {
		return &Uint64{direct: direct, Array: a}, nil
	}

	r := &Uint64{}

	switch v := a.(type) {
	case *array.Int8:
		r.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	default:
		return nil, fmt.Errorf("cannot use %s for gotype uint64", a.String())
	}
}

type Float32 struct {
	direct *array.Float32
	// Embed the array
	arrow.Array
	getFunc func(int) float32
}

func (a *Float32) IsDirect() bool {
	return a.direct != nil
}

func (a *Float32) Value(i int) float32 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type float32")
	}
}

func NewFloat32(a arrow.Array) (*Float32, error) {
	if direct, ok := a.(*array.Float32); ok {
		return &Float32{direct: direct, Array: a}, nil
	}

	r := &Float32{}

	switch v := a.(type) {
	case *array.Int8:
		r.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	default:
		return nil, fmt.Errorf("cannot use %s for gotype float32", a.String())
	}
}

type Float64 struct {
	direct *array.Float64
	// Embed the array
	arrow.Array
	getFunc func(int) float64
}

func (a *Float64) IsDirect() bool {
	return a.direct != nil
}

func (a *Float64) Value(i int) float64 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type float64")
	}
}

func NewFloat64(a arrow.Array) (*Float64, error) {
	if direct, ok := a.(*array.Float64); ok {
		return &Float64{direct: direct, Array: a}, nil
	}

	r := &Float64{}

	switch v := a.(type) {
	case *array.Int8:
		r.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	default:
		return nil, fmt.Errorf("cannot use %s for gotype float64", a.String())
	}
}
