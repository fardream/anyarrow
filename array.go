package anyarrow

import (
	"fmt"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
)

type arrowArray struct {
	arrow.Array
}

// Byte provides convenient access to [arrow.Array]'s element as byte
type Byte struct {
	arrowArray

	direct  *array.Uint8
	getFunc func(int) byte
}

var _ arrow.Array = (*Byte)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.Uint8].
func (a *Byte) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as byte
func (a *Byte) Value(i int) byte {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type byte")
	}
}

// NewByte wraps the provided [arrow.Array].
func NewByte(a arrow.Array) (*Byte, error) {
	if direct, ok := a.(*array.Uint8); ok {
		return &Byte{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &Byte{}

	switch v := a.(type) {
	case *array.Int8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Date32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Date64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) byte {
			return byte(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.INT8:
			dictvalues, ok := v.Dictionary().(*array.Int8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT16:
			dictvalues, ok := v.Dictionary().(*array.Int16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT32:
			dictvalues, ok := v.Dictionary().(*array.Int32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT64:
			dictvalues, ok := v.Dictionary().(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT8:
			dictvalues, ok := v.Dictionary().(*array.Uint8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT16:
			dictvalues, ok := v.Dictionary().(*array.Uint16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT32:
			dictvalues, ok := v.Dictionary().(*array.Uint32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT64:
			dictvalues, ok := v.Dictionary().(*array.Uint64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.TIMESTAMP:
			dictvalues, ok := v.Dictionary().(*array.Timestamp)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Timestamp", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DURATION:
			dictvalues, ok := v.Dictionary().(*array.Duration)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Duration", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT32:
			dictvalues, ok := v.Dictionary().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT64:
			dictvalues, ok := v.Dictionary().(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE32:
			dictvalues, ok := v.Dictionary().(*array.Date32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE64:
			dictvalues, ok := v.Dictionary().(*array.Date64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) byte {
				return byte(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for byte", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype byte", a.String())
	}
}

// Int8 provides convenient access to [arrow.Array]'s element as int8
type Int8 struct {
	arrowArray

	direct  *array.Int8
	getFunc func(int) int8
}

var _ arrow.Array = (*Int8)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.Int8].
func (a *Int8) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as int8
func (a *Int8) Value(i int) int8 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type int8")
	}
}

// NewInt8 wraps the provided [arrow.Array].
func NewInt8(a arrow.Array) (*Int8, error) {
	if direct, ok := a.(*array.Int8); ok {
		return &Int8{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &Int8{}

	switch v := a.(type) {
	case *array.Int8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Date32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Date64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int8 {
			return int8(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.INT8:
			dictvalues, ok := v.Dictionary().(*array.Int8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT16:
			dictvalues, ok := v.Dictionary().(*array.Int16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT32:
			dictvalues, ok := v.Dictionary().(*array.Int32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT64:
			dictvalues, ok := v.Dictionary().(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT8:
			dictvalues, ok := v.Dictionary().(*array.Uint8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT16:
			dictvalues, ok := v.Dictionary().(*array.Uint16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT32:
			dictvalues, ok := v.Dictionary().(*array.Uint32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT64:
			dictvalues, ok := v.Dictionary().(*array.Uint64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.TIMESTAMP:
			dictvalues, ok := v.Dictionary().(*array.Timestamp)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Timestamp", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DURATION:
			dictvalues, ok := v.Dictionary().(*array.Duration)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Duration", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT32:
			dictvalues, ok := v.Dictionary().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT64:
			dictvalues, ok := v.Dictionary().(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE32:
			dictvalues, ok := v.Dictionary().(*array.Date32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE64:
			dictvalues, ok := v.Dictionary().(*array.Date64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int8 {
				return int8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for int8", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype int8", a.String())
	}
}

// Int16 provides convenient access to [arrow.Array]'s element as int16
type Int16 struct {
	arrowArray

	direct  *array.Int16
	getFunc func(int) int16
}

var _ arrow.Array = (*Int16)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.Int16].
func (a *Int16) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as int16
func (a *Int16) Value(i int) int16 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type int16")
	}
}

// NewInt16 wraps the provided [arrow.Array].
func NewInt16(a arrow.Array) (*Int16, error) {
	if direct, ok := a.(*array.Int16); ok {
		return &Int16{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &Int16{}

	switch v := a.(type) {
	case *array.Int8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Date32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Date64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int16 {
			return int16(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.INT8:
			dictvalues, ok := v.Dictionary().(*array.Int8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT16:
			dictvalues, ok := v.Dictionary().(*array.Int16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT32:
			dictvalues, ok := v.Dictionary().(*array.Int32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT64:
			dictvalues, ok := v.Dictionary().(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT8:
			dictvalues, ok := v.Dictionary().(*array.Uint8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT16:
			dictvalues, ok := v.Dictionary().(*array.Uint16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT32:
			dictvalues, ok := v.Dictionary().(*array.Uint32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT64:
			dictvalues, ok := v.Dictionary().(*array.Uint64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.TIMESTAMP:
			dictvalues, ok := v.Dictionary().(*array.Timestamp)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Timestamp", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DURATION:
			dictvalues, ok := v.Dictionary().(*array.Duration)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Duration", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT32:
			dictvalues, ok := v.Dictionary().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT64:
			dictvalues, ok := v.Dictionary().(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE32:
			dictvalues, ok := v.Dictionary().(*array.Date32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE64:
			dictvalues, ok := v.Dictionary().(*array.Date64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int16 {
				return int16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for int16", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype int16", a.String())
	}
}

// Int32 provides convenient access to [arrow.Array]'s element as int32
type Int32 struct {
	arrowArray

	direct  *array.Int32
	getFunc func(int) int32
}

var _ arrow.Array = (*Int32)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.Int32].
func (a *Int32) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as int32
func (a *Int32) Value(i int) int32 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type int32")
	}
}

// NewInt32 wraps the provided [arrow.Array].
func NewInt32(a arrow.Array) (*Int32, error) {
	if direct, ok := a.(*array.Int32); ok {
		return &Int32{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &Int32{}

	switch v := a.(type) {
	case *array.Int8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Date32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Date64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int32 {
			return int32(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.INT8:
			dictvalues, ok := v.Dictionary().(*array.Int8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT16:
			dictvalues, ok := v.Dictionary().(*array.Int16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT32:
			dictvalues, ok := v.Dictionary().(*array.Int32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT64:
			dictvalues, ok := v.Dictionary().(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT8:
			dictvalues, ok := v.Dictionary().(*array.Uint8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT16:
			dictvalues, ok := v.Dictionary().(*array.Uint16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT32:
			dictvalues, ok := v.Dictionary().(*array.Uint32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT64:
			dictvalues, ok := v.Dictionary().(*array.Uint64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.TIMESTAMP:
			dictvalues, ok := v.Dictionary().(*array.Timestamp)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Timestamp", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DURATION:
			dictvalues, ok := v.Dictionary().(*array.Duration)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Duration", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT32:
			dictvalues, ok := v.Dictionary().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT64:
			dictvalues, ok := v.Dictionary().(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE32:
			dictvalues, ok := v.Dictionary().(*array.Date32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE64:
			dictvalues, ok := v.Dictionary().(*array.Date64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int32 {
				return int32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for int32", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype int32", a.String())
	}
}

// Int64 provides convenient access to [arrow.Array]'s element as int64
type Int64 struct {
	arrowArray

	direct  *array.Int64
	getFunc func(int) int64
}

var _ arrow.Array = (*Int64)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.Int64].
func (a *Int64) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as int64
func (a *Int64) Value(i int) int64 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type int64")
	}
}

// NewInt64 wraps the provided [arrow.Array].
func NewInt64(a arrow.Array) (*Int64, error) {
	if direct, ok := a.(*array.Int64); ok {
		return &Int64{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &Int64{}

	switch v := a.(type) {
	case *array.Int8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Date32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Date64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) int64 {
			return int64(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.INT8:
			dictvalues, ok := v.Dictionary().(*array.Int8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT16:
			dictvalues, ok := v.Dictionary().(*array.Int16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT32:
			dictvalues, ok := v.Dictionary().(*array.Int32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT64:
			dictvalues, ok := v.Dictionary().(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT8:
			dictvalues, ok := v.Dictionary().(*array.Uint8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT16:
			dictvalues, ok := v.Dictionary().(*array.Uint16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT32:
			dictvalues, ok := v.Dictionary().(*array.Uint32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT64:
			dictvalues, ok := v.Dictionary().(*array.Uint64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.TIMESTAMP:
			dictvalues, ok := v.Dictionary().(*array.Timestamp)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Timestamp", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DURATION:
			dictvalues, ok := v.Dictionary().(*array.Duration)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Duration", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT32:
			dictvalues, ok := v.Dictionary().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT64:
			dictvalues, ok := v.Dictionary().(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE32:
			dictvalues, ok := v.Dictionary().(*array.Date32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE64:
			dictvalues, ok := v.Dictionary().(*array.Date64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) int64 {
				return int64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for int64", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype int64", a.String())
	}
}

// Uint8 provides convenient access to [arrow.Array]'s element as uint8
type Uint8 struct {
	arrowArray

	direct  *array.Uint8
	getFunc func(int) uint8
}

var _ arrow.Array = (*Uint8)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.Uint8].
func (a *Uint8) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as uint8
func (a *Uint8) Value(i int) uint8 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type uint8")
	}
}

// NewUint8 wraps the provided [arrow.Array].
func NewUint8(a arrow.Array) (*Uint8, error) {
	if direct, ok := a.(*array.Uint8); ok {
		return &Uint8{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &Uint8{}

	switch v := a.(type) {
	case *array.Int8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Date32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Date64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint8 {
			return uint8(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.INT8:
			dictvalues, ok := v.Dictionary().(*array.Int8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT16:
			dictvalues, ok := v.Dictionary().(*array.Int16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT32:
			dictvalues, ok := v.Dictionary().(*array.Int32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT64:
			dictvalues, ok := v.Dictionary().(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT8:
			dictvalues, ok := v.Dictionary().(*array.Uint8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT16:
			dictvalues, ok := v.Dictionary().(*array.Uint16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT32:
			dictvalues, ok := v.Dictionary().(*array.Uint32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT64:
			dictvalues, ok := v.Dictionary().(*array.Uint64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.TIMESTAMP:
			dictvalues, ok := v.Dictionary().(*array.Timestamp)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Timestamp", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DURATION:
			dictvalues, ok := v.Dictionary().(*array.Duration)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Duration", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT32:
			dictvalues, ok := v.Dictionary().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT64:
			dictvalues, ok := v.Dictionary().(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE32:
			dictvalues, ok := v.Dictionary().(*array.Date32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE64:
			dictvalues, ok := v.Dictionary().(*array.Date64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint8 {
				return uint8(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for uint8", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype uint8", a.String())
	}
}

// Uint16 provides convenient access to [arrow.Array]'s element as uint16
type Uint16 struct {
	arrowArray

	direct  *array.Uint16
	getFunc func(int) uint16
}

var _ arrow.Array = (*Uint16)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.Uint16].
func (a *Uint16) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as uint16
func (a *Uint16) Value(i int) uint16 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type uint16")
	}
}

// NewUint16 wraps the provided [arrow.Array].
func NewUint16(a arrow.Array) (*Uint16, error) {
	if direct, ok := a.(*array.Uint16); ok {
		return &Uint16{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &Uint16{}

	switch v := a.(type) {
	case *array.Int8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Date32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Date64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint16 {
			return uint16(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.INT8:
			dictvalues, ok := v.Dictionary().(*array.Int8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT16:
			dictvalues, ok := v.Dictionary().(*array.Int16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT32:
			dictvalues, ok := v.Dictionary().(*array.Int32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT64:
			dictvalues, ok := v.Dictionary().(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT8:
			dictvalues, ok := v.Dictionary().(*array.Uint8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT16:
			dictvalues, ok := v.Dictionary().(*array.Uint16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT32:
			dictvalues, ok := v.Dictionary().(*array.Uint32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT64:
			dictvalues, ok := v.Dictionary().(*array.Uint64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.TIMESTAMP:
			dictvalues, ok := v.Dictionary().(*array.Timestamp)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Timestamp", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DURATION:
			dictvalues, ok := v.Dictionary().(*array.Duration)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Duration", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT32:
			dictvalues, ok := v.Dictionary().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT64:
			dictvalues, ok := v.Dictionary().(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE32:
			dictvalues, ok := v.Dictionary().(*array.Date32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE64:
			dictvalues, ok := v.Dictionary().(*array.Date64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint16 {
				return uint16(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for uint16", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype uint16", a.String())
	}
}

// Uint32 provides convenient access to [arrow.Array]'s element as uint32
type Uint32 struct {
	arrowArray

	direct  *array.Uint32
	getFunc func(int) uint32
}

var _ arrow.Array = (*Uint32)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.Uint32].
func (a *Uint32) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as uint32
func (a *Uint32) Value(i int) uint32 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type uint32")
	}
}

// NewUint32 wraps the provided [arrow.Array].
func NewUint32(a arrow.Array) (*Uint32, error) {
	if direct, ok := a.(*array.Uint32); ok {
		return &Uint32{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &Uint32{}

	switch v := a.(type) {
	case *array.Int8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Date32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Date64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint32 {
			return uint32(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.INT8:
			dictvalues, ok := v.Dictionary().(*array.Int8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT16:
			dictvalues, ok := v.Dictionary().(*array.Int16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT32:
			dictvalues, ok := v.Dictionary().(*array.Int32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT64:
			dictvalues, ok := v.Dictionary().(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT8:
			dictvalues, ok := v.Dictionary().(*array.Uint8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT16:
			dictvalues, ok := v.Dictionary().(*array.Uint16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT32:
			dictvalues, ok := v.Dictionary().(*array.Uint32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT64:
			dictvalues, ok := v.Dictionary().(*array.Uint64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.TIMESTAMP:
			dictvalues, ok := v.Dictionary().(*array.Timestamp)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Timestamp", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DURATION:
			dictvalues, ok := v.Dictionary().(*array.Duration)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Duration", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT32:
			dictvalues, ok := v.Dictionary().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT64:
			dictvalues, ok := v.Dictionary().(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE32:
			dictvalues, ok := v.Dictionary().(*array.Date32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE64:
			dictvalues, ok := v.Dictionary().(*array.Date64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint32 {
				return uint32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for uint32", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype uint32", a.String())
	}
}

// Uint64 provides convenient access to [arrow.Array]'s element as uint64
type Uint64 struct {
	arrowArray

	direct  *array.Uint64
	getFunc func(int) uint64
}

var _ arrow.Array = (*Uint64)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.Uint64].
func (a *Uint64) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as uint64
func (a *Uint64) Value(i int) uint64 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type uint64")
	}
}

// NewUint64 wraps the provided [arrow.Array].
func NewUint64(a arrow.Array) (*Uint64, error) {
	if direct, ok := a.(*array.Uint64); ok {
		return &Uint64{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &Uint64{}

	switch v := a.(type) {
	case *array.Int8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Date32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Date64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) uint64 {
			return uint64(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.INT8:
			dictvalues, ok := v.Dictionary().(*array.Int8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT16:
			dictvalues, ok := v.Dictionary().(*array.Int16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT32:
			dictvalues, ok := v.Dictionary().(*array.Int32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT64:
			dictvalues, ok := v.Dictionary().(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT8:
			dictvalues, ok := v.Dictionary().(*array.Uint8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT16:
			dictvalues, ok := v.Dictionary().(*array.Uint16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT32:
			dictvalues, ok := v.Dictionary().(*array.Uint32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT64:
			dictvalues, ok := v.Dictionary().(*array.Uint64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.TIMESTAMP:
			dictvalues, ok := v.Dictionary().(*array.Timestamp)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Timestamp", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DURATION:
			dictvalues, ok := v.Dictionary().(*array.Duration)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Duration", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT32:
			dictvalues, ok := v.Dictionary().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT64:
			dictvalues, ok := v.Dictionary().(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE32:
			dictvalues, ok := v.Dictionary().(*array.Date32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE64:
			dictvalues, ok := v.Dictionary().(*array.Date64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) uint64 {
				return uint64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for uint64", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype uint64", a.String())
	}
}

// Float32 provides convenient access to [arrow.Array]'s element as float32
type Float32 struct {
	arrowArray

	direct  *array.Float32
	getFunc func(int) float32
}

var _ arrow.Array = (*Float32)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.Float32].
func (a *Float32) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as float32
func (a *Float32) Value(i int) float32 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type float32")
	}
}

// NewFloat32 wraps the provided [arrow.Array].
func NewFloat32(a arrow.Array) (*Float32, error) {
	if direct, ok := a.(*array.Float32); ok {
		return &Float32{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &Float32{}

	switch v := a.(type) {
	case *array.Int8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Date32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Date64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float32 {
			return float32(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.INT8:
			dictvalues, ok := v.Dictionary().(*array.Int8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT16:
			dictvalues, ok := v.Dictionary().(*array.Int16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT32:
			dictvalues, ok := v.Dictionary().(*array.Int32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT64:
			dictvalues, ok := v.Dictionary().(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT8:
			dictvalues, ok := v.Dictionary().(*array.Uint8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT16:
			dictvalues, ok := v.Dictionary().(*array.Uint16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT32:
			dictvalues, ok := v.Dictionary().(*array.Uint32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT64:
			dictvalues, ok := v.Dictionary().(*array.Uint64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.TIMESTAMP:
			dictvalues, ok := v.Dictionary().(*array.Timestamp)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Timestamp", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DURATION:
			dictvalues, ok := v.Dictionary().(*array.Duration)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Duration", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT32:
			dictvalues, ok := v.Dictionary().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT64:
			dictvalues, ok := v.Dictionary().(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE32:
			dictvalues, ok := v.Dictionary().(*array.Date32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE64:
			dictvalues, ok := v.Dictionary().(*array.Date64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float32 {
				return float32(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for float32", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype float32", a.String())
	}
}

// Float64 provides convenient access to [arrow.Array]'s element as float64
type Float64 struct {
	arrowArray

	direct  *array.Float64
	getFunc func(int) float64
}

var _ arrow.Array = (*Float64)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.Float64].
func (a *Float64) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as float64
func (a *Float64) Value(i int) float64 {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type float64")
	}
}

// NewFloat64 wraps the provided [arrow.Array].
func NewFloat64(a arrow.Array) (*Float64, error) {
	if direct, ok := a.(*array.Float64); ok {
		return &Float64{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &Float64{}

	switch v := a.(type) {
	case *array.Int8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Int16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Int32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Int64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Uint8:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Uint16:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Uint32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Uint64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Timestamp:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Duration:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Float32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Float64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Date32:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Date64:
		r.arrowArray.Array = a
		r.getFunc = func(i int) float64 {
			return float64(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.INT8:
			dictvalues, ok := v.Dictionary().(*array.Int8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT16:
			dictvalues, ok := v.Dictionary().(*array.Int16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT32:
			dictvalues, ok := v.Dictionary().(*array.Int32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.INT64:
			dictvalues, ok := v.Dictionary().(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Int64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT8:
			dictvalues, ok := v.Dictionary().(*array.Uint8)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint8", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT16:
			dictvalues, ok := v.Dictionary().(*array.Uint16)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint16", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT32:
			dictvalues, ok := v.Dictionary().(*array.Uint32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.UINT64:
			dictvalues, ok := v.Dictionary().(*array.Uint64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Uint64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.TIMESTAMP:
			dictvalues, ok := v.Dictionary().(*array.Timestamp)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Timestamp", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DURATION:
			dictvalues, ok := v.Dictionary().(*array.Duration)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Duration", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT32:
			dictvalues, ok := v.Dictionary().(*array.Float32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.FLOAT64:
			dictvalues, ok := v.Dictionary().(*array.Float64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Float64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE32:
			dictvalues, ok := v.Dictionary().(*array.Date32)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date32", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.DATE64:
			dictvalues, ok := v.Dictionary().(*array.Date64)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Date64", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) float64 {
				return float64(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for float64", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype float64", a.String())
	}
}

// String provides convenient access to [arrow.Array]'s element as string
type String struct {
	arrowArray

	direct  *array.String
	getFunc func(int) string
}

var _ arrow.Array = (*String)(nil)

// IsDirect indicates if the underlying [arrow.Array] is an [array.String].
func (a *String) IsDirect() bool {
	return a.direct != nil
}

// Value retrieves the element at index i as string
func (a *String) Value(i int) string {
	if a.direct != nil {
		return a.direct.Value(i)
	} else if a.getFunc != nil {
		return a.getFunc(i)
	} else {
		panic("uninitialized accessor for go type string")
	}
}

// NewString wraps the provided [arrow.Array].
func NewString(a arrow.Array) (*String, error) {
	if direct, ok := a.(*array.String); ok {
		return &String{direct: direct, arrowArray: arrowArray{Array: a}}, nil
	}

	r := &String{}

	switch v := a.(type) {
	case *array.String:
		r.arrowArray.Array = a
		r.getFunc = func(i int) string {
			return string(v.Value(i))
		}
		return r, nil

	case *array.Binary:
		r.arrowArray.Array = a
		r.getFunc = func(i int) string {
			return string(v.Value(i))
		}
		return r, nil

	case *array.LargeString:
		r.arrowArray.Array = a
		r.getFunc = func(i int) string {
			return string(v.Value(i))
		}
		return r, nil

	case *array.LargeBinary:
		r.arrowArray.Array = a
		r.getFunc = func(i int) string {
			return string(v.Value(i))
		}
		return r, nil

	case *array.Dictionary:
		dt, ok := v.DataType().(*arrow.DictionaryType)
		if !ok {
			return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
		}
		switch dt.ValueType.ID() {
		case arrow.STRING:
			dictvalues, ok := v.Dictionary().(*array.String)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type String", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) string {
				return string(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.BINARY:
			dictvalues, ok := v.Dictionary().(*array.Binary)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type Binary", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) string {
				return string(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.LARGE_STRING:
			dictvalues, ok := v.Dictionary().(*array.LargeString)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type LargeString", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) string {
				return string(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		case arrow.LARGE_BINARY:
			dictvalues, ok := v.Dictionary().(*array.LargeBinary)
			if !ok {
				return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type LargeBinary", v.Dictionary().DataType().String())
			}

			r.arrowArray.Array = a
			r.getFunc = func(i int) string {
				return string(dictvalues.Value(v.GetValueIndex(i)))
			}

			return r, nil

		default:
			return nil, fmt.Errorf("cannot use %s dictionary for string", dt.ValueType.String())
		}

	default:
		return nil, fmt.Errorf("cannot use %s for gotype string", a.String())
	}
}
