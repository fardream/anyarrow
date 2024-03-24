package anyarrow

import (
    "fmt"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
)

{{range .}}type {{.GoName}} struct {
	direct *array.{{.ArrowType}}
    // Embed the array
	arrow.Array
    getFunc func(int) {{.GoType}}
}

func (a *{{.GoName}}) IsDirect() bool {
    return a.direct != nil
}

func (a *{{.GoName}}) Value(i int) {{.GoType}} {
    if a.direct != nil {
        return a.direct.Value(i)
    } else if a.getFunc != nil {
        return a.getFunc(i)
    } else {
        panic("uninitialized accessor for go type {{.GoType}}")
    }
}

func New{{.GoName}}(a arrow.Array) (*{{.GoName}}, error) {
    if direct, ok := a.(*array.{{.ArrowType}}); ok {
        return &{{.GoName}}{direct: direct, Array: a}, nil
    }

    r := &{{.GoName}}{}
    {{ $gotype := .GoType}}
    switch v:= a.(type) {
{{range .ArrowTypes}}case *array.{{.Array}}:
        r.Array = a
        r.getFunc =  func(i int) {{$gotype}} {
                return {{$gotype}}(v.Value(i))
        }
        return r, nil

{{end -}}
    case *array.Dictionary:
        dt, ok := v.DataType().(*arrow.DictionaryType)
        if !ok {
            return nil, fmt.Errorf("arrow dictionary's datatype is not dictionary")
        }
        switch dt.ValueType.ID() {
{{range .ArrowTypes}}case arrow.{{.ID}}:
       		dictvalues, ok := v.Dictionary().(*array.{{.Array}})
       		if !ok {
       		    return nil, fmt.Errorf("cannot convert arrow dictionary's dictionary %s to type {{.Array}}", v.Dictionary().DataType().String())
       		}
       		r.Array = a
       		r.getFunc = func(i int) {{$gotype}} {
       		    return {{$gotype}}(dictvalues.Value(v.GetValueIndex(i)))
       		}

       		return r, nil
{{end -}}
        default:
            return nil, fmt.Errorf("cannot use %s dictionary for {{$gotype}}", dt.ValueType.String())
        }
    default:
        return nil, fmt.Errorf("cannot use %s for gotype {{$gotype}}", a.String())
    }
}
{{end}}
