package main

import (
	"bytes"
	_ "embed"
	"os"
	"text/template"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"mvdan.cc/gofumpt/format"
)

func orpanic(err error) {
	if err != nil {
		panic(err)
	}
}

func must[T any](a T, err error) T {
	orpanic(err)

	return a
}

//go:embed any.go.tpl
var anyGoTplStr string

type pair struct {
	gotype    string
	arrowtype string
}

type ArrowType struct {
	Array string
	ID    string
}
type genValue struct {
	t          pair
	ArrowTypes []ArrowType
}

var titleCaser = cases.Title(language.AmericanEnglish)

func (p genValue) GoName() string {
	return titleCaser.String(p.t.gotype)
}

func (p genValue) GoType() string {
	return p.t.gotype
}

func (p genValue) ArrowType() string {
	return p.t.arrowtype
}

func main() {
	allArrowTypes := []string{
		"Int8", "Int16", "Int32", "Int64",
		"Uint8", "Uint16", "Uint32", "Uint64",
		"Timestamp", "Duration",
		"Float32", "Float64", "Date32", "Date64",
	}

	allpairs := []pair{
		{"byte", "Uint8"},
		{"int8", "Int8"},
		{"int16", "Int16"},
		{"int32", "Int32"},
		{"int64", "Int64"},
		{"uint8", "Uint8"},
		{"uint16", "Uint16"},
		{"uint32", "Uint32"},
		{"uint64", "Uint64"},
		{"float32", "Float32"},
		{"float64", "Float64"},
	}

	tmpl := must(template.New("any_go").Parse(anyGoTplStr))

	var b bytes.Buffer

	genvalues := []genValue{}

	upperCaser := cases.Upper(language.AmericanEnglish)
	for _, p := range allpairs {
		v := genValue{
			t: p,
		}
		for _, a := range allArrowTypes {
			v.ArrowTypes = append(v.ArrowTypes, ArrowType{
				Array: a,
				ID:    upperCaser.String(a),
			})
		}
		genvalues = append(genvalues, v)
	}

	genvalues = append(genvalues, genValue{
		t: pair{"string", "String"},
		ArrowTypes: []ArrowType{
			{Array: "String", ID: "STRING"},
			{Array: "Binary", ID: "BINARY"},
			{Array: "LargeString", ID: "LARGE_STRING"},
			{Array: "LargeBinary", ID: "LARGE_BINARY"},
		},
	})
	orpanic(tmpl.Execute(&b, genvalues))

	orpanic(os.WriteFile("array.go", must(format.Source(b.Bytes(), format.Options{
		LangVersion: "v1.21.6",
		ModulePath:  "github.com/fardream/anyarrow",
	})), 0o660))
}
