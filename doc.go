// Package anyarrow provides a convenient accessor to arrow arrays.
//
// instead of figuring out the type, the user
// can simply create a new [Int8] by using [NewInt8] with [arrow.Array],
// which implements the [arrow.Array] interface and have a [Value] function
// that returns an int8 by performing the proper cast.
//
// arrow's dictionary, which is categorical data, is also supported.
package anyarrow

//go:generate go run ./cmd/gen
