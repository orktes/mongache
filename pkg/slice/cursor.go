package slice

import (
	"context"
	"fmt"
	"io"
	"reflect"
)

// Cursor implement a cursor for an arbitary slice type
type Cursor struct {
	slice  reflect.Value
	offset int32
	length int32
}

// NewCursor returns a new cursor
func NewCursor(slice interface{}) (*Cursor, error) {
	val := reflect.ValueOf(slice)

	switch val.Kind() {
	case reflect.Slice:
	case reflect.Array:
	default:
		return nil, fmt.Errorf("%T is not a slice or array")
	}

	return &Cursor{slice: val, length: int32(val.Len())}, nil
}

func (cur *Cursor) Next(ctx context.Context) (interface{}, error) {
	if cur.offset == cur.length {
		return nil, io.EOF
	}

	v := cur.slice.Index(int(cur.offset))
	cur.offset++
	return v.Interface(), nil
}

func (cur *Cursor) Skip(ctx context.Context, n int32) error {
	cur.offset += n
	if cur.offset > cur.length {
		cur.offset = cur.length
	}
	return nil
}

func (cur *Cursor) Close(ctx context.Context) error {
	return nil
}

func (cur *Cursor) Position(ctx context.Context) (int32, error) {
	return int32(cur.offset), nil
}
