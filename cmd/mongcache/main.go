package main

import (
	"context"
	"fmt"
	"io"

	"github.com/orktes/mongache/pkg/server"
	"gopkg.in/mgo.v2/bson"
)

type sliceIter struct {
	offset int32
	data   []interface{}
}

func (si *sliceIter) Next(ctx context.Context) (interface{}, error) {
	if si.offset == int32(len(si.data)) {
		return nil, io.EOF
	}

	v := si.data[si.offset]
	si.offset++
	return v, nil
}

func (si *sliceIter) Skip(ctx context.Context, n int32) error {
	si.offset += n
	if si.offset > int32(len(si.data)) {
		si.offset = int32(len(si.data))
	}
	return nil
}

func (si *sliceIter) Close(ctx context.Context) error {
	println("cursor closed")
	return nil
}

func (si *sliceIter) Position(ctx context.Context) (int32, error) {
	return int32(si.offset), nil
}

func main() {
	s := server.Server{
		Handler: func(collection string, q bson.M, fields bson.M) (server.Cursor, error) {
			fmt.Printf("%s %+v\n", collection, q)

			data := make([]interface{}, 3000)
			for i := 0; i < 3000; i++ {
				data[i] = map[string]interface{}{
					"foo": fmt.Sprintf("bar_%d", i),
				}
			}

			return &sliceIter{
				data: data,
			}, nil
		},
	}
	err := s.Listen(":6666")
	panic(err)
}
