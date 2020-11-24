package main

import (
	"fmt"

	"github.com/orktes/mongache/pkg/server"
	"github.com/orktes/mongache/pkg/slice"
	"go.mongodb.org/mongo-driver/bson"
)

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

			return slice.NewCursor(data)
		},
	}
	err := s.ListenAddr(":6666")
	panic(err)
}
