package server

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/orktes/mongache/pkg/slice"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func TestServerPing(t *testing.T) {
	s := &Server{}
	s.init()

	ctx := context.Background()

	cli, err := mongo.NewClient(&options.ClientOptions{Dialer: &dialer{s: s}})
	assert.NoError(t, err)
	assert.NoError(t, cli.Connect(ctx))
	defer cli.Disconnect(ctx)

	assert.NoError(t, cli.Ping(ctx, readpref.Primary()))
}

func TestServerReadOne(t *testing.T) {
	s := &Server{
		Handler: func(collection string, q bson.M, fields bson.M) (Cursor, error) {
			assert.Equal(t, "foo.test", collection)
			assert.Equal(t, bson.M{"test": true}, q)
			return slice.NewCursor([]map[string]interface{}{
				{
					"foo": "bar",
				},
			})
		},
	}
	s.init()

	ctx := context.Background()

	cli, err := mongo.NewClient(&options.ClientOptions{Dialer: &dialer{s: s}})
	assert.NoError(t, err)
	assert.NoError(t, cli.Connect(ctx))
	defer cli.Disconnect(ctx)

	res := cli.Database("foo").Collection("test").FindOne(ctx, bson.M{"test": true})

	var resMap map[string]interface{}
	assert.NoError(t, res.Decode(&resMap))

	assert.Equal(t, map[string]interface{}{
		"foo": "bar",
	}, resMap)
}

func TestServerReadCursorAll(t *testing.T) {
	data := make([]map[string]interface{}, 3000)
	for i := 0; i < 3000; i++ {
		data[i] = map[string]interface{}{
			"foo": fmt.Sprintf("bar_%d", i),
		}
	}

	s := &Server{
		Handler: func(collection string, q bson.M, fields bson.M) (Cursor, error) {
			assert.Equal(t, bson.M{}, q)

			return slice.NewCursor(data)
		},
	}
	s.init()

	ctx := context.Background()

	cli, err := mongo.NewClient(&options.ClientOptions{Dialer: &dialer{s: s}})
	assert.NoError(t, err)
	assert.NoError(t, cli.Connect(ctx))
	defer cli.Disconnect(ctx)

	cur, err := cli.Database("foo").Collection("test").Find(ctx, bson.M{})
	assert.NoError(t, err)

	var result []map[string]interface{}
	assert.NoError(t, cur.All(ctx, &result))

	assert.Equal(t, data, result)
}

func TestServerReadCursorAllWithBatchSize(t *testing.T) {
	data := make([]map[string]interface{}, 3000)
	for i := 0; i < 3000; i++ {
		data[i] = map[string]interface{}{
			"foo": fmt.Sprintf("bar_%d", i),
		}
	}

	s := &Server{
		Handler: func(collection string, q bson.M, fields bson.M) (Cursor, error) {
			assert.Equal(t, bson.M{}, q)

			return slice.NewCursor(data)
		},
	}
	s.init()

	ctx := context.Background()

	cli, err := mongo.NewClient(&options.ClientOptions{Dialer: &dialer{s: s}})
	assert.NoError(t, err)
	assert.NoError(t, cli.Connect(ctx))
	defer cli.Disconnect(ctx)

	batchSize := int32(50)
	cur, err := cli.Database("foo").Collection("test").Find(ctx, bson.M{}, &options.FindOptions{BatchSize: &batchSize})
	assert.NoError(t, err)

	var result []map[string]interface{}
	assert.NoError(t, cur.All(ctx, &result))

	assert.Equal(t, data, result)
}

func TestServerReadCursorLimit(t *testing.T) {
	data := make([]map[string]interface{}, 3000)
	for i := 0; i < 3000; i++ {
		data[i] = map[string]interface{}{
			"foo": fmt.Sprintf("bar_%d", i),
		}
	}

	s := &Server{
		Handler: func(collection string, q bson.M, fields bson.M) (Cursor, error) {
			return slice.NewCursor(data)
		},
	}
	s.init()

	ctx := context.Background()

	cli, err := mongo.NewClient(&options.ClientOptions{Dialer: &dialer{s: s}})
	assert.NoError(t, err)
	assert.NoError(t, cli.Connect(ctx))
	defer cli.Disconnect(ctx)

	limit := int64(-10)
	cur, err := cli.Database("foo").Collection("test").Find(ctx, bson.M{}, &options.FindOptions{Limit: &limit})
	assert.NoError(t, err)
	var result []map[string]interface{}
	assert.NoError(t, cur.All(ctx, &result))

	assert.Equal(t, data[:10], result)
}

func TestServerReadCursorSkip(t *testing.T) {
	data := make([]map[string]interface{}, 3000)
	for i := 0; i < 3000; i++ {
		data[i] = map[string]interface{}{
			"foo": fmt.Sprintf("bar_%d", i),
		}
	}

	s := &Server{
		Handler: func(collection string, q bson.M, fields bson.M) (Cursor, error) {
			assert.Equal(t, bson.M{}, q)

			return slice.NewCursor(data)
		},
	}
	s.init()

	ctx := context.Background()

	cli, err := mongo.NewClient(&options.ClientOptions{Dialer: &dialer{s: s}})
	assert.NoError(t, err)
	assert.NoError(t, cli.Connect(ctx))
	defer cli.Disconnect(ctx)

	skip := int64(10)
	cur, err := cli.Database("foo").Collection("test").Find(ctx, bson.M{}, &options.FindOptions{Skip: &skip})
	assert.NoError(t, err)

	var result []map[string]interface{}
	assert.NoError(t, cur.All(ctx, &result))

	assert.Equal(t, data[10:], result)
}

type dialer struct {
	s *Server
}

func (d *dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	a, b := net.Pipe()
	go d.s.handleConn(a)
	return b, nil
}
