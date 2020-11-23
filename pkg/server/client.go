package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/orktes/mongache/pkg/mongoproto"
	"gopkg.in/mgo.v2/bson"
)

const defaultReturnSize = 1000

type client struct {
	server       *Server
	conn         net.Conn
	requestCount int32
}

func (c *client) reqID() int32 {
	return atomic.AddInt32(&c.requestCount, int32(1))
}

func (c *client) processKillCursors(ctx context.Context, killCursorsOp *mongoproto.OpKillCursors) error {

	for _, curID := range killCursorsOp.CursorIDs {
		cur, ok := c.server.getCursor(curID)
		if ok {
			c.server.removeCursor(curID)
			if err := cur.Close(ctx); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *client) processGetMore(ctx context.Context, getMoreOp *mongoproto.OpGetMore) error {
	reqID := c.reqID()
	cursorID := int64(0)
	start := int32(0)

	var flags mongoproto.OpReplyFlags
	var docs [][]byte

	if getMoreOp.NumberToReturn == 0 {
		getMoreOp.NumberToReturn = defaultReturnSize
	}

	cur, ok := c.server.getCursor(getMoreOp.CursorID)
	if !ok {
		flags = mongoproto.OpReplyCursorNotFound
	} else {
		var err error
		start, err = cur.Position(ctx)
		if err != nil {
			return err
		}

		closeCursor := getMoreOp.NumberToReturn < 0

		numReturn := getMoreOp.NumberToReturn
		if closeCursor {
			numReturn = -numReturn
		}

		for i := int32(0); i < numReturn; i++ {
			v, err := cur.Next(ctx)
			if err != nil {
				// TODO figure out what to do here
				if err == io.EOF {
					closeCursor = true
				}

				break
			}

			b, ok := v.([]byte)
			if !ok {
				b, err = bson.Marshal(v)
				if err != nil {
					return err
				}
			}

			docs = append(docs, b)
		}

		if closeCursor {
			c.server.removeCursor(getMoreOp.CursorID)
			if err := cur.Close(ctx); err != nil {
				return err
			}
		} else {
			cursorID = getMoreOp.CursorID
		}
	}

	reply := mongoproto.OpReply{
		Header: mongoproto.MsgHeader{
			RequestID:     reqID,
			ResponseTo:    getMoreOp.Header.RequestID,
			MessageLength: 36 + docLen(docs),
			OpCode:        mongoproto.OpCodeReply,
		},
		Documents:      docs,
		StartingFrom:   start,
		NumberReturned: int32(len(docs)),
		Flags:          flags,
		CursorID:       cursorID,
	}
	_, err := reply.WriteTo(c.conn)
	return err
}

func (c *client) processQuery(ctx context.Context, queryOp *mongoproto.OpQuery) error {
	reqID := c.reqID()
	cursorID := int64(0)

	var docs [][]byte

	collectionName := queryOp.FullCollectionName

	if queryOp.NumberToReturn == 0 {
		queryOp.NumberToReturn = defaultReturnSize
	}

	if collectionName == "admin.$cmd" {
		b, err := bson.Marshal(map[string]interface{}{"maxWireVersion": 2, "minWireVersion": 2, "ok": true})
		if err != nil {
			return err
		}

		docs = append(docs, b)
	} else {

		var query bson.M
		err := bson.Unmarshal(queryOp.Query, &query)
		if err != nil {
			return err
		}

		var fields bson.M
		if len(queryOp.ReturnFieldsSelector) > 0 {
			err = bson.Unmarshal(queryOp.ReturnFieldsSelector, &fields)
			if err != nil {
				return err
			}
		}

		cur, err := c.server.Handler(queryOp.FullCollectionName, query, fields)
		if err != nil {
			return err
		}

		cur.Skip(ctx, queryOp.NumberToSkip)

		closeCursor := queryOp.NumberToReturn <= 0

		numReturn := queryOp.NumberToReturn
		if closeCursor {
			numReturn = -numReturn
		}

		for i := int32(0); i < numReturn; i++ {
			v, err := cur.Next(ctx)
			if err != nil {
				// TODO figure out what to do here
				if err == io.EOF {
					closeCursor = true
				}

				break
			}

			b, ok := v.([]byte)
			if !ok {
				b, err = bson.Marshal(v)
				if err != nil {
					return err
				}
			}

			docs = append(docs, b)
		}

		if closeCursor {
			cur.Close(ctx)
		} else {
			cursorID = c.server.storeCursor(cur)
		}
	}

	reply := mongoproto.OpReply{
		Header: mongoproto.MsgHeader{
			RequestID:     reqID,
			ResponseTo:    queryOp.Header.RequestID,
			MessageLength: 36 + docLen(docs),
			OpCode:        mongoproto.OpCodeReply,
		},
		Documents:      docs,
		NumberReturned: int32(len(docs)),
		CursorID:       cursorID,
	}

	_, err := reply.WriteTo(c.conn)
	return err
}

func (c *client) process() error {
	for {

		op, err := mongoproto.OpFromReader(c.conn)
		if err != nil {
			return err
		}

		ctx := context.Background()

		fmt.Printf("%#v\n", op)

		switch v := op.(type) {
		case *mongoproto.OpGetMore:
			if err := c.processGetMore(ctx, v); err != nil {
				return err
			}
		case *mongoproto.OpQuery:
			if err := c.processQuery(ctx, v); err != nil {
				return err
			}
		case *mongoproto.OpKillCursors:
			if err := c.processKillCursors(ctx, v); err != nil {
				return err
			}
		}
	}

}

func (c *client) close(ctx context.Context) error {

	return nil
}

func docLen(docs [][]byte) int32 {
	c := int32(0)
	for _, doc := range docs {
		c += int32(len(doc))
	}

	return c
}
