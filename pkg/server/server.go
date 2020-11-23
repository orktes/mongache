package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"gopkg.in/mgo.v2/bson"
)

type Cursor interface {
	Next(ctx context.Context) (interface{}, error)
	Skip(ctx context.Context, n int32) error
	Position(ctx context.Context) (int32, error)
	Close(ctx context.Context) error
}

type QueryHandler func(collection string, q bson.M, fields bson.M) (Cursor, error)

type Server struct {
	ln net.Listener

	cursorsMutex    sync.RWMutex
	cursorIDCounter int64
	cursors         map[int64]Cursor

	Handler QueryHandler
}

func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.cursors = map[int64]Cursor{}
	s.ln = ln

	return s.listen()
}

func (s *Server) storeCursor(c Cursor) int64 {
	id := atomic.AddInt64(&s.cursorIDCounter, 1)
	s.cursorsMutex.Lock()
	defer s.cursorsMutex.Unlock()
	s.cursors[id] = c
	return id
}

func (s *Server) removeCursor(id int64) {
	s.cursorsMutex.RLock()
	defer s.cursorsMutex.RUnlock()
	delete(s.cursors, id)
}

func (s *Server) getCursor(id int64) (Cursor, bool) {
	s.cursorsMutex.RLock()
	defer s.cursorsMutex.RUnlock()
	c, ok := s.cursors[id]
	return c, ok
}

func (s *Server) listen() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return err
		}

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	cli := &client{conn: conn, server: s}
	defer func() {
		err := cli.close(context.TODO())
		if err != nil {
			panic(err)
		}
	}()

	if err := cli.process(); err != nil {
		fmt.Printf("Err: %s\n", err.Error())
	}
}
