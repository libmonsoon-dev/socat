package controller

import (
	"context"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

type subscription struct {
	connectionID uuid.UUID
	server       *Server
	ch           chan *websocket.Conn
}

func (s *subscription) wait(ctx context.Context) (*websocket.Conn, error) {
	defer s.server.unsubscribe(s.connectionID)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case conn := <-s.ch:
		return conn, nil
	}
}
