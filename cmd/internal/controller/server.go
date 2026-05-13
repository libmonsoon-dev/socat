package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"slices"
	"sync"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

const (
	AuthorizationHeader = "Authorization"
	ConnectionIDHeader  = "Connection-ID"
)

type Config struct {
	Address   string
	AuthToken string
}

func NewServer(ctx context.Context, conf Config, writeTo string) (*Server, error) {
	server := &Server{
		ctx:                     ctx,
		slaves:                  make(map[*websocket.Conn]struct{}),
		connectionSubscriptions: make(map[uuid.UUID]chan *websocket.Conn),
		authToken:               conf.AuthToken,
		writeTo:                 writeTo,
	}

	host, port, err := net.SplitHostPort(conf.Address)
	if err != nil {
		return nil, fmt.Errorf("parse addr: %w", err)
	}

	server.httpServer = &http.Server{Addr: net.JoinHostPort(host, port), Handler: server}
	return server, nil
}

var _ http.Handler = new(Server)

type Server struct {
	ctx                     context.Context
	mu                      sync.RWMutex
	slaves                  map[*websocket.Conn]struct{}
	connectionSubscriptions map[uuid.UUID]chan *websocket.Conn

	httpServer *http.Server
	authToken  string
	writeTo    string
}

func (s *Server) Run() error {
	err := s.httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("controller server: %w", err)
	}

	return nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(AuthorizationHeader) != s.authToken {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	connectionID, _ := uuid.Parse(r.Header.Get(ConnectionIDHeader))

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		slog.Error("websocket accept", "addr", r.RemoteAddr, "error", err)
		return
	}

	s.mu.Lock()
	if connectionID == uuid.Nil {
		slog.Debug("slave connected", "addr", r.RemoteAddr)
		s.slaves[conn] = struct{}{}
	} else {
		slog.Debug("data connected", "addr", r.RemoteAddr)
		s.connectionSubscriptions[connectionID] <- conn
	}
	s.mu.Unlock()
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for conn := range s.slaves {
		addr := websocket.NetConn(context.Background(), conn, websocket.MessageText).RemoteAddr()
		slog.Debug("slave disconnected", "addr", addr)
		go func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
		delete(s.slaves, conn)
	}

	return s.httpServer.Shutdown(s.ctx)
}

func (s *Server) NewConnection(ctx context.Context) (conn *websocket.Conn, err error) {
	connectionID := uuid.Must(uuid.NewV7())
	req := newRequest(newConnectionRequest{
		ID:      connectionID,
		WriteTo: s.writeTo,
	})
	jsonReq, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	sub := s.subscribe(connectionID)
	s.send(ctx, jsonReq)
	conn, err = sub.wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("wait for connection: %w", err)
	}

	return conn, nil
}

func (s *Server) send(ctx context.Context, jsonReq []byte) {
	s.mu.RLock()
	slaves := slices.Collect(maps.Keys(s.slaves))
	s.mu.RUnlock()

	if len(s.slaves) != 1 {
		slog.Warn("unexpected number of slaves", "count", len(s.slaves))
	}

	for _, conn := range slaves {
		err := conn.Write(ctx, websocket.MessageText, jsonReq)
		if err != nil {
			slog.Error("failed to write to slave", "error", err)
			addr := websocket.NetConn(ctx, conn, websocket.MessageText).RemoteAddr()
			slog.Debug("slave disconnected", "addr", addr)
			go conn.Close(websocket.StatusNormalClosure, "")

			s.mu.Lock()
			delete(s.slaves, conn)
			s.mu.Unlock()
		}
	}
}

func (s *Server) subscribe(connectionID uuid.UUID) *subscription {
	sub := &subscription{connectionID: connectionID, server: s, ch: make(chan *websocket.Conn, 1)}

	s.mu.Lock()
	s.connectionSubscriptions[connectionID] = sub.ch
	s.mu.Unlock()

	return sub
}

func (s *Server) unsubscribe(connectionID uuid.UUID) {
	s.mu.Lock()
	delete(s.connectionSubscriptions, connectionID)
	s.mu.Unlock()
}
