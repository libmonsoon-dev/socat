package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/libmonsoon-dev/socat/cmd/internal/controller"
	"golang.org/x/sync/errgroup"
)

func NewReverseSlave(config Config) *ReverseSlave {
	return &ReverseSlave{config: config}
}

type ReverseSlave struct {
	config Config
}

func (s *ReverseSlave) Run(ctx context.Context, group *errgroup.Group) {
	client, err := controller.NewClient(ctx, group, s.config.Controller, s)
	if err != nil {
		group.Go(func() error {
			return fmt.Errorf("new controller client: %w", err)
		})
		return
	}

	group.Go(func() error {
		return client.ReadLoop(ctx)
	})
}

func (s *ReverseSlave) NewConnection(ctx context.Context, id uuid.UUID, writeAddr string) {
	upstream, err := d.DialContext(ctx, "tcp", writeAddr)
	if err != nil {
		slog.Error("dial tcp", "addr", writeAddr, "error", err)
		return
	}
	defer upstream.Close()

	host, port, err := net.SplitHostPort(s.config.Controller.Address)
	if err != nil {
		slog.Error("split host:port", "addr", writeAddr, "error", err)
		return
	}

	conn, _, err := controller.Dial(ctx, host, port, s.config.Controller.AuthToken, id)
	if err != nil {
		slog.Error("dial controller", "addr", s.config.Controller.Address, "error", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	var connGroup errgroup.Group
	connGroup.Go(func() error {
		<-ctx.Done()
		upstream.Close()
		conn.Close(websocket.StatusNormalClosure, "")
		return nil
	})

	connGroup.Go(func() error {
		_, err = io.Copy(upstream, websocket.NetConn(ctx, conn, websocket.MessageBinary))
		return err
	})

	connGroup.Go(func() error {
		_, err = io.Copy(websocket.NetConn(ctx, conn, websocket.MessageBinary), upstream)
		return err
	})

	err = connGroup.Wait()
	if err != nil {
		slog.Error("copy to tcp", "writeAddr", writeAddr, "error", err)
	}
}
