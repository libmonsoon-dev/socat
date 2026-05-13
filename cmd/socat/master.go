package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/coder/websocket"
	"github.com/libmonsoon-dev/socat/cmd/internal/controller"
	"golang.org/x/sync/errgroup"
)

func NewReverseMaster(config Config) *ReverseMaster {
	return &ReverseMaster{config}
}

type ReverseMaster struct {
	config Config
}

func (m *ReverseMaster) Run(ctx context.Context, group *errgroup.Group) {
	cServer, err := controller.NewServer(ctx, m.config.Controller, m.config.WriteTCP)
	if err != nil {
		group.Go(func() error {
			return fmt.Errorf("could not create controller server: %w", err)
		})

		return
	}
	group.Go(func() error {
		return cServer.Run()
	})

	group.Go(func() error {
		defer cServer.Close()

		<-ctx.Done()
		return nil
	})

	group.Go(func() error {
		return m.tcpAcceptLoop(ctx, group, cServer)
	})
}

func (m *ReverseMaster) tcpAcceptLoop(ctx context.Context, group *errgroup.Group, controller *controller.Server) error {
	reader, err := lc.Listen(ctx, "tcp", m.config.ReadTCP)
	if err != nil {
		return fmt.Errorf("listen %s tcp: %w", m.config.ReadTCP, err)
	}
	defer reader.Close()

	go func() {
		<-ctx.Done()
		_ = reader.Close()
	}()

	for {
		conn, err := reader.Accept()
		if err != nil {
			return fmt.Errorf("accept tcp: %w", err)
		}

		group.Go(func() error {
			defer conn.Close()

			upstream, err := controller.NewConnection(ctx)
			if err != nil {
				return fmt.Errorf("new connection: %w", err)
			}
			defer upstream.Close(websocket.StatusNormalClosure, "")

			var connGroup errgroup.Group
			connGroup.Go(func() error {
				<-ctx.Done()
				conn.Close()
				upstream.Close(websocket.StatusNormalClosure, "")
				return nil
			})

			connGroup.Go(func() error {
				_, err = io.Copy(websocket.NetConn(ctx, upstream, websocket.MessageBinary), conn)
				return err
			})

			connGroup.Go(func() error {
				_, err = io.Copy(conn, websocket.NetConn(ctx, upstream, websocket.MessageBinary))
				return err
			})

			err = connGroup.Wait()
			if err != nil {
				slog.Error("copy to tcp", "readAddr", m.config.ReadTCP, "writeAddr", m.config.WriteTCP, "error", err)
			}

			return nil
		})
	}
}
