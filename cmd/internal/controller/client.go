package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

func NewClient(ctx context.Context, group *errgroup.Group, conf Config, slave Slave) (*Client, error) {
	host, port, err := net.SplitHostPort(conf.Address)
	if err != nil {
		return nil, fmt.Errorf("parse addr %s: %w", conf.Address, err)
	}

	conn, _, err := Dial(ctx, host, port, conf.AuthToken, uuid.Nil)
	if err != nil {
		return nil, fmt.Errorf("websocket dial to %s: %w", conf.Address, err)
	}

	return &Client{ctx: ctx, group: group, conn: conn, slave: slave}, nil
}

func Dial(ctx context.Context, host, port, authToken string, connectionID uuid.UUID) (*websocket.Conn, *http.Response, error) {
	return websocket.Dial(
		ctx,
		//TODO: wss
		(&url.URL{Scheme: "ws", Host: net.JoinHostPort(host, port)}).String(),
		&websocket.DialOptions{
			HTTPHeader: http.Header{
				AuthorizationHeader: []string{authToken},
				ConnectionIDHeader:  []string{connectionID.String()},
			},
		},
	)
}

type Client struct {
	ctx   context.Context
	group *errgroup.Group

	conn *websocket.Conn

	slave Slave
}

type Slave interface {
	NewConnection(ctx context.Context, id uuid.UUID, addr string)
}

func (c *Client) ReadLoop(ctx context.Context) error {
	defer c.conn.Close(websocket.StatusNormalClosure, "")

	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read from websocket: %w", err)
		}

		var req message
		err = json.Unmarshal(data, &req)
		if err != nil {
			return fmt.Errorf("unmarshal request %s: %w", string(data), err)
		}

		switch req.Type {
		case newConnectionRequestType:
			c.handleNewConnectionRequest(ctx, req)

		default:
			slog.Debug("read message from websocket", "data", string(data))
		}
	}
}

func (c *Client) handleNewConnectionRequest(ctx context.Context, req message) {
	var request newConnectionRequest

	err := json.Unmarshal(req.Data, &request)
	if err != nil {
		slog.Error("unmarshal request data", "data", string(req.Data), "error", err)
		return
	}

	c.group.Go(func() error {
		c.slave.NewConnection(ctx, request.ID, request.WriteTo)
		return nil
	})
}
