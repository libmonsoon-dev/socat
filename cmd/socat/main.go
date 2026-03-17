package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	readUDP  string
	writeUDP string
	readTCP  string
	writeTCP string

	limit int
)

func main() {
	flag.StringVar(&readUDP, "readudp", "", "Read from UDP address")
	flag.StringVar(&writeUDP, "writeudp", "", "Write to UDP address")
	flag.StringVar(&readTCP, "readtcp", "", "Read from TCP address")
	flag.StringVar(&writeTCP, "writetcp", "", "Write to TCP address")
	flag.IntVar(&limit, "limit", 1000, "Limit number of connections")

	flag.Parse()

	ctx, stopNotify := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopNotify()

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(limit)

	if readUDP != "" && writeUDP != "" {
		group.Go(func() error {
			return acceptLoop(ctx, group, "udp", writeUDP, readUDP)
		})
	}

	if readTCP != "" && writeTCP != "" {
		group.Go(func() error {
			return acceptLoop(ctx, group, "tcp", writeTCP, readTCP)
		})
	}

	err := group.Wait()
	if err != nil {
		log.Printf("%s, exit", err)
	}
}

func acceptLoop(ctx context.Context, group *errgroup.Group, network, writeAddr, readAddr string) error {
	d := net.Dialer{Timeout: 5 * time.Second}
	lc := net.ListenConfig{KeepAliveConfig: net.KeepAliveConfig{Enable: true}}

	reader, err := lc.Listen(ctx, network, readUDP)
	if err != nil {
		return fmt.Errorf("listen %s %s: %w", readAddr, network, err)
	}
	defer reader.Close()

	for {
		conn, err := reader.Accept()
		if err != nil {
			return fmt.Errorf("accept %s: %w", network, err)
		}

		group.Go(func() error {
			defer conn.Close()

			upstream, err := d.DialContext(ctx, network, writeAddr)
			if err != nil {
				log.Printf("dial %s %s: %s", writeAddr, network, err)
				return nil
			}
			defer upstream.Close()

			var connGroup errgroup.Group
			connGroup.Go(func() error {
				_, err = io.Copy(upstream, conn)
				return err
			})

			connGroup.Go(func() error {
				_, err = io.Copy(conn, upstream)
				return err
			})

			err = connGroup.Wait()
			if err != nil {
				log.Printf("copy %s to %s %s: %s", readAddr, writeAddr, network, err)
			}

			return nil
		})
	}
}
