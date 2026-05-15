package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/libmonsoon-dev/socat/cmd/internal/controller"
	"golang.org/x/sync/errgroup"
)

var (
	reverse string
	limit   int

	readUDP  string
	writeUDP string
	readTCP  string
	writeTCP string

	controllerAddr string
)

const maxDatagramSize = 65535

func main() {
	flag.StringVar(&reverse, "reverse", "", "[master|slave] Reverse mode")
	flag.IntVar(&limit, "limit", 1000, "Limit number of connections")
	flag.StringVar(&readUDP, "readudp", "", "Read from UDP address")
	flag.StringVar(&writeUDP, "writeudp", "", "Write to UDP address")
	flag.StringVar(&readTCP, "readtcp", "", "Read from TCP address")
	flag.StringVar(&writeTCP, "writetcp", "", "Write to TCP address")
	flag.StringVar(&controllerAddr, "controller", "", "Controller address")

	flag.Parse()

	ctx, stopNotify := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopNotify()

	conf := Config{
		ReadUDP:  readUDP,
		WriteUDP: writeUDP,
		ReadTCP:  readTCP,
		WriteTCP: writeTCP,
		Controller: controller.Config{
			Address: controllerAddr,
		},
	}

	switch {
	case reverse == "":
		conf.Mode = DefaultMode

	case reverse == "master":
		conf.Mode = ReverseMasterMode

	case reverse == "slave":
		conf.Mode = ReverseSlaveMode

	default:
		flag.Usage()
		os.Exit(1)
	}

	if conf.Mode == ReverseMasterMode || conf.Mode == ReverseSlaveMode {
		conf.Controller.AuthToken = getEnv("SOCAT_CONTROLLER_AUTH_TOKEN")
	}

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(limit)
	New(conf).Run(ctx, group)
	err := group.Wait()
	if err != nil {
		slog.Error("exit", "error", err)
	}
}
