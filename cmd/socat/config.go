package main

import "github.com/libmonsoon-dev/socat/cmd/internal/controller"

type Config struct {
	Mode Mode

	ReadUDP  string
	WriteUDP string
	ReadTCP  string
	WriteTCP string

	Controller controller.Config
}
