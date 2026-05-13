package main

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type Runner interface {
	Run(ctx context.Context, group *errgroup.Group)
}

func New(config Config) Runner {
	switch config.Mode {
	case ReverseMasterMode:
		return NewReverseMaster(config)
	case ReverseSlaveMode:
		return NewReverseSlave(config)
	default:
		return NewSocat(config)
	}
}
