package main

type Mode int

const (
	DefaultMode Mode = iota
	ReverseMasterMode
	ReverseSlaveMode
)
