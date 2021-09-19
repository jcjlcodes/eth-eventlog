package events

import (
	"github.com/ethereum/go-ethereum"
)

// EventLog represents a sequence of events matching a filter.
type EventLog interface {
	Streamer

	Append(*Block) error
	Rollback(uint64) error
	SetNext(uint64) error
	FirstBlock() uint64
	NextBlock() uint64
	Filter() ethereum.FilterQuery
	Close() error
}
