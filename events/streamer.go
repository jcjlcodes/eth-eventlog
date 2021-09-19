package events

import (
	"time"
)

type Action int

const (
	Append Action = iota
	Rollback
	SetNext
)

type Message struct {
	Action Action
	Number uint64
	Block  *Block
}

type Subscription struct {
	C    chan *Message
	Err  chan error
	Done chan struct{}
}

type Streamer interface {
	Stream(done chan struct{}, from uint64) (*Subscription, error)
}

type CanceledError string

const Canceled CanceledError = CanceledError("")

func (CanceledError) Error() string {
	return ""
}

func sendOrDone(c chan *Message, done chan struct{}, m *Message) error {
	select {
	case <-done:
		return Canceled
	case c <- m:
		return nil
	}
}

func waitOrDone(done chan struct{}, d time.Duration) error {
	select {
	case <-done:
		return Canceled
	case <-time.After(d):
		return nil
	}
}
