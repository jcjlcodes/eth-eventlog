package events

import (
	"github.com/ethereum/go-ethereum"

	epb "github.com/jcjlcodes/eth-eventlog/proto/events"
)

// InMemoryEventLog is an in-memory implementation of the EventLog interface.
type InMemoryEventLog struct {
	filter     ethereum.FilterQuery
	blockSlice *BlockSlice
}

func NewInMemoryEventLog(from uint64, filter ethereum.FilterQuery) *InMemoryEventLog {
	return &InMemoryEventLog{
		filter:     filter,
		blockSlice: EmptyBlockSlice(from),
	}
}

func (l *InMemoryEventLog) FirstBlock() uint64 {
	return l.blockSlice.Start
}

func (l *InMemoryEventLog) NextBlock() uint64 {
	return l.blockSlice.End
}

func (l *InMemoryEventLog) Filter() ethereum.FilterQuery {
	return l.filter
}

func (l *InMemoryEventLog) Append(b *Block) error {
	if err := l.blockSlice.Append(b); err != nil {
		return err
	}
	return nil
}

func (l *InMemoryEventLog) Rollback(n uint64) error {
	if err := l.blockSlice.Rollback(n); err != nil {
		return err
	}
	return nil
}

func (l *InMemoryEventLog) SetNext(n uint64) error {
	if err := l.blockSlice.Extend(n); err != nil {
		return err
	}
	return nil
}

func (l *InMemoryEventLog) Close() error {
	return nil
}

func (l *InMemoryEventLog) Stream(done chan struct{}, from uint64) (*Subscription, error) {
	c := make(chan *Message)
	errc := make(chan error, 1)

	go func() {
		err := l.stream(c, done, from)
		close(c)
		errc <- err
	}()

	return &Subscription{
		C:    c,
		Err:  errc,
		Done: done,
	}, nil
}

func (l *InMemoryEventLog) stream(c chan *Message, done chan struct{}, from uint64) error {
	b := *l.blockSlice
	b.DeleteBeforeBlock(from)
	for _, blk := range b.Blocks {
		m := &Message{
			Action: Append,
			Block:  blk,
		}
		if err := sendOrDone(c, done, m); err != nil {
			return err
		}
	}
	if err := sendOrDone(c, done, &Message{
		Action: SetNext,
		Number: l.NextBlock(),
	}); err != nil {
		return err
	}
	return nil
}

func (l *InMemoryEventLog) ToProto() *epb.EventLogFile {
	return &epb.EventLogFile{
		Filter:     FilterQueryToProto(&l.filter),
		BlockSlice: BlockSliceToProto(l.blockSlice),
	}
}

func InMemoryEventLogFromProto(pb *epb.EventLogFile) (*InMemoryEventLog, error) {
	filter, err := FilterQueryFromProto(pb.Filter)
	if err != nil {
		return nil, err
	}
	blockSlice, err := BlockSliceFromProto(pb.BlockSlice)
	if err != nil {
		return nil, err
	}
	return &InMemoryEventLog{
		filter:     filter,
		blockSlice: blockSlice,
	}, nil
}
