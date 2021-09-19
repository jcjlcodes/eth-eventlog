package events

import (
	"fmt"
)

// LiveEventLog combines an EventLog and a ChainStreamer to make a new Streamer
// that streams first from the EventLog, and then from the ChainStreamer. When
// streaming from the ChainStreamer the messages are both sent to the EventLog
// and the subscriber.
type LiveEventLog struct {
	eventlog EventLog
	streamer ChainStreamer
}

func NewLiveEventLog(e EventLog, s ChainStreamer) *LiveEventLog {
	return &LiveEventLog{
		eventlog: e,
		streamer: s,
	}
}

func (l *LiveEventLog) Stream(done chan struct{}, from uint64) (*Subscription, error) {
	if from < l.eventlog.FirstBlock() {
		return nil, fmt.Errorf("got from=%d; want from >= %d", from, l.eventlog.FirstBlock())
	}

	c := make(chan *Message)
	errc := make(chan error, 1)

	go func() {
		err := l.stream(c, done, from)
		errc <- err
		close(c)
	}()

	return &Subscription{
		C:    c,
		Err:  errc,
		Done: done,
	}, nil
}

func (l *LiveEventLog) stream(c chan *Message, done chan struct{}, from uint64) error {

	nextBlock := from

	// 1. Stream all events from the eventlog.

	elSub, err := l.eventlog.Stream(done, nextBlock)
	if err != nil {
		return err
	}

	for m := range elSub.C {
		if m.Action == Rollback {
			return fmt.Errorf("got unexpected Rollback from eventlog")
		}
		switch m.Action {
		case Append:
			nextBlock = m.Block.Number + 1
		case SetNext:
			nextBlock = m.Number
		}
		if err := sendOrDone(c, done, m); err != nil {
			return err
		}
	}
	if err := <-elSub.Err; err != nil {
		return err
	}

	// 2. Start streaming from chain.

	l.streamer.Filter = l.eventlog.Filter()
	chSub, err := l.streamer.Stream(done, nextBlock)
	if err != nil {
		return err
	}
	for m := range chSub.C {
		switch m.Action {
		case Append:
			if err := l.eventlog.Append(m.Block); err != nil {
				return err
			}
		case Rollback:
			if err := l.eventlog.Rollback(m.Number); err != nil {
				return err
			}
		case SetNext:
			if err := l.eventlog.SetNext(m.Number); err != nil {
				return err
			}
		}
		if err := sendOrDone(c, done, m); err != nil {
			return err
		}
	}
	if err := <-chSub.Err; err != nil {
		return err
	}

	return nil
}
