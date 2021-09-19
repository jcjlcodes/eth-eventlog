package events

import (
	"context"
	"log"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/ethclient"
)

const DefaultBatchOverlap uint64 = 10     // overlap between polls
const DefaultFetchBatchSize uint64 = 2000 // size of call to getLogs
const MaxEventlogSize uint64 = 1024       // blocks
const DefaultPollInterval int = 15        // seconds

// ChainStreamer implements a Streamer for the Ethereum blockchain.
type ChainStreamer struct {
	Ctx            context.Context
	Url            string
	Filter         ethereum.FilterQuery
	FetchBatchSize uint64
	BatchOverlap   uint64
	FetchTxDetails bool
}

func (cr *ChainStreamer) Stream(done chan struct{}, from uint64) (*Subscription, error) {
	cs, err := cr.makeChainStreamer(done, from)
	if err != nil {
		return nil, err
	}

	go func() {
		err := cs.run()
		close(cs.c)
		cs.err <- err
	}()

	return &Subscription{C: cs.c, Err: cs.err, Done: done}, nil
}

type chainStreamer struct {
	filter ethereum.FilterQuery

	c    chan *Message
	done chan struct{}
	err  chan error

	ctx     context.Context
	client  *ethclient.Client
	history *BlockSlice
	next    uint64

	from           uint64
	fetchBatchSize uint64
	batchOverlap   uint64
	fetchTxDetails bool
}

func (cr *ChainStreamer) makeChainStreamer(done chan struct{}, from uint64) (*chainStreamer, error) {

	bo := cr.BatchOverlap
	if bo == 0 {
		bo = DefaultBatchOverlap
	}
	fbs := cr.FetchBatchSize
	if fbs == 0 {
		fbs = DefaultFetchBatchSize
	}

	client, err := ethclient.DialContext(cr.Ctx, cr.Url)
	if err != nil {
		return nil, err
	}

	return &chainStreamer{
		filter: cr.Filter,

		c:    make(chan *Message),
		done: done,
		err:  make(chan error, 1),

		ctx:     cr.Ctx,
		client:  client,
		history: EmptyBlockSlice(from),

		from:           from,
		next:           from,
		fetchBatchSize: fbs,
		batchOverlap:   bo,
		fetchTxDetails: cr.FetchTxDetails,
	}, nil
}

func (cs *chainStreamer) run() error {
	for {

		// 1. Get a BlockSlice from chain.

		from := cs.next - cs.batchOverlap
		if cs.next < cs.from+cs.batchOverlap {
			from = cs.from
		}

		b, err := cs.fetch(from)
		if err != nil {
			return err
		}

		// 2. Process the blocks.

		if err := cs.process(b); err != nil {
			return err
		}

		// 3. If we are polling at head, wait.

		if b.DistanceFromHead == 0 {
			if err := waitOrDone(cs.done, time.Duration(DefaultPollInterval)*time.Second); err != nil {
				return err
			}
		}
	}
}

func (cs *chainStreamer) process(b *BlockSlice) error {
	// 1. Check whether the new batch agrees with the stored history in the
	// overlap. If they don't, there has been a chain reorganization and we
	// must roll back to the last agreed upon block.

	log.Printf("processing batch %d:%d (%d non-empty blocks)\n", b.Start, b.End, len(b.Blocks))

	ok, lastGoodBlock, err := MatchBlocks(b, cs.history)
	if err != nil {
		return err
	}
	if !ok {
		log.Printf("MatchHistory returned false, %d\n", lastGoodBlock)
		if lastGoodBlock+1 < cs.from {
			lastGoodBlock = cs.from - 1
		}
		cs.next = lastGoodBlock + 1
		if err := cs.history.Rollback(cs.next); err != nil {
			return err
		}
		m := &Message{
			Action: Rollback,
			Number: cs.next,
		}
		if err := sendOrDone(cs.c, cs.done, m); err != nil {
			return err
		}
		log.Printf("  ..new cs.next=%d\n", cs.next)

		// We can't recover from no matching events, so emit nothing.
		if cs.next < b.Start {
			return nil
		}
	}

	// 2. Remove the overlap with history.

	b.DeleteBeforeBlock(cs.next)

	// 3. Emit events to internal eventlog and output channel.

	log.Printf("emitting %d blocks from BlockSlice %d:%d\n", len(b.Blocks), b.Start, b.End)
	if err := cs.history.Concat(b); err != nil {
		return err
	}
	if cs.history.End >= MaxEventlogSize {
		cs.history.DeleteBeforeBlock(cs.history.End - MaxEventlogSize)
	}
	for _, blk := range b.Blocks {
		m := &Message{
			Action: Append,
			Block:  blk,
		}
		if err := sendOrDone(cs.c, cs.done, m); err != nil {
			return err
		}
	}

	// 4. Update cs.next to end of this batch.

	cs.next = b.End
	if err := sendOrDone(cs.c, cs.done, &Message{
		Action: SetNext,
		Number: cs.next,
	}); err != nil {
		return err
	}
	return nil
}

// fetch returns a batch of logs from a given block number. The events in the
// block are guaranteed to be sorted by increasing (BlockNumber, Index).
func (cs *chainStreamer) fetch(from uint64) (*BlockSlice, error) {
	batchSize := cs.fetchBatchSize
	if batchSize == 0 {
		batchSize = 2000
	}

	to := from + batchSize - 1

	batch, err := GetLogs(cs.ctx, cs.client, &ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: cs.filter.Addresses,
		Topics:    cs.filter.Topics,
	}, cs.fetchTxDetails)
	if err != nil {
		return nil, err
	}
	return batch, nil
}
