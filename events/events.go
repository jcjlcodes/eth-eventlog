// Package events provides data structures and functions to stream and store
// events (logs) from the Ethereum blockchain.
//
// Messages in the event stream have three possible actions:
//   Append a Block
//   Rollback to a given Block (happens on chain reorganization)
//   SetNext to a given block number.
//
// Depending on the event filter used to retrieve logs, the stream may not
// contain logs for every block. The SetNext message allows the stream to
// signal that blocks have been read, but no events found. The Append and
// Rollback messages are straightforward.
//
// The central interfaces are Streamer and EventLog (which is also a Streamer).
// A Streamer knows how to stream events. The ChainStreamer implements this to
// stream from the Ethereum blockchain by overlapping eth_getLogs calls,
// sending Rollback messages when a chain reorganization is detected. An
// EventLog implements both the receiving methods of a stream (Append,
// Rollback, SetNext), and the Streamer interface to emit the stored events.
package events

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Event struct {
	Address common.Address
	Topics  []common.Hash
	Data    []byte

	BlockNumber uint64
	BlockHash   common.Hash
	Index       uint64 // index of log in block

	TxHash  common.Hash
	TxIndex uint64 // index of tx in block
	TxData  []byte
	TxValue *big.Int
	TxFrom  common.Address
	TxGas   uint64
}

func (e *Event) Log() *types.Log {
	return &types.Log{
		Address:     e.Address,
		Topics:      e.Topics,
		Data:        e.Data,
		BlockNumber: e.BlockNumber,
		BlockHash:   e.BlockHash,
		Index:       uint(e.Index),
		TxHash:      e.TxHash,
		TxIndex:     uint(e.TxIndex),
	}
}

type Block struct {
	Number uint64
	Hash   common.Hash
	Events []Event
}

// MatchHistory compares the new blocks with the old where they overlap. It
// returns true if all blocks in the overlap have the same hash. The second
// return value indicates the latest block that agrees. If no block agrees,
// it returns (false, 0, nil).
func MatchBlocks(new, old *BlockSlice) (bool, uint64, error) {
	if new.End < old.End {
		return false, 0, fmt.Errorf("new blocks end before old blocks: got new=%d:%d; old=%d:%d", new.Start, new.End, old.Start, old.End)
	}

	lastGoodBlock := uint64(0)
	o := *old
	o.DeleteBeforeBlock(new.Start)

	ok := true
	for i := 0; i < len(o.Blocks); i++ {
		ob := o.Blocks[i]
		nb := new.Blocks[i]
		if !bytes.Equal(ob.Hash.Bytes(), nb.Hash.Bytes()) {
			ok = false
			break
		}
		lastGoodBlock = nb.Number
	}
	return ok, lastGoodBlock, nil
}

// GetLogs returns a batch of logs matching a query. The blocks in the
// block are guaranteed to be sorted by increasing Number, and the events
// therein by Index.
func GetLogs(ctx context.Context, client *ethclient.Client, q *ethereum.FilterQuery) (*BlockSlice, error) {
	head, err := client.BlockNumber(ctx)
	if err != nil {
		return nil, err
	}

	if q.ToBlock.Uint64() >= head {
		q.ToBlock.SetUint64(head)
	}

	logs, err := client.FilterLogs(ctx, *q)
	if err != nil {
		return nil, err
	}
	sort.Slice(logs, func(i, j int) bool {
		if logs[i].BlockNumber == logs[j].BlockNumber {
			return logs[i].Index < logs[j].Index
		}
		return logs[i].BlockNumber < logs[j].BlockNumber
	})
	slice := &BlockSlice{
		Start:            q.FromBlock.Uint64(),
		End:              q.ToBlock.Uint64() + 1,
		DistanceFromHead: head - q.ToBlock.Uint64(),
		Blocks:           make([]*Block, 0),
	}

	if len(logs) == 0 {
		return slice, nil
	}

	var block *Block = nil
	for _, l := range logs {
		if block == nil || l.BlockNumber != block.Number {
			if block != nil {
				slice.Blocks = append(slice.Blocks, block)
			}
			block = &Block{
				Number: l.BlockNumber,
				Hash:   l.BlockHash,
				Events: make([]Event, 0),
			}
		}
		e := Event{
			Address: l.Address,
			Topics:  l.Topics,
			Data:    l.Data,

			BlockNumber: l.BlockNumber,
			BlockHash:   l.BlockHash,
			Index:       uint64(l.Index),

			TxHash:  l.TxHash,
			TxIndex: uint64(l.TxIndex),
		}
		block.Events = append(block.Events, e)
	}
	if block != nil {
		slice.Blocks = append(slice.Blocks, block)
	}

	return slice, nil
}

func AddTransactionData(ctx context.Context, client *ethclient.Client, bs *BlockSlice) error {
	transactions := make(map[string]*types.Transaction)
	transactionSenders := make(map[string]common.Address)
	getTransaction := func(e *Event) (*types.Transaction, common.Address, error) {
		h := e.TxHash
		key := h.Hex()
		if tx, ok := transactions[key]; ok {
			return tx, transactionSenders[key], nil
		}
		tx, _, err := client.TransactionByHash(ctx, h)
		if err != nil {
			return nil, common.Address{}, err
		}
		sender, err := client.TransactionSender(ctx, tx, e.BlockHash, uint(e.TxIndex))
		if err != nil {
			sender = common.Address{}
		}
		transactions[key] = tx
		transactionSenders[key] = sender
		return tx, sender, nil
	}

	for _, b := range bs.Blocks {
		for i := range b.Events {
			e := &b.Events[i]
			tx, sender, err := getTransaction(e)
			if err != nil {
				return err
			}
			e.TxData = tx.Data()
			e.TxValue = tx.Value()
			e.TxFrom = sender
			e.TxGas = tx.Gas()
		}
	}
	return nil
}
