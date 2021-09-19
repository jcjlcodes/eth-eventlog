package events

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"

	epb "github.com/jcjlcodes/eth-eventlog/proto/events"
)

// EventToProto creates a proto representation of an Event.
func EventToProto(e *Event) *epb.Event {
	topics := make([][]byte, len(e.Topics))
	for i, t := range e.Topics {
		topics[i] = t.Bytes()
	}
	return &epb.Event{
		Address: e.Address.Bytes(),
		Topics:  topics,
		Data:    e.Data,

		BlockNumber: e.BlockNumber,
		BlockHash:   e.BlockHash.Bytes(),
		Index:       e.Index,

		TxHash:  e.TxHash.Bytes(),
		TxIndex: e.TxIndex,
		TxData:  e.TxData,
		TxValue: BigIntToString(e.TxValue),
		TxFrom:  e.TxFrom.Bytes(),
		TxGas:   e.TxGas,
	}
}

// EventFromProto creates an Event from its proto representation.
func EventFromProto(pb *epb.Event) (*Event, error) {
	if len(pb.Address) != common.AddressLength {
		return nil, fmt.Errorf("invalid address")
	}
	topics := make([]common.Hash, len(pb.Topics))
	for i, t := range pb.Topics {
		topics[i] = common.BytesToHash(t)
	}
	txValue, err := BigIntFromString(pb.TxValue)
	if err != nil {
		return nil, err
	}
	return &Event{
		Address: common.BytesToAddress(pb.Address),
		Topics:  topics,
		Data:    pb.Data,

		BlockNumber: pb.BlockNumber,
		BlockHash:   common.BytesToHash(pb.BlockHash),
		Index:       pb.Index,

		TxHash:  common.BytesToHash(pb.TxHash),
		TxIndex: pb.TxIndex,
		TxData:  pb.TxData,
		TxValue: txValue,
		TxFrom:  common.BytesToAddress(pb.TxFrom),
		TxGas:   pb.TxGas,
	}, nil
}

// message Block {
//     uint64 number = 1;
//     bytes hash = 2;
//     repeated Event events = 3;
// }
func BlockToProto(b *Block) *epb.Block {
	events := make([]*epb.Event, len(b.Events))
	for i, e := range b.Events {
		events[i] = EventToProto(&e)
	}
	return &epb.Block{
		Number: b.Number,
		Hash:   b.Hash.Bytes(),
		Events: events,
	}
}

func BlockFromProto(pb *epb.Block) (*Block, error) {
	events := make([]Event, len(pb.Events))
	for i, pbe := range pb.Events {
		e, err := EventFromProto(pbe)
		if err != nil {
			return nil, err
		}
		events[i] = *e
	}
	return &Block{
		Number: pb.Number,
		Hash:   common.BytesToHash(pb.Hash),
		Events: events,
	}, nil
}

func BlockSliceToProto(bs *BlockSlice) *epb.BlockSlice {
	pb := &epb.BlockSlice{
		Start:            bs.Start,
		End:              bs.End,
		DistanceFromHead: bs.DistanceFromHead,
	}

	// Careful, because bs.Blocks might be extended while we work.
	blocks := make([]*epb.Block, len(bs.Blocks))
	for i, b := range bs.Blocks {
		if bs.Blocks[i].Number >= pb.End {
			break
		}
		blocks[i] = BlockToProto(b)
	}
	pb.Blocks = blocks
	return pb
}

func BlockSliceFromProto(pb *epb.BlockSlice) (*BlockSlice, error) {
	blocks := make([]*Block, len(pb.Blocks))
	for i, pbb := range pb.Blocks {
		b, err := BlockFromProto(pbb)
		if err != nil {
			return nil, err
		}
		blocks[i] = b
	}
	return &BlockSlice{
		Start:            pb.Start,
		End:              pb.End,
		DistanceFromHead: pb.DistanceFromHead,
		Blocks:           blocks,
	}, nil
}

func FilterQueryToProto(q *ethereum.FilterQuery) *epb.FilterQuery {
	addresses := make([][]byte, len(q.Addresses))
	for i, a := range q.Addresses {
		addresses[i] = a.Bytes()
	}
	topics := make([]*epb.FilterQuery_Topic, len(q.Topics))
	for i, t := range q.Topics {
		data := make([][]byte, len(t))
		for j, h := range t {
			data[j] = h.Bytes()
		}
		topics[i] = &epb.FilterQuery_Topic{
			Data: data,
		}
	}
	return &epb.FilterQuery{
		Addresses: addresses,
		FromBlock: BigIntToString(q.FromBlock),
		ToBlock:   BigIntToString(q.ToBlock),
		Topics:    topics,
	}
}

func BigIntFromString(s string) (*big.Int, error) {
	if s == "<nil>" || s == "" {
		return nil, nil
	}
	x, ok := new(big.Int).SetString(s, 0)
	if !ok {
		return nil, fmt.Errorf("could not parse big int: %s", s)
	}
	return x, nil
}
func BigIntToString(x *big.Int) string {
	if x == nil {
		return ""
	}
	return "0x" + x.Text(16)
}

func FilterQueryFromProto(pb *epb.FilterQuery) (ethereum.FilterQuery, error) {
	addresses := make([]common.Address, len(pb.Addresses))
	for i, pba := range pb.Addresses {
		addresses[i] = common.BytesToAddress(pba)
	}
	topics := make([][]common.Hash, len(pb.Topics))
	for i, pbt := range pb.Topics {
		topic := make([]common.Hash, len(pbt.Data))
		for j, td := range pbt.Data {
			topic[j] = common.BytesToHash(td)
		}
		topics[i] = topic
	}
	fromBlock, err := BigIntFromString(pb.FromBlock)
	if err != nil {
		return ethereum.FilterQuery{}, err
	}
	toBlock, err := BigIntFromString(pb.ToBlock)
	if err != nil {
		return ethereum.FilterQuery{}, err
	}
	return ethereum.FilterQuery{
		Addresses: addresses,
		FromBlock: fromBlock,
		ToBlock:   toBlock,
		Topics:    topics,
	}, nil
}
