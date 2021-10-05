package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"google.golang.org/protobuf/proto"

	"github.com/jcjlcodes/eth-eventlog/events"
)

var nodeFlag = flag.String("node", "", "Ethereum JSON-RPC node url")
var outputFlag = flag.String("output", "", "Output directory")
var txFlag = flag.Bool("tx", false, "If true, fetches Tx details.")
var startFlag = flag.Uint64("start", 10, "How many blocks behind HEAD to start")

type LogTransfer struct {
	From   common.Address
	To     common.Address
	Tokens *big.Int
}

func run() error {

	log.Printf("nodeFlag: %v", *nodeFlag)
	log.Printf("outputFlag: %v", *outputFlag)
	log.Printf("txFlag: %v", *txFlag)
	log.Printf("startFlag: %v", *startFlag)

	ctx := context.Background()

	if err := os.MkdirAll(*outputFlag, 0755); err != nil {
		return err
	}

	client, err := ethclient.DialContext(ctx, *nodeFlag)
	if err != nil {
		return err
	}
	defer client.Close()

	head, err := client.BlockNumber(ctx)
	if err != nil {
		return err
	}
	log.Printf("head=%d", head)

	start := head - *startFlag

	contractAddress := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48") // USDC ERC20
	eventlog := events.NewInMemoryEventLog(start, ethereum.FilterQuery{
		Addresses: []common.Address{contractAddress},
	})
	cs := events.ChainStreamer{
		Ctx:            ctx,
		Url:            *nodeFlag,
		FetchTxDetails: *txFlag,
	}
	livelog := events.NewLiveEventLog(eventlog, cs)

	done := make(chan struct{})
	sub, err := livelog.Stream(done, start)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filepath.Join(*outputFlag, "streamed.txt"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	lastCheckpoint := start
	for m := range sub.C {
		switch m.Action {
		case events.Append:
			file.WriteString(fmt.Sprintf("Block %d\n", m.Block.Number))
			for _, ev := range m.Block.Events {
				if *txFlag {
					file.WriteString(fmt.Sprintf("  %d/%d (%d bytes) %s\n", ev.BlockNumber, ev.Index, len(ev.TxData), ev.TxFrom.Hex()))
				} else {
					file.WriteString(fmt.Sprintf("  %d/%d %s\n", ev.BlockNumber, ev.Index, ev.BlockHash.Hex()))
				}
			}

			if m.Block.Number > lastCheckpoint+10 {
				if err := saveProto(
					eventlog.ToProto(),
					filepath.Join(*outputFlag, fmt.Sprintf("eventlog-%d.pb", m.Block.Number))); err != nil {
					return err
				}
				lastCheckpoint = m.Block.Number
			}

		case events.Rollback:
			file.WriteString(fmt.Sprintf("Rollback %d\n", m.Number))
		case events.SetNext:
			file.WriteString(fmt.Sprintf("SetNext %d\n", m.Number))
		}
	}
	if err := <-sub.Err; err != nil {
		if errors.Is(err, events.Canceled) {
			log.Println("got canceled err -- OK")
		} else {
			return err
		}
	}

	return nil
}

func saveProto(m proto.Message, fn string) error {
	bs, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	if err := os.WriteFile(fn, bs, 0644); err != nil {
		return err
	}
	return nil
}

func dumpEventLog(l events.EventLog, fn string) error {
	file, err := os.OpenFile(filepath.Join(*outputFlag, fn), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	done := make(chan struct{})
	defer close(done)
	sub, err := l.Stream(done, l.FirstBlock())
	if err != nil {
		return err
	}
	for m := range sub.C {
		switch m.Action {
		case events.Append:
			_, err := file.WriteString(fmt.Sprintf("Block %d\n", m.Block.Number))
			if err != nil {
				return err
			}
			for _, e := range m.Block.Events {
				_, err := file.WriteString(fmt.Sprintf("%d/%d %s %s\n", e.BlockNumber, e.Index, e.BlockHash.Hex(), e.TxFrom.Hex()))
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func main() {

	log.SetFlags(log.Lmsgprefix | log.Lshortfile)
	flag.Parse()

	if err := run(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
