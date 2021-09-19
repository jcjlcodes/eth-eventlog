package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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

type LogTransfer struct {
	From   common.Address
	To     common.Address
	Tokens *big.Int
}

func run() error {

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
	fmt.Printf("head=%d\n", head)

	contractAddress := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48") // USDC ERC20
	eventlog := events.NewInMemoryEventLog(head-30, ethereum.FilterQuery{
		Addresses: []common.Address{contractAddress},
	})
	cs := events.ChainStreamer{
		Ctx:            ctx,
		Url:            *nodeFlag,
		FetchTxDetails: false,
	}
	livelog := events.NewLiveEventLog(eventlog, cs)

	done := make(chan struct{})
	sub, err := livelog.Stream(done, head-30)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(*outputFlag, "streamed.txt"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Println()
	endAt := uint64(0)
	for m := range sub.C {
		switch m.Action {
		case events.Append:
			file.WriteString(fmt.Sprintf("Block %d\n", m.Block.Number))
			for _, ev := range m.Block.Events {
				file.WriteString(fmt.Sprintf("  %d/%d %s\n", ev.BlockNumber, ev.Index, ev.BlockHash.Hex()))
			}
		case events.Rollback:
			file.WriteString(fmt.Sprintf("Rollback %d\n", m.Number))
			endAt = m.Number + 20
		}
		if endAt != 0 && eventlog.NextBlock() >= endAt {
			fmt.Println("Ending subscription")
			close(done)
		}
	}
	if err := <-sub.Err; err != nil {
		if errors.Is(err, events.Canceled) {
			fmt.Println("got canceled err -- OK")
		} else {
			return err
		}
	}

	fmt.Println("Dumping eventlog")
	if err := dumpEventLog(eventlog, "eventlog.txt"); err != nil {
		return err
	}
	if err := saveProto(eventlog.ToProto(), filepath.Join(*outputFlag, "eventlog.pb")); err != nil {
		return err
	}

	fmt.Println("Done")
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
		_, err := file.WriteString(fmt.Sprintf("Block %d\n", m.Block.Number))
		if err != nil {
			return err
		}
		for _, e := range m.Block.Events {
			_, err := file.WriteString(fmt.Sprintf("%d/%d %s\n", e.BlockNumber, e.Index, e.BlockHash.Hex()))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func main() {

	flag.Parse()

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
