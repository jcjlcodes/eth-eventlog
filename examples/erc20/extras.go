package erc20

import (
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/core/types"
)

// Erc20Contract
type Erc20Contract struct {
	abi *abi.ABI
}

func NewErc20Contract() (*Erc20Contract, error) {
	a, err := abi.JSON(strings.NewReader(Erc20ABI))
	if err != nil {
		return nil, err
	}
	return &Erc20Contract{abi: &a}, nil
}

// UnpackLog unpacks a retrieved log into the provided output structure.
func (c *Erc20Contract) UnpackLog(out interface{}, event string, log types.Log) error {
	if len(log.Data) > 0 {
		if err := c.abi.UnpackIntoInterface(out, event, log.Data); err != nil {
			return err
		}
	}
	var indexed abi.Arguments
	for _, arg := range c.abi.Events[event].Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}
	return abi.ParseTopics(out, indexed, log.Topics[1:])
}

// ParseTransfer is a log parse operation binding the contract event 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef.
//
// Solidity: event Transfer(address indexed from, address indexed to, uint256 tokens)
func (c *Erc20Contract) UnpackTransfer(log types.Log) (*Erc20Transfer, error) {
	event := new(Erc20Transfer)
	if err := c.UnpackLog(event, "Transfer", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
