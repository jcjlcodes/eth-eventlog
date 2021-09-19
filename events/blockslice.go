package events

import "fmt"

type BlockSlice struct {
	Start            uint64
	End              uint64
	DistanceFromHead uint64
	Blocks           []*Block
}

func EmptyBlockSlice(from uint64) *BlockSlice {
	return &BlockSlice{
		Start:  from,
		End:    from,
		Blocks: make([]*Block, 0),
	}
}

func (b *BlockSlice) Append(blk *Block) error {
	if blk.Number < b.End {
		return fmt.Errorf("got blk.Number=%d; want blk.Number>=%d", blk.Number, b.End)
	}
	b.Blocks = append(b.Blocks, blk)
	b.End = blk.Number + 1
	b.DistanceFromHead = 0
	return nil
}

func (b *BlockSlice) Concat(other *BlockSlice) error {
	if b.End != other.Start {
		return fmt.Errorf("got other.Start=%d; want %d", other.Start, b.End)
	}
	b.Blocks = append(b.Blocks, other.Blocks...)
	b.End = other.End
	b.DistanceFromHead = other.DistanceFromHead
	return nil
}

func (b *BlockSlice) Rollback(n uint64) error {
	if n > b.End {
		return fmt.Errorf("n=%d; want n <= %d", n, b.End)
	}
	if n < b.Start {
		return fmt.Errorf("n=%d; want n >= %d", n, b.Start)
	}
	b.DeleteFromBlock(n)
	return nil
}

func (b *BlockSlice) DeleteBeforeBlock(n uint64) {
	var i int
	for i = 0; i < len(b.Blocks); i++ {
		if b.Blocks[i].Number >= n {
			break
		}
	}
	b.Blocks = b.Blocks[i:]
	b.Start = n
}

func (b *BlockSlice) DeleteFromBlock(n uint64) {
	var i int
	for i = len(b.Blocks) - 1; i >= 0; i-- {
		if b.Blocks[i].Number < n {
			break
		}
	}
	b.Blocks = b.Blocks[:i+1]
	b.DistanceFromHead -= b.End - n
	b.End = n
}

func (b *BlockSlice) Extend(n uint64) error {
	if n < b.End {
		return fmt.Errorf("n=%d; want n >= %d", n, b.End)
	}
	b.End = n
	return nil
}
