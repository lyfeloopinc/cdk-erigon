package stages

import (
	"sync/atomic"

	"github.com/ledgerwatch/erigon/zk/datastream/client"
	"github.com/ledgerwatch/erigon/zk/datastream/slice_manager"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
)

type TestDatastreamClient struct {
	fullL2Blocks          []types.FullL2Block
	gerUpdates            []types.GerUpdate
	lastWrittenTimeAtomic atomic.Int64
	highestL2Block        atomic.Uint64
	streamingAtomic       atomic.Bool
	progress              atomic.Uint64
	errChan               chan error
	isStarted             bool
	sliceManager          *slice_manager.SliceManager
}

func NewTestDatastreamClient(fullL2Blocks []types.FullL2Block, gerUpdates []types.GerUpdate) *TestDatastreamClient {
	client := &TestDatastreamClient{
		fullL2Blocks: fullL2Blocks,
		gerUpdates:   gerUpdates,
		errChan:      make(chan error, 100),
	}

	client.sliceManager = slice_manager.NewSliceManager()

	highestL2Block := 0
	for _, block := range fullL2Blocks {
		if int(block.L2BlockNumber) > highestL2Block {
			highestL2Block = int(block.L2BlockNumber)
		}
		blockCopy := block
		client.sliceManager.AddItem(&blockCopy)
	}

	client.highestL2Block.Store(uint64(highestL2Block))

	for _, update := range gerUpdates {
		client.sliceManager.AddItem(&update)
	}

	return client
}

func (c *TestDatastreamClient) EnsureConnected() (bool, error) {
	return true, nil
}

func (c *TestDatastreamClient) ReadAllEntriesToChannel() error {
	return nil
}

func (c *TestDatastreamClient) GetErrChan() chan error {
	return c.errChan
}

func (c *TestDatastreamClient) GetL2BlockByNumber(blockNum uint64) (*types.FullL2Block, int, error) {
	for _, l2Block := range c.fullL2Blocks {
		if l2Block.L2BlockNumber == blockNum {
			return &l2Block, types.CmdErrOK, nil
		}
	}

	return nil, -1, nil
}

func (c *TestDatastreamClient) GetLatestL2Block() (*types.FullL2Block, error) {
	if len(c.fullL2Blocks) == 0 {
		return nil, nil
	}
	return &c.fullL2Blocks[len(c.fullL2Blocks)-1], nil
}

func (c *TestDatastreamClient) GetLastWrittenTimeAtomic() *atomic.Int64 {
	return &c.lastWrittenTimeAtomic
}
func (c *TestDatastreamClient) GetHighestL2BlockAtomic() *atomic.Uint64 { return &c.highestL2Block }
func (c *TestDatastreamClient) GetStatus() client.Status                { return client.Status{} }
func (c *TestDatastreamClient) UpdateProgress(u uint64)                 { c.progress.Store(u) }
func (c *TestDatastreamClient) GetStreamingAtomic() *atomic.Bool {
	return &c.streamingAtomic
}

func (c *TestDatastreamClient) GetProgressAtomic() *atomic.Uint64 {
	return &c.progress
}

func (c *TestDatastreamClient) ReadBatches(start uint64, end uint64) ([][]*types.FullL2Block, error) {
	return nil, nil
}

func (c *TestDatastreamClient) Start() error {
	c.isStarted = true
	return nil
}

func (c *TestDatastreamClient) Stop() {
	c.isStarted = false
}

func (c *TestDatastreamClient) GetSliceManager() *slice_manager.SliceManager {
	return nil
}
