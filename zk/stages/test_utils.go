package stages

import (
	"sync/atomic"

	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/datastream/slice_manager"
	"github.com/ledgerwatch/erigon/zk/datastream/client"
)

type TestDatastreamClient struct {
	fullL2Blocks          []types.FullL2Block
	gerUpdates            []types.GerUpdate
	lastWrittenTimeAtomic atomic.Int64
	streamingAtomic       atomic.Bool
	progress              atomic.Uint64
	entriesChan           chan interface{}
	errChan               chan error
}

func (c *TestDatastreamClient) GetStatus() client.Status {
	//TODO implement me
	panic("implement me")
}

func (c *TestDatastreamClient) UpdateProgress(u uint64) {
	//TODO implement me
	panic("implement me")
}

func NewTestDatastreamClient(fullL2Blocks []types.FullL2Block, gerUpdates []types.GerUpdate) *TestDatastreamClient {
	client := &TestDatastreamClient{
		fullL2Blocks: fullL2Blocks,
		gerUpdates:   gerUpdates,
		entriesChan:  make(chan interface{}, 1000),
		errChan:      make(chan error, 100),
	}

	return client
}

func (c *TestDatastreamClient) EnsureConnected() (bool, error) {
	return true, nil
}

func (c *TestDatastreamClient) ReadAllEntriesToChannel() error {
	c.streamingAtomic.Store(true)

	for i, _ := range c.fullL2Blocks {
		c.entriesChan <- &c.fullL2Blocks[i]
	}
	for i, _ := range c.gerUpdates {
		c.entriesChan <- &c.gerUpdates[i]
	}

	return nil
}

func (c *TestDatastreamClient) GetEntryChan() chan interface{} {
	return c.entriesChan
}

func (c *TestDatastreamClient) GetErrChan() chan error {
	return c.errChan
}

func (c *TestDatastreamClient) GetLastWrittenTimeAtomic() *atomic.Int64 {
	return &c.lastWrittenTimeAtomic
}
func (c *TestDatastreamClient) GetStreamingAtomic() *atomic.Bool {
	return &c.streamingAtomic
}
func (c *TestDatastreamClient) GetProgressAtomic() *atomic.Uint64 {
	return &c.progress
}
func (c *TestDatastreamClient) GetSliceManager() *slice_manager.SliceManager {
	return nil
}
