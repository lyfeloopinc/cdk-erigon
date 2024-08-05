package stages

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

type TestDatastreamClient struct {
	fullL2Blocks          []types.FullL2Block
	gerUpdates            []types.GerUpdate
	lastWrittenTimeAtomic atomic.Int64
	streamingAtomic       atomic.Bool
	progress              atomic.Uint64
	l2BlockChan           chan types.FullL2Block
	l2TxChan              chan types.L2TransactionProto
	gerUpdatesChan        chan types.GerUpdate
	errChan               chan error
	batchStartChan        chan types.BatchStart
	batchEndChan          chan types.BatchEnd
}

func NewTestDatastreamClient(fullL2Blocks []types.FullL2Block, gerUpdates []types.GerUpdate) *TestDatastreamClient {
	client := &TestDatastreamClient{
		fullL2Blocks:   fullL2Blocks,
		gerUpdates:     gerUpdates,
		l2BlockChan:    make(chan types.FullL2Block, 100),
		gerUpdatesChan: make(chan types.GerUpdate, 100),
		errChan:        make(chan error, 100),
		batchStartChan: make(chan types.BatchStart, 100),
	}

	return client
}

func (c *TestDatastreamClient) EnsureConnected() (bool, error) {
	return true, nil
}

func (c *TestDatastreamClient) ReadAllEntriesToChannel() error {
	c.streamingAtomic.Store(true)

	for _, block := range c.fullL2Blocks {
		c.l2BlockChan <- block
	}
	for _, update := range c.gerUpdates {
		c.gerUpdatesChan <- update
	}

	return nil
}

func (c *TestDatastreamClient) GetL2BlockChan() chan types.FullL2Block {
	return c.l2BlockChan
}

func (c *TestDatastreamClient) GetL2BlockByNumber(blockNum uint64) (*types.FullL2Block, error) {
	for _, l2Block := range c.fullL2Blocks {
		if l2Block.L2BlockNumber == blockNum {
			return &l2Block, nil
		}
	}

	return nil, nil
}

func (c *TestDatastreamClient) GetL2TxChan() chan types.L2TransactionProto {
	return c.l2TxChan
}

func (c *TestDatastreamClient) GetGerUpdatesChan() chan types.GerUpdate {
	return c.gerUpdatesChan
}

func (c *TestDatastreamClient) GetErrChan() chan error {
	return c.errChan
}

func (c *TestDatastreamClient) GetBatchStartChan() chan types.BatchStart {
	return c.batchStartChan
}

func (c *TestDatastreamClient) GetBatchEndChan() chan types.BatchEnd {
	return c.batchEndChan
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

type TestErigonDbReader struct {
	ctx context.Context
	db  kv.RwDB
}

func NewTestErigonDbReader(ctx context.Context, db kv.RwDB) *TestErigonDbReader {
	return &TestErigonDbReader{ctx: ctx, db: db}
}

func (r *TestErigonDbReader) GetBodyTransactions(fromBlockNo uint64, toBlockNo uint64) (*[]ethTypes.Transaction, error) {
	var (
		txs *[]ethTypes.Transaction
		err error
	)
	err = r.db.Update(r.ctx, func(tx kv.RwTx) error {
		rawdb.GetBodyTransactions(tx, fromBlockNo, toBlockNo)
		return nil
	})
	return txs, err
}

func (r *TestErigonDbReader) ReadCanonicalHash(blockNo uint64) (common.Hash, error) {
	var (
		hash common.Hash
		err  error
	)
	err = r.db.View(r.ctx, func(tx kv.Tx) error {
		hash, err = rawdb.ReadCanonicalHash(tx, blockNo)
		return nil
	})
	return hash, err
}

func (r *TestErigonDbReader) GetHeader(blockNo uint64) (*ethTypes.Header, error) {
	var (
		header *ethTypes.Header
		err    error
	)
	err = r.db.View(r.ctx, func(tx kv.Tx) error {
		hash, err := rawdb.ReadCanonicalHash(tx, blockNo)
		if err != nil {
			return fmt.Errorf("failed to read canonical hash: %w", err)
		}

		header = rawdb.ReadHeader(tx, hash, blockNo)
		return nil
	})
	return header, err
}

type TestHermezDbReaderWrapper struct {
	ctx    context.Context
	reader *hermez_db.HermezDbReader
	db     kv.RwDB
}

func NewTestHermezDbReaderWrapper(ctx context.Context, reader *hermez_db.HermezDbReader, db kv.RwDB) *TestHermezDbReaderWrapper {
	return &TestHermezDbReaderWrapper{
		ctx:    ctx,
		reader: reader,
		db:     db,
	}
}

func (r *TestHermezDbReaderWrapper) GetEffectiveGasPricePercentage(txHash common.Hash) (uint8, error) {
	panic("not implemented") // TODO: Implement
}

func (r *TestHermezDbReaderWrapper) GetStateRoot(l2BlockNo uint64) (common.Hash, error) {
	panic("not implemented") // TODO: Implement
}

func (r *TestHermezDbReaderWrapper) GetBatchNoByL2Block(l2BlockNo uint64) (uint64, error) {
	var (
		batchNo uint64
		err     error
	)
	err = r.db.View(r.ctx, func(tx kv.Tx) error {
		c, err := tx.Cursor(hermez_db.BLOCKBATCHES)
		if err != nil {
			return err
		}
		defer c.Close()

		k, v, err := c.Seek(hermez_db.Uint64ToBytes(l2BlockNo))
		if err != nil {
			return err
		}

		if k == nil {
			return nil
		}

		if hermez_db.BytesToUint64(k) != l2BlockNo {
			return nil
		}

		batchNo = hermez_db.BytesToUint64(v)
		return nil
	})

	return batchNo, err
}

func (r *TestHermezDbReaderWrapper) GetBatchGlobalExitRoots(fromBatchNum uint64, toBatchNum uint64) (*[]types.GerUpdate, error) {
	panic("not implemented") // TODO: Implement
}

func (r *TestHermezDbReaderWrapper) GetBlockGlobalExitRoot(l2BlockNo uint64) (common.Hash, error) {
	panic("not implemented") // TODO: Implement
}

func (r *TestHermezDbReaderWrapper) GetBlockL1BlockHash(l2BlockNo uint64) (common.Hash, error) {
	panic("not implemented") // TODO: Implement
}

func (r *TestHermezDbReaderWrapper) GetGerForL1BlockHash(l1BlockHash common.Hash) (common.Hash, error) {
	panic("not implemented") // TODO: Implement
}

func (r *TestHermezDbReaderWrapper) GetIntermediateTxStateRoot(blockNum uint64, txhash common.Hash) (common.Hash, error) {
	panic("not implemented") // TODO: Implement
}

func (r *TestHermezDbReaderWrapper) GetReusedL1InfoTreeIndex(blockNum uint64) (bool, error) {
	panic("not implemented") // TODO: Implement
}
