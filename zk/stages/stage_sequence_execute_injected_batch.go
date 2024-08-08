package stages

import (
	"fmt"
	"math"

	"errors"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/zk/l1infotree"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/erigon/zk/utils"
)

const (
	injectedBatchBlockNumber = 1
	injectedBatchBatchNumber = 1
)

func processInjectedInitialBatch(
	batchContext *BatchContext,
	batchState *BatchState,
) error {
	// set the block height for the fork we're running at to ensure contract interactions are correct
	if err := utils.RecoverySetBlockConfigForks(injectedBatchBlockNumber, batchState.forkId, batchContext.cfg.chainConfig, batchContext.s.LogPrefix()); err != nil {
		return err
	}

	header, parentBlock, err := prepareHeader(batchContext.sdb.tx, 0, math.MaxUint64, math.MaxUint64, batchState.forkId, batchContext.cfg.zk.AddressSequencer)
	if err != nil {
		return err
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(batchContext.sdb.tx, hash, number)
	}
	getHashFn := core.GetHashFn(header, getHeader)
	blockContext := core.NewEVMBlockContext(header, getHashFn, batchContext.cfg.engine, &batchContext.cfg.zk.AddressSequencer, parentBlock.ExcessDataGas())

	injected, err := batchContext.sdb.hermezDb.GetL1InjectedBatch(0)
	if err != nil {
		return err
	}

	fakeL1TreeUpdate := &zktypes.L1InfoTreeUpdate{
		GER:        injected.LastGlobalExitRoot,
		ParentHash: injected.L1ParentHash,
		Timestamp:  injected.Timestamp,
	}

	ibs := state.New(batchContext.sdb.stateReader)

	// the injected batch block timestamp should also match that of the injected batch
	header.Time = injected.Timestamp

	parentRoot := parentBlock.Root()
	if err = handleStateForNewBlockStarting(batchContext, ibs, injectedBatchBlockNumber, injectedBatchBatchNumber, injected.Timestamp, &parentRoot, fakeL1TreeUpdate, true); err != nil {
		return err
	}

	txn, receipt, execResult, effectiveGas, accInputHash, err := handleInjectedBatch(batchContext, ibs, &blockContext, injected, header, parentBlock, batchState.forkId)
	if err != nil {
		return err
	}

	batchState.blockState.builtBlockElements = BuiltBlockElements{
		transactions:     types.Transactions{*txn},
		receipts:         types.Receipts{receipt},
		executionResults: []*core.ExecutionResult{execResult},
		effectiveGases:   []uint8{effectiveGas},
	}
	batchCounters := vm.NewBatchCounterCollector(batchContext.sdb.smt.GetDepth(), uint16(batchState.forkId), batchContext.cfg.zk.VirtualCountersSmtReduction, batchContext.cfg.zk.ShouldCountersBeUnlimited(batchState.isL1Recovery()), nil)

	if _, err = doFinishBlockAndUpdateState(batchContext, ibs, header, parentBlock, batchState, injected.LastGlobalExitRoot, injected.L1ParentHash, 0, 0, batchCounters); err != nil {
		return err
	}

	batchContext.sdb.hermezDb.WriteAccInputHash(batchState.batchNumber, accInputHash)

	// deleting the partially processed flag
	return batchContext.sdb.hermezDb.DeleteIsBatchPartiallyProcessed(injectedBatchBatchNumber)
}

func handleInjectedBatch(
	batchContext *BatchContext,
	ibs *state.IntraBlockState,
	blockContext *evmtypes.BlockContext,
	injected *zktypes.L1InjectedBatch,
	header *types.Header,
	parentBlock *types.Block,
	forkId uint64,
) (*types.Transaction, *types.Receipt, *core.ExecutionResult, uint8, *common.Hash, error) {
	decodedBlocks, err := zktx.DecodeBatchL2Blocks(injected.Transaction, forkId)
	if err != nil {
		return nil, nil, nil, 0, nil, err
	}
	if len(decodedBlocks) == 0 || len(decodedBlocks) > 1 {
		return nil, nil, nil, 0, nil, errors.New("expected 1 block for the injected batch")
	}
	if len(decodedBlocks[0].Transactions) == 0 {
		return nil, nil, nil, 0, nil, errors.New("expected 1 transaction in the injected batch")
	}

	batchCounters := vm.NewBatchCounterCollector(batchContext.sdb.smt.GetDepth(), uint16(forkId), batchContext.cfg.zk.VirtualCountersSmtReduction, batchContext.cfg.zk.ShouldCountersBeUnlimited(false), nil)

	// process the tx and we can ignore the counters as an overflow at this stage means no network anyway
	effectiveGas := DeriveEffectiveGasPrice(*batchContext.cfg, decodedBlocks[0].Transactions[0])
	receipt, execResult, _, err := attemptAddTransaction(*batchContext.cfg, batchContext.sdb, ibs, batchCounters, blockContext, header, decodedBlocks[0].Transactions[0], effectiveGas, false, forkId, 0 /* use 0 for l1InfoIndex in injected batch */, nil)
	if err != nil {
		return nil, nil, nil, 0, nil, err
	}

	accInputHash, err := calcInjectedAccInputHash(injected, &decodedBlocks[0].Transactions[0], effectiveGas, forkId)

	return &decodedBlocks[0].Transactions[0], receipt, execResult, effectiveGas, &accInputHash, nil
}

func calcInjectedAccInputHash(injected *zktypes.L1InjectedBatch, transaction *types.Transaction, effectiveGas uint8, forkId uint64) (accinputHash common.Hash, err error) {
	batchRaw := l1infotree.BatchRawV2{
		Blocks: []l1infotree.L2BlockRaw{
			// no block previously - should this be 0 or the block timestamp itself?
			// use 0 for l1InfoIndex in injected batch
			*createBlockRaw(0, 0, uint16(forkId), []types.Transaction{*transaction}, []uint8{effectiveGas}),
		},
	}

	batchL2Data, err := l1infotree.EncodeBatchV2(&batchRaw)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to encode batch: %w", err)
	}

	return l1infotree.CalculateAccInputHash(common.Hash{}, batchL2Data, l1InfoRoot, injected.Timestamp, injected.Sequencer, common.Hash{} /* this has value only for the injected batch*/)
}
