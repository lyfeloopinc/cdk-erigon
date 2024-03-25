package stages

import (
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon-lib/kv"
	"context"
	"fmt"
	"github.com/ledgerwatch/log/v3"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

type LimboCfg struct {
	db     kv.RwDB
	limbo  *legacy_executor_verifier.Limbo
	txPool *txpool.TxPool
}

func StageLimboCfg(
	db kv.RwDB,
	limbo *legacy_executor_verifier.Limbo,
	txPool *txpool.TxPool,
) LimboCfg {
	return LimboCfg{
		db:     db,
		limbo:  limbo,
		txPool: txPool,
	}
}

/*
SpawnLimboStage checks on the status of the limbo mode (when we detect a bad batch via the executor).
If we detect that we are in a bad state and have arrived here then we know all other stages have finished their processing
and, we are now in a newly spawned stage loop.

The task here is to take all the transactions from the bad batch onwards and put them into a LimboTransactions collection
and put them back in the pool.  When we get to the sequencer stage next it will detect that the transactions have been
placed there and start to work through them.
*/
func SpawnLimboStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	initialCycle bool,
	cfg LimboCfg,
) (err error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting limbo stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished limbo stage", logPrefix))

	newTx := false
	if tx == nil {
		newTx = true
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	limbo := cfg.limbo
	cond := limbo.StateCond

	isLimbo, limboState, limboBatch := limbo.CheckLimboMode()
	if !isLimbo {
		return nil
	}

	// we have been here before and initiated the unwind already and are in the recovery state, so now
	// we need to continue the loop
	if limboState == legacy_executor_verifier.LimboRecovery {
		return nil
	}

	hermezDb := hermez_db.NewHermezDb(tx)

	// wait until the limbo state is ready to progress
	for {
		isLimbo, limboState, limboBatch = limbo.CheckLimboMode()
		if limboState == legacy_executor_verifier.LimboDrained {
			break
		}
		cond.L.Lock()
		limbo.StateCond.Wait()
		cond.L.Unlock()
	}

	// now that we know the lowest batch number with an issue we need to unwind
	unwindBlock, err := hermezDb.GetHighestBlockInBatch(limboBatch - 1)
	if err != nil {
		return err
	}
	u.UnwindTo(unwindBlock, libcommon.Hash{})

	highestBatch, err := stages.GetStageProgress(tx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	// get all the blocks for each of the batches from the problem batch to the tip
	var batchBlocks [][]uint64
	for i := limboBatch; i <= highestBatch; i++ {
		blocks, err := hermezDb.GetL2BlockNosByBatch(i)
		if err != nil {
			return err
		}
		batchBlocks = append(batchBlocks, blocks)
	}

	// now get all the transactions for each of these blocks
	for idx, blocks := range batchBlocks {
		lowestBlock := blocks[0]
		highestBlock := blocks[len(blocks)-1]
		rawTransactions, err := rawdb.RawTransactionsRange(tx, lowestBlock, highestBlock)
		if err != nil {
			return err
		}
		decodedTransactions, err := types.DecodeTransactions(rawTransactions)
		if err != nil {
			return err
		}
		limboTransactions := txpool.LimboTransactions{
			BatchNumber:  limboBatch + uint64(idx),
			Transactions: decodedTransactions,
		}

		if idx == 0 {
			// first batch which is the known problem batch
			cfg.txPool.SetLimboBadTransactions(limboTransactions)
		} else {
			// unknown if these transactions are good or bad
			cfg.txPool.AddLimboUnknownTransactions(limboTransactions)
		}
	}

	cfg.limbo.StartRecovery()

	if newTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindLimboStage() error {
	return nil
}

func PruneLimboStage() error {
	return nil
}
