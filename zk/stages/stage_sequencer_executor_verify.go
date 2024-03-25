package stages

import (
	"context"
	"sort"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"fmt"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/log/v3"
)

type SequencerExecutorVerifyCfg struct {
	db       kv.RwDB
	verifier *legacy_executor_verifier.LegacyExecutorVerifier
	txPool   *txpool.TxPool
	limbo    *legacy_executor_verifier.Limbo
}

func StageSequencerExecutorVerifyCfg(
	db kv.RwDB,
	verifier *legacy_executor_verifier.LegacyExecutorVerifier,
	txPool *txpool.TxPool,
	limbo *legacy_executor_verifier.Limbo,
) SequencerExecutorVerifyCfg {
	return SequencerExecutorVerifyCfg{
		db:       db,
		verifier: verifier,
		txPool:   txPool,
		limbo:    limbo,
	}
}

func SpawnSequencerExecutorVerifyStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerExecutorVerifyCfg,
	initialCycle bool,
	quiet bool,
) error {
	var err error
	freshTx := tx == nil
	if freshTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	hermezDb := hermez_db.NewHermezDb(tx)

	// progress here is at the batch level
	progress, err := stages.GetStageProgress(tx, stages.SequenceExecutorVerify)
	if err != nil {
		return err
	}

	// if we're in limbo mode at the start of this stage then we can just skip it as
	// the execution stage will be handling verifications synchronously at that point
	inLimbo, _, limboBatch := cfg.limbo.CheckLimboMode()
	if inLimbo {
		return nil
	}

	// get the latest responses from the verifier then sort them, so we can make sure we're handling verifications
	// in order
	responses := cfg.verifier.GetAllResponses()

	// sort responses by batch number in ascending order
	sort.Slice(responses, func(i, j int) bool {
		return responses[i].BatchNumber < responses[j].BatchNumber
	})

	for _, response := range responses {
		// ensure that the first response is the next batch based on the current stage progress
		// otherwise just return early until we get it
		if response.BatchNumber != progress+1 {
			return nil
		}

		// now check that we are indeed in a good state to continue
		if !response.Valid {
			// [limbo] collect failing batches

		}

		// store the witness
		errWitness := hermezDb.WriteWitness(response.BatchNumber, response.Witness)
		if errWitness != nil {
			log.Warn("Failed to write witness", "batch", response.BatchNumber, "err", errWitness)
		}

		// now let the verifier know we have got this message, so it can release it
		cfg.verifier.RemoveResponse(response.BatchNumber)
		progress = response.BatchNumber
	}

	// update stage progress batch number to 'progress'
	if err = stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, progress); err != nil {
		return err
	}

	// progress here is at the block level
	intersProgress, err := stages.GetStageProgress(tx, stages.IntermediateHashes)
	if err != nil {
		return err
	}

	// we need to get the batch number for the latest block, so we can search for new batches to send for
	// verification
	intersBatch, err := hermezDb.GetBatchNoByL2Block(intersProgress)
	if err != nil {
		return err
	}

	// send off the new batches to the verifier to be processed
	for batch := progress + 1; batch <= intersBatch; batch++ {
		// we do not need to verify batch 1 as this is the injected batch so just updated progress and move on
		if batch == injectedBatchNumber {
			if err = stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, injectedBatchNumber); err != nil {
				return err
			}
		} else {
			// we need the state root of the last block in the batch to send to the executor
			blocks, err := hermezDb.GetL2BlockNosByBatch(batch)
			if err != nil {
				return err
			}
			sort.Slice(blocks, func(i, j int) bool {
				return blocks[i] > blocks[j]
			})
			lastBlockNumber := blocks[0]
			block, err := rawdb.ReadBlockByNumber(tx, lastBlockNumber)
			if err != nil {
				return err
			}

			if inLimbo {
				result, err2 := cfg.verifier.VerifySynchronously(tx, &legacy_executor_verifier.VerifierRequest{BatchNumber: batch, StateRoot: block.Root()})
				if err2 != nil {
					return err2
				}

				if !result.Valid {
					txs := block.Body().Transactions
					if len(txs) > 1 {
						return fmt.Errorf("block %d has more than 1 tx in limbo", lastBlockNumber)
					}

					tx0 := txs[0]

					// [limbo] - txpool - remove the offending tx, and add back the previously yielded txs
					cfg.txPool.RemoveUnverifiedTx(tx0.Hash())
					cfg.txPool.PutBackYieldedNotVerifiedTxs()

					cfg.limbo.ExitLimboMode()

					// unwind node to known last good block
					lastGoodBlockNumber, err := hermezDb.GetHighestBlockInBatch(limboBatch - 1)
					if err != nil {
						return err
					}
					u.UnwindTo(lastGoodBlockNumber, libcommon.Hash{})
					return nil
				}
			} else {
				cfg.verifier.AddRequest(&legacy_executor_verifier.VerifierRequest{BatchNumber: batch, StateRoot: block.Root()})
			}
		}
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindSequencerExecutorVerifyStage(
	u *stagedsync.UnwindState,
	s *stagedsync.StageState,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerExecutorVerifyCfg,
	initialCycle bool,
) error {
	// TODO [limbo] implement unwind - do we need it? this only has stage progress...
	return nil
}

func PruneSequencerExecutorVerifyStage(
	s *stagedsync.PruneState,
	tx kv.RwTx,
	cfg SequencerExecutorVerifyCfg,
	ctx context.Context,
	initialCycle bool,
) error {
	return nil
}
