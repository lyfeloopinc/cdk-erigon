package stages

import (
	"context"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/log/v3"
)

type SequencerInterhashesCfg struct {
	db          kv.RwDB
	accumulator *shards.Accumulator
}

func StageSequencerInterhashesCfg(
	db kv.RwDB,
	accumulator *shards.Accumulator,
) SequencerInterhashesCfg {
	return SequencerInterhashesCfg{
		db:          db,
		accumulator: accumulator,
	}
}

func SpawnSequencerInterhashesStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerInterhashesCfg,
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

	to, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	if err := s.Update(tx, to); err != nil {
		return err
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindSequencerInterhashesStage(
	u *stagedsync.UnwindState,
	s *stagedsync.StageState,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerInterhashesCfg,
	initialCycle bool,
) error {
	log.Info(fmt.Sprintf("[%s] Unwinding sequencer interhashes", s.LogPrefix()), "block", u.UnwindPoint)

	from, err := stages.GetStageProgress(tx, stages.IntermediateHashes)
	if err != nil {
		return err
	}

	toBlock, err := rawdb.ReadBlockByNumber(tx, u.UnwindPoint)
	if err != nil {
		return err
	}
	expectedHash := toBlock.Hash()

	// SMT
	_, err = unwindZkSMT(s.LogPrefix(), from, u.UnwindPoint, tx, true, &expectedHash, ctx.Done())
	if err != nil {
		return err

	}

	// Stage state
	if err := stages.SaveStageProgress(tx, stages.IntermediateHashes, u.UnwindPoint); err != nil {
		return err

	}

	return nil
}

func PruneSequencerInterhashesStage(
	s *stagedsync.PruneState,
	tx kv.RwTx,
	cfg SequencerInterhashesCfg,
	ctx context.Context,
	initialCycle bool,
) error {
	log.Warn("Pruning SequencerInterhashes not implemented")
	return nil
}
