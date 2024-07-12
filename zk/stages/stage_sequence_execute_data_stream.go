package stages

import (
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	verifier "github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"context"
)

type SequencerBatchStreamWriter struct {
	ctx           context.Context
	db            kv.RwDB
	logPrefix     string
	batchVerifier *BatchVerifier
	sdb           *stageDb
	streamServer  *server.DataStreamServer
	hasExecutors  bool
}

func (sbc *SequencerBatchStreamWriter) CheckForUpdates(lastBatch uint64) (bool, error) {
	responses, err := sbc.batchVerifier.CheckProgress()
	if err != nil {
		return false, err
	}

	if len(responses) == 0 {
		return false, nil
	}

	newTx, err := sbc.writeBlockDetails(lastBatch, responses)
	if err != nil {
		return false, err
	}

	sbc.sdb.SetTx(newTx)

	return true, nil
}

func (sbc *SequencerBatchStreamWriter) writeBlockDetails(lastBatch uint64, verifiedResponses []*verifier.VerifierResponse) (kv.RwTx, error) {
	if !sbc.hasExecutors {
		for _, response := range verifiedResponses {
			err := sbc.sdb.hermezDb.WriteBatchCounters(response.BatchNumber, response.OriginalCounters)
			if err != nil {
				return nil, err
			}

			err = sbc.sdb.hermezDb.WriteIsBatchPartiallyProcessed(response.BatchNumber)
			if err != nil {
				return nil, err
			}

			if err = sbc.streamServer.WriteBlockToStream(sbc.logPrefix, sbc.sdb.tx, sbc.sdb.hermezDb, response.BatchNumber, lastBatch, response.BlockNumber); err != nil {
				return nil, err
			}
		}
	}

	return sbc.updateTransactionFromVerifierResponses(verifiedResponses)
}

func (sbc *SequencerBatchStreamWriter) updateTransactionFromVerifierResponses(
	verifierResponses []*verifier.VerifierResponse,
) (kv.RwTx, error) {
	if len(verifierResponses) == 0 {
		return nil, nil
	}

	if err := sbc.sdb.tx.Commit(); err != nil {
		return nil, err
	}

	newTx, err := sbc.db.BeginRw(sbc.ctx)
	if err != nil {
		return nil, err
	}

	return newTx, nil
}
