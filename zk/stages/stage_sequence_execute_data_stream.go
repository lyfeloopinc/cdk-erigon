package stages

import (
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
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
	//overlay       *memdb.MemoryMutation
}

func (sbc *SequencerBatchStreamWriter) CheckAndCommitUpdates(lastBatch uint64) error {
	responses, err := sbc.batchVerifier.CheckProgress()
	if err != nil {
		return err
	}

	if len(responses) == 0 {
		return nil
	}

	err = sbc.writeBlockDetails(lastBatch, responses)
	if err != nil {
		return err
	}

	return nil
}

func (sbc *SequencerBatchStreamWriter) writeBlockDetails(lastBatch uint64, verifiedBundles []*BundleWithTransaction) error {
	if !sbc.hasExecutors {
		for _, bundle := range verifiedBundles {
			response := bundle.Bundle.Response
			err := sbc.sdb.hermezDb.WriteBatchCounters(response.BatchNumber, response.OriginalCounters)
			if err != nil {
				return err
			}

			err = sbc.sdb.hermezDb.WriteIsBatchPartiallyProcessed(response.BatchNumber)
			if err != nil {
				return err
			}

			if err = sbc.streamServer.WriteBlockToStream(sbc.logPrefix, sbc.sdb.tx, sbc.sdb.hermezDb, response.BatchNumber, lastBatch, response.BlockNumber); err != nil {
				return err
			}

			// we can commit the nested transaction here if we know the data is good otherwise roll it back and return an error
			if response.Valid {
				//err = bundle.Tx.Commit()
				//if err != nil {
				//	return err
				//}
			} else {
				return response.Error
			}
		}
	}

	return nil
}
