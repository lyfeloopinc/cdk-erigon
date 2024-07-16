package stages

import (
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"context"
	verifier "github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
)

type SequencerBatchStreamWriter struct {
	ctx           context.Context
	db            kv.RwDB
	logPrefix     string
	batchVerifier *BatchVerifier
	sdb           *stageDb
	streamServer  *server.DataStreamServer
	hasExecutors  bool
	lastBatch     uint64
}

type BlockStatus struct {
	BlockNumber uint64
	Valid       bool
	Error       error
}

func (sbc *SequencerBatchStreamWriter) CheckAndCommitUpdates() ([]BlockStatus, int, error) {
	var written []BlockStatus
	responses, remaining, err := sbc.batchVerifier.CheckProgress()
	if err != nil {
		return written, remaining, err
	}

	if len(responses) == 0 {
		return written, remaining, nil
	}

	written, err = sbc.writeBlockDetails(responses)
	if err != nil {
		return written, remaining, err
	}

	return written, remaining, nil
}

func (sbc *SequencerBatchStreamWriter) writeBlockDetails(verifiedBundles []*verifier.VerifierBundle) ([]BlockStatus, error) {
	var written []BlockStatus
	for _, bundle := range verifiedBundles {
		response := bundle.Response

		if response.Valid {
			if err := sbc.streamServer.WriteBlockToStream(sbc.logPrefix, sbc.sdb.tx, sbc.sdb.hermezDb, response.BatchNumber, sbc.lastBatch, response.BlockNumber); err != nil {
				return written, err
			}

			// once we have handled the very first block we can update the last batch to be the current batch safely so that
			// we don't keep adding batch bookmarks in between blocks
			sbc.lastBatch = response.BatchNumber
		}

		status := BlockStatus{
			BlockNumber: response.BlockNumber,
			Valid:       response.Valid,
			Error:       response.Error,
		}

		written = append(written, status)

		// just break early if there is an invalid response as we don't want to process the remainder anyway
		if !response.Valid {
			break
		}
	}

	return written, nil
}
