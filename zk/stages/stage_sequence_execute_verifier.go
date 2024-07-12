package stages

import (
	verifier "github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"sync"
	"errors"
	"github.com/ledgerwatch/log/v3"
	"fmt"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
)

type PromiseWithTransaction struct {
	Promise *verifier.Promise[*BundleWithTransaction]
	Tx      kv.RwTx
}

type BundleWithTransaction struct {
	Bundle *verifier.VerifierBundle
	Tx     kv.RwTx
}

type BatchVerifier struct {
	hasExecutor bool
	forkId      uint64
	mtxPromises *sync.Mutex
	promises    []*PromiseWithTransaction
	stop        bool
	errors      chan error
}

func NewBatchVerifier(
	hasExecutors bool,
	forkId uint64,
) *BatchVerifier {
	return &BatchVerifier{
		hasExecutor: hasExecutors,
		forkId:      forkId,
		mtxPromises: &sync.Mutex{},
		promises:    make([]*PromiseWithTransaction, 0),
		errors:      make(chan error),
	}
}

func (bv *BatchVerifier) AddNewCheck(
	batchNumber uint64,
	blockNumber uint64,
	stateRoot common.Hash,
	counters map[string]int,
	tx kv.RwTx,
) {
	request := verifier.NewVerifierRequest(batchNumber, blockNumber, bv.forkId, stateRoot, counters)

	var promise *verifier.Promise[*BundleWithTransaction]
	if bv.hasExecutor {
		// todo: implement promise for this
	} else {
		promise = bv.syncPromise(request, tx)
	}

	withTx := &PromiseWithTransaction{
		Promise: promise,
		Tx:      tx,
	}

	bv.appendPromise(withTx)
}

func (bv *BatchVerifier) appendPromise(promise *PromiseWithTransaction) {
	bv.mtxPromises.Lock()
	defer bv.mtxPromises.Unlock()
	bv.promises = append(bv.promises, promise)
}

func (bv *BatchVerifier) CheckProgress() ([]*BundleWithTransaction, error) {
	bv.mtxPromises.Lock()
	defer bv.mtxPromises.Unlock()

	var responses []*BundleWithTransaction

	// not a stop signal, so we can start to process our promises now
	processed := 0
	for idx, promise := range bv.promises {
		bundleWithTx, err := promise.Promise.TryGet()
		if bundleWithTx == nil && err == nil {
			// nothing to process in this promise so we skip it
			break
		}

		if err != nil {
			// let leave it for debug purposes
			// a cancelled promise is removed from v.promises => it should never appear here, that's why let's panic if it happens, because it will indicate for massive error
			if errors.Is(err, verifier.ErrPromiseCancelled) {
				panic("this should never happen")
			}

			log.Error("error on our end while preparing the verification request, re-queueing the task", "err", err)

			if bundleWithTx == nil {
				// we can't proceed here until this promise is attempted again
				break
			}

			if bundleWithTx.Bundle.Request.IsOverdue() {
				// signal an error, the caller can check on this and stop the process if needs be
				return nil, fmt.Errorf("error: batch %d couldn't be processed in 30 minutes", bundleWithTx.Bundle.Request.BatchNumber)
			}

			// re-queue the task - it should be safe to replace the index of the slice here as we only add to it
			if bv.hasExecutor {
				// todo: implement promise for this
			} else {
				prom := bv.syncPromise(bundleWithTx.Bundle.Request, bundleWithTx.Tx)
				bv.promises[idx] = &PromiseWithTransaction{
					Promise: prom,
					Tx:      bundleWithTx.Tx,
				}
			}

			// break now as we know we can't proceed here until this promise is attempted again
			break
		}

		processed++
		responses = append(responses, bundleWithTx)
	}

	// remove processed promises from the list
	bv.removeProcessedPromises(processed)

	return responses, nil
}

func (bv *BatchVerifier) removeProcessedPromises(processed int) {
	if processed == 0 {
		return
	}

	if processed == len(bv.promises) {
		bv.promises = make([]*PromiseWithTransaction, 0)
		return
	}

	bv.promises = bv.promises[processed:]
}

func (bv *BatchVerifier) syncPromise(request *verifier.VerifierRequest, tx kv.RwTx) *verifier.Promise[*BundleWithTransaction] {
	return verifier.NewPromiseSync[*BundleWithTransaction](func() (*BundleWithTransaction, error) {
		response := &verifier.VerifierResponse{
			BatchNumber:      request.BatchNumber,
			BlockNumber:      request.BlockNumber,
			Valid:            true,
			OriginalCounters: request.Counters,
			Witness:          nil,
			ExecutorResponse: nil,
			Error:            nil,
		}
		bundle := verifier.NewVerifierBundle(request, response)
		withTx := &BundleWithTransaction{
			Bundle: bundle,
			Tx:     tx,
		}
		return withTx, nil
	})
}
