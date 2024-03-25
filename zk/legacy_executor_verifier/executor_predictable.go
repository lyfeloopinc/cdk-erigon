package legacy_executor_verifier

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"sync"
)

type ExecutorPredictable struct {
	mut               sync.Mutex
	verificationCount int
	failEvery         int
}

func NewExecutorPredictable(failEvery int) *ExecutorPredictable {
	return &ExecutorPredictable{
		failEvery: failEvery,
	}
}

func NewExecutorPredictables(failEvery int) []*ExecutorPredictable {
	return []*ExecutorPredictable{
		NewExecutorPredictable(failEvery),
	}
}

func (e *ExecutorPredictable) Verify(p *Payload, erigonStateRoot *common.Hash) (bool, error) {
	e.mut.Lock()
	e.verificationCount++
	count := e.verificationCount
	e.mut.Unlock()

	if e.failEvery > 0 && count%e.failEvery == 0 {
		return false, nil
	}
	return true, nil
}
