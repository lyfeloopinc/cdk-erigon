package legacy_executor_verifier

import (
	"sync"
	"github.com/ledgerwatch/log/v3"
)

type LimboState int

const (
	LimboNoLimbo  = LimboState(iota) // all good
	LimboDraining = LimboState(iota) // limbo detected and now working through the remaining verifications
	LimboDrained  = LimboState(iota) // remaining verifications have all been drained, and we have the lowest known bad batch number
	LimboRecovery = LimboState(iota) // the state set by the limbo stage to inform the execution stage to begin recovery
)

type Limbo struct {
	state     LimboState
	batchNo   uint64 // the lowest failing batch
	m         *sync.Mutex
	StateCond *sync.Cond
}

func NewLimbo() *Limbo {
	mtx := &sync.Mutex{}
	return &Limbo{
		state:     LimboNoLimbo,
		m:         mtx,
		batchNo:   0,
		StateCond: sync.NewCond(mtx),
	}
}

func (l *Limbo) StartLimboProcess(batchNo uint64) {
	l.m.Lock()
	defer l.m.Unlock()
	defer l.StateCond.Broadcast()

	log.Info("Entering limbo mode")
	l.state = LimboDraining
	// record lowest batch no
	if l.batchNo == 0 || batchNo < l.batchNo {
		l.batchNo = batchNo
	}
}

// CheckUpdateLowestBatchNumber updates the limbo batch number if it is lower than
// the batch that triggered the state.  We always want to know the lowest batch number
// with an issue to handle limbo mode properly
func (l *Limbo) CheckUpdateLowestBatchNumber(batchNo uint64) {
	l.m.Lock()
	defer l.m.Unlock()
	if batchNo < l.batchNo {
		l.batchNo = batchNo
	}
}

func (l *Limbo) StartRecovery() {
	l.m.Lock()
	defer l.m.Unlock()
	defer l.StateCond.Broadcast()
	l.state = LimboRecovery
}

func (l *Limbo) GetState() LimboState {
	l.m.Lock()
	defer l.m.Unlock()
	return l.state
}

func (l *Limbo) ExitLimboMode() {
	l.m.Lock()
	defer l.m.Unlock()
	defer l.StateCond.Broadcast()
	log.Info("Exiting limbo mode")
	l.state = LimboNoLimbo
	l.batchNo = 0
}

func (l *Limbo) CheckLimboMode() (bool, LimboState, uint64) {
	l.m.Lock()
	defer l.m.Unlock()
	return l.state > 0, l.state, l.batchNo
}

func (l *Limbo) SetDrained() {
	l.m.Lock()
	defer l.m.Unlock()
	defer l.StateCond.Broadcast()
	l.state = LimboDrained
}
