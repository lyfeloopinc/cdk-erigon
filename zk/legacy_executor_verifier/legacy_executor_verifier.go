package legacy_executor_verifier

import (
	"context"
	"encoding/hex"
	"strconv"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier/proto/github.com/0xPolygonHermez/zkevm-node/state/runtime/executor"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/ledgerwatch/log/v3"
	"errors"
	"time"
)

const (
	maximumInflightRequests = 1024 // todo [zkevm] this should probably be from config
	maximumVerifyErrors     = 10

	ROLLUP_ID = 1 // todo [zkevm] this should be read from config to anticipate more than 1 rollup per manager contract
)

var (
	sleepWaitForTransient = time.Millisecond * 500
	ErrNoBlocks           = errors.New("no blocks found for this batch")
)

type VerifierRequest struct {
	BatchNumber uint64
	StateRoot   common.Hash
	CheckCount  int
	VerifyCount int
}

type VerifierResponse struct {
	BatchNumber uint64
	Valid       bool
	Witness     []byte
}

type ILegacyExecutor interface {
	Verify(*Payload, *VerifierRequest) (bool, error)
}

type WitnessGenerator interface {
	GenerateWitness(tx kv.Tx, ctx context.Context, startBlock, endBlock uint64, debug, witnessFull bool) ([]byte, error)
}

type LegacyExecutorVerifier struct {
	db            kv.RwDB
	cfg           ethconfig.Zk
	executors     []ILegacyExecutor
	executorLocks []*sync.Mutex
	available     *sync.Cond

	requestChan  chan *VerifierRequest
	responseChan chan *VerifierResponse
	responses    []*VerifierResponse
	generalMtx   *sync.Mutex
	quit         chan struct{}
	openRequests int

	streamServer     *server.DataStreamServer
	witnessGenerator WitnessGenerator
	l1Syncer         *syncer.L1Syncer
	executorGrpc     executor.ExecutorServiceClient
	limbo            *Limbo
}

func NewLegacyExecutorVerifier(
	cfg ethconfig.Zk,
	executors []ILegacyExecutor,
	chainCfg *chain.Config,
	db kv.RwDB,
	witnessGenerator WitnessGenerator,
	l1Syncer *syncer.L1Syncer,
	limbo *Limbo,
) *LegacyExecutorVerifier {
	executorLocks := make([]*sync.Mutex, len(executors))
	for i := range executorLocks {
		executorLocks[i] = &sync.Mutex{}
	}

	streamServer := server.NewDataStreamServer(nil, chainCfg.ChainID.Uint64(), server.ExecutorOperationMode)

	availableLock := sync.Mutex{}
	verifier := &LegacyExecutorVerifier{
		db:               db,
		cfg:              cfg,
		executors:        executors,
		executorLocks:    executorLocks,
		available:        sync.NewCond(&availableLock),
		requestChan:      make(chan *VerifierRequest, maximumInflightRequests),
		responseChan:     make(chan *VerifierResponse, maximumInflightRequests),
		responses:        make([]*VerifierResponse, 0),
		generalMtx:       &sync.Mutex{},
		quit:             make(chan struct{}),
		streamServer:     streamServer,
		witnessGenerator: witnessGenerator,
		l1Syncer:         l1Syncer,
		limbo:            limbo,
	}

	return verifier
}

func (v *LegacyExecutorVerifier) VerifySynchronously(tx kv.RwTx, request *VerifierRequest) (*VerifierResponse, error) {
	ctx := context.Background()
	return v.handleRequestSync(ctx, tx, request)
}

func (v *LegacyExecutorVerifier) StopWork() {
	close(v.quit)
}

func (v *LegacyExecutorVerifier) StartWork() {
	go func() {
	LOOP:
		for {
			select {
			case <-v.quit:
				break LOOP
			case request := <-v.requestChan:
				go func() {
					ctx := context.Background()
					err := v.handleRequestAsync(ctx, request)
					if err != nil {
						log.Error("[Verifier] error handling request", "err", err)

						time.Sleep(sleepWaitForTransient)

						// requeue the request, could be a transient error
						v.requestChan <- request
					}
				}()
			case response := <-v.responseChan:
				v.handleResponse(response)
			}
		}
	}()
}

func (v *LegacyExecutorVerifier) handleRequestAsync(ctx context.Context, request *VerifierRequest) error {
	// if we have no executor config then just skip this step and treat everything as OK
	if len(v.executors) == 0 {
		response := &VerifierResponse{
			BatchNumber: request.BatchNumber,
			Valid:       true,
		}
		v.responseChan <- response
		return nil
	}

	// todo [zkevm] for now just using one executor but we need to use more
	execer := v.executors[0]

	// mapmutation has some issue with us not having a quit channel on the context call to `Done` so
	// here we're creating a cancelable context and just deferring the cancel
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tx, err := v.db.BeginRo(innerCtx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	payload, err := v.buildPayload(innerCtx, tx, request)

	if err != nil {
		if errors.Is(err, ErrNoBlocks) {
			request.CheckCount++
			v.requestChan <- request
			return nil
		}
		return err
	}

	success, err := execer.Verify(payload, request)
	if err != nil {
		if errors.Is(err, ErrExecutorUnplannedError) {
			// only return the error after some more attempts at verifying
			request.VerifyCount++
			if request.VerifyCount > maximumVerifyErrors {
				return err
			}
			// allow for an increasing timeout to attempt basic handling of transient errors
			time.Sleep(sleepWaitForTransient * time.Duration(request.VerifyCount) * time.Millisecond)
			v.requestChan <- request
			return nil
		}

		if errors.Is(err, ErrExecutorStateRootMismatch) {
			v.beginLimbo(request)

			v.responseChan <- &VerifierResponse{
				BatchNumber: request.BatchNumber,
				Valid:       false,
			}

			// here we're returning a non error state to avoid this request being re-queued over
			// and over
			return nil
		}

		return err
	}

	if !success {
		v.beginLimbo(request)
	}

	response := &VerifierResponse{
		BatchNumber: request.BatchNumber,
		Valid:       success,
	}

	v.responseChan <- response

	return nil
}

func (v *LegacyExecutorVerifier) beginLimbo(request *VerifierRequest) {
	inLimbo, _, _ := v.limbo.CheckLimboMode()
	if !inLimbo {
		v.limbo.StartLimboProcess(request.BatchNumber)
	}
}

func (v *LegacyExecutorVerifier) handleRequestSync(ctx context.Context, tx kv.RwTx, request *VerifierRequest) (*VerifierResponse, error) {
	// if we have no executor config then just skip this step and treat everything as OK
	if len(v.executors) == 0 {
		response := &VerifierResponse{
			BatchNumber: request.BatchNumber,
			Valid:       true,
		}
		return response, nil
	}

	// todo [zkevm] for now just using one executor but we need to use more
	execer := v.executors[0]

	// mapmutation has some issue with us not having a quit channel on the context call to `Done` so
	// here we're creating a cancelable context and just deferring the cancel
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	payload, err := v.buildPayload(innerCtx, tx, request)
	if err != nil {
		return nil, err
	}

	success, err := execer.Verify(payload, request)
	if err != nil {
		return nil, err
	}

	response := &VerifierResponse{
		BatchNumber: request.BatchNumber,
		Valid:       success,
	}

	return response, nil
}

func (v *LegacyExecutorVerifier) buildPayload(ctx context.Context, tx kv.Tx, request *VerifierRequest) (*Payload, error) {
	hermezDb := hermez_db.NewHermezDbReader(tx)

	// get the data stream bytes
	blocks, err := hermezDb.GetL2BlockNosByBatch(request.BatchNumber)
	if err != nil {
		return nil, err
	}

	// we might not have blocks yet as the underlying stage loop might still be running and the tx hasn't been
	// committed yet so just requeue the request
	if len(blocks) == 0 {
		return nil, ErrNoBlocks
	}

	stream, err := v.GetStreamBytes(request, tx, blocks, hermezDb)
	if err != nil {
		return nil, err
	}

	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	witness, err := v.witnessGenerator.GenerateWitness(tx, innerCtx, blocks[0], blocks[len(blocks)-1], false, v.cfg.WitnessFull)
	if err != nil {
		return nil, err
	}

	log.Debug("witness generated", "data", hex.EncodeToString(witness))

	oldAccInput, err := v.l1Syncer.GetOldAccInputHash(ctx, &v.cfg.L1PolygonRollupManager, ROLLUP_ID, request.BatchNumber)
	if err != nil {
		return nil, err
	}

	// now we need to figure out the timestamp limit for this payload.  It must be:
	// timestampLimit >= currentTimestamp (from batch pre-state) + deltaTimestamp
	// so to ensure we have a good value we can take the timestamp of the last block in the batch
	// and just add 5 minutes
	lastBlock, err := rawdb.ReadBlockByNumber(tx, blocks[len(blocks)-1])
	if err != nil {
		return nil, ErrNoBlocks
	}
	timestampLimit := lastBlock.Time()

	payload := &Payload{
		Witness:           witness,
		DataStream:        stream,
		Coinbase:          v.cfg.SequencerAddress.String(),
		OldAccInputHash:   oldAccInput.Bytes(),
		L1InfoRoot:        nil, // we can't know this ahead of time so will always be nil in this context
		TimestampLimit:    timestampLimit,
		ForcedBlockhashL1: []byte{0}, // we aren't handling forced batches right now
		ContextId:         strconv.Itoa(int(request.BatchNumber)),
	}

	return payload, nil
}

func (v *LegacyExecutorVerifier) GetStreamBytes(request *VerifierRequest, tx kv.Tx, blocks []uint64, hermezDb *hermez_db.HermezDbReader) ([]byte, error) {
	lastBlock, err := rawdb.ReadBlockByNumber(tx, blocks[0]-1)
	if err != nil {
		return nil, err
	}
	var streamBytes []byte
	for _, blockNumber := range blocks {
		block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
		if err != nil {
			return nil, err
		}
		sBytes, err := v.streamServer.CreateAndBuildStreamEntryBytes(block, hermezDb, lastBlock, request.BatchNumber, true)
		if err != nil {
			return nil, err
		}
		streamBytes = append(streamBytes, sBytes...)
		lastBlock = block
	}
	return streamBytes, nil
}

func (v *LegacyExecutorVerifier) handleResponse(response *VerifierResponse) {
	v.generalMtx.Lock()
	defer v.generalMtx.Unlock()

	v.openRequests--

	// a check if we're in limbo mode and to handle progressing to the next phase if we are
	limbo, _, _ := v.limbo.CheckLimboMode()
	if limbo {
		v.limbo.CheckUpdateLowestBatchNumber(response.BatchNumber)
		if v.openRequests == 0 {
			// ensure the responses slice is empty
			v.responses = v.responses[:0]

			// we're done with handling in flight requests, so set the drained state
			v.limbo.SetDrained()
		}
	} else {
		// we only care about responses when we're not in limbo mode
		v.responses = append(v.responses, response)
	}
}

func (v *LegacyExecutorVerifier) AddRequest(request *VerifierRequest) {
	v.generalMtx.Lock()
	defer v.generalMtx.Unlock()

	// check we don't already have a response for this to save doubling up work
	for _, response := range v.responses {
		if response.BatchNumber == request.BatchNumber {
			return
		}
	}

	v.openRequests++

	v.requestChan <- request
}

func (v *LegacyExecutorVerifier) GetAllResponses() []*VerifierResponse {
	v.generalMtx.Lock()
	defer v.generalMtx.Unlock()
	result := make([]*VerifierResponse, len(v.responses))
	copy(result, v.responses)
	return result
}

func (v *LegacyExecutorVerifier) RemoveResponse(batchNumber uint64) {
	v.generalMtx.Lock()
	defer v.generalMtx.Unlock()

	result := make([]*VerifierResponse, 0, len(v.responses))
	for _, response := range v.responses {
		if response.BatchNumber != batchNumber {
			result = append(result, response)
		}
	}
	v.responses = result
}
