package l1infotree

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/core/types"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
)

const (
	changeL2Block = uint8(0x0b)
	sizeUInt32    = 4
)

var (
	// ErrBatchV2DontStartWithChangeL2Block is returned when the batch start directly with a trsansaction (without a changeL2Block)
	ErrBatchV2DontStartWithChangeL2Block = errors.New("batch v2 must start with changeL2Block before Tx (suspect a V1 Batch or a ForcedBatch?))")
	// ErrInvalidBatchV2 is returned when the batch is invalid.
	ErrInvalidBatchV2 = errors.New("invalid batch v2")
	// ErrInvalidRLP is returned when the rlp is invalid.
	ErrInvalidRLP = errors.New("invalid rlp codification")
)

// ChangeL2BlockHeader is the header of a L2 block.
type ChangeL2BlockHeader struct {
	DeltaTimestamp  uint32
	IndexL1InfoTree uint32
}

// L2BlockRaw is the raw representation of a L2 block.
type L2BlockRaw struct {
	BlockNumber uint64
	ChangeL2BlockHeader
	Transactions []L2TxRaw
}

// BatchRawV2 is the  representation of a batch of transactions.
type BatchRawV2 struct {
	Blocks []L2BlockRaw
}

// ForcedBatchRawV2 is the  representation of a forced batch of transactions.
type ForcedBatchRawV2 struct {
	Transactions []L2TxRaw
}

// L2TxRaw is the raw representation of a L2 transaction  inside a L2 block.
type L2TxRaw struct {
	ForkId               uint16             // valid always
	EfficiencyPercentage uint8              // valid always
	TxAlreadyEncoded     bool               // If true the tx is already encoded (data field is used)
	Tx                   *types.Transaction // valid if TxAlreadyEncoded == false
	Data                 []byte             // valid if TxAlreadyEncoded == true
}

// EncodeBatchV2 encodes a batch of transactions into a byte slice.
func EncodeBatchV2(batch *BatchRawV2) ([]byte, error) {
	if batch == nil {
		return nil, fmt.Errorf("batch is nil: %w", ErrInvalidBatchV2)
	}
	if len(batch.Blocks) == 0 {
		return nil, fmt.Errorf("a batch need minimum a L2Block: %w", ErrInvalidBatchV2)
	}

	encoder := NewBatchV2Encoder()
	for _, block := range batch.Blocks {
		encoder.AddBlockHeader(block.ChangeL2BlockHeader)
		err := encoder.AddTransactions(block.Transactions)
		if err != nil {
			return nil, fmt.Errorf("can't encode tx: %w", err)
		}
	}
	return encoder.GetResult(), nil
}

// BatchV2Encoder is a builder of the batchl2data used by EncodeBatchV2
type BatchV2Encoder struct {
	batchData []byte
}

// NewBatchV2Encoder creates a new BatchV2Encoder.
func NewBatchV2Encoder() *BatchV2Encoder {
	return &BatchV2Encoder{}
}

// AddBlockHeader adds a block header to the batch.
func (b *BatchV2Encoder) AddBlockHeader(l2BlockHeader ChangeL2BlockHeader) {
	b.batchData = l2BlockHeader.Encode(b.batchData)
}

// AddTransactions adds a set of transactions to the batch.
func (b *BatchV2Encoder) AddTransactions(transactions []L2TxRaw) error {
	for i := range transactions {
		tx := transactions[i]
		err := b.AddTransaction(&tx)
		if err != nil {
			return fmt.Errorf("can't encode tx: %w", err)
		}
	}
	return nil
}

// AddTransaction adds a transaction to the batch.
func (b *BatchV2Encoder) AddTransaction(transaction *L2TxRaw) error {
	var err error
	b.batchData, err = transaction.Encode(b.batchData)
	if err != nil {
		return fmt.Errorf("can't encode tx: %w", err)
	}
	return nil
}

// GetResult returns the batch data.
func (b *BatchV2Encoder) GetResult() []byte {
	return b.batchData
}

// Encode encodes a batch of l2blocks header into a byte slice.
func (c ChangeL2BlockHeader) Encode(batchData []byte) []byte {
	batchData = append(batchData, changeL2Block)
	batchData = binary.BigEndian.AppendUint32(batchData, c.DeltaTimestamp)
	batchData = binary.BigEndian.AppendUint32(batchData, c.IndexL1InfoTree)

	return batchData
}

// Encode encodes a transaction into a byte slice.
func (tx L2TxRaw) Encode(batchData []byte) ([]byte, error) {
	if tx.TxAlreadyEncoded {
		batchData = append(batchData, tx.Data...)
	} else {
		rlpTx, err := zktx.TransactionToL2Data(*tx.Tx, tx.ForkId, tx.EfficiencyPercentage)
		if err != nil {
			return nil, fmt.Errorf("can't encode tx to RLP: %w", err)
		}
		batchData = append(batchData, rlpTx...)
	}
	batchData = append(batchData, tx.EfficiencyPercentage)
	return batchData, nil
}
