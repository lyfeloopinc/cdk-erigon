package types

import (
	"fmt"

	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
)

type FileEntryIterator interface {
	NextFileEntry() (*FileEntry, error)
}

func ReadParsedProto(iterator FileEntryIterator) (
	parsedEntry interface{},
	err error,
) {
	file, err := iterator.NextFileEntry()
	if err != nil {
		err = fmt.Errorf("read file entry error: %v", err)
		return
	}

	switch file.EntryType {
	case BookmarkEntryType:
		parsedEntry, err = UnmarshalBookmark(file.Data)
	case EntryTypeGerUpdate:
		parsedEntry, err = DecodeGerUpdateProto(file.Data)
	case EntryTypeBatchStart:
		parsedEntry, err = UnmarshalBatchStart(file.Data)
	case EntryTypeBatchEnd:
		parsedEntry, err = UnmarshalBatchEnd(file.Data)
	case EntryTypeL2Block:
		var l2Block *FullL2Block
		if l2Block, err = UnmarshalL2Block(file.Data); err != nil {
			return
		}

		txs := []L2TransactionProto{}

		var innerFile *FileEntry
		var l2Tx *L2TransactionProto
	LOOP:
		for {
			if innerFile, err = iterator.NextFileEntry(); err != nil {
				return
			}

			if innerFile.IsL2Tx() {
				if l2Tx, err = UnmarshalTx(innerFile.Data); err != nil {
					return
				}
				txs = append(txs, *l2Tx)
			} else if innerFile.IsL2BlockEnd() {
				var l2BlockEnd *L2BlockEndProto
				if l2BlockEnd, err = UnmarshalL2BlockEnd(innerFile.Data); err != nil {
					return
				}
				if l2BlockEnd.GetBlockNumber() != l2Block.L2BlockNumber {
					err = fmt.Errorf("block end number (%d) not equal to block number (%d)", l2BlockEnd.GetBlockNumber(), l2Block.L2BlockNumber)
					return
				}
				break LOOP
			} else if innerFile.IsBookmark() {
				var bookmark *BookmarkProto
				if bookmark, err = UnmarshalBookmark(innerFile.Data); err != nil || bookmark == nil {
					return
				}
				if bookmark.BookmarkType() == datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK {
					break LOOP
				} else {
					err = fmt.Errorf("unexpected bookmark type inside block: %v", bookmark.Type())
					return
				}
			} else if innerFile.IsBatchEnd() {
				if _, err = UnmarshalBatchEnd(file.Data); err != nil {
					return
				}
				break LOOP
			} else {
				err = fmt.Errorf("unexpected entry type inside a block: %d", innerFile.EntryType)
				return
			}
		}

		l2Block.L2Txs = txs
		parsedEntry = l2Block
		return
	case EntryTypeL2Tx:
		err = fmt.Errorf("unexpected l2Tx out of block")
	default:
		err = fmt.Errorf("unexpected entry type: %d", file.EntryType)
	}
	return
}
