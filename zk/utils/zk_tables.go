package utils

import (
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/gateway-fm/cdk-erigon-lib/kv/memdb"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
)

func PopulateMemoryMutationTables(batch *memdb.MemoryMutation) error {
	for _, table := range hermez_db.HermezDbTables {
		if err := batch.CreateBucket(table); err != nil {
			return err
		}
	}

	for _, table := range db.HermezSmtTables {
		if err := batch.CreateBucket(table); err != nil {
			return err
		}
	}

	for _, table := range kv.ChaindataTables {
		if err := batch.CreateBucket(table); err != nil {
			return err
		}
	}

	return nil
}
