#!/bin/bash

line=$(grep -n -m 1 "ERIGON_VERSIONS" ./tables.go | cut -d: -f1)
sed -i "$line a\DISCARDED_TRANSACTIONS_BY_BLOCK=\"discarded_transactions_by_block\"" ./tables.go
line=$(grep -n -m 1 "DISCARDED_TRANSACTIONS_BY_BLOCK" ./tables.go | cut -d: -f1)
sed -i "$line a\DISCARDED_TRANSACTIONS_BY_HASH=\"discarded_transactions_by_hash\"" ./tables.go

line=$(grep -n -m 1 "ERIGON_VERSIONS," ./tables.go | cut -d: -f1)
sed -i "$line a\DISCARDED_TRANSACTIONS_BY_BLOCK," ./tables.go
line=$(grep -n -m 1 "DISCARDED_TRANSACTIONS_BY_BLOCK," ./tables.go | cut -d: -f1)
sed -i "$line a\DISCARDED_TRANSACTIONS_BY_HASH," ./tables.go
