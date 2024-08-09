package stages

import (
	"errors"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/log/v3"

	"strings"

	"context"
	"math/big"

	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/status-im/keycard-go/hexutils"
)

const (
	storageValBaseDec = 10
	storageValBaseHex = 16
)

func verifyLastHash(dbSmt *smt.SMT, expectedRootHash *common.Hash, checkRoot bool, logPrefix string) error {
	hash := common.BigToHash(dbSmt.LastRoot())

	if checkRoot && hash != *expectedRootHash {
		panic(fmt.Sprintf("[%s] Wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash))
	}
	log.Info(fmt.Sprintf("[%s] Trie root matches", logPrefix), "hash", hash.Hex())
	return nil
}

type accountValue struct {
	key       *utils.NodeKey
	keySource []byte
	value     *utils.NodeValue8
}

func calcAccountValuesToChan(ctx context.Context, a accounts.Account, as map[string]string, accountCode []byte, addr common.Address, valueChan chan accountValue) error {
	// get the account balance and nonce
	accountStateValues, err := calcAccountStateToKV(addr, a.Balance.ToBig(), new(big.Int).SetUint64(a.Nonce))
	if err != nil {
		return err
	}

	for _, av := range accountStateValues {
		valueChan <- av
	}

	bytecodeValues, err := calcContractBytecodeToKV(addr, accountCode)
	if err != nil {
		return err
	}

	for _, av := range bytecodeValues {
		valueChan <- av
	}

	// store the account storage
	_, err = calcContractStorageToKV(ctx, addr, as, valueChan)
	return err
}

func calcContractBytecodeToKV(ethAddr common.Address, accountCode []byte) ([]accountValue, error) {
	addrString := ethAddr.String()
	accountValues := make([]accountValue, 0, 2)
	ach := hexutils.BytesToHex(accountCode)
	if len(ach) > 0 {
		bytecode := "0x" + ach
		keyContractCode, err := utils.KeyContractCode(addrString)
		if err != nil {
			return nil, err
		}

		keyContractLength, err := utils.KeyContractLength(addrString)
		if err != nil {
			return nil, err
		}

		hashedBytecode, err := utils.HashContractBytecode(bytecode)
		if err != nil {
			return nil, err
		}

		parsedBytecode := strings.TrimPrefix(bytecode, "0x")
		if len(parsedBytecode)%2 != 0 {
			parsedBytecode = "0" + parsedBytecode
		}

		bi := utils.ConvertHexToBigInt(hashedBytecode)
		bytecodeLength := len(parsedBytecode) / 2

		x := utils.ScalarToArrayBig(bi)
		valueContractCode, err := utils.NodeValue8FromBigIntArray(x)
		if err != nil {
			return nil, err
		}

		x = utils.ScalarToArrayBig(big.NewInt(int64(bytecodeLength)))
		valueContractLength, err := utils.NodeValue8FromBigIntArray(x)
		if err != nil {
			return nil, err
		}

		if !valueContractCode.IsZero() {
			ks := utils.EncodeKeySource(utils.SC_CODE, ethAddr, emptyHash)
			accountValues = append(accountValues, accountValue{key: &keyContractCode, keySource: ks, value: valueContractCode})
		}

		if !valueContractLength.IsZero() {
			ks := utils.EncodeKeySource(utils.SC_LENGTH, ethAddr, emptyHash)
			accountValues = append(accountValues, accountValue{key: &keyContractLength, keySource: ks, value: valueContractLength})
		}
	}
	return accountValues, nil
}

// if channel is set - send to it and return empty accountValues slice
// no need to use memory in this case
func calcContractStorageToKV(ctx context.Context, ethAddr common.Address, storage map[string]string, kvChan chan accountValue) ([]accountValue, error) {
	a := utils.ConvertHexToBigInt(ethAddr.String())
	add := utils.ScalarToArrayBig(a)

	storageValues := make([]accountValue, 0, len(storage))
	for k, v := range storage {
		select {
		case <-ctx.Done():
			return nil, errors.New("context done")
		default:
		}

		if v == "" {
			continue
		}
		kv, err := calcContractStorageSlotToKV(ethAddr, add, k, v)
		if err != nil {
			return nil, err
		}
		if kv != nil {
			if kvChan != nil {
				kvChan <- *kv
			} else {
				storageValues = append(storageValues, *kv)
			}
		}
	}

	return storageValues, nil
}

// return nil if parsedValue is zero
func calcContractStorageSlotToKV(address common.Address, add []*big.Int, k, v string) (*accountValue, error) {
	keyStoragePosition, err := utils.KeyContractStorage(add, k)
	if err != nil {
		return nil, err
	}

	base := storageValBaseDec
	if strings.HasPrefix(v, "0x") {
		v = v[2:]
		base = storageValBaseHex
	}

	val, _ := new(big.Int).SetString(v, base)

	x := utils.ScalarToArrayBig(val)
	parsedValue, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return nil, err
	}
	if parsedValue.IsZero() {
		return nil, nil
	}

	sp, _ := utils.StrValToBigInt(k)
	ks := utils.EncodeKeySource(utils.SC_STORAGE, address, common.BigToHash(sp))
	return &accountValue{key: &keyStoragePosition, keySource: ks, value: parsedValue}, nil
}

func calcAccountStateToKV(ethAddr common.Address, balance, nonce *big.Int) ([]accountValue, error) {
	addrString := ethAddr.String()
	keyBalance, err := utils.KeyEthAddrBalance(addrString)
	if err != nil {
		return nil, err
	}
	keyNonce, err := utils.KeyEthAddrNonce(addrString)
	if err != nil {
		return nil, err
	}

	x := utils.ScalarToArrayBig(balance)
	valueBalance, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return nil, err
	}

	x = utils.ScalarToArrayBig(nonce)
	valueNonce, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return nil, err
	}

	accountValues := make([]accountValue, 0, 2)

	if !valueBalance.IsZero() {
		ks := utils.EncodeKeySource(utils.KEY_BALANCE, ethAddr, emptyHash)
		accountValues = append(accountValues, accountValue{key: &keyBalance, keySource: ks, value: valueBalance})
	}
	if !valueNonce.IsZero() {
		ks := utils.EncodeKeySource(utils.KEY_NONCE, ethAddr, emptyHash)
		accountValues = append(accountValues, accountValue{key: &keyNonce, keySource: ks, value: valueNonce})
	}

	return accountValues, nil
}
