package client

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"
)

func Test_readHeaderEntry(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult *types.HeaderEntry
		expectedError  error
	}
	testCases := []testCase{
		{
			name:  "Happy path",
			input: []byte{101, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: &types.HeaderEntry{
				PacketType:   101,
				HeadLength:   29,
				StreamType:   types.StreamType(1),
				TotalLength:  24,
				TotalEntries: 64,
			},
			expectedError: nil,
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{20, 21, 22, 23, 24, 20},
			expectedResult: nil,
			expectedError:  fmt.Errorf("failed to read header bytes reading from server: unexpected EOF"),
		},
	}

	for _, testCase := range testCases {
		c := NewClient(context.Background(), "", 0, 0, 0)
		server, conn := net.Pipe()
		defer server.Close()
		defer c.Stop()

		c.conn = conn
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				server.Write(testCase.input)
				server.Close()
			}()

			header, err := c.readHeaderEntry()
			require.Equal(t, testCase.expectedError, err)
			assert.DeepEqual(t, testCase.expectedResult, header)
		})
	}
}

func Test_readResultEntry(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult *types.ResultEntry
		expectedError  error
	}
	testCases := []testCase{
		{
			name:  "Happy path",
			input: []byte{0, 0, 0, 9, 0, 0, 0, 0},
			expectedResult: &types.ResultEntry{
				PacketType: 1,
				Length:     9,
				ErrorNum:   0,
				ErrorStr:   []byte{},
			},
			expectedError: nil,
		},
		{
			name:  "Happy path - error str length",
			input: []byte{0, 0, 0, 19, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expectedResult: &types.ResultEntry{
				PacketType: 1,
				Length:     19,
				ErrorNum:   0,
				ErrorStr:   []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
			expectedError: nil,
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{20, 21, 22, 23, 24, 20},
			expectedResult: nil,
			expectedError:  fmt.Errorf("failed to read main result bytes reading from server: unexpected EOF"),
		},
		{
			name:           "Invalid error length",
			input:          []byte{0, 0, 0, 12, 0, 0, 0, 0, 20, 21},
			expectedResult: nil,
			expectedError:  fmt.Errorf("failed to read result errStr bytes reading from server: unexpected EOF"),
		},
	}

	for _, testCase := range testCases {
		c := NewClient(context.Background(), "", 0, 0, 0)
		server, conn := net.Pipe()
		defer server.Close()
		defer c.Stop()

		c.conn = conn
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				server.Write(testCase.input)
				server.Close()
			}()

			result, err := c.readResultEntry([]byte{1})
			require.Equal(t, testCase.expectedError, err)
			assert.DeepEqual(t, testCase.expectedResult, result)
		})
	}
}

func Test_readFileEntry(t *testing.T) {
	type testCase struct {
		name           string
		input          []byte
		expectedResult *types.FileEntry
		expectedError  error
	}
	testCases := []testCase{
		{
			name:  "Happy path",
			input: []byte{2, 0, 0, 0, 29, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: &types.FileEntry{
				PacketType: 2,
				Length:     29,
				EntryType:  types.EntryType(1),
				EntryNum:   45,
				Data:       []byte{0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			},
			expectedError: nil,
		}, {
			name:  "Happy path - no data",
			input: []byte{2, 0, 0, 0, 17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45},
			expectedResult: &types.FileEntry{
				PacketType: 2,
				Length:     17,
				EntryType:  types.EntryType(1),
				EntryNum:   45,
				Data:       []byte{},
			},
			expectedError: nil,
		},
		{
			name:           "Invalid packet type",
			input:          []byte{5, 0, 0, 0, 17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45},
			expectedResult: nil,
			expectedError:  fmt.Errorf("expected data packet type 2 or 254 and received 5"),
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{2, 21, 22, 23, 24, 20},
			expectedResult: nil,
			expectedError:  fmt.Errorf("error reading file bytes: reading from server: unexpected EOF"),
		}, {
			name:           "Invalid data length",
			input:          []byte{2, 0, 0, 0, 31, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: nil,
			expectedError:  fmt.Errorf("error reading file data bytes: reading from server: unexpected EOF"),
		},
	}
	for _, testCase := range testCases {
		c := NewClient(context.Background(), "", 0, 0, 0)
		server, conn := net.Pipe()
		defer c.Stop()
		defer server.Close()

		c.conn = conn
		t.Run(testCase.name, func(t *testing.T) {
			go func() {
				server.Write(testCase.input)
				server.Close()
			}()

			result, err := c.readFileEntry()
			require.Equal(t, testCase.expectedError, err)
			assert.DeepEqual(t, testCase.expectedResult, result)
		})
	}
}

func Test_readParsedProto(t *testing.T) {
	c := NewClient(context.Background(), "", 0, 0, 0)
	server, conn := net.Pipe()
	c.conn = conn

	createFileEntry := func(entryType types.EntryType, num uint64, data []byte) *types.FileEntry {
		return &types.FileEntry{
			PacketType: PtData,
			Length:     types.FileEntryMinSize + uint32(len(data)),
			EntryType:  entryType,
			EntryNum:   num,
			Data:       data,
		}
	}

	l2Block := &datastream.L2Block{
		Number:        3,
		BatchNumber:   1,
		Timestamp:     uint64(time.Now().UnixMilli()),
		Hash:          common.HexToHash("0x123456").Bytes(),
		BlockGasLimit: 10000000,
	}
	l2BlockProto := &types.L2BlockProto{L2Block: l2Block}
	l2BlockRaw, err := l2BlockProto.Marshal()
	require.NoError(t, err)

	l2Tx := &datastream.Transaction{
		L2BlockNumber: l2Block.GetNumber(),
		Index:         0,
		IsValid:       true,
		Debug:         &datastream.Debug{Message: "Hello world!"},
	}
	l2TxProto := &types.TxProto{Transaction: l2Tx}
	l2TxRaw, err := l2TxProto.Marshal()
	require.NoError(t, err)

	l2BlockEnd := &types.L2BlockEndProto{Number: l2Block.GetNumber()}
	l2BlockEndRaw, err := l2BlockEnd.Marshal()
	require.NoError(t, err)

	fileEntries := make([]*types.FileEntry, 3)
	fileEntries[0] = createFileEntry(types.EntryTypeL2Block, 1, l2BlockRaw)
	fileEntries[1] = createFileEntry(types.EntryTypeL2Tx, 2, l2TxRaw)
	fileEntries[2] = createFileEntry(types.EntryTypeL2BlockEnd, 3, l2BlockEndRaw)

	var (
		writeErr    error
		parsedEntry interface{}
	)

	go func() {
		for _, fe := range fileEntries {
			_, writeErr = server.Write(fe.Encode())
		}
		server.Close()
	}()

	require.NoError(t, writeErr)

	parsedEntry, err = c.readParsedProto()
	require.NoError(t, err)
	require.NoError(t, err)
	expectedL2Tx := types.ConvertToL2TransactionProto(l2Tx)
	expectedL2Block := types.ConvertToFullL2Block(l2Block)
	expectedL2Block.L2Txs = append(expectedL2Block.L2Txs, *expectedL2Tx)
	require.Equal(t, expectedL2Block, parsedEntry)
}
