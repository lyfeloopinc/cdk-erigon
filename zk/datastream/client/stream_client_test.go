package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
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
			expectedError:  errors.New("failed to read header bytes reading from server: unexpected EOF"),
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
			expectedError:  errors.New("failed to read main result bytes reading from server: unexpected EOF"),
		},
		{
			name:           "Invalid error length",
			input:          []byte{0, 0, 0, 12, 0, 0, 0, 0, 20, 21},
			expectedResult: nil,
			expectedError:  errors.New("failed to read result errStr bytes reading from server: unexpected EOF"),
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
			expectedError:  errors.New("expected data packet type 2 or 254 and received 5"),
		},
		{
			name:           "Invalid byte array length",
			input:          []byte{2, 21, 22, 23, 24, 20},
			expectedResult: nil,
			expectedError:  errors.New("error reading file bytes: reading from server: unexpected EOF"),
		}, {
			name:           "Invalid data length",
			input:          []byte{2, 0, 0, 0, 31, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 64},
			expectedResult: nil,
			expectedError:  errors.New("error reading file data bytes: reading from server: unexpected EOF"),
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
	serverConn, clientConn := net.Pipe()
	c.conn = clientConn
	defer func() {
		serverConn.Close()
		clientConn.Close()
	}()

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

	var (
		errCh = make(chan error)
		wg    sync.WaitGroup
	)
	wg.Add(1)

	go func() {
		defer wg.Done()
		fileEntries := []*types.FileEntry{
			createFileEntry(t, types.EntryTypeL2Block, 1, l2BlockRaw),
			createFileEntry(t, types.EntryTypeL2Tx, 2, l2TxRaw),
			createFileEntry(t, types.EntryTypeL2BlockEnd, 3, l2BlockEndRaw),
		}
		for _, fe := range fileEntries {
			_, writeErr := serverConn.Write(fe.Encode())
			if writeErr != nil {
				errCh <- writeErr
				break
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	parsedEntry, err := c.readParsedProto()
	require.NoError(t, err)
	serverErr := <-errCh
	require.NoError(t, serverErr)
	expectedL2Tx := types.ConvertToL2TransactionProto(l2Tx)
	expectedL2Block := types.ConvertToFullL2Block(l2Block)
	expectedL2Block.L2Txs = append(expectedL2Block.L2Txs, *expectedL2Tx)
	require.Equal(t, expectedL2Block, parsedEntry)
}

func TestStreamClient_GetLatestL2Block(t *testing.T) {
	const u64BytesLength = 8

	serverConn, clientConn := net.Pipe()
	defer func() {
		serverConn.Close()
		clientConn.Close()
	}()

	// readAndValidateUint64 from the connection in order to unblock future write operations
	readAndValidateUint64 := func(conn net.Conn, expected uint64, paramName string) error {
		valueRaw, err := readBuffer(conn, u64BytesLength)
		if err != nil {
			return fmt.Errorf("failed to read %s parameter: %w", paramName, err)
		}

		value := binary.BigEndian.Uint64(valueRaw)
		if value != expected {
			return fmt.Errorf("%s parameter value mismatch between expected %d and actual %d", paramName, expected, value)
		}
		return nil
	}

	// Set up the client with the server connection
	c := NewClient(context.Background(), "", 0, 0, 0)
	c.conn = clientConn

	expectedL2Block := &datastream.L2Block{
		Number:        5,
		BatchNumber:   1,
		Timestamp:     uint64(time.Now().UnixMilli()),
		Hash:          common.HexToHash("0x123456").Bytes(),
		BlockGasLimit: 10000000,
	}
	l2BlockProto := &types.L2BlockProto{L2Block: expectedL2Block}
	l2BlockRaw, err := l2BlockProto.Marshal()
	require.NoError(t, err)

	var (
		errCh = make(chan error)
		wg    sync.WaitGroup
	)
	wg.Add(1)

	// Prepare the server to send responses in a separate goroutine
	go func() {
		defer wg.Done()

		// Read the command and the stream type to avoid future writes to block
		if err := readAndValidateUint64(serverConn, uint64(CmdHeader), "command"); err != nil {
			errCh <- err
			return
		}

		// Read the stream type
		if err := readAndValidateUint64(serverConn, uint64(StSequencer), "stream type"); err != nil {
			errCh <- err
			return
		}

		// Write result entry
		re := &types.ResultEntry{
			PacketType: PtResult,
			ErrorNum:   types.CmdErrOK,
			Length:     types.ResultEntryMinSize,
			ErrorStr:   nil,
		}
		_, err = serverConn.Write(re.Encode())
		if err != nil {
			errCh <- fmt.Errorf("failed to write result entry to the connection: %w", err)
		}

		// Write header entry
		he := &types.HeaderEntry{
			PacketType:   uint8(CmdHeader),
			HeadLength:   types.HeaderSize,
			Version:      2,
			SystemId:     1,
			StreamType:   types.StreamType(StSequencer),
			TotalEntries: 4,
		}
		_, err = serverConn.Write(he.Encode())
		if err != nil {
			errCh <- fmt.Errorf("failed to write header entry to the connection: %w", err)
		}

		// Read the command
		if err := readAndValidateUint64(serverConn, uint64(CmdEntry), "command"); err != nil {
			errCh <- err
			return
		}

		// Read the stream type
		if err := readAndValidateUint64(serverConn, uint64(StSequencer), "stream type"); err != nil {
			errCh <- err
			return
		}

		// Read the entry number

		if err := readAndValidateUint64(serverConn, he.TotalEntries-1, "entry number"); err != nil {
			errCh <- err
			return
		}

		// Write the ResultEntry
		_, err = serverConn.Write(re.Encode())
		if err != nil {
			errCh <- fmt.Errorf("failed to write result entry to the connection: %w", err)
			return
		}

		// Write the FileEntry containing the L2 block information
		fe := createFileEntry(t, types.EntryTypeL2Block, 1, l2BlockRaw)
		_, err = serverConn.Write(fe.Encode())
		if err != nil {
			errCh <- fmt.Errorf("failed to write the l2 block file entry to the connection: %w", err)
			return
		}

		serverConn.Close()
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	// ACT
	l2Block, err := c.GetLatestL2Block()
	require.NoError(t, err)

	// ASSERT
	serverErr := <-errCh
	require.NoError(t, serverErr)

	expectedFullL2Block := types.ConvertToFullL2Block(expectedL2Block)
	require.Equal(t, expectedFullL2Block, l2Block)
}

// createFileEntry is a helper function that creates FileEntry
func createFileEntry(t *testing.T, entryType types.EntryType, num uint64, data []byte) *types.FileEntry {
	t.Helper()
	return &types.FileEntry{
		PacketType: PtData,
		Length:     types.FileEntryMinSize + uint32(len(data)),
		EntryType:  entryType,
		EntryNum:   num,
		Data:       data,
	}
}
