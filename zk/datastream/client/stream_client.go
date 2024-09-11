package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/log/v3"
	"sync"
	"github.com/ledgerwatch/erigon/zk/datastream/slice_manager"
	"sync/atomic"
	"io"
)

type StreamType uint64
type Command uint64

type EntityDefinition struct {
	Name       string
	StreamType StreamType
	Definition reflect.Type
}

const (
	versionProto         = 2 // converted to proto
	versionAddedBlockEnd = 3 // Added block end
)

type Status struct {
	Stable bool
}

type StreamClient struct {
	ctx          context.Context
	server       string // Server address to connect IP:port
	version      int
	streamType   StreamType
	conn         net.Conn
	id           string            // Client id
	Header       types.HeaderEntry // Header info received (from Header command)
	checkTimeout time.Duration     // time to wait for data before reporting an error

	// atomic
	streaming       atomic.Bool
	lastWrittenTime atomic.Int64
	progress        atomic.Uint64
	highestL2Block  atomic.Uint64

	// stream status
	status      Status
	statusMutex sync.Mutex

	// data slice for sharing data between goroutines
	sliceManager *slice_manager.SliceManager

	// keeps track of the latest fork from the stream to assign to l2 blocks
	currentFork uint64
}

const (
	// StreamTypeSequencer represents a Sequencer stream
	StSequencer StreamType = 1

	// Packet types
	PtPadding = 0
	PtHeader  = 1    // Just for the header page
	PtData    = 2    // Data entry
	PtDataRsp = 0xfe // PtDataRsp is packet type for command response with data
	PtResult  = 0xff // Not stored/present in file (just for client command result)
)

// Creates a new client fo datastream
// server must be in format "url:port"
func NewClient(ctx context.Context, server string, version int, checkTimeout time.Duration, latestDownloadedForkId uint16, highestDownloadedBlock uint64) *StreamClient {
	c := &StreamClient{
		ctx:          ctx,
		checkTimeout: checkTimeout,
		server:       server,
		version:      version,
		streamType:   StSequencer,
		id:           "",
		currentFork:  uint64(latestDownloadedForkId),
	}

	// set progress
	if highestDownloadedBlock > 0 {
		c.progress.Store(highestDownloadedBlock)
	}

	// data slice for sharing data between go routines
	c.sliceManager = slice_manager.NewSliceManager()

	// set initial status
	c.status = Status{
		Stable: false,
	}

	return c
}

func (c *StreamClient) Start() {
	go c.StartStreaming()
}

func (c *StreamClient) GetSliceManager() *slice_manager.SliceManager {
	return c.sliceManager
}

func (c *StreamClient) GetStatus() Status {
	c.statusMutex.Lock()
	defer c.statusMutex.Unlock()
	return c.status
}

func (c *StreamClient) IsVersion3() bool {
	return c.version >= versionAddedBlockEnd
}

func (c *StreamClient) GetLastWrittenTimeAtomic() *atomic.Int64 {
	return &c.lastWrittenTime
}

func (c *StreamClient) GetStreamingAtomic() *atomic.Bool {
	return &c.streaming
}

func (c *StreamClient) GetProgressAtomic() *atomic.Uint64 {
	return &c.progress
}

func (c *StreamClient) GetHighestL2BlockAtomic() *atomic.Uint64 {
	return &c.highestL2Block
}

func (c *StreamClient) StartStreaming() {
	c.streaming.Store(true)

	streamConnFailCount := 0
	streamConnFailThreshold := 10

	for {
		select {
		case <-c.ctx.Done():
			log.Warn("[Datastream client] Context done - stopping")
			return
		default:
			if c.conn == nil {
				if err := c.start(); err != nil {
					c.setStatusStable(false)
					c.conn = nil
					streamConnFailCount++
					if streamConnFailCount >= streamConnFailThreshold {
						streamConnFailMinutes := streamConnFailCount * 6 / 60
						log.Info(fmt.Sprintf("Failed to reconnect the datastream client for %d minute(s): %s", streamConnFailMinutes, err))
					}
					time.Sleep(6 * time.Second)
					continue
				}
			}
			streamConnFailCount = 0
			c.setStatusStable(true)

			// find highest l2 block
			_, err := c.FindHighestL2BlockNumber()
			if err != nil {
				log.Warn(fmt.Sprintf("Failed to find the highest l2 block number: %s", err))
			}

			if err := c.ReadAllEntries(); err != nil {
				c.setStatusStable(false)

				shouldDisconnect := false
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// Handle timeout (connection might be broken or slow)
					shouldDisconnect = false
				} else if err == io.EOF {
					// Graceful connection closure by the remote side
					log.Info("stream closed by remote")
					shouldDisconnect = true
				} else {
					shouldDisconnect = true
				}

				if shouldDisconnect {
					if c.conn != nil {
						err = c.conn.Close()
						if err != nil {
							log.Warn(fmt.Sprintf("Failed to close the data stream connection: %s", err))
						}
						c.conn = nil
					}
					continue
				}
			}

			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (c *StreamClient) setStatusStable(stable bool) {
	c.statusMutex.Lock()
	defer c.statusMutex.Unlock()
	c.status.Stable = stable
}

// Opens a TCP connection to the server
func (c *StreamClient) start() error {
	// Connect to server
	var err error
	c.conn, err = net.Dial("tcp", c.server)
	if err != nil {
		return fmt.Errorf("error connecting to server %s: %w", c.server, err)
	}

	c.id = c.conn.LocalAddr().String()

	return nil
}

func (c *StreamClient) Stop() {
	if err := c.sendStopCmd(); err != nil {
		log.Warn(fmt.Sprintf("Failed to send the stop command to the data stream server: %s", err))
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

// reads entries to the end of the stream
// at end will wait for new entries to arrive
func (c *StreamClient) ReadAllEntries() error {
	var bookmark *types.BookmarkProto
	progress := c.progress.Load()
	if progress == 0 {
		bookmark = types.NewBookmarkProto(0, datastream.BookmarkType_BOOKMARK_TYPE_BATCH)
	} else {
		bookmark = types.NewBookmarkProto(progress, datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK)
	}

	protoBookmark, err := bookmark.Marshal()
	if err != nil {
		return err
	}

	if err := c.initiateDownloadBookmark(protoBookmark); err != nil {
		return err
	}

	return c.readAllFullL2Blocks()
}

// runs the prerequisites for entries download
func (c *StreamClient) initiateDownloadBookmark(bookmark []byte) error {
	// send start command
	if err := c.sendStartBookmarkCmd(bookmark); err != nil {
		return err
	}

	if err := c.afterStartCommand(); err != nil {
		return fmt.Errorf("after start command error: %w", err)
	}

	return nil
}

func (c *StreamClient) afterStartCommand() error {
	// Read packet
	packet, err := readBuffer(c.conn, 1)
	if err != nil {
		return fmt.Errorf("read buffer error %w", err)
	}

	// Read server result entry for the command
	r, err := c.readResultEntry(packet)
	if err != nil {
		return fmt.Errorf("read result entry error: %w", err)
	}

	if err := r.GetError(); err != nil {
		return fmt.Errorf("got Result error code %d: %w", r.ErrorNum, err)
	}

	return nil
}

// reads all entries from the server and sends them to a channel
// sends the parsed FullL2Blocks with transactions to a channel
func (c *StreamClient) readAllFullL2Blocks() error {
	var err error

LOOP:
	for {
		select {
		default:
		case <-c.ctx.Done():
			log.Warn("[Datastream client] Context done - stopping")
			break LOOP
		}

		if c.checkTimeout > 0 {
			c.conn.SetReadDeadline(time.Now().Add(c.checkTimeout))
		}

		parsedProto, localErr := c.readParsedProto()
		if localErr != nil {
			err = localErr
			break
		}
		c.lastWrittenTime.Store(time.Now().UnixNano())

		switch parsedProto := parsedProto.(type) {
		case *types.BookmarkProto:
			continue
		case *types.BatchStart:
			c.currentFork = parsedProto.ForkId
			c.sliceManager.AddItem(parsedProto)
		case *types.GerUpdate:
			c.sliceManager.AddItem(parsedProto)
		case *types.BatchEnd:
			c.sliceManager.AddItem(parsedProto)
		case *types.FullL2Block:
			parsedProto.ForkId = c.currentFork
			log.Trace("writing block to channel", "blockNumber", parsedProto.L2BlockNumber, "batchNumber", parsedProto.BatchNumber)
			c.progress.Store(parsedProto.L2BlockNumber)
			c.sliceManager.AddItem(parsedProto)
		default:
			err = fmt.Errorf("unexpected entry type: %v", parsedProto)
			break LOOP
		}
	}

	return err
}

func (c *StreamClient) readParsedProto() (
	parsedEntry interface{},
	err error,
) {
	file, err := c.readFileEntry()
	if err != nil {
		err = fmt.Errorf("read file entry error: %w", err)
		return
	}

	switch file.EntryType {
	case types.BookmarkEntryType:
		parsedEntry, err = types.UnmarshalBookmark(file.Data)
	case types.EntryTypeGerUpdate:
		parsedEntry, err = types.DecodeGerUpdateProto(file.Data)
	case types.EntryTypeBatchStart:
		parsedEntry, err = types.UnmarshalBatchStart(file.Data)
	case types.EntryTypeBatchEnd:
		parsedEntry, err = types.UnmarshalBatchEnd(file.Data)
	case types.EntryTypeL2Block:
		var l2Block *types.FullL2Block
		if l2Block, err = types.UnmarshalL2Block(file.Data); err != nil {
			return
		}

		txs := []types.L2TransactionProto{}

		var innerFile *types.FileEntry
		var l2Tx *types.L2TransactionProto
	LOOP:
		for {
			if innerFile, err = c.readFileEntry(); err != nil {
				return
			}

			if innerFile.IsL2Tx() {
				if l2Tx, err = types.UnmarshalTx(innerFile.Data); err != nil {
					return
				}
				txs = append(txs, *l2Tx)
			} else if innerFile.IsL2BlockEnd() {
				var l2BlockEnd *types.L2BlockEndProto
				if l2BlockEnd, err = types.UnmarshalL2BlockEnd(innerFile.Data); err != nil {
					return
				}
				if l2BlockEnd.GetBlockNumber() != l2Block.L2BlockNumber {
					err = fmt.Errorf("block end number (%d) not equal to block number (%d)", l2BlockEnd.GetBlockNumber(), l2Block.L2BlockNumber)
					return
				}
				break LOOP
			} else if innerFile.IsBookmark() {
				var bookmark *types.BookmarkProto
				if bookmark, err = types.UnmarshalBookmark(innerFile.Data); err != nil || bookmark == nil {
					return
				}
				if bookmark.BookmarkType() == datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK {
					break LOOP
				} else {
					err = fmt.Errorf("unexpected bookmark type inside block: %v", bookmark.Type())
					return
				}
			} else if innerFile.IsBatchEnd() {
				if _, err = types.UnmarshalBatchEnd(file.Data); err != nil {
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
	case types.EntryTypeL2Tx:
		err = fmt.Errorf("unexpected l2Tx out of block")
	default:
		err = fmt.Errorf("unexpected entry type: %d", file.EntryType)
	}
	return
}

// reads file bytes from socket and tries to parse them
// returns the parsed FileEntry
func (c *StreamClient) readFileEntry() (file *types.FileEntry, err error) {
	if c.conn == nil {
		return file, errors.New("connection is nil")
	}

	// Read packet type
	packet, err := readBuffer(c.conn, 1)
	if err != nil {
		return file, fmt.Errorf("failed to read packet type: %w", err)
	}

	// Check packet type
	if packet[0] == PtResult {
		// Read server result entry for the command
		r, err := c.readResultEntry(packet)
		if err != nil {
			return file, err
		}
		if err := r.GetError(); err != nil {
			return file, fmt.Errorf("got Result error code %d: %w", r.ErrorNum, err)
		}
		return file, nil
	} else if packet[0] != PtData {
		return file, fmt.Errorf("error expecting data packet type %d and received %d", PtData, packet[0])
	}

	// Read the rest of fixed size fields
	buffer, err := readBuffer(c.conn, types.FileEntryMinSize-1)
	if err != nil {
		return file, fmt.Errorf("error reading file bytes: %w", err)
	}
	buffer = append(packet, buffer...)

	// Read variable field (data)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < types.FileEntryMinSize {
		return file, errors.New("error reading data entry: wrong data length")
	}

	// Read rest of the file data
	bufferAux, err := readBuffer(c.conn, length-types.FileEntryMinSize)
	if err != nil {
		return file, fmt.Errorf("error reading file data bytes: %w", err)
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary data to data entry struct
	if file, err = types.DecodeFileEntry(buffer); err != nil {
		return file, fmt.Errorf("decode file entry error: %w", err)
	}

	return
}

func (c *StreamClient) readDataEntry() (*types.FileEntry, error) {
	file, err := readBuffer(c.conn, types.FileEntryMinSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read header bytes %w", err)
	}

	fileLength := binary.BigEndian.Uint32(file[1:5])
	if fileLength >= types.FileEntryMinSize {
		// Read the rest of fixed size fields
		buffer, err := readBuffer(c.conn, fileLength-types.FileEntryMinSize)
		if err != nil {
			return nil, fmt.Errorf("failed to read header bytes %w", err)
		}
		file = append(file, buffer...)
	}

	return types.DecodeFileEntry(file)
}

// reads header bytes from socket and tries to parse them
// returns the parsed HeaderEntry
func (c *StreamClient) readHeaderEntry() (h *types.HeaderEntry, err error) {

	// Read header stream bytes
	binaryHeader, err := readBuffer(c.conn, types.HeaderSizePreEtrog)
	if err != nil {
		return h, fmt.Errorf("failed to read header bytes %w", err)
	}

	headLength := binary.BigEndian.Uint32(binaryHeader[1:5])
	if headLength == types.HeaderSize {
		// Read the rest of fixed size fields
		buffer, err := readBuffer(c.conn, types.HeaderSize-types.HeaderSizePreEtrog)
		if err != nil {
			return h, fmt.Errorf("failed to read header bytes %w", err)
		}
		binaryHeader = append(binaryHeader, buffer...)
	}

	// Decode bytes stream to header entry struct
	if h, err = types.DecodeHeaderEntry(binaryHeader); err != nil {
		return h, fmt.Errorf("error decoding binary header: %w", err)
	}

	return
}

// reads result bytes and tries to parse them
// returns the parsed ResultEntry
func (c *StreamClient) readResultEntry(packet []byte) (re *types.ResultEntry, err error) {
	if len(packet) != 1 {
		return re, fmt.Errorf("expected packet size of 1, got: %d", len(packet))
	}

	// Read the rest of fixed size fields
	buffer, err := readBuffer(c.conn, types.ResultEntryMinSize-1)
	if err != nil {
		return re, fmt.Errorf("failed to read main result bytes %w", err)
	}
	buffer = append(packet, buffer...)

	// Read variable field (errStr)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < types.ResultEntryMinSize {
		return re, fmt.Errorf("%s Error reading result entry", c.id)
	}

	// read the rest of the result
	bufferAux, err := readBuffer(c.conn, length-types.ResultEntryMinSize)
	if err != nil {
		return re, fmt.Errorf("failed to read result errStr bytes %w", err)
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary entry result
	if re, err = types.DecodeResultEntry(buffer); err != nil {
		return re, fmt.Errorf("decode result entry error: %w", err)
	}

	return re, nil
}

func (c *StreamClient) GetTotalEntries() uint64 {
	return c.Header.TotalEntries
}

// Command header: Get status
// Returns the current status of the header.
// If started, terminate the connection.
func (c *StreamClient) GetHeader() (*types.HeaderEntry, error) {
	if err := c.sendHeaderCmd(); err != nil {
		return nil, fmt.Errorf("%s send header error: %w", c.id, err)
	}

	// Read packet
	packet, err := readBuffer(c.conn, 1)
	if err != nil {
		return nil, fmt.Errorf("%s read buffer: %w", c.id, err)
	}

	// Check packet type
	if packet[0] != PtResult {
		return nil, fmt.Errorf("%s error expecting result packet type %d and received %d", c.id, PtResult, packet[0])
	}

	// Read server result entry for the command
	r, err := c.readResultEntry(packet)
	if err != nil {
		return nil, fmt.Errorf("%s read result entry error: %w", c.id, err)
	}
	if err := r.GetError(); err != nil {
		return nil, fmt.Errorf("%s got Result error code %d: %w", c.id, r.ErrorNum, err)
	}

	// Read header entry
	h, err := c.readHeaderEntry()
	if err != nil {
		return nil, fmt.Errorf("%s read header entry error: %w", c.id, err)
	}

	c.Header = *h

	return &c.Header, nil
}

// Command entry: Get entry at entryNo
// If started, terminate the connection.
func (c *StreamClient) GetEntry(entryNo uint64) (interface{}, error) {
	if err := c.sendEntryCommand(entryNo); err != nil {
		return nil, fmt.Errorf("%s send header error: %w", c.id, err)
	}

	// Read packet
	packet, err := readBuffer(c.conn, 1)
	if err != nil {
		return nil, fmt.Errorf("%s read buffer: %w", c.id, err)
	}

	// Check packet type
	if packet[0] != PtResult {
		return nil, fmt.Errorf("%s error expecting result packet type %d and received %d", c.id, PtResult, packet[0])
	}

	// Read server result entry for the command
	r, err := c.readResultEntry(packet)
	if err != nil {
		return nil, fmt.Errorf("%s read result entry error: %w", c.id, err)
	}
	if err := r.GetError(); err != nil {
		return nil, fmt.Errorf("%s got Result error code %d: %w", c.id, r.ErrorNum, err)
	}

	// Read data entry
	fe, err := c.readDataEntry()
	if err != nil {
		return nil, fmt.Errorf("%s read header entry error: %w", c.id, err)
	}

	return fe, nil
}

// TODO: only used in correctness checker!
func (c *StreamClient) ExecutePerFile(bookmark *types.BookmarkProto, function func(file *types.FileEntry) error) error {
	// Get header from server
	if _, err := c.GetHeader(); err != nil {
		return fmt.Errorf("%s get header error: %w", c.id, err)
	}

	protoBookmark, err := bookmark.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal bookmark: %w", err)
	}

	if err := c.initiateDownloadBookmark(protoBookmark); err != nil {
		return err
	}
	count := uint64(0)
	logTicker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-logTicker.C:
			fmt.Println("Entries read count: ", count)
		default:
		}
		if c.Header.TotalEntries == count {
			break
		}
		file, err := c.readFileEntry()
		if err != nil {
			return fmt.Errorf("reading file entry: %w", err)
		}
		if err := function(file); err != nil {
			return fmt.Errorf("executing function: %w", err)

		}
		count++
	}

	return nil
}

// read stream size (TotalEntries) and iterate backwards to find highest l2 block number
func (c *StreamClient) FindHighestL2BlockNumber() (uint64, error) {
	// Get header from server
	if _, err := c.GetHeader(); err != nil {
		return 0, fmt.Errorf("%s get header error: %w", c.id, err)
	}

	entryNo := c.Header.TotalEntries

	// send entry command and iterate backwards until we get an L2 block
	for entryNo > 0 {
		fe, err := c.GetEntry(entryNo)
		if err != nil {
			continue
		}
		entryNo--

		if fe == nil {
			continue
		}

		c.start() // if we get a result we must restart connection

		// parse fe
		if fileEntry, ok := fe.(*types.FileEntry); ok {
			if fileEntry.EntryType == types.EntryTypeL2Block {
				l2Block, err := types.UnmarshalL2Block(fileEntry.Data)
				if err != nil {
					return 0, fmt.Errorf("%s unmarshal L2 block error: %w", c.id, err)
				}

				// store highest block number
				currentHighest := c.highestL2Block.Load()
				if l2Block.L2BlockNumber > currentHighest {
					c.highestL2Block.Store(l2Block.L2BlockNumber)
				}

				return l2Block.L2BlockNumber, nil
			}
		}

		fe = nil

		// check if it's a L2 block
		//	if fe.IsL2Block() {
		//		l2Block, err := types.UnmarshalL2Block(fe.Data)
		//		if err != nil {
		//			return 0, fmt.Errorf("%s unmarshal L2 block error: %w", c.id, err)
		//		}
		//
		//		// store highest block number
		//		currentHighest := c.highestL2Block.Load()
		//		if l2Block.L2BlockNumber > currentHighest {
		//			c.highestL2Block.Store(l2Block.L2BlockNumber)
		//		}
		//
		//		return l2Block.L2BlockNumber, nil
		//	}
	}

	c.start()

	return 0, fmt.Errorf("%s no L2 block found", c.id)
}
