package client

import "fmt"

const (
	// Commands
	CmdUnknown       Command = 0
	CmdStart         Command = 1
	CmdStop          Command = 2
	CmdHeader        Command = 3
	CmdStartBookmark Command = 4 // CmdStartBookmark for the start from bookmark TCP client command
	CmdEntry         Command = 5 // CmdEntry for the get entry TCP client command
	CmdBookmark      Command = 6 // CmdBookmark for the get bookmark TCP client command
)

// sendHeaderCmd sends the header command to the server.
func (c *StreamClient) sendHeaderCmd() error {
	err := c.sendCommandAndStreamType(CmdHeader)
	if err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	return nil
}

// sendBookmarkCmd sends either CmdStartBookmark or CmdBookmark for the provided bookmark value.
// In case streaming parameter is set to true, the CmdStartBookmark is sent, otherwise the CmdBookmark.
func (c *StreamClient) sendBookmarkCmd(bookmark []byte, streaming bool) error {
	// in case we want to stream the entries, CmdStartBookmark is sent, otherwise CmdBookmark command
	command := CmdStartBookmark
	if !streaming {
		command = CmdBookmark
	}

	// Send the command
	if err := c.sendCommandAndStreamType(command); err != nil {
		return err
	}

	// Send bookmark length
	if err := writeFullUint32ToConn(c.conn, uint32(len(bookmark))); err != nil {
		return err
	}

	// Send the bookmark to retrieve
	return writeBytesToConn(c.conn, bookmark)
}

// sendEntryCommand sends an entry command to the server, indicating
// that the client wishes to stream the given entry number.
func (c *StreamClient) sendEntryCommand(entryNum uint64) error {
	err := c.sendCommandAndStreamType(CmdEntry)
	if err != nil {
		return err
	}

	// Send entry number
	if err := writeFullUint64ToConn(c.conn, entryNum); err != nil {
		return err
	}

	return nil
}

// sendStartCmd sends a start command to the server, indicating
// that the client wishes to start streaming from the given entry number.
func (c *StreamClient) sendStartCmd(from uint64) error {
	err := c.sendCommandAndStreamType(CmdStart)
	if err != nil {
		return err
	}

	// Send starting/from entry number
	return writeFullUint64ToConn(c.conn, from)
}

// sendEntryCmd sends the get data stream entry by number command to a TCP connection
func (c *StreamClient) sendEntryCmd(entryNum uint64) error {
	// Send CmdEntry command
	if err := c.sendCommandAndStreamType(CmdEntry); err != nil {
		return err
	}

	// Send entry number
	return writeFullUint64ToConn(c.conn, entryNum)
}

// sendHeaderCmd sends the header command to the server.
func (c *StreamClient) sendStopCmd() error {
	err := c.sendCommandAndStreamType(CmdStop)
	if err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	return nil
}

func (c *StreamClient) sendCommandAndStreamType(cmd Command) error {
	// Send command
	if err := writeFullUint64ToConn(c.conn, uint64(cmd)); err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	// Send stream type
	if err := writeFullUint64ToConn(c.conn, uint64(c.streamType)); err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	return nil
}
