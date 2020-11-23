package mongoproto

import (
	"fmt"
	"io"
)

// OpKillCursors is used to close an active cursor in the database. This is necessary
// to ensure that database resources are reclaimed at the end of the query.
// http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-kill-cursors
type OpKillCursors struct {
	Header    MsgHeader
	CursorIDs []int64
}

func (op *OpKillCursors) OpCode() OpCode {
	return OpCodeUpdate
}

func (op *OpKillCursors) String() string {

	return fmt.Sprintf("OpKillCursors (%d)", op.CursorIDs)
}

func (op *OpKillCursors) FromReader(r io.Reader) error {
	var b [8]byte
	// Zero
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return err
	}

	// number of cursor ids
	num := getInt32(b[:], 4)

	curIDBuf := make([]byte, num*8)

	_, err = io.ReadFull(r, curIDBuf[:])
	if err != nil {
		return err
	}

	for i := 0; i < int(num); i++ {
		op.CursorIDs = append(op.CursorIDs, getInt64(curIDBuf, i*8))
	}

	return nil
}
