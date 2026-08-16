package model

import "time"

type BinlogRowEvent struct {
	source    SourceType
	offset    Offset
	txID      string
	changes   []RowChange
	timestamp time.Time
}

func NewBinlogRowEvent(source SourceType, offset Offset, txID string, changes []RowChange) *BinlogRowEvent {
	return NewBinlogRowEventAt(source, offset, txID, time.Now().UTC(), changes)
}

func NewBinlogRowEventAt(source SourceType, offset Offset, txID string, timestamp time.Time, changes []RowChange) *BinlogRowEvent {
	return &BinlogRowEvent{
		source:    source,
		offset:    offset,
		txID:      txID,
		changes:   changes,
		timestamp: normalizeTimestamp(timestamp),
	}
}

func (e *BinlogRowEvent) Source() SourceType   { return e.source }
func (e *BinlogRowEvent) Offset() Offset       { return e.offset }
func (e *BinlogRowEvent) TxID() string         { return e.txID }
func (e *BinlogRowEvent) Changes() []RowChange { return e.changes }
func (e *BinlogRowEvent) Timestamp() time.Time { return e.timestamp }

type BinlogDDLEvent struct {
	source    SourceType
	offsetVal MySQLOffset
	txID      string
	query     string
	timestamp time.Time
}

func NewBinlogDDLEvent(src SourceType, offset MySQLOffset, txID, query string) *BinlogDDLEvent {
	return NewBinlogDDLEventAt(src, offset, txID, time.Now().UTC(), query)
}

func NewBinlogDDLEventAt(src SourceType, offset MySQLOffset, txID string, timestamp time.Time, query string) *BinlogDDLEvent {
	return &BinlogDDLEvent{
		source:    src,
		offsetVal: offset,
		txID:      txID,
		query:     query,
		timestamp: normalizeTimestamp(timestamp),
	}
}

func (e *BinlogDDLEvent) Source() SourceType   { return e.source }
func (e *BinlogDDLEvent) Offset() Offset       { return e.offsetVal }
func (e *BinlogDDLEvent) TxID() string         { return e.txID }
func (e *BinlogDDLEvent) Query() string        { return e.query }
func (e *BinlogDDLEvent) Type() string         { return "ddl" }
func (e *BinlogDDLEvent) Timestamp() time.Time { return e.timestamp }
