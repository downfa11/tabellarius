package model

import "time"

type TxBoundaryKind string

const (
	TxBegin    TxBoundaryKind = "BEGIN"
	TxCommit   TxBoundaryKind = "COMMIT"
	TxRollback TxBoundaryKind = "ROLLBACK"
)

type TransactionBoundaryEvent struct {
	source    SourceType
	offset    Offset
	txID      string
	kind      TxBoundaryKind
	timestamp time.Time
}

func NewTransactionBoundaryEvent(source SourceType, offset Offset, txID string, kind TxBoundaryKind) *TransactionBoundaryEvent {
	return NewTransactionBoundaryEventAt(source, offset, txID, kind, time.Now().UTC())
}

func NewTransactionBoundaryEventAt(source SourceType, offset Offset, txID string, kind TxBoundaryKind, timestamp time.Time) *TransactionBoundaryEvent {
	return &TransactionBoundaryEvent{
		source:    source,
		offset:    offset,
		txID:      txID,
		kind:      kind,
		timestamp: normalizeTimestamp(timestamp),
	}
}

func (e *TransactionBoundaryEvent) Source() SourceType   { return e.source }
func (e *TransactionBoundaryEvent) Offset() Offset       { return e.offset }
func (e *TransactionBoundaryEvent) TxID() string         { return e.txID }
func (e *TransactionBoundaryEvent) Kind() TxBoundaryKind { return e.kind }
func (e *TransactionBoundaryEvent) Timestamp() time.Time { return e.timestamp }

type TransactionEvent struct {
	source    SourceType
	offset    Offset
	txID      string
	changes   []RowChange
	timestamp time.Time
}

func NewTransactionEvent(source SourceType, offset Offset, txID string, changes []RowChange) *TransactionEvent {
	return NewTransactionEventAt(source, offset, txID, time.Now().UTC(), changes)
}

func NewTransactionEventAt(source SourceType, offset Offset, txID string, timestamp time.Time, changes []RowChange) *TransactionEvent {
	return &TransactionEvent{
		source:    source,
		offset:    offset,
		txID:      txID,
		changes:   changes,
		timestamp: normalizeTimestamp(timestamp),
	}
}

func (e *TransactionEvent) Source() SourceType   { return e.source }
func (e *TransactionEvent) Offset() Offset       { return e.offset }
func (e *TransactionEvent) TxID() string         { return e.txID }
func (e *TransactionEvent) Changes() []RowChange { return e.changes }
func (e *TransactionEvent) Timestamp() time.Time { return e.timestamp }
