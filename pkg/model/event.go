package model

import "time"

type Event interface {
	Source() SourceType
	Offset() Offset
}

// TimestampedEvent provides the UTC event time used by CDC envelopes.
// Event remains unchanged so existing implementations remain compatible.
type TimestampedEvent interface {
	Event
	Timestamp() time.Time
}

func normalizeTimestamp(timestamp time.Time) time.Time {
	if timestamp.IsZero() {
		return time.Now().UTC()
	}
	return timestamp.UTC()
}

type SourceType string

const (
	SourceMySQLBinlog SourceType = "mysql-binlog"
	SourcePostgresWal SourceType = "postgres-wal"
)

type OpType string

const (
	OpInsert OpType = "INSERT"
	OpUpdate OpType = "UPDATE"
	OpDelete OpType = "DELETE"
)
