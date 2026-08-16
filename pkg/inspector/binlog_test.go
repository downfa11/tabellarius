package inspector

import (
	"testing"
	"time"

	"github.com/downfa11-org/tabellarius/pkg/model"
	"github.com/go-mysql-org/go-mysql/replication"
)

func TestParseDSN(t *testing.T) {
	b := &BinlogInspector{
		dsn: "user:pass@tcp(localhost:3307)/mydb",
	}

	if err := b.parseDSN(); err != nil {
		t.Fatalf("parseDSN failed: %v", err)
	}
	if b.user != "user" || b.password != "pass" {
		t.Fatalf("auth parse failed")
	}
	if b.host != "localhost" || b.port != 3307 {
		t.Fatalf("host parse failed: %s:%d", b.host, b.port)
	}
}

func TestEmitRowEvents_ConfiguredTableEmitsOperationsWithPK(t *testing.T) {
	tests := []struct {
		name      string
		eventType replication.EventType
		rows      [][]interface{}
		before    string
		after     string
	}{
		{
			name:      "INSERT",
			eventType: replication.WRITE_ROWS_EVENTv2,
			rows:      [][]interface{}{{1, "after"}},
			after:     "after",
		},
		{
			name:      "DELETE",
			eventType: replication.DELETE_ROWS_EVENTv2,
			rows:      [][]interface{}{{1, "before"}},
			before:    "before",
		},
		{
			name:      "UPDATE",
			eventType: replication.UPDATE_ROWS_EVENTv2,
			rows:      [][]interface{}{{1, "before"}, {1, "after"}},
			before:    "before",
			after:     "after",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := make(chan model.Event, 1)
			b := &BinlogInspector{
				currentFile: "binlog.000001",
				currentTxID: "tx-1",
				tableMeta: map[string]*tableMeta{
					"test.users": {
						pkName:  "id",
						pkIndex: 0,
						columns: []string{"id", "name"},
					},
				},
			}

			ev := &replication.RowsEvent{
				Table: &replication.TableMapEvent{
					Schema: []byte("test"),
					Table:  []byte("users"),
				},
				Rows: tt.rows,
			}
			header := &replication.EventHeader{EventType: tt.eventType, LogPos: 123, Timestamp: 1786851296}

			b.emitRowEvents(out, header, ev)

			select {
			case got := <-out:
				rowEvt, ok := got.(*model.BinlogRowEvent)
				if !ok {
					t.Fatalf("unexpected event type: %T", got)
				}
				if rowEvt.TxID() != "tx-1" {
					t.Fatalf("unexpected txID: %s", rowEvt.TxID())
				}
				if gotTimestamp := rowEvt.Timestamp(); !gotTimestamp.Equal(time.Unix(1786851296, 0).UTC()) {
					t.Fatalf("row timestamp = %s, want binlog header timestamp", gotTimestamp)
				}
				if len(rowEvt.Changes()) != 1 || len(rowEvt.Changes()[0].Rows) != 1 {
					t.Fatalf("unexpected changes: %#v", rowEvt.Changes())
				}

				change := rowEvt.Changes()[0]
				if change.Op != model.OpType(tt.name) {
					t.Fatalf("expected %s, got %s", tt.name, change.Op)
				}
				row := change.Rows[0]
				if row.PK["id"] != 1 {
					t.Fatalf("unexpected primary key: %#v", row.PK)
				}
				if tt.before == "" && row.Before != nil || tt.before != "" && row.Before["name"] != tt.before {
					t.Fatalf("unexpected before row: %#v", row.Before)
				}
				if tt.after == "" && row.After != nil || tt.after != "" && row.After["name"] != tt.after {
					t.Fatalf("unexpected after row: %#v", row.After)
				}
			default:
				t.Fatal("no row event emitted")
			}
		})
	}
}

func TestEmitRowEvents_UpdateInvalid(t *testing.T) {
	out := make(chan model.Event, 1)

	b := &BinlogInspector{
		currentTxID: "tx-1",
		tableMeta: map[string]*tableMeta{
			"test.users": {
				columns: []string{"id", "name"},
			},
		},
	}

	ev := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("test"),
			Table:  []byte("users"),
		},
		Rows: [][]interface{}{
			{1, "before"},
		},
	}

	header := &replication.EventHeader{
		EventType: replication.UPDATE_ROWS_EVENTv2,
		LogPos:    10,
	}

	b.emitRowEvents(out, header, ev)
	close(out)

	if _, ok := <-out; ok {
		t.Fatalf("expected no events for invalid update")
	}
}

func TestEmitRowEvents_SkipsUnconfiguredTable(t *testing.T) {
	out := make(chan model.Event, 1)
	b := &BinlogInspector{
		tableMeta: map[string]*tableMeta{
			"test.users": {pkName: "id", pkIndex: 0, columns: []string{"id", "name"}},
		},
	}
	ev := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("test"),
			Table:  []byte("payments"),
		},
		Rows: [][]interface{}{{1, "ignored"}},
	}

	b.emitRowEvents(out, &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2, LogPos: 123}, ev)

	if len(out) != 0 {
		t.Fatal("expected no row event for an unconfigured table")
	}
	if b.currentTxID != "tx:123" {
		t.Fatalf("unexpected transaction ID for unconfigured table: %s", b.currentTxID)
	}
}

func TestEmitRowEvents_SkipsAuditSinkOutsideCaptureAllowList(t *testing.T) {
	out := make(chan model.Event, 1)
	b := &BinlogInspector{
		tableMeta: map[string]*tableMeta{
			"sales.orders": {pkName: "id", pkIndex: 0, columns: []string{"id", "total"}},
		},
	}
	ev := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("sales"),
			Table:  []byte("revision_history"),
		},
		Rows: [][]interface{}{{101, "order updated"}},
	}

	b.emitRowEvents(out, &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2, LogPos: 124}, ev)

	if len(out) != 0 {
		t.Fatal("expected no row event for an audit sink outside the capture allow-list")
	}
	if b.currentTxID != "tx:124" {
		t.Fatalf("unexpected transaction ID for audit sink: %s", b.currentTxID)
	}
}

func TestBinlogEventTimestampFallsBackWhenHeaderTimestampIsMissing(t *testing.T) {
	timestamp := binlogEventTimestamp(&replication.EventHeader{LogPos: 123})
	if timestamp.IsZero() {
		t.Fatal("fallback timestamp must not be zero")
	}
	if timestamp.Location() != time.UTC {
		t.Fatalf("fallback timestamp location = %s, want UTC", timestamp.Location())
	}
}
