package source

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/downfa11-org/tabellarius/pkg/config"
	"github.com/downfa11-org/tabellarius/pkg/model"
)

func TestNewFromConfig_ReturnsPublisherInitializationError(t *testing.T) {
	cfg := &config.Config{
		Database: config.Database{
			Type:   model.MySQL,
			Schema: "test",
		},
		CDCServer: config.CDCServer{
			OffsetFile:      "/tmp/offset",
			PublisherConfig: filepath.Join(t.TempDir(), "missing-publisher.yaml"),
		},
	}

	src, err := NewFromConfig(cfg)
	if err == nil {
		t.Fatal("expected publisher initialization failure")
	}
	if src != nil {
		t.Fatal("expected no source when publisher initialization fails")
	}
	if !strings.Contains(err.Error(), "initialize cursus publisher") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type sourceTestPublisher struct {
	published []model.Event
	err       error
}

func (p *sourceTestPublisher) Publish(event model.Event) error {
	p.published = append(p.published, event)
	return p.err
}

func (p *sourceTestPublisher) Close() error { return nil }

func TestRunPublishesTimestampedTransactionBeforeCheckpoint(t *testing.T) {
	timestamp := time.Date(2026, time.August, 16, 3, 34, 56, 123456789, time.UTC)
	offset := model.MySQLOffset{File: "mysql-bin.000001", Pos: 42}
	publisher := &sourceTestPublisher{}
	var checkpoints []model.Offset
	source := &TabellariusSource{
		pub: publisher,
		checkpoint: func(got model.Offset) error {
			checkpoints = append(checkpoints, got)
			return nil
		},
	}
	in := make(chan model.Event, 2)
	in <- model.NewBinlogRowEventAt(model.SourceMySQLBinlog, offset, "tx-1", timestamp, []model.RowChange{{Schema: "commerce", Table: "orders", Op: model.OpInsert, Rows: []model.RowData{{After: map[string]any{"id": 1}}}}})
	in <- model.NewTransactionBoundaryEventAt(model.SourceMySQLBinlog, offset, "tx-1", model.TxCommit, timestamp)
	close(in)

	if err := source.run(context.Background(), in); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("published event count = %d, want 1", len(publisher.published))
	}
	transaction, ok := publisher.published[0].(*model.TransactionEvent)
	if !ok {
		t.Fatalf("published event type = %T, want *model.TransactionEvent", publisher.published[0])
	}
	if !transaction.Timestamp().Equal(timestamp) {
		t.Fatalf("transaction timestamp = %s, want %s", transaction.Timestamp(), timestamp)
	}
	if len(checkpoints) != 1 || checkpoints[0].Compare(offset) != 0 {
		t.Fatalf("checkpoint = %#v, want %v", checkpoints, offset)
	}
}

func TestRunDoesNotCheckpointWhenPublishFails(t *testing.T) {
	offset := model.MySQLOffset{File: "mysql-bin.000001", Pos: 42}
	publisher := &sourceTestPublisher{err: errors.New("broker unavailable")}
	checkpointed := false
	source := &TabellariusSource{
		pub: publisher,
		checkpoint: func(model.Offset) error {
			checkpointed = true
			return nil
		},
	}
	in := make(chan model.Event, 2)
	in <- model.NewBinlogRowEvent(model.SourceMySQLBinlog, offset, "tx-1", []model.RowChange{{Schema: "commerce", Table: "orders", Op: model.OpInsert, Rows: []model.RowData{{After: map[string]any{"id": 1}}}}})
	in <- model.NewTransactionBoundaryEvent(model.SourceMySQLBinlog, offset, "tx-1", model.TxCommit)
	close(in)

	err := source.run(context.Background(), in)
	if err == nil || !strings.Contains(err.Error(), "publish transaction") {
		t.Fatalf("expected publish failure, got %v", err)
	}
	if checkpointed {
		t.Fatal("checkpoint must not advance when publish fails")
	}
}
