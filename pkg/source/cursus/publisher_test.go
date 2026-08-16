package cursus

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/sdk"
	"github.com/downfa11-org/tabellarius/pkg/model"
)

type fakeProducer struct {
	message          string
	acked            int
	ackOnFlush       bool
	createTopic      string
	createPartitions int
	createErr        error
	closed           bool
}

func (p *fakeProducer) PublishMessage(message string) (uint64, error) {
	p.message = message
	return 1, nil
}

func (p *fakeProducer) CreateTopic(topic string, partitions int) error {
	p.createTopic = topic
	p.createPartitions = partitions
	return p.createErr
}

func (p *fakeProducer) Flush() {
	if p.ackOnFlush {
		p.acked++
	}
}

func (p *fakeProducer) GetUniqueAckCount() int { return p.acked }

func (p *fakeProducer) Close() error {
	p.closed = true
	return nil
}

func publisherTestConfig(autoCreate bool) *sdk.PublisherConfig {
	cfg := sdk.NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{"broker:9000"}
	cfg.Topic = "commerce.revision-history.v1"
	cfg.AutoCreateTopics = autoCreate
	cfg.Partitions = 1
	cfg.AckTimeoutMS = 25
	return cfg
}

func TestNewPublisherCreatesAndVerifiesConfiguredTopic(t *testing.T) {
	cfg := publisherTestConfig(true)
	fake := &fakeProducer{}
	checks := 0

	publisher, err := newPublisher(cfg, func(*sdk.PublisherConfig) (publisherClient, error) {
		return fake, nil
	}, func(*sdk.PublisherConfig) error {
		checks++
		if fake.createTopic == "" {
			return errors.New("topic not found")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("newPublisher() error = %v", err)
	}
	if publisher == nil {
		t.Fatal("expected publisher")
	}
	if fake.createTopic != cfg.Topic || fake.createPartitions != cfg.Partitions {
		t.Fatalf("unexpected topic creation: topic=%q partitions=%d", fake.createTopic, fake.createPartitions)
	}
	if checks != 1 {
		t.Fatalf("expected post-create topic verification, got %d checks", checks)
	}
}

func TestNewPublisherRejectsMissingTopicWhenAutoCreateDisabled(t *testing.T) {
	cfg := publisherTestConfig(false)
	missing := errors.New("ERROR: topic_not_found topic=commerce.revision-history.v1")

	publisher, err := newPublisher(cfg, func(*sdk.PublisherConfig) (publisherClient, error) {
		t.Fatal("producer must not start when the topic is missing")
		return nil, nil
	}, func(*sdk.PublisherConfig) error {
		return missing
	})
	if publisher != nil {
		t.Fatal("expected no publisher")
	}
	if err == nil || !strings.Contains(err.Error(), "auto_create_topics=false") || !strings.Contains(err.Error(), cfg.Topic) {
		t.Fatalf("expected clear missing topic error, got %v", err)
	}
}

func TestPublisherReturnsOnlyAfterBrokerAcknowledgement(t *testing.T) {
	fake := &fakeProducer{ackOnFlush: true}
	publisher := &Publisher{pub: fake, cfg: publisherTestConfig(true)}
	event := model.NewTransactionEvent(
		model.SourceMySQLBinlog,
		model.MySQLOffset{File: "mysql-bin.000001", Pos: 42},
		"tx-1",
		[]model.RowChange{{Schema: "commerce", Table: "revision_history", Op: model.OpInsert, Rows: []model.RowData{{After: map[string]any{"id": 1}}}}},
	)

	if err := publisher.Publish(event); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if fake.acked != 1 {
		t.Fatalf("expected one broker acknowledgement, got %d", fake.acked)
	}

	var payload eventPayload
	if err := json.Unmarshal([]byte(fake.message), &payload); err != nil {
		t.Fatalf("published payload is not JSON: %v", err)
	}
	if payload.Type != "transaction" || payload.TxID != "tx-1" || len(payload.Changes) != 1 {
		t.Fatalf("unexpected published payload: %+v", payload)
	}
}

func TestPublisherFailsWhenBrokerDoesNotAcknowledge(t *testing.T) {
	publisher := &Publisher{pub: &fakeProducer{}, cfg: publisherTestConfig(true)}
	publisher.cfg.AckTimeoutMS = 1
	event := model.NewTransactionBoundaryEvent(model.SourceMySQLBinlog, model.MySQLOffset{}, "tx-1", model.TxCommit)

	err := publisher.Publish(event)
	if err == nil || !strings.Contains(err.Error(), "acknowledgement timeout") {
		t.Fatalf("expected acknowledgement timeout, got %v", err)
	}
}

func TestLoadPublisherConfigPreservesSDKConfigShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publisher.yaml")
	data := []byte("broker_addrs:\n  - cursus-broker:9000\ntopic: commerce.revision-history.v1\nauto_create_topics: true\npartitions: 1\nacks: \"1\"\nenable_idempotence: true\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := loadPublisherConfig(path, "")
	if err != nil {
		t.Fatalf("loadPublisherConfig() error = %v", err)
	}
	if cfg.Topic != "commerce.revision-history.v1" || !cfg.AutoCreateTopics || cfg.Partitions != 1 || cfg.BrokerAddrs[0] != "cursus-broker:9000" {
		t.Fatalf("unexpected publisher config: %+v", cfg)
	}
}
