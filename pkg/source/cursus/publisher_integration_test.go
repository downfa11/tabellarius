//go:build integration

package cursus

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/downfa11-org/tabellarius/pkg/model"
)

func TestCursusPublisherCreatesTopicPublishesAndConsumerGroupReads(t *testing.T) {
	addr := os.Getenv("CURSUS_ADDR")
	if addr == "" {
		t.Fatal("CURSUS_ADDR must name a running empty Cursus broker")
	}

	cfg := sdk.NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{addr}
	cfg.Topic = "commerce.revision-history.v1"
	cfg.AutoCreateTopics = true
	cfg.Partitions = 1
	cfg.Acks = "1"
	cfg.EnableIdempotence = true
	cfg.BatchSize = 1
	cfg.AckTimeoutMS = 5000
	cfg.FlushTimeoutMS = 10000

	publisher, err := newPublisher(cfg, func(cfg *sdk.PublisherConfig) (publisherClient, error) {
		return sdk.NewProducer(cfg)
	}, checkTopic)
	if err != nil {
		t.Fatalf("newPublisher() error = %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("publisher.Close() error = %v", err)
		}
	}()

	consumerClientCfg := sdk.NewDefaultConsumerConfig()
	consumerClientCfg.BrokerAddrs = []string{addr}
	client, err := sdk.NewConsumerClient(consumerClientCfg)
	if err != nil {
		t.Fatalf("NewConsumerClient() error = %v", err)
	}
	offsets, err := client.ListOffsets(cfg.Topic, 0)
	if err != nil {
		t.Fatalf("created topic is not queryable: %v", err)
	}
	if len(offsets) != 1 {
		t.Fatalf("created topic has %d partitions, want 1", len(offsets))
	}
	t.Logf("topic created: topic=%s offsets=%+v", cfg.Topic, offsets)

	event := model.NewTransactionEvent(
		model.SourceMySQLBinlog,
		model.MySQLOffset{File: "mysql-bin.000001", Pos: 42},
		"integration-tx-1",
		[]model.RowChange{{
			Schema: "commerce",
			Table:  "revision_history",
			Op:     model.OpInsert,
			Rows:   []model.RowData{{After: map[string]any{"id": 1, "revision": 1}}},
		}},
	)
	if err := publisher.Publish(event); err != nil {
		t.Fatalf("publisher.Publish() error = %v", err)
	}
	t.Log("row-change event acknowledged by broker")

	consumerCfg := sdk.NewDefaultConsumerConfig()
	consumerCfg.BrokerAddrs = []string{addr}
	consumerCfg.Topic = cfg.Topic
	consumerCfg.GroupID = fmt.Sprintf("tabellarius-integration-%d", time.Now().UnixNano())
	consumerCfg.AutoOffsetReset = sdk.AutoOffsetResetEarliest
	consumerCfg.PollInterval = 50 * time.Millisecond
	consumerCfg.PollTimeoutMS = 1000
	consumerCfg.AutoCommitInterval = 50 * time.Millisecond
	consumer, err := sdk.NewConsumer(consumerCfg)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	received := make(chan sdk.Message, 1)
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- consumer.Start(func(message sdk.Message) error {
			select {
			case received <- message:
			default:
			}
			return nil
		})
	}()

	select {
	case message := <-received:
		var payload eventPayload
		if err := json.Unmarshal([]byte(message.Payload), &payload); err != nil {
			t.Fatalf("consumer received invalid JSON: %v", err)
		}
		if payload.TxID != "integration-tx-1" || len(payload.Changes) != 1 || payload.Changes[0].Table != "revision_history" {
			t.Fatalf("consumer received unexpected payload: %+v", payload)
		}
		t.Logf("consumer group received row-change event: offset=%d tx_id=%s", message.Offset, payload.TxID)
	case err := <-consumerDone:
		t.Fatalf("consumer group stopped before receiving a message: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("consumer group did not receive the published row-change event")
	}

	if err := consumer.Close(); err != nil {
		t.Errorf("consumer.Close() error = %v", err)
	}
	select {
	case err := <-consumerDone:
		if err != nil {
			t.Errorf("consumer.Start() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("consumer did not stop after Close")
	}
}

func TestCursusPublisherRejectsMissingTopicWhenAutoCreateDisabled(t *testing.T) {
	addr := os.Getenv("CURSUS_ADDR")
	if addr == "" {
		t.Fatal("CURSUS_ADDR must name a running Cursus broker")
	}

	cfg := sdk.NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{addr}
	cfg.Topic = fmt.Sprintf("commerce.missing-topic-%d", time.Now().UnixNano())
	cfg.AutoCreateTopics = false
	cfg.Partitions = 1

	publisher, err := newPublisher(cfg, func(*sdk.PublisherConfig) (publisherClient, error) {
		t.Fatal("producer must not initialize when auto_create_topics is false and the topic is missing")
		return nil, nil
	}, checkTopic)
	if publisher != nil {
		t.Fatal("expected no publisher")
	}
	if err == nil || !strings.Contains(err.Error(), "auto_create_topics=false") || !strings.Contains(err.Error(), cfg.Topic) {
		t.Fatalf("expected clear missing-topic startup error, got %v", err)
	}
	t.Logf("missing topic rejected before startup: %v", err)
}
