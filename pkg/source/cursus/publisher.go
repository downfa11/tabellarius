package cursus

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/downfa11-org/tabellarius/pkg/model"
	"gopkg.in/yaml.v3"
)

const legacyDefaultTopic = "tabellarius.cdc"

type Publisher struct {
	mu             sync.Mutex
	pub            publisherClient
	cfg            *sdk.PublisherConfig
	failure        error
	failedSequence uint64
}

type publisherClient interface {
	PublishMessage(message string) (uint64, error)
	CreateTopic(topic string, partitions int) error
	Flush()
	GetUniqueAckCount() int
	Close() error
}

type publisherFactory func(*sdk.PublisherConfig) (publisherClient, error)
type topicProbe func(*sdk.PublisherConfig) error

type eventPayload struct {
	Type      string            `json:"type"`
	Source    model.SourceType  `json:"source"`
	Offset    string            `json:"offset"`
	Timestamp string            `json:"timestamp"`
	TxID      string            `json:"tx_id,omitempty"`
	Kind      string            `json:"kind,omitempty"`
	Query     string            `json:"query,omitempty"`
	Changes   []model.RowChange `json:"changes,omitempty"`
}

// NewCursusPublisher loads the publisher config, ensures its topic is usable,
// and only then returns a publisher that may receive CDC events.
func NewCursusPublisher(configPath, fallbackAddr string) (*Publisher, error) {
	cfg, err := loadPublisherConfig(configPath, fallbackAddr)
	if err != nil {
		return nil, err
	}
	return newPublisher(cfg, func(cfg *sdk.PublisherConfig) (publisherClient, error) {
		return sdk.NewProducer(cfg)
	}, checkTopic)
}

func loadPublisherConfig(configPath, fallbackAddr string) (*sdk.PublisherConfig, error) {
	cfg := sdk.NewDefaultPublisherConfig()
	if fallbackAddr != "" {
		cfg.BrokerAddrs = []string{fallbackAddr}
	}

	if configPath == "" {
		// publisher_addr was the original Tabellarius setting. Keep it usable
		// while moving it to the SDK publisher configuration.
		cfg.Topic = legacyDefaultTopic
		cfg.AutoCreateTopics = true
		return cfg, validatePublisherConfig(cfg)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read publisher config %s: %w", configPath, err)
	}

	var metadata struct {
		LogLevel string `yaml:"log_level"`
	}
	if err := yaml.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("parse publisher config %s: %w", configPath, err)
	}

	var fields map[string]any
	if err := yaml.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("parse publisher config %s: %w", configPath, err)
	}
	delete(fields, "log_level")
	normalized, err := yaml.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("normalize publisher config %s: %w", configPath, err)
	}
	if err := yaml.Unmarshal(normalized, cfg); err != nil {
		return nil, fmt.Errorf("parse publisher config %s: %w", configPath, err)
	}

	switch strings.ToLower(strings.TrimSpace(metadata.LogLevel)) {
	case "", "info":
		cfg.LogLevel = sdk.LogLevelInfo
	case "debug":
		cfg.LogLevel = sdk.LogLevelDebug
	case "warn", "warning":
		cfg.LogLevel = sdk.LogLevelWarn
	case "error":
		cfg.LogLevel = sdk.LogLevelError
	default:
		return nil, fmt.Errorf("invalid publisher log_level %q", metadata.LogLevel)
	}

	return cfg, validatePublisherConfig(cfg)
}

func validatePublisherConfig(cfg *sdk.PublisherConfig) error {
	if cfg == nil {
		return fmt.Errorf("publisher config is required")
	}
	if len(cfg.BrokerAddrs) == 0 {
		return fmt.Errorf("publisher broker_addrs is required")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return fmt.Errorf("publisher topic is required")
	}
	if cfg.Partitions <= 0 {
		return fmt.Errorf("publisher partitions must be positive")
	}
	return nil
}

func newPublisher(cfg *sdk.PublisherConfig, factory publisherFactory, probe topicProbe) (*Publisher, error) {
	if err := validatePublisherConfig(cfg); err != nil {
		return nil, err
	}

	log.Printf("component=cursus_publisher event=initialize topic=%q brokers=%v auto_create_topics=%t partitions=%d", cfg.Topic, cfg.BrokerAddrs, cfg.AutoCreateTopics, cfg.Partitions)
	if !cfg.AutoCreateTopics {
		log.Printf("component=cursus_publisher event=topic_verify_started topic=%q", cfg.Topic)
		if err := probe(cfg); err != nil {
			log.Printf("component=cursus_publisher event=topic_verify_failed topic=%q auto_create_topics=false error=%q", cfg.Topic, err)
			return nil, fmt.Errorf("cursus topic %q is unavailable and auto_create_topics=false: %w", cfg.Topic, err)
		}
	}

	pub, err := factory(cfg)
	if err != nil {
		log.Printf("component=cursus_publisher event=initialize_failed topic=%q error=%q", cfg.Topic, err)
		return nil, fmt.Errorf("initialize cursus producer for topic %q: %w", cfg.Topic, err)
	}
	publisher := &Publisher{pub: pub, cfg: cfg}

	if cfg.AutoCreateTopics {
		log.Printf("component=cursus_publisher event=topic_create_started topic=%q partitions=%d", cfg.Topic, cfg.Partitions)
		if err := pub.CreateTopic(cfg.Topic, cfg.Partitions); err != nil {
			_ = pub.Close()
			log.Printf("component=cursus_publisher event=topic_create_failed topic=%q error=%q", cfg.Topic, err)
			return nil, fmt.Errorf("create cursus topic %q: %w", cfg.Topic, err)
		}
	}

	if err := probe(cfg); err != nil {
		_ = pub.Close()
		log.Printf("component=cursus_publisher event=topic_verify_failed topic=%q error=%q", cfg.Topic, err)
		return nil, fmt.Errorf("verify cursus topic %q after initialization: %w", cfg.Topic, err)
	}

	log.Printf("component=cursus_publisher event=topic_ready topic=%q partitions=%d", cfg.Topic, cfg.Partitions)
	return publisher, nil
}

func checkTopic(cfg *sdk.PublisherConfig) error {
	consumerCfg := sdk.NewDefaultConsumerConfig()
	consumerCfg.BrokerAddrs = append([]string(nil), cfg.BrokerAddrs...)
	consumerCfg.Topic = cfg.Topic
	consumerCfg.UseTLS = cfg.UseTLS
	consumerCfg.TLSCertPath = cfg.TLSCertPath
	consumerCfg.TLSKeyPath = cfg.TLSKeyPath
	consumerCfg.Principal = cfg.Principal
	consumerCfg.AuthToken = cfg.AuthToken
	consumerCfg.ProtocolVersion = cfg.ProtocolVersion
	consumerCfg.ProtocolFeatures = append([]string(nil), cfg.ProtocolFeatures...)
	consumerCfg.RequireProtocolFeatures = cfg.RequireProtocolFeatures
	consumerCfg.ProtocolNegotiationTimeoutMS = cfg.ProtocolNegotiationTimeoutMS

	client, err := sdk.NewConsumerClient(consumerCfg)
	if err != nil {
		return fmt.Errorf("create consumer client: %w", err)
	}
	_, err = client.ListOffsets(cfg.Topic)
	return err
}

func (p *Publisher) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pub != nil {
		return p.pub.Close()
	}
	return nil
}

func (p *Publisher) Publish(evt model.Event) error {
	if p == nil {
		return fmt.Errorf("broker publisher not initialized")
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pub == nil {
		return fmt.Errorf("broker publisher not initialized")
	}
	if p.failure != nil {
		return fmt.Errorf("cursus publisher fenced after sequence=%d failure: %w", p.failedSequence, p.failure)
	}

	payload, err := marshalEvent(evt)
	if err != nil {
		return fmt.Errorf("marshal event for topic %q: %w", p.cfg.Topic, err)
	}

	ackCount := p.pub.GetUniqueAckCount()
	sequence, err := p.pub.PublishMessage(string(payload))
	if err != nil {
		fenced := p.fence(sequence, fmt.Errorf("enqueue event for cursus topic %q: %w", p.cfg.Topic, err))
		log.Printf("component=cursus_publisher event=publish_failed stage=enqueue topic=%q sequence=%d error=%q", p.cfg.Topic, sequence, fenced)
		return fenced
	}

	p.pub.Flush()
	// The SDK exposes an aggregate acknowledgement count rather than a
	// sequence-specific waiter. Publish is serialized, and any timeout fences
	// the producer, so no later sequence can mistake a late acknowledgement for
	// this sequence's acknowledgement.
	if err := p.waitForAcknowledgement(ackCount + 1); err != nil {
		fenced := p.fence(sequence, err)
		log.Printf("component=cursus_publisher event=publish_failed stage=ack topic=%q sequence=%d error=%q", p.cfg.Topic, sequence, fenced)
		return fenced
	}

	log.Printf("component=cursus_publisher event=publish_ack topic=%q sequence=%d source=%s offset=%s type=%T", p.cfg.Topic, sequence, evt.Source(), evt.Offset().String(), evt)
	p.logPublishedEvent(evt)
	return nil
}

func (p *Publisher) fence(sequence uint64, publishErr error) error {
	if p.failure == nil {
		p.failure = publishErr
		p.failedSequence = sequence
		if err := p.pub.Close(); err != nil {
			log.Printf("component=cursus_publisher event=close_after_failure_failed topic=%q sequence=%d error=%q", p.cfg.Topic, sequence, err)
		}
	}
	return fmt.Errorf("cursus publisher fenced after sequence=%d failure: %w", p.failedSequence, p.failure)
}

func (p *Publisher) waitForAcknowledgement(want int) error {
	timeout := 5 * time.Second
	if p.cfg != nil && p.cfg.AckTimeoutMS > 0 {
		timeout = time.Duration(p.cfg.AckTimeoutMS) * time.Millisecond
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if p.pub.GetUniqueAckCount() >= want {
			return nil
		}
		select {
		case <-deadline.C:
			return fmt.Errorf("broker acknowledgement timeout after %s for topic %q", timeout, p.cfg.Topic)
		case <-ticker.C:
		}
	}
}

func (p *Publisher) logPublishedEvent(evt model.Event) {
	prefix := fmt.Sprintf("[publish] source=%s offset=%s type=%T", evt.Source(), evt.Offset().String(), evt)
	switch e := evt.(type) {
	case *model.TransactionBoundaryEvent:
		log.Printf("%s [tx] kind=%s txID=%s", prefix, e.Kind(), e.TxID())
	case *model.BinlogDDLEvent:
		log.Printf("%s [ddl] txID=%s query=%s", prefix, e.TxID(), e.Query())
	case model.RowChangeEvent:
		for ci, change := range e.Changes() {
			for ri, row := range change.Rows {
				if change.Op == model.OpUpdate && row.Before != nil && row.After != nil {
					beforeJSON, _ := json.Marshal(row.Before)
					afterJSON, _ := json.Marshal(row.After)
					log.Printf("%s [row][%d:%d] table=%s.%s txID=%s op=UPDATE before=%s after=%s", prefix, ci, ri, change.Schema, change.Table, e.TxID(), string(beforeJSON), string(afterJSON))
					continue
				}
				log.Printf("%s [row][%d:%d] table=%s.%s txID=%s op=%s", prefix, ci, ri, change.Schema, change.Table, e.TxID(), change.Op)
			}
		}
	default:
		log.Printf("%s [unknown event]", prefix)
	}
}

func marshalEvent(evt model.Event) ([]byte, error) {
	payload := eventPayload{
		Source:    evt.Source(),
		Offset:    evt.Offset().String(),
		Timestamp: envelopeTimestamp(evt).Format(time.RFC3339Nano),
	}
	switch e := evt.(type) {
	case *model.TransactionBoundaryEvent:
		payload.Type = "transaction_boundary"
		payload.TxID = e.TxID()
		payload.Kind = string(e.Kind())
	case *model.BinlogDDLEvent:
		payload.Type = "ddl"
		payload.TxID = e.TxID()
		payload.Query = e.Query()
	case model.RowChangeEvent:
		payload.Type = "transaction"
		payload.TxID = e.TxID()
		payload.Changes = e.Changes()
	default:
		payload.Type = "unknown"
	}
	return json.Marshal(payload)
}

func envelopeTimestamp(evt model.Event) time.Time {
	if timestamped, ok := evt.(model.TimestampedEvent); ok {
		if timestamp := timestamped.Timestamp(); !timestamp.IsZero() {
			return timestamp.UTC()
		}
	}

	fallback := time.Now().UTC()
	log.Printf("component=cursus_publisher event=timestamp_fallback reason=missing_event_timestamp source=%s offset=%s timestamp=%s", evt.Source(), evt.Offset().String(), fallback.Format(time.RFC3339Nano))
	return fallback
}
