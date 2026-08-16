package source

import (
	"path/filepath"
	"strings"
	"testing"

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
