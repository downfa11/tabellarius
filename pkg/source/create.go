package source

import (
	"fmt"

	"github.com/downfa11-org/tabellarius/pkg/config"
	"github.com/downfa11-org/tabellarius/pkg/inspector"
	"github.com/downfa11-org/tabellarius/pkg/model"
	"github.com/downfa11-org/tabellarius/pkg/source/cursus"
	"github.com/downfa11-org/tabellarius/pkg/util"
)

func NewFromConfig(cfg *config.Config) (*TabellariusSource, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}

	switch cfg.Database.Type {
	case model.MySQL, model.MariaDB:
		return NewMySQLSource(cfg.Database.Type, cfg.Database.Schema, cfg.DSN(), cfg.CDCServer.OffsetFile, cfg.CDCServer.PublisherConfig, cfg.CDCServer.PublisherAddr, cfg.Tables)
	case model.Postgres:
		return nil, fmt.Errorf("postgres source not implemented")
	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Database.Type)
	}
}

func NewMySQLSource(dbType model.DatabaseType, dbSchema, dbDSN string, offsetPath, publisherConfigPath, publisherAddr string, tables []config.Table) (*TabellariusSource, error) {
	binlogOffset := offsetPath + ".binlog"
	ins, err := inspector.NewBinlogInspector(dbType, dbSchema, dbDSN, binlogOffset, util.GenerateID(), tables)
	if err != nil {
		return nil, fmt.Errorf("create binlog inspector: %w", err)
	}

	pub, err := cursus.NewCursusPublisher(publisherConfigPath, publisherAddr)
	if err != nil {
		return nil, fmt.Errorf("initialize cursus publisher: %w", err)
	}

	return &TabellariusSource{
		ins: ins,
		pub: pub,
	}, nil
}
