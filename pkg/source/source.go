package source

import (
	"context"
	"fmt"
	"log"

	"github.com/downfa11-org/tabellarius/pkg/inspector"
	"github.com/downfa11-org/tabellarius/pkg/model"
)

type publisher interface {
	Publish(model.Event) error
	Close() error
}

type TabellariusSource struct {
	ins        inspector.Inspector[model.Event]
	pub        publisher
	checkpoint func(model.Offset) error
}

func (s *TabellariusSource) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	ch := make(chan model.Event, 128)

	go func() {
		defer close(ch)
		_ = s.ins.Start(runCtx, ch)
	}()

	go func() {
		defer cancel()
		if err := s.run(runCtx, ch); err != nil {
			log.Printf("component=tabellarius_source event=stopped reason=publish_or_checkpoint_failure error=%q", err)
		}
	}()
}

func (s *TabellariusSource) Close() error {
	if s != nil && s.pub != nil {
		return s.pub.Close()
	}
	return nil
}

func (s *TabellariusSource) run(ctx context.Context, in <-chan model.Event) error {
	txBuffer := map[string][]model.RowChange{}

	for {
		select {
		case <-ctx.Done():
			return nil

		case evt, ok := <-in:
			if !ok {
				return nil
			}

			switch e := evt.(type) {
			case model.RowChangeEvent:
				txBuffer[e.TxID()] = append(txBuffer[e.TxID()], e.Changes()...)
			case *model.BinlogDDLEvent:
				if err := s.pub.Publish(e); err != nil {
					return fmt.Errorf("publish DDL tx_id=%q offset=%s: %w", e.TxID(), e.Offset().String(), err)
				}
				if err := s.checkpointOffset(e.Offset()); err != nil {
					return fmt.Errorf("checkpoint DDL tx_id=%q offset=%s: %w", e.TxID(), e.Offset().String(), err)
				}
			case *model.TransactionBoundaryEvent:
				switch e.Kind() {
				case model.TxCommit:
					changes := txBuffer[e.TxID()]
					if len(changes) > 0 {
						txEvt := model.NewTransactionEventAt(e.Source(), e.Offset(), e.TxID(), e.Timestamp(), changes)
						if err := s.pub.Publish(txEvt); err != nil {
							return fmt.Errorf("publish transaction tx_id=%q offset=%s: %w", e.TxID(), e.Offset().String(), err)
						}
					}
					if err := s.checkpointOffset(e.Offset()); err != nil {
						return fmt.Errorf("checkpoint transaction tx_id=%q offset=%s: %w", e.TxID(), e.Offset().String(), err)
					}
					delete(txBuffer, e.TxID())

				case model.TxRollback:
					delete(txBuffer, e.TxID())
					if err := s.checkpointOffset(e.Offset()); err != nil {
						return fmt.Errorf("checkpoint rollback tx_id=%q offset=%s: %w", e.TxID(), e.Offset().String(), err)
					}
				}
			}
		}
	}
}

func (s *TabellariusSource) checkpointOffset(offset model.Offset) error {
	if s.checkpoint == nil {
		return fmt.Errorf("checkpoint is not configured")
	}
	return s.checkpoint(offset)
}
