package analysisworker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/analysis"
	"tuba/product/internal/telemetry"
)

type Consumer interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

type DeadLetter interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

type Sink interface {
	PutAnalysis(context.Context, analysis.Result) error
}

type Worker struct {
	Organization string
	Namespace    string
	Consumer     Consumer
	DeadLetter   DeadLetter
	Sink         Sink
	Metrics      *telemetry.Registry
}

func (w Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		message, err := w.Consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		for ctx.Err() == nil {
			if err := w.process(ctx, message); err == nil {
				break
			} else {
				slog.Error("analysis result processing failed; offset will not be committed", "partition", message.Partition, "offset", message.Offset, "error", err)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(2 * time.Second):
				}
			}
		}
	}
	return nil
}

func (w Worker) process(ctx context.Context, message kafka.Message) error {
	if w.Metrics != nil {
		w.Metrics.Inc("tuba_analysis_results_received_total")
	}
	result, err := analysis.Parse(message.Value, w.Organization, w.Namespace)
	if err != nil {
		dead := kafka.Message{Key: message.Key, Value: message.Value, Headers: []kafka.Header{
			{Key: "source-topic", Value: []byte(message.Topic)},
			{Key: "validation-error", Value: []byte(err.Error())},
		}}
		if writeErr := w.DeadLetter.WriteMessages(ctx, dead); writeErr != nil {
			return fmt.Errorf("dead letter publish: %w", writeErr)
		}
		if w.Metrics != nil {
			w.Metrics.Inc("tuba_analysis_results_dead_letter_total")
		}
	} else if err := w.Sink.PutAnalysis(ctx, result); err != nil {
		return err
	} else if w.Metrics != nil {
		w.Metrics.Inc("tuba_analysis_results_written_total")
	}
	return w.Consumer.CommitMessages(ctx, message)
}
