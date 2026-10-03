package controlworker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"tuba/product/internal/telemetry"
)

// RetentionGapChecker detects consumer-group checkpoints that fell behind the
// topic's retention horizon: when a partition's earliest available offset is
// past the group's committed offset, the messages in between were deleted by
// retention before being consumed — a data-loss gap. The checker only detects
// and reports (log + metrics); it never resets, seeks, or commits offsets, so
// a gap never silently jumps a consumer to latest.
type RetentionGapChecker struct {
	// Offsets is the broker access path; kafkaOffsetSource in production, a
	// fake in tests.
	Offsets OffsetSource
	// Groups are the consumer groups to check every sweep.
	Groups []string
	// PollInterval is the delay between sweeps in Run. Default 5 minutes.
	PollInterval time.Duration
	Metrics    *telemetry.Registry
}

// OffsetSource reads committed group offsets and earliest available partition
// offsets. Committed returns only partitions with a real commit (no commit =
// nothing to compare, the consumer simply has not started).
type OffsetSource interface {
	Committed(ctx context.Context, group string) (map[TopicPartition]int64, error)
	Earliest(ctx context.Context, partitions []TopicPartition) (map[TopicPartition]int64, error)
}

// kafkaOffsetSource implements OffsetSource against a real cluster.
type kafkaOffsetSource struct {
	client *kafka.Client
	addr   net.Addr
}

// NewKafkaOffsetSource builds the production OffsetSource.
func NewKafkaOffsetSource(transport *kafka.Transport, brokers []string) OffsetSource {
	return &kafkaOffsetSource{
		client: &kafka.Client{Transport: transport},
		addr:   kafka.TCP(brokers...),
	}
}

func (k *kafkaOffsetSource) Committed(ctx context.Context, group string) (map[TopicPartition]int64, error) {
	resp, err := k.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{Addr: k.addr, GroupID: group})
	if err != nil {
		return nil, fmt.Errorf("offset fetch group %s: %w", group, err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("offset fetch group %s: %w", group, resp.Error)
	}
	out := make(map[TopicPartition]int64)
	for topic, partitions := range resp.Topics {
		for _, p := range partitions {
			if p.Error != nil || p.CommittedOffset < 0 {
				continue
			}
			out[TopicPartition{Topic: topic, Partition: int32(p.Partition)}] = p.CommittedOffset
		}
	}
	return out, nil
}

func (k *kafkaOffsetSource) Earliest(ctx context.Context, partitions []TopicPartition) (map[TopicPartition]int64, error) {
	byTopic := make(map[string][]kafka.OffsetRequest)
	for _, tp := range partitions {
		byTopic[tp.Topic] = append(byTopic[tp.Topic], kafka.FirstOffsetOf(int(tp.Partition)))
	}
	resp, err := k.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Addr: k.addr, Topics: byTopic})
	if err != nil {
		return nil, fmt.Errorf("list offsets: %w", err)
	}
	out := make(map[TopicPartition]int64)
	for topic, offsets := range resp.Topics {
		for _, po := range offsets {
			if po.Error != nil {
				return nil, fmt.Errorf("list offsets %s[%d]: %w", topic, po.Partition, po.Error)
			}
			out[TopicPartition{Topic: topic, Partition: int32(po.Partition)}] = po.FirstOffset
		}
	}
	return out, nil
}

// Gap describes one partition whose committed offset is behind the earliest
// offset still retained: Lost messages were deleted before consumption.
type Gap struct {
	Group     string
	Topic     string
	Partition int32
	Committed int64
	Earliest  int64
	Lost      int64
}

func (c RetentionGapChecker) pollInterval() time.Duration {
	if c.PollInterval <= 0 {
		return 5 * time.Minute
	}
	return c.PollInterval
}

// Run sweeps immediately and then on every PollInterval until ctx ends.
func (c RetentionGapChecker) Run(ctx context.Context) error {
	for {
		if err := c.Sweep(ctx); err != nil {
			slog.Error("retention gap check failed", "error", err)
			c.inc("tuba_control_worker_retention_gap_check_failures_total")
		}
		if !wait(ctx, c.pollInterval()) {
			return nil
		}
	}
}

// Sweep compares every configured group's committed offsets against the
// earliest retained offsets and reports each gap. A failure on any group does
// not skip the remaining groups.
func (c RetentionGapChecker) Sweep(ctx context.Context) error {
	var gaps []Gap
	var sweepErr error
	for _, group := range c.Groups {
		groupGaps, err := c.checkGroup(ctx, group)
		if err != nil {
			slog.Error("retention gap check group failed", "group", group, "error", err)
			c.inc("tuba_control_worker_retention_gap_check_failures_total")
			sweepErr = err
			continue
		}
		gaps = append(gaps, groupGaps...)
	}
	for _, gap := range gaps {
		slog.Error("consumer checkpoint behind retention horizon; messages were deleted before consumption — offsets are NOT auto-advanced",
			"group", gap.Group, "topic", gap.Topic, "partition", gap.Partition,
			"committed", gap.Committed, "earliest", gap.Earliest, "lost", gap.Lost)
	}
	if c.Metrics != nil {
		c.Metrics.Set("tuba_control_worker_retention_gap_partitions", uint64(len(gaps)))
		var lost uint64
		for _, gap := range gaps {
			lost += uint64(gap.Lost)
		}
		c.Metrics.Set("tuba_control_worker_retention_gap_messages", lost)
	}
	return sweepErr
}

func (c RetentionGapChecker) checkGroup(ctx context.Context, group string) ([]Gap, error) {
	committed, err := c.Offsets.Committed(ctx, group)
	if err != nil {
		return nil, err
	}
	partitions := make([]TopicPartition, 0, len(committed))
	for tp := range committed {
		partitions = append(partitions, tp)
	}
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic != partitions[j].Topic {
			return partitions[i].Topic < partitions[j].Topic
		}
		return partitions[i].Partition < partitions[j].Partition
	})
	if len(partitions) == 0 {
		return nil, nil
	}
	earliest, err := c.Offsets.Earliest(ctx, partitions)
	if err != nil {
		return nil, err
	}
	var gaps []Gap
	for _, tp := range partitions {
		first, ok := earliest[tp]
		if !ok {
			return nil, fmt.Errorf("earliest offset missing for %s[%d]", tp.Topic, tp.Partition)
		}
		if first > committed[tp] {
			gaps = append(gaps, Gap{
				Group: group, Topic: tp.Topic, Partition: tp.Partition,
				Committed: committed[tp], Earliest: first, Lost: first - committed[tp],
			})
		}
	}
	return gaps, nil
}

func (c RetentionGapChecker) inc(name string) {
	if c.Metrics != nil {
		c.Metrics.Inc(name)
	}
}

// ParseGapCheckGroups parses the CONTROL_WORKER_GAP_CHECK_GROUPS value: a
// comma-separated group list; empty means the checker is disabled.
func ParseGapCheckGroups(raw string) []string {
	var groups []string
	for _, part := range strings.Split(raw, ",") {
		if group := strings.TrimSpace(part); group != "" {
			groups = append(groups, group)
		}
	}
	return groups
}
