package sourceadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// COL-03 的五个验收场景之一是“上下文换代行为符合合同”。合同要求重建的 Topic
// 必须换新的来源上下文或 epoch，且不得复用旧 consumer group offset。组 ID 由
// Topic 推导、Topic 承载来源上下文，所以换代必须落到不同的组，否则重建后的
// 适配器会从退役代次停下的位置继续，把它从未读过的记录静默跳过。
func TestGroupIDSeparatesSourceContextGenerations(t *testing.T) {
	first := "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
	second := "tuba.source.ctx_fedcba9876543210fedcba9876543210.v1"
	if GroupID(first) == GroupID(second) {
		t.Fatalf("two source contexts share the group id %q; a rebuilt topic would reuse the retired offsets", GroupID(first))
	}
	// The binding is (context, topic): the same context under a new topic version
	// is a new generation too and must move the group as well.
	if GroupID(first) == GroupID("tuba.source.ctx_0123456789abcdef0123456789abcdef.v2") {
		t.Fatalf("a topic version change kept the group id %q", GroupID(first))
	}
}

func TestGroupIDMatchesTopicContract(t *testing.T) {
	topic := "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
	digest := sha256.Sum256([]byte(topic))
	want := "tuba-source-adapter-" + hex.EncodeToString(digest[:8])
	if got := GroupID(topic); got != want {
		t.Fatalf("GroupID(%q) = %q, want %q", topic, got, want)
	}
	if got := GroupID(topic, "rollout_2"); got != want+"-rollout_2" {
		t.Fatalf("GroupID with suffix = %q, want %q", got, want+"-rollout_2")
	}
}
