package sourceadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

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
