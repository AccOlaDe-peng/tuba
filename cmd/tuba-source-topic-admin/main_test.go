package main

import (
	"strings"
	"testing"
	"tuba/product/internal/sourceadapter"
)

const testTopic = "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"

// The live deployment isolates adapter consumer groups with a suffix
// (SOURCE_ADAPTER_CONSUMER_GROUP_SUFFIX), so the running adapter joins
// GroupID(topic, suffix). Issuing the group ACL for the unsuffixed name leaves
// the adapter unauthorized to consume even though the topic ACLs look right,
// which is why the suffix has to reach the plan rather than only the client.
func TestAdapterGroupIDAppliesSuffix(t *testing.T) {
	plain, err := adapterGroupID(testTopic, "")
	if err != nil {
		t.Fatal(err)
	}
	if want := sourceadapter.GroupID(testTopic); plain != want {
		t.Fatalf("empty suffix = %q, want %q", plain, want)
	}

	suffixed, err := adapterGroupID(testTopic, "rollout_2")
	if err != nil {
		t.Fatal(err)
	}
	if want := sourceadapter.GroupID(testTopic, "rollout_2"); suffixed != want {
		t.Fatalf("suffixed = %q, want %q", suffixed, want)
	}
	if suffixed == plain {
		t.Fatal("suffix did not change the group id")
	}
}

func TestAdapterGroupIDRejectsInvalidSuffix(t *testing.T) {
	// Mirrors the accepted shape elsewhere in the codebase (lowercase slug,
	// letters/digits/underscore/hyphen, starting alphanumeric, 63 max).
	invalid := []string{
		"Has-Upper", "has space", "-leading", "sla/sh", "dot.ted", strings.Repeat("a", 64),
	}
	for _, suffix := range invalid {
		if _, err := adapterGroupID(testTopic, suffix); err == nil {
			t.Fatalf("suffix %q was accepted", suffix)
		}
	}
}
