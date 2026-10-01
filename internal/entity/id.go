package entity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// stableHash implements the contracts/ids.md v1 rule: every field is written
// as its UTF-8 byte length, a colon, then the field bytes, in order, before
// hashing. Callers must not concatenate raw values themselves.
func stableHash(prefix string, fields ...string) string {
	h := sha256.New()
	for _, field := range append([]string{prefix}, fields...) {
		_, _ = fmt.Fprintf(h, "%d:", len(field))
		_, _ = h.Write([]byte(field))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// SpaceID derives the stable identity space ID from the tenant and the
// normalized space name: the same space registered twice yields the same ID.
func SpaceID(organizationID, spaceName string) string {
	return "is:" + stableHash("identity-space-v1", organizationID, spaceName)
}

// EntityID derives the stable entity.id per contracts/ids.md: the canonical
// input is tenant, entity type, authority (identity space) and canonical key.
// The same real identity registered any number of times produces the same
// ID; a different canonical key — including a reused weak identifier's new
// occurrence suffix — produces a different ID.
func EntityID(organizationID, entityType, authority, canonicalKey string) string {
	return "ent:" + stableHash("entity-v1", organizationID, entityType, authority, canonicalKey)
}
