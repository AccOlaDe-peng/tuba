package entity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// UIM role names. Every role is attributed independently; the mapping from
// UIM fields to roles is versioned by RoleMappingVersionV1.
const (
	RoleActor             = "actor"
	RoleTarget            = "target"
	RoleGroup             = "group"
	RoleHost              = "host"
	RoleSourceDevice      = "source_device"
	RoleDestinationDevice = "destination_device"
)

var ErrUIMRoleEvent = errors.New("UIM event is not attributable")

// RolesFromUIM extracts the role observations carried by a UIM event
// (contracts/uim/domain-contract.v1.yaml): user → actor account,
// user.target → target account, group → group account, host → host device,
// source/destination → source/destination devices. It returns the event.id
// and event time verbatim — attribution is a projection of the event and
// never rewrites it.
//
// Identity spaces are not carried by events; they are a deployment mapping
// supplied by the caller (accountSpace for account roles, deviceSpace for
// device roles).
func RolesFromUIM(event map[string]any, accountSpace, deviceSpace string) (eventID string, at time.Time, roles []RoleObservation, err error) {
	eventID = strings.TrimSpace(nestedString(event, "event", "id"))
	if eventID == "" {
		return "", time.Time{}, nil, fmt.Errorf("%w: event.id missing", ErrUIMRoleEvent)
	}
	stamp, err := time.Parse(time.RFC3339Nano, nestedString(event, "@timestamp"))
	if err != nil {
		return "", time.Time{}, nil, fmt.Errorf("%w: @timestamp invalid", ErrUIMRoleEvent)
	}
	at = stamp.UTC()

	if user := nestedMap(event, "user"); user != nil {
		if ids := accountIdentifiers(user); len(ids) > 0 {
			roles = append(roles, RoleObservation{Role: RoleActor, EntityType: TypeAccount, Space: accountSpace, Identifiers: ids})
		}
		if target := nestedMap(user, "target"); target != nil {
			if ids := accountIdentifiers(target); len(ids) > 0 {
				roles = append(roles, RoleObservation{Role: RoleTarget, EntityType: TypeAccount, Space: accountSpace, Identifiers: ids})
			}
		}
	}
	if group := nestedMap(event, "group"); group != nil {
		if ids := accountIdentifiers(group); len(ids) > 0 {
			roles = append(roles, RoleObservation{Role: RoleGroup, EntityType: TypeAccount, Space: accountSpace, Identifiers: ids})
		}
	}
	if name := nestedString(event, "host", "name"); name != "" {
		roles = append(roles, RoleObservation{Role: RoleHost, EntityType: TypeDevice, Space: deviceSpace,
			Identifiers: []Identifier{{Kind: KindHostname, Value: name}}})
	}
	if ip := nestedString(event, "source", "ip"); ip != "" {
		roles = append(roles, RoleObservation{Role: RoleSourceDevice, EntityType: TypeDevice, Space: deviceSpace,
			Identifiers: []Identifier{{Kind: KindIP, Value: ip}}})
	}
	if ip := nestedString(event, "destination", "ip"); ip != "" {
		roles = append(roles, RoleObservation{Role: RoleDestinationDevice, EntityType: TypeDevice, Space: deviceSpace,
			Identifiers: []Identifier{{Kind: KindIP, Value: ip}}})
	}
	return eventID, at, roles, nil
}

// accountIdentifiers builds the identifier list for an account-ish UIM
// object: a SID-looking id is a strong sid; a DOMAIN\user name is an ntname;
// a plain name is a username. Values that fail normalization are reported as
// unresolved evidence by the attributor, never guessed at here.
func accountIdentifiers(obj map[string]any) []Identifier {
	var ids []Identifier
	if id := strings.TrimSpace(nestedString(obj, "id")); id != "" {
		ids = append(ids, Identifier{Kind: KindSID, Value: id})
	}
	if name := strings.TrimSpace(nestedString(obj, "name")); name != "" {
		if strings.Contains(name, `\`) {
			ids = append(ids, Identifier{Kind: KindNTName, Value: name})
		} else if strings.Contains(name, "@") {
			ids = append(ids, Identifier{Kind: KindUPN, Value: name})
		} else {
			ids = append(ids, Identifier{Kind: KindUsername, Value: name})
		}
	}
	return ids
}

// AttributeEvent is the convenience composition: extract roles from a UIM
// event, attribute each role at the event time. The event map is only read.
func (a *Attributor) AttributeEvent(ctx context.Context, organizationID string, event map[string]any, accountSpace, deviceSpace string) ([]RoleAttribution, error) {
	eventID, at, roles, err := RolesFromUIM(event, accountSpace, deviceSpace)
	if err != nil {
		return nil, err
	}
	if len(roles) == 0 {
		return nil, fmt.Errorf("%w: event %s carries no attributable role", ErrNoRoles, eventID)
	}
	return a.Attribute(ctx, organizationID, eventID, at, roles)
}

// nestedMap reads event[keys...] as a nested object path (last level map).
func nestedMap(event map[string]any, keys ...string) map[string]any {
	current := event
	for i, key := range keys {
		value, ok := current[key]
		if !ok {
			return nil
		}
		if i == len(keys)-1 {
			if m, ok := value.(map[string]any); ok {
				return m
			}
			return nil
		}
		next, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		current = next
	}
	return current
}

// nestedString reads a dotted path of maps ending in a string.
func nestedString(event map[string]any, keys ...string) string {
	if len(keys) == 1 {
		value, ok := event[keys[0]]
		if !ok {
			return ""
		}
		s, ok := value.(string)
		if !ok {
			return ""
		}
		return s
	}
	m := nestedMap(event, keys[:len(keys)-1]...)
	if m == nil {
		return ""
	}
	return nestedString(m, keys[len(keys)-1])
}
