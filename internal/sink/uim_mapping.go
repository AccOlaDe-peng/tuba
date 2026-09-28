package sink

import "encoding/json"

func uimIndexMappingBody(alias string) ([]byte, error) {
	keyword := func() map[string]any { return map[string]any{"type": "keyword"} }
	object := func(properties map[string]any) map[string]any { return map[string]any{"properties": properties} }
	properties := map[string]any{
		"@timestamp":   map[string]any{"type": "date"},
		"organization": object(map[string]any{"id": keyword()}),
		"event": object(map[string]any{
			"id": keyword(), "kind": keyword(), "category": keyword(), "type": keyword(), "action": keyword(),
			"outcome": keyword(), "dataset": keyword(), "code": keyword(), "duration": map[string]any{"type": "long"},
		}),
		"vendor": object(map[string]any{"name": keyword(), "product": keyword(), "dataset": keyword(), "payload": map[string]any{"type": "flattened"}}),
		"user": object(map[string]any{
			"id": keyword(), "name": keyword(), "domain": keyword(),
			"target": object(map[string]any{"id": keyword(), "name": keyword(), "domain": keyword()}),
		}),
		"group":       object(map[string]any{"id": keyword(), "name": keyword()}),
		"host":        object(map[string]any{"id": keyword(), "name": keyword()}),
		"source":      object(map[string]any{"ip": map[string]any{"type": "ip"}, "address": keyword(), "port": map[string]any{"type": "integer"}}),
		"destination": object(map[string]any{"ip": map[string]any{"type": "ip"}, "address": keyword(), "port": map[string]any{"type": "integer"}, "domain": keyword()}),
		"network": object(map[string]any{
			"transport": keyword(), "protocol": keyword(), "direction": keyword(), "community_id": keyword(),
			"bytes": map[string]any{"type": "long"}, "packets": map[string]any{"type": "long"},
		}),
		"dns": object(map[string]any{
			"question": object(map[string]any{"name": keyword()}), "response_code": keyword(),
		}),
		"http": object(map[string]any{
			"request":  object(map[string]any{"method": keyword()}),
			"response": object(map[string]any{"status_code": map[string]any{"type": "integer"}}),
		}),
		"url": object(map[string]any{"domain": keyword(), "path": keyword(), "scheme": keyword()}),
		"tls": object(map[string]any{"version": keyword(), "cipher": keyword()}),
		"ueba": object(map[string]any{
			"schema":  object(map[string]any{"version": keyword()}),
			"quality": object(map[string]any{"status": keyword(), "reasons": keyword(), "usable_for": keyword()}),
			"route":   object(map[string]any{"domain": keyword(), "generation": keyword()}),
			"event":   object(map[string]any{"type": keyword(), "semantic_tags": keyword()}),
			"provenance": object(map[string]any{
				"raw_event_id": keyword(), "release_id": keyword(), "dip_version": keyword(), "uim_version": keyword(),
			}),
		}),
	}
	return json.Marshal(map[string]any{
		"settings": map[string]any{"number_of_shards": 1, "number_of_replicas": 0},
		"mappings": map[string]any{"dynamic": "strict", "properties": properties},
		"aliases":  map[string]any{alias: map[string]any{}},
	})
}
