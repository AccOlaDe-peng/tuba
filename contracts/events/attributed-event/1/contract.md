# Attributed event contribution v1

This contract is the output boundary of the entity attribution stage (topic
`tuba.attributed.events.v1`, physical `tuba.collector.{namespace}.attributed.events.v1`).
Every role attribution of one UIM event produces **one contribution message**;
a five-role event yields five messages. `event_id` and `event_time` are the
input event's `event.id` and `@timestamp` carried verbatim — attribution is a
projection and never rewrites the event.

## Partition keys (Kafka message key, duplicated into `partition_key`)

- `state=resolved`: `<organization_id>:<entity_id>` — every contribution that
  names the same entity lands in the same partition, giving downstream
  per-entity bounded-window state (F02) a single ordered stream per entity.
  A multi-role event therefore fans out: each resolved entity sees the event's
  contribution in its own partition under its own independent contribution key.
- `state=unresolved|ambiguous`: `<organization_id>:unresolved:<state>:<reason>`
  — event-level unattributed detection input. These messages are **never
  dropped or silently filtered**; the dedicated key namespace keeps them
  consumable downstream without polluting any entity partition. `entity_id`
  must be absent for these states; `reason` is the stable cause code.

`entity_id` values carry the `ent:` prefix, so an unresolved key can never
collide with a resolved entity key. Consumers must treat `partition_key` as
identical to the Kafka key and must not re-derive it.

`attribution_id` is the stable id from contracts/ids.md; redelivery of the
same event reproduces the same contribution and must be deduplicated by
`attribution_id` (delivery is at-least-once). `evidence` is the full
attribution evidence record (identifiers, candidates, adjudication path) so
unresolved/ambiguous contributions remain triageable without PG access.
