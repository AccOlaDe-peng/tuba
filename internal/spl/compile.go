package spl

import (
	"fmt"
	"strings"
	"time"
)

// FieldCaps is the field whitelist view of one catalog dataset (Q01). The API
// layer builds it from the DatasetDecl registry; validation is fail-closed:
// any field not returned by Lookup is rejected.
type FieldCaps interface {
	// Lookup returns the declared type and capabilities of a field.
	Lookup(name string) (typ string, searchable, aggregable, ok bool)
}

func isNumericType(typ string) bool {
	switch typ {
	case "long", "integer", "short", "byte", "double", "float", "half_float", "scaled_float", "unsigned_long":
		return true
	}
	return false
}

func (c Cmp) validate(caps FieldCaps) error {
	typ, searchable, _, ok := caps.Lookup(c.Field)
	if !ok {
		return fmt.Errorf("spl_field: field %q is not declared in the dataset catalog", c.Field)
	}
	if !searchable {
		return fmt.Errorf("spl_field: field %q is not searchable", c.Field)
	}
	if c.Op != "=" && c.Op != "!=" && !c.IsNumber && !isNumericType(typ) && typ != "date" {
		return fmt.Errorf("spl_field: range comparison on non-numeric field %q requires a numeric value", c.Field)
	}
	return nil
}

func validateExpr(e Expr, caps FieldCaps) error {
	switch n := e.(type) {
	case Cmp:
		return n.validate(caps)
	case And:
		if err := validateExpr(n.L, caps); err != nil {
			return err
		}
		return validateExpr(n.R, caps)
	case Or:
		if err := validateExpr(n.L, caps); err != nil {
			return err
		}
		return validateExpr(n.R, caps)
	case Not:
		return validateExpr(n.E, caps)
	}
	return fmt.Errorf("spl_parse: unknown expression")
}

func (p *Plan) validateAggs(caps FieldCaps) error {
	for _, agg := range p.Aggs {
		if agg.Func == "count" {
			continue
		}
		typ, _, aggregable, ok := caps.Lookup(agg.Field)
		if !ok {
			return fmt.Errorf("spl_field: field %q is not declared in the dataset catalog", agg.Field)
		}
		if !aggregable {
			return fmt.Errorf("spl_field: field %q is not aggregable", agg.Field)
		}
		if (agg.Func == "sum" || agg.Func == "avg") && !isNumericType(typ) {
			return fmt.Errorf("spl_field: %s requires a numeric field, %q is %s", agg.Func, agg.Field, typ)
		}
		if (agg.Func == "min" || agg.Func == "max") && !isNumericType(typ) && typ != "date" {
			return fmt.Errorf("spl_field: %s requires a numeric or date field, %q is %s", agg.Func, agg.Field, typ)
		}
	}
	for _, by := range p.By {
		_, _, aggregable, ok := caps.Lookup(by)
		if !ok {
			return fmt.Errorf("spl_field: field %q is not declared in the dataset catalog", by)
		}
		if !aggregable {
			return fmt.Errorf("spl_field: field %q is not aggregable", by)
		}
	}
	return nil
}

// Validate enforces the catalog field whitelist and per-mode rules.
func (p *Plan) Validate(caps FieldCaps) error {
	if p.Filter != nil {
		if err := validateExpr(p.Filter, caps); err != nil {
			return err
		}
	}
	for _, f := range p.Fields {
		if _, _, _, ok := caps.Lookup(f); !ok {
			return fmt.Errorf("spl_field: field %q is not declared in the dataset catalog", f)
		}
	}
	switch p.Mode {
	case ModeStats, ModeTimechart, ModeTop, ModeRare:
		if err := p.validateAggs(caps); err != nil {
			return err
		}
	}
	for _, sf := range p.Sort {
		if p.Mode == ModeEvents {
			_, _, aggregable, ok := caps.Lookup(sf.Field)
			if !ok {
				return fmt.Errorf("spl_field: field %q is not declared in the dataset catalog", sf.Field)
			}
			if !aggregable {
				return fmt.Errorf("spl_field: field %q is not sortable", sf.Field)
			}
			continue
		}
		ok := false
		for _, by := range p.By {
			if by == sf.Field {
				ok = true
			}
		}
		for _, agg := range p.Aggs {
			if agg.Alias != "" && agg.Alias == sf.Field {
				ok = true
			}
		}
		if !ok {
			return fmt.Errorf("spl_field: cannot sort aggregated results by %q (only by-fields and aggregation aliases)", sf.Field)
		}
	}
	return nil
}

// ---- Compiler: logical plan -> bounded Elasticsearch query ----

// Env carries the server-side enforced scope. Domain and Generation are added
// as mandatory terms when non-empty (uim-domain datasets); the dataset itself
// is fixed by the index path.
type Env struct {
	Organization string
	Index        string // concrete index pattern with namespace resolved
	Domain       string // uba.route.domain term, empty to skip
	Generation   string // uba.route.generation term, empty to skip
	From, To     time.Time
}

func (c Cmp) clause() map[string]any {
	var value any = c.Value
	if c.IsNumber {
		value = c.Number
	}
	switch c.Op {
	case "=":
		return map[string]any{"term": map[string]any{c.Field: value}}
	case "!=":
		return map[string]any{"bool": map[string]any{"must_not": []any{map[string]any{"term": map[string]any{c.Field: value}}}}}
	}
	key := map[string]string{">": "gt", ">=": "gte", "<": "lt", "<=": "lte"}[c.Op]
	return map[string]any{"range": map[string]any{c.Field: map[string]any{key: value}}}
}

func compileExpr(e Expr) map[string]any {
	switch n := e.(type) {
	case Cmp:
		return n.clause()
	case And:
		return map[string]any{"bool": map[string]any{"filter": []any{compileExpr(n.L), compileExpr(n.R)}}}
	case Or:
		return map[string]any{"bool": map[string]any{"should": []any{compileExpr(n.L), compileExpr(n.R)}, "minimum_should_match": 1}}
	case Not:
		return map[string]any{"bool": map[string]any{"must_not": []any{compileExpr(n.E)}}}
	}
	return map[string]any{"match_none": map[string]any{}}
}

func metricAggs(aggs []AggFunc) map[string]any {
	out := map[string]any{}
	for i, agg := range aggs {
		name := agg.Alias
		if name == "" {
			name = agg.Func
			if agg.Field != "" {
				name = agg.Func + "_" + strings.ReplaceAll(agg.Field, ".", "_")
			}
			if _, exists := out[name]; exists {
				name = fmt.Sprintf("%s_%d", name, i)
			}
		}
		if agg.Func == "count" {
			out[name] = map[string]any{"value_count": map[string]any{"field": "event.id"}}
			continue
		}
		out[name] = map[string]any{agg.Func: map[string]any{"field": agg.Field}}
	}
	return out
}

// Compile builds the bounded ES request body for the plan. The four scoping
// elements (tenant, namespace via Index, dataset, generation) and the time
// range are always enforced server-side; a 30s timeout is embedded in the
// body. Bucket budgets are computed before execution and overflow is a stable
// budget_exceeded error.
func (p *Plan) Compile(env Env, limit int, cursor []any) (map[string]any, error) {
	filters := []any{
		map[string]any{"term": map[string]any{"organization.id": env.Organization}},
	}
	if env.Domain != "" {
		filters = append(filters, map[string]any{"term": map[string]any{"ueba.route.domain": env.Domain}})
	}
	if env.Generation != "" {
		filters = append(filters, map[string]any{"term": map[string]any{"ueba.route.generation": env.Generation}})
	}
	filters = append(filters, map[string]any{"range": map[string]any{"@timestamp": map[string]any{
		"gte": env.From.UTC().Format(time.RFC3339Nano),
		"lte": env.To.UTC().Format(time.RFC3339Nano),
	}}})
	if p.Filter != nil {
		filters = append(filters, compileExpr(p.Filter))
	}
	query := map[string]any{
		"query":   map[string]any{"bool": map[string]any{"filter": filters}},
		"timeout": QueryTimeout.String(),
	}
	switch p.Mode {
	case ModeEvents:
		query["size"] = limit
		query["track_total_hits"] = true
		sort := []any{}
		if len(p.Sort) == 0 {
			sort = append(sort, map[string]any{"@timestamp": "desc"})
		} else {
			for _, sf := range p.Sort {
				dir := "asc"
				if sf.Desc {
					dir = "desc"
				}
				sort = append(sort, map[string]any{sf.Field: dir})
			}
		}
		hasID := false
		for _, sf := range p.Sort {
			if sf.Field == "event.id" {
				hasID = true
			}
		}
		if !hasID {
			sort = append(sort, map[string]any{"event.id": "asc"})
		}
		query["sort"] = sort
		if len(p.Fields) > 0 {
			query["_source"] = map[string]any{"includes": p.Fields}
		}
		if cursor != nil {
			query["search_after"] = cursor
		}
	case ModeStats:
		query["size"] = 0
		metrics := metricAggs(p.Aggs)
		if len(p.By) == 0 {
			query["track_total_hits"] = true
			query["aggs"] = metrics
			break
		}
		sources := []any{}
		for _, by := range p.By {
			sources = append(sources, map[string]any{by: map[string]any{"terms": map[string]any{"field": by}}})
		}
		composite := map[string]any{"size": MaxBuckets, "sources": sources}
		if len(p.Sort) > 0 {
			// composite order is source order; honor a single by-field sort.
			if len(p.Sort) == 1 {
				dir := "asc"
				if p.Sort[0].Desc {
					dir = "desc"
				}
				sources = []any{map[string]any{p.Sort[0].Field: map[string]any{"terms": map[string]any{"field": p.Sort[0].Field, "order": dir}}}}
				for _, by := range p.By {
					if by != p.Sort[0].Field {
						sources = append(sources, map[string]any{by: map[string]any{"terms": map[string]any{"field": by}}})
					}
				}
				composite["sources"] = sources
			}
		}
		query["aggs"] = map[string]any{"buckets": map[string]any{"composite": composite, "aggs": metrics}}
	case ModeTimechart:
		query["size"] = 0
		spans := int64(env.To.Sub(env.From) / p.Span)
		if env.To.Sub(env.From)%p.Span != 0 {
			spans++
		}
		perSpan := int64(1)
		if len(p.By) == 1 {
			perSpan = int64(DefaultTopN)
		}
		if spans*perSpan > int64(MaxBuckets) {
			return nil, fmt.Errorf("budget_exceeded: timechart would produce %d buckets (max %d); widen span or narrow the time range", spans*perSpan, MaxBuckets)
		}
		metrics := metricAggs(p.Aggs)
		histogram := map[string]any{
			"field":          "@timestamp",
			"fixed_interval": spanString(p.Span),
			"min_doc_count":  0,
			"extended_bounds": map[string]any{
				"min": env.From.UTC().Format(time.RFC3339Nano),
				"max": env.To.UTC().Format(time.RFC3339Nano),
			},
		}
		if len(p.By) == 1 {
			query["aggs"] = map[string]any{"timechart": map[string]any{
				"date_histogram": histogram,
				"aggs": map[string]any{"by": map[string]any{
					"terms": map[string]any{"field": p.By[0], "size": DefaultTopN},
					"aggs":  metrics,
				}},
			}}
		} else {
			query["aggs"] = map[string]any{"timechart": map[string]any{"date_histogram": histogram, "aggs": metrics}}
		}
	case ModeTop, ModeRare:
		query["size"] = 0
		order := "desc"
		if p.Mode == ModeRare {
			order = "asc"
		}
		query["aggs"] = map[string]any{p.Mode: map[string]any{
			"terms": map[string]any{"field": p.By[0], "size": p.TopN, "order": map[string]any{"_count": order}},
		}}
	}
	return query, nil
}

func spanString(d time.Duration) string {
	if d%time.Second != 0 {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int64(d/time.Hour))
	}
	if d%time.Minute == 0 {
		return fmt.Sprintf("%dm", int64(d/time.Minute))
	}
	return fmt.Sprintf("%ds", int64(d/time.Second))
}
