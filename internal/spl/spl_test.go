package spl

import (
	"strings"
	"testing"
	"time"
)

func TestParseEventsModes(t *testing.T) {
	p, err := Parse(`search authentication where event.outcome = "failure" and (source.port >= 1024 or not user.name = "root") | fields user.name, source.ip | sort -@timestamp | head 50`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Mode != ModeEvents || p.Head != 50 || len(p.Fields) != 2 || len(p.Sort) != 1 || !p.Sort[0].Desc || p.Sort[0].Field != "@timestamp" {
		t.Fatalf("plan=%+v", p)
	}
	and, ok := p.Filter.(And)
	if !ok {
		t.Fatalf("filter=%T", p.Filter)
	}
	if _, ok := and.R.(Or); !ok {
		t.Fatalf("right=%T", and.R)
	}
}

func TestParseStats(t *testing.T) {
	p, err := Parse(`search network | stats count, sum(network.bytes) as total_bytes by network.transport, vendor.name`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Mode != ModeStats || len(p.Aggs) != 2 || p.Aggs[1].Alias != "total_bytes" || len(p.By) != 2 {
		t.Fatalf("plan=%+v", p)
	}
}

func TestParseTimechartTopRare(t *testing.T) {
	p, err := Parse(`search authentication | timechart span=1h count by event.outcome`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Mode != ModeTimechart || p.Span != time.Hour || len(p.By) != 1 {
		t.Fatalf("plan=%+v", p)
	}
	if _, err := ParseSpan("1d"); err != nil {
		t.Fatal(err)
	}
	p, err = Parse(`search authentication | top 20 source.ip`)
	if err != nil || p.Mode != ModeTop || p.TopN != 20 || p.By[0] != "source.ip" {
		t.Fatalf("top: %v %+v", err, p)
	}
	p, err = Parse(`search authentication | rare event.action`)
	if err != nil || p.Mode != ModeRare || p.TopN != DefaultTopN {
		t.Fatalf("rare: %v %+v", err, p)
	}
}

func TestParseRejectsOutOfSubset(t *testing.T) {
	for _, q := range []string{
		// arbitrary ES DSL / JSON body
		`{"query":{"match_all":{}}}`,
		// subquery
		`search authentication [search network]`,
		// join
		`search authentication | join user.name [search iam]`,
		// eval
		`search authentication | eval x = 1 + 1`,
		// regex literal
		`search authentication where user.name = /a.*/`,
		// wildcard
		`search authentication where user.name = "adm*"`,
		`search authentication where user.name = adm*`,
		// unknown command
		`search authentication | transaction user.name`,
		`search authentication | rex field=x "(?<y>.*)"`,
		// missing search prefix
		`authentication where x = 1`,
		// double transform
		`search authentication | stats count | top 5 user.name`,
		`search authentication | top 5 user.name | rare user.name`,
		// unbounded head
		`search authentication | head 1001`,
		`search authentication | head 0`,
		// top out of range
		`search authentication | top 101 user.name`,
		// unsupported aggregation
		`search authentication | stats dc(user.name) by user.name`,
		// timechart without span
		`search authentication | timechart count`,
		// fields in agg mode
		`search authentication | stats count by user.name | fields user.name`,
		// too long
		"search authentication where user.name = \"" + strings.Repeat("a", MaxQueryLength) + "\"",
	} {
		if _, err := Parse(q); err == nil {
			t.Errorf("expected rejection: %s", q)
		}
	}
}

type testCaps struct {
	fields map[string]struct {
		typ                string
		searchable, aggregable bool
	}
}

func (c testCaps) Lookup(name string) (string, bool, bool, bool) {
	f, ok := c.fields[name]
	if !ok {
		return "", false, false, false
	}
	return f.typ, f.searchable, f.aggregable, true
}

func caps() testCaps {
	type fc = struct {
		typ                string
		searchable, aggregable bool
	}
	return testCaps{fields: map[string]fc{
		"@timestamp":      {"date", true, true},
		"event.outcome":   {"keyword", true, true},
		"user.name":       {"keyword", true, true},
		"network.bytes":   {"long", false, true},
		"event.original":  {"text", true, false},
		"organization.id": {"keyword", true, true},
	}}
}

func mustParse(t *testing.T, q string) *Plan {
	t.Helper()
	p, err := Parse(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	return p
}

func TestValidateFieldWhitelist(t *testing.T) {
	valid := []string{
		`search authentication where event.outcome = "failure"`,
		`search network | stats sum(network.bytes) by organization.id`,
		`search authentication | top 10 user.name`,
		`search authentication | sort -@timestamp | head 5`,
	}
	for _, q := range valid {
		if err := mustParse(t, q).Validate(caps()); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
	invalid := []string{
		// undeclared field anywhere
		`search authentication where not.in.catalog = "x"`,
		`search authentication | fields not.in.catalog`,
		`search authentication | stats count by not.in.catalog`,
		`search authentication | top 5 not.in.catalog`,
		`search authentication | sort not.in.catalog`,
		// search condition on non-searchable field
		`search network where network.bytes = 5`,
		// stats by non-aggregable field
		`search raw | stats count by event.original`,
		// sum/avg on keyword
		`search authentication | stats sum(user.name) by user.name`,
		`search authentication | stats avg(event.outcome)`,
		// sort on non-aggregable
		`search raw | sort event.original`,
		// sort aggregated results by a non-alias
		`search authentication | stats count by user.name | sort event.outcome`,
	}
	for _, q := range invalid {
		if err := mustParse(t, q).Validate(caps()); err == nil {
			t.Errorf("expected whitelist rejection: %s", q)
		} else if !strings.HasPrefix(err.Error(), "spl_field:") && !strings.HasPrefix(err.Error(), "spl_parse:") {
			t.Errorf("%s: unexpected error class %v", q, err)
		}
	}
	// sort by alias after stats is allowed
	if err := mustParse(t, `search network | stats sum(network.bytes) as total by organization.id | sort total`).Validate(caps()); err != nil {
		t.Error(err)
	}
}

func testEnv() Env {
	return Env{
		Organization: "tenant_a",
		Index:        "logs-ueba.authentication-tenant_a",
		Domain:       "authentication",
		Generation:   "g1",
		From:         time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC),
	}
}

func filtersOf(t *testing.T, body map[string]any) []any {
	t.Helper()
	filters, ok := body["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
	if !ok {
		t.Fatalf("no filters in %+v", body)
	}
	return filters
}

func TestCompileEnforcesFourElementsAndTimeout(t *testing.T) {
	body, err := mustParse(t, `search authentication where event.outcome = "failure"`).Compile(testEnv(), DefaultLimit, nil)
	if err != nil {
		t.Fatal(err)
	}
	if body["timeout"] != QueryTimeout.String() {
		t.Fatalf("timeout missing: %+v", body)
	}
	if body["size"] != DefaultLimit {
		t.Fatalf("default limit: %+v", body)
	}
	filters := filtersOf(t, body)
	var tenant, domain, generation, timerange, userFilter bool
	for _, raw := range filters {
		clause := raw.(map[string]any)
		if term, ok := clause["term"].(map[string]any); ok {
			if term["organization.id"] == "tenant_a" {
				tenant = true
			}
			if term["ueba.route.domain"] == "authentication" {
				domain = true
			}
			if term["ueba.route.generation"] == "g1" {
				generation = true
			}
			if term["event.outcome"] == "failure" {
				userFilter = true
			}
		}
		if r, ok := clause["range"].(map[string]any); ok {
			if _, ok := r["@timestamp"]; ok {
				timerange = true
			}
		}
	}
	if !tenant || !domain || !generation || !timerange || !userFilter {
		t.Fatalf("scope enforcement missing: tenant=%v domain=%v generation=%v range=%v filter=%v\n%+v", tenant, domain, generation, timerange, userFilter, filters)
	}
	// sort defaults to @timestamp desc with event.id tie-break
	if len(body["sort"].([]any)) != 2 {
		t.Fatalf("sort=%+v", body["sort"])
	}
}

func TestCompileBooleanExpr(t *testing.T) {
	body, err := mustParse(t, `search authentication where not (event.outcome = "failure" or event.outcome = "unknown")`).Compile(testEnv(), 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	filters := filtersOf(t, body)
	last := filters[len(filters)-1].(map[string]any)
	mustNot, ok := last["bool"].(map[string]any)["must_not"].([]any)
	if !ok || len(mustNot) != 1 {
		t.Fatalf("must_not=%+v", last)
	}
	inner := mustNot[0].(map[string]any)
	if inner["bool"].(map[string]any)["minimum_should_match"] != 1 {
		t.Fatalf("or clause=%+v", inner)
	}
}

func TestCompileStatsCompositeBudget(t *testing.T) {
	body, err := mustParse(t, `search network | stats sum(network.bytes) by organization.id`).Compile(testEnv(), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if body["size"] != 0 {
		t.Fatalf("agg query must be size 0: %+v", body)
	}
	comp := body["aggs"].(map[string]any)["buckets"].(map[string]any)["composite"].(map[string]any)
	if comp["size"] != MaxBuckets {
		t.Fatalf("composite size=%v (bucket budget)", comp["size"])
	}
	// no by: plain metric aggs, no composite
	body, err = mustParse(t, `search network | stats sum(network.bytes), count`).Compile(testEnv(), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := body["aggs"].(map[string]any)["buckets"]; ok {
		t.Fatalf("unexpected composite: %+v", body["aggs"])
	}
}

func TestCompileTimechartBudgetExceeded(t *testing.T) {
	// 31 days at 1m spans with a by field: ~44640*10 buckets > 10000.
	env := testEnv()
	env.From = env.To.Add(-31 * 24 * time.Hour)
	_, err := mustParse(t, `search authentication | timechart span=1m count by event.outcome`).Compile(env, 0, nil)
	if err == nil || !strings.HasPrefix(err.Error(), "budget_exceeded:") {
		t.Fatalf("expected budget_exceeded, got %v", err)
	}
	// same range at 1h spans without by: 745 buckets, fine.
	body, err := mustParse(t, `search authentication | timechart span=1h count`).Compile(env, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := body["aggs"].(map[string]any)["timechart"].(map[string]any)["date_histogram"].(map[string]any)
	if h["fixed_interval"] != "1h" {
		t.Fatalf("histogram=%+v", h)
	}
}

func TestCompileTopRare(t *testing.T) {
	body, err := mustParse(t, `search authentication | top 25 user.name`).Compile(testEnv(), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	terms := body["aggs"].(map[string]any)["top"].(map[string]any)["terms"].(map[string]any)
	if terms["size"] != 25 || terms["order"].(map[string]any)["_count"] != "desc" {
		t.Fatalf("terms=%+v", terms)
	}
	body, err = mustParse(t, `search authentication | rare user.name`).Compile(testEnv(), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	terms = body["aggs"].(map[string]any)["rare"].(map[string]any)["terms"].(map[string]any)
	if terms["order"].(map[string]any)["_count"] != "asc" || terms["size"] != DefaultTopN {
		t.Fatalf("terms=%+v", terms)
	}
}
