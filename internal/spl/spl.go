// Package spl implements the SPL subset, logical plan and Elasticsearch
// compiler mandated by design baseline §7. Supported grammar (anything else
// is rejected fail-closed):
//
//	search <dataset> [WHERE <bool-expr>]
//	[| fields f1, f2, ...]
//	[| stats count|sum(f)|min(f)|max(f)|avg(f) [as alias] [, ...] [BY f1, f2]]
//	[| timechart span=<duration> count|sum(f)|min(f)|max(f)|avg(f) [BY f]]
//	[| sort [-]f1, [-]f2, ...]
//	[| head <n>]
//	[| top [n] f | rare [n] f]
//
// bool-expr supports =, !=, >, >=, <, <= with AND/OR/NOT and parentheses.
// Subqueries, joins, eval, regex, wildcards and any raw ES DSL are not part of
// the subset and are rejected during lexing/parsing — user input can never be
// compiled into arbitrary DSL.
package spl

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Design baseline §7 limits.
const (
	MaxBuckets       = 10000
	DefaultLimit     = 100
	MaxLimit         = 1000
	MaxTimeRangeDays = 31
	QueryTimeout     = 30 * time.Second
	MaxQueryLength   = 2048
	MaxTopN          = 100
	DefaultTopN      = 10
)

// ---- Logical plan ----

type Expr interface{ exprNode() }

type Cmp struct {
	Field    string
	Op       string // =, !=, >, >=, <, <=
	Value    string
	Number   float64
	IsNumber bool
}

type And struct{ L, R Expr }
type Or struct{ L, R Expr }
type Not struct{ E Expr }

func (Cmp) exprNode() {}
func (And) exprNode() {}
func (Or) exprNode()  {}
func (Not) exprNode() {}

type AggFunc struct {
	Func  string // count | sum | min | max | avg
	Field string // empty for count
	Alias string
}

type SortField struct {
	Field string
	Desc  bool
}

const (
	ModeEvents    = "events"
	ModeStats     = "stats"
	ModeTimechart = "timechart"
	ModeTop       = "top"
	ModeRare      = "rare"
)

// Plan is the structured logical intermediate representation of an SPL query.
type Plan struct {
	Dataset string
	Filter  Expr
	Fields  []string
	Mode    string
	Aggs    []AggFunc
	By      []string
	Span    time.Duration // timechart only
	TopN    int           // top/rare only
	Sort    []SortField
	Head    int // 0 = unset
}

// ---- Lexer ----

type token struct {
	kind string // ident | number | qstring | punct
	text string
}

func lex(input string) ([]token, error) {
	var tokens []token
	i := 0
	for i < len(input) {
		c := input[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case strings.IndexByte("|(),", c) >= 0:
			tokens = append(tokens, token{kind: "punct", text: string(c)})
			i++
		case c == '=' :
			tokens = append(tokens, token{kind: "op", text: "="})
			i++
		case c == '!':
			if i+1 < len(input) && input[i+1] == '=' {
				tokens = append(tokens, token{kind: "op", text: "!="})
				i += 2
			} else {
				return nil, fmt.Errorf("spl_parse: '!' is only valid as '!='")
			}
		case c == '>' || c == '<':
			if i+1 < len(input) && input[i+1] == '=' {
				tokens = append(tokens, token{kind: "op", text: string([]byte{c, '='})})
				i += 2
			} else {
				tokens = append(tokens, token{kind: "op", text: string(c)})
				i++
			}
		case c == '"':
			j := i + 1
			var b strings.Builder
			for j < len(input) && input[j] != '"' {
				if input[j] == '\\' && j+1 < len(input) {
					j++
				}
				b.WriteByte(input[j])
				j++
			}
			if j >= len(input) {
				return nil, fmt.Errorf("spl_parse: unterminated string literal")
			}
			tokens = append(tokens, token{kind: "qstring", text: b.String()})
			i = j + 1
		case strings.IndexByte("{}[];/`$*\\'", c) >= 0:
			return nil, fmt.Errorf("spl_parse: character %q is not supported (no subqueries, regex, wildcards or raw DSL)", string(c))
		default:
			j := i
			for j < len(input) && !strings.ContainsRune(" \t\n\r|(),=!<>\"{}[];/`$*\\'", rune(input[j])) {
				j++
			}
			tokens = append(tokens, token{kind: "word", text: input[i:j]})
			i = j
		}
	}
	return tokens, nil
}

// ---- Parser ----

type parser struct {
	tokens []token
	pos    int
}

func (p *parser) peek() (token, bool) {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos], true
	}
	return token{}, false
}

func (p *parser) next() (token, error) {
	t, ok := p.peek()
	if !ok {
		return token{}, fmt.Errorf("spl_parse: unexpected end of query")
	}
	p.pos++
	return t, nil
}

func (p *parser) expectPunct(want string) error {
	t, err := p.next()
	if err != nil || t.kind != "punct" || t.text != want {
		return fmt.Errorf("spl_parse: expected %q", want)
	}
	return nil
}

func (p *parser) keyword(word string) bool {
	t, ok := p.peek()
	if ok && t.kind == "word" && strings.EqualFold(t.text, word) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) fieldName() (string, error) {
	t, err := p.next()
	if err != nil || t.kind != "word" {
		return "", fmt.Errorf("spl_parse: expected a field name")
	}
	return t.text, nil
}

// Parse turns SPL text into a logical plan. Anything outside the documented
// subset is rejected.
func Parse(input string) (*Plan, error) {
	if len(input) == 0 || len(input) > MaxQueryLength {
		return nil, fmt.Errorf("spl_parse: query must be 1..%d characters", MaxQueryLength)
	}
	tokens, err := lex(input)
	if err != nil {
		return nil, err
	}
	p := &parser{tokens: tokens}
	if !p.keyword("search") {
		return nil, fmt.Errorf("spl_parse: query must start with 'search <dataset>'")
	}
	dataset, err := p.fieldName()
	if err != nil {
		return nil, fmt.Errorf("spl_parse: 'search' must name a dataset")
	}
	plan := &Plan{Dataset: dataset, Mode: ModeEvents}
	if p.keyword("where") {
		plan.Filter, err = p.parseOr()
		if err != nil {
			return nil, err
		}
	}
	for {
		t, ok := p.peek()
		if !ok {
			return plan, nil
		}
		if t.kind != "punct" || t.text != "|" {
			return nil, fmt.Errorf("spl_parse: unexpected token %q", t.text)
		}
		p.pos++
		if err := p.parseCommand(plan); err != nil {
			return nil, err
		}
	}
}

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.keyword("or") {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = Or{L: left, R: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.keyword("and") {
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = And{L: left, R: right}
	}
	return left, nil
}

func (p *parser) parseUnary() (Expr, error) {
	if p.keyword("not") {
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return Not{E: inner}, nil
	}
	t, ok := p.peek()
	if ok && t.kind == "punct" && t.text == "(" {
		p.pos++
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if err := p.expectPunct(")"); err != nil {
			return nil, err
		}
		return inner, nil
	}
	return p.parseComparison()
}

func (p *parser) parseComparison() (Expr, error) {
	name, err := p.fieldName()
	if err != nil {
		return nil, err
	}
	t, err := p.next()
	if err != nil || t.kind != "op" {
		return nil, fmt.Errorf("spl_parse: expected a comparison operator after %q", name)
	}
	value, err := p.next()
	if err != nil || (value.kind != "word" && value.kind != "qstring") {
		return nil, fmt.Errorf("spl_parse: expected a comparison value after %q", name)
	}
	cmp := Cmp{Field: name, Op: t.text, Value: value.text}
	if strings.ContainsAny(cmp.Value, "*?") {
		return nil, fmt.Errorf("spl_parse: wildcards are not supported in comparison values")
	}
	if value.kind == "word" {
		if n, convErr := strconv.ParseFloat(value.text, 64); convErr == nil {
			cmp.Number = n
			cmp.IsNumber = true
		}
	}
	return cmp, nil
}

func (p *parser) fieldList() ([]string, error) {
	name, err := p.fieldName()
	if err != nil {
		return nil, err
	}
	fields := []string{name}
	for {
		t, ok := p.peek()
		if !ok || t.kind != "punct" || t.text != "," {
			return fields, nil
		}
		p.pos++
		name, err := p.fieldName()
		if err != nil {
			return nil, err
		}
		fields = append(fields, name)
	}
}

func (p *parser) aggFunc() (AggFunc, error) {
	t, err := p.next()
	if err != nil || t.kind != "word" {
		return AggFunc{}, fmt.Errorf("spl_parse: expected an aggregation function")
	}
	fn := strings.ToLower(t.text)
	if fn == "count" {
		return AggFunc{Func: fn}, nil
	}
	if fn != "sum" && fn != "min" && fn != "max" && fn != "avg" {
		return AggFunc{}, fmt.Errorf("spl_parse: unsupported aggregation %q (allowed: count, sum, min, max, avg)", t.text)
	}
	if err := p.expectPunct("("); err != nil {
		return AggFunc{}, err
	}
	name, err := p.fieldName()
	if err != nil {
		return AggFunc{}, err
	}
	if err := p.expectPunct(")"); err != nil {
		return AggFunc{}, err
	}
	return AggFunc{Func: fn, Field: name}, nil
}

func (p *parser) setMode(plan *Plan, mode string) error {
	if plan.Mode != ModeEvents || len(plan.Aggs) > 0 || plan.TopN > 0 {
		return fmt.Errorf("spl_parse: only one of stats/timechart/top/rare is allowed per query")
	}
	plan.Mode = mode
	return nil
}

func (p *parser) parseCommand(plan *Plan) error {
	t, err := p.next()
	if err != nil || t.kind != "word" {
		return fmt.Errorf("spl_parse: expected a command after '|'")
	}
	switch strings.ToLower(t.text) {
	case "fields":
		if plan.Mode != ModeEvents {
			return fmt.Errorf("spl_parse: 'fields' is only allowed in event mode")
		}
		fields, err := p.fieldList()
		if err != nil {
			return err
		}
		plan.Fields = fields
	case "stats":
		if err := p.setMode(plan, ModeStats); err != nil {
			return err
		}
		agg, err := p.aggFunc()
		if err != nil {
			return err
		}
		plan.Aggs = []AggFunc{agg}
		for {
			tok, ok := p.peek()
			if !ok || tok.kind != "punct" || tok.text != "," {
				break
			}
			p.pos++
			agg, err := p.aggFunc()
			if err != nil {
				return err
			}
			plan.Aggs = append(plan.Aggs, agg)
		}
		if p.keyword("as") {
			alias, err := p.fieldName()
			if err != nil {
				return err
			}
			plan.Aggs[len(plan.Aggs)-1].Alias = alias
		}
		if p.keyword("by") {
			by, err := p.fieldList()
			if err != nil {
				return err
			}
			plan.By = by
		}
	case "timechart":
		if err := p.setMode(plan, ModeTimechart); err != nil {
			return err
		}
		if !p.keyword("span") {
			return fmt.Errorf("spl_parse: 'timechart' requires span=<duration>")
		}
		if eq, eqErr := p.next(); eqErr != nil || eq.kind != "op" || eq.text != "=" {
			return fmt.Errorf("spl_parse: 'timechart' requires span=<duration>")
		}
		tok, err := p.next()
		if err != nil || tok.kind != "word" {
			return fmt.Errorf("spl_parse: invalid span value")
		}
		span, err := ParseSpan(tok.text)
		if err != nil {
			return err
		}
		plan.Span = span
		agg, err := p.aggFunc()
		if err != nil {
			return err
		}
		plan.Aggs = []AggFunc{agg}
		if p.keyword("by") {
			by, err := p.fieldList()
			if err != nil {
				return err
			}
			if len(by) != 1 {
				return fmt.Errorf("spl_parse: 'timechart' supports at most one by field")
			}
			plan.By = by
		}
	case "sort":
		first, err := p.sortField()
		if err != nil {
			return err
		}
		plan.Sort = []SortField{first}
		for {
			tok, ok := p.peek()
			if !ok || tok.kind != "punct" || tok.text != "," {
				break
			}
			p.pos++
			f, err := p.sortField()
			if err != nil {
				return err
			}
			plan.Sort = append(plan.Sort, f)
		}
	case "head":
		if plan.Mode != ModeEvents {
			return fmt.Errorf("spl_parse: 'head' is only allowed in event mode")
		}
		tok, err := p.next()
		if err != nil || tok.kind != "word" {
			return fmt.Errorf("spl_parse: 'head' requires a count")
		}
		n, err := strconv.Atoi(tok.text)
		if err != nil || n < 1 || n > MaxLimit {
			return fmt.Errorf("spl_parse: 'head' must be 1..%d", MaxLimit)
		}
		plan.Head = n
	case "top", "rare":
		mode := strings.ToLower(t.text)
		if err := p.setMode(plan, mode); err != nil {
			return err
		}
		tok, err := p.next()
		if err != nil || tok.kind != "word" {
			return fmt.Errorf("spl_parse: '%s' requires a field", mode)
		}
		n := DefaultTopN
		if v, convErr := strconv.Atoi(tok.text); convErr == nil {
			n = v
			if n < 1 || n > MaxTopN {
				return fmt.Errorf("spl_parse: '%s' count must be 1..%d", mode, MaxTopN)
			}
			tok, err = p.next()
			if err != nil || tok.kind != "word" {
				return fmt.Errorf("spl_parse: '%s' requires a field", mode)
			}
		}
		plan.TopN = n
		plan.By = []string{tok.text}
	default:
		return fmt.Errorf("spl_parse: unsupported command %q (allowed: fields, stats, timechart, sort, head, top, rare)", t.text)
	}
	return nil
}

func (p *parser) sortField() (SortField, error) {
	name, err := p.fieldName()
	if err != nil {
		return SortField{}, err
	}
	sf := SortField{Field: name}
	if strings.HasPrefix(name, "-") {
		sf.Field = strings.TrimPrefix(name, "-")
		sf.Desc = true
		if sf.Field == "" {
			return SortField{}, fmt.Errorf("spl_parse: invalid sort field")
		}
	}
	return sf, nil
}

// ParseSpan parses a timechart span such as 5m, 1h or 1d.
func ParseSpan(text string) (time.Duration, error) {
	if strings.HasSuffix(text, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(text, "d"))
		if err != nil || n < 1 || n > MaxTimeRangeDays {
			return 0, fmt.Errorf("spl_parse: invalid span %q", text)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(text)
	if err != nil || d < time.Second || d > MaxTimeRangeDays*24*time.Hour {
		return 0, fmt.Errorf("spl_parse: invalid span %q", text)
	}
	return d, nil
}
