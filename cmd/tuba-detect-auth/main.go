package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"tuba/product/internal/config"
	"tuba/product/internal/detection"
	"tuba/product/internal/es"
	"tuba/product/internal/event"
)

// diagnosticPurpose marks this command as a diagnostic tool only (F08, design
// baseline §6): the single authoritative analysis scheduling entry is the
// Python tuba-analysis-worker. This CLI must never be wired into online
// scheduling or production emission paths; the assembly guard in
// internal/analysis/assembly_test.go asserts no other binary links the Go
// detection packages.
const diagnosticPurpose = "diagnostic"

const diagnosticBanner = "tuba-detect-auth is a DIAGNOSTIC tool only. " +
	"The authoritative production scheduling entry is the Python tuba-analysis-worker " +
	"(design baseline §6, F08); this CLI is not part of any online scheduling or emission path."

func summary(from, to string, events, anomalies int, write bool) map[string]any {
	return map[string]any{
		"purpose": diagnosticPurpose,
		"window_start": from,
		"window_end":   to,
		"events":       events,
		"anomalies":    anomalies,
		"written":      write,
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "%s\n\nUsage of %s:\n", diagnosticBanner, os.Args[0])
		flag.PrintDefaults()
	}
	from := flag.String("from", "", "inclusive RFC3339 event time")
	to := flag.String("to", "", "exclusive RFC3339 event time")
	write := flag.Bool("write", false, "write anomalies to Elasticsearch (diagnostic inspection only)")
	flag.Parse()
	log.Printf("diagnostic use only: %s", diagnosticBanner)
	start, err := time.Parse(time.RFC3339, *from)
	if err != nil {
		log.Fatal("invalid -from")
	}
	end, err := time.Parse(time.RFC3339, *to)
	if err != nil {
		log.Fatal("invalid -to")
	}
	if !start.Before(end) || end.Sub(start) > time.Hour {
		log.Fatal("window must be positive and at most one hour")
	}
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	client, err := es.New(cfg.ESURL, cfg.ESAPIKey)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	index := "logs-ueba.authentication-" + cfg.Namespace
	query := map[string]any{"size": 10000, "sort": []any{map[string]any{"@timestamp": "asc"}, map[string]any{"event.id": "asc"}},
		"query": map[string]any{"bool": map[string]any{"filter": []any{
			map[string]any{"term": map[string]any{"organization.id": cfg.Organization}},
			map[string]any{"range": map[string]any{"@timestamp": map[string]any{"gte": start.Add(-30 * time.Minute).Format(time.RFC3339), "lt": end.Format(time.RFC3339)}}},
		}}}}
	var result es.SearchResult
	status, err := client.Do(ctx, "POST", "/"+index+"/_search", query, &result)
	if err != nil || status != 200 {
		log.Fatalf("search failed: status=%d error=%v", status, err)
	}
	if len(result.Hits.Hits) >= 10000 {
		log.Fatal("window exceeds 10000 events; split the interval")
	}
	events := make([]event.Authentication, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		e, err := event.Parse(hit.Source, cfg.Organization)
		if err != nil {
			log.Fatalf("stored event %s invalid: %v", hit.ID, err)
		}
		events = append(events, e)
	}
	anomalies, err := detection.FailureThenSuccess(events, cfg.Organization, 5, 30*time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	count := 0
	for _, doc := range anomalies {
		at, err := time.Parse(time.RFC3339Nano, doc.Source["@timestamp"].(string))
		if err != nil {
			log.Fatal(err)
		}
		if at.Before(start) || !at.Before(end) {
			continue
		}
		count++
		if *write {
			status, err := client.Do(ctx, "PUT", "/ueba-anomalies-"+cfg.Namespace+"/_create/"+es.EscapeID(doc.ID), doc.Source, nil)
			if err != nil || (status != 201 && status != 409) {
				log.Fatalf("anomaly write failed: status=%d error=%v", status, err)
			}
		}
	}
	_ = json.NewEncoder(os.Stdout).Encode(summary(*from, *to, len(events), count, *write))
}
