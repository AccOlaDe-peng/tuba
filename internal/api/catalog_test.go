package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"tuba/product/internal/auth"
	"tuba/product/internal/es"
)

func TestCatalogListsAllDatasets(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/catalog", nil)
	Server{}.catalog(recorder, request, auth.Principal{Organization: "tenant_a"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		CatalogVersion   int            `json:"catalog_version"`
		ActiveGeneration string         `json:"active_generation"`
		Datasets         []DatasetDecl  `json:"datasets"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.CatalogVersion != 1 || response.ActiveGeneration != "g1" {
		t.Fatalf("unexpected catalog header: %+v", response)
	}
	names := map[string]string{}
	for _, dataset := range response.Datasets {
		names[dataset.Name] = dataset.Kind
		if dataset.ActiveGeneration != "g1" {
			t.Fatalf("dataset %s missing active generation", dataset.Name)
		}
		if !strings.Contains(dataset.IndexPattern, "<namespace>") {
			t.Fatalf("dataset %s index pattern must pin namespace: %s", dataset.Name, dataset.IndexPattern)
		}
	}
	for _, domain := range []string{"authentication", "session", "iam", "directory", "network", "dns", "web", "tls"} {
		if names[domain] != "uim-domain" {
			t.Fatalf("domain %s missing from catalog: %v", domain, names)
		}
	}
	if names["raw"] != "raw" || names["quarantine"] != "quarantine" {
		t.Fatalf("raw/quarantine datasets missing: %v", names)
	}
}

func TestCatalogDatasetDetailAndNotFound(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/network", nil)
	request.SetPathValue("name", "network")
	Server{}.catalogDataset(recorder, request, auth.Principal{})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d", recorder.Code)
	}
	var dataset DatasetDecl
	if err := json.Unmarshal(recorder.Body.Bytes(), &dataset); err != nil {
		t.Fatal(err)
	}
	if dataset.Name != "network" || len(dataset.Fields) == 0 || len(dataset.QualityStatuses) != 2 {
		t.Fatalf("unexpected dataset: %+v", dataset)
	}
	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/api/v1/catalog/nope", nil)
	request.SetPathValue("name", "nope")
	Server{}.catalogDataset(recorder, request, auth.Principal{})
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("unknown dataset status=%d", recorder.Code)
	}
}

// catalog 注册表与 contracts/uim/domain-catalog.v1.yaml 不得漂移：
// yaml 声明的每个 common_required 字段必须出现在每个 UIM 域数据集的字段
// 清单里；反向地，Go 侧每个公共字段名也必须能在 yaml 文本中找到。
func TestCatalogMatchesDomainCatalogContract(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/uim/domain-catalog.v1.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	required := []string{
		"@timestamp", "organization.id", "event.id", "event.kind", "event.category",
		"event.dataset", "vendor.name", "vendor.product", "vendor.dataset",
		"ueba.schema.version", "ueba.quality.status", "ueba.route.domain",
		"ueba.route.generation", "ueba.provenance.raw_event_id", "ueba.provenance.release_id",
	}
	for _, dataset := range catalogDatasets() {
		if dataset.Kind != "uim-domain" {
			continue
		}
		declared := map[string]bool{}
		for _, f := range dataset.Fields {
			declared[f.Name] = true
			if !strings.Contains(text, f.Name) {
				t.Errorf("dataset %s field %s not found in domain-catalog.v1.yaml", dataset.Name, f.Name)
			}
		}
		for _, name := range required {
			if !declared[name] {
				t.Errorf("dataset %s missing contract-required field %s", dataset.Name, name)
			}
		}
	}
	// eventDomains 快速判定表与 catalog 保持一致。
	for domain := range eventDomains {
		if _, ok := findDataset(domain); !ok {
			t.Errorf("eventDomains entry %s missing from catalog", domain)
		}
	}
}

func TestEventsEnforcesActiveGenerationAndQuality(t *testing.T) {
	var query map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			t.Error(err)
		}
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
	}))
	defer server.Close()
	client, err := es.New(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/events?domain=network&quality=qualified", nil)
	recorder := httptest.NewRecorder()
	Server{ES: client}.events(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "tenant_a"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	filters := query["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
	var generation, quality any
	for _, raw := range filters {
		term, ok := raw.(map[string]any)["term"].(map[string]any)
		if !ok {
			continue
		}
		if v, ok := term["ueba.route.generation"]; ok {
			generation = v
		}
		if v, ok := term["ueba.quality.status"]; ok {
			quality = v
		}
	}
	if generation != "g1" {
		t.Fatalf("active generation not enforced: %+v", filters)
	}
	if quality != "qualified" {
		t.Fatalf("quality filter not applied: %+v", filters)
	}
}

func TestEventsRejectsUndeclaredQuality(t *testing.T) {
	client, err := es.New("http://127.0.0.1:9200", "test-key")
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/events?domain=network&quality=banana", nil)
	Server{ES: client}.events(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "tenant_a"})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}
