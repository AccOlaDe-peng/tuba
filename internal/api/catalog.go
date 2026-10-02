package api

import (
	"net/http"

	"tuba/product/internal/auth"
	"tuba/product/internal/catalog"
)

// Q01 Data Model / Dataset / Query Catalog. The registry itself lives in
// internal/catalog (single authoritative declaration shared with the Q03
// export executor in the control worker); these aliases keep the API surface
// unchanged. catalog_test.go cross-checks the registry against
// contracts/uim/domain-catalog.v1.yaml so they cannot drift apart silently.

type DatasetDecl = catalog.DatasetDecl
type FieldDecl = catalog.FieldDecl

const (
	SensitivityPublic    = catalog.SensitivityPublic
	SensitivityInternal  = catalog.SensitivityInternal
	SensitivitySensitive = catalog.SensitivitySensitive

	activeGenerationV1 = catalog.ActiveGenerationV1
)

var (
	catalogDatasets      = catalog.Datasets
	findDataset          = catalog.Find
	qualityStatusAllowed = catalog.QualityStatusAllowed
)

func (s Server) catalog(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	datasets := catalogDatasets()
	writeJSON(w, http.StatusOK, map[string]any{
		"catalog_version":    1,
		"active_generation":  activeGenerationV1,
		"max_time_range_day": 31,
		"datasets":           datasets,
	})
}

func (s Server) catalogDataset(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	dataset, ok := findDataset(r.PathValue("name"))
	if !ok {
		http.Error(w, "unknown dataset", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, dataset)
}
