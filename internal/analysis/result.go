package analysis

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Result is the versioned boundary between analytical workers and Go sinks.
type Result struct {
	ContractVersion string          `json:"contract_version"`
	ResultType      string          `json:"result_type"`
	ResultID        string          `json:"result_id"`
	OrganizationID string          `json:"organization_id"`
	Namespace      string          `json:"namespace"`
	RuleID         string          `json:"rule_id"`
	RuleVersion    string          `json:"rule_version"`
	RunID          string          `json:"run_id"`
	Document       json.RawMessage `json:"document"`
}

func Parse(raw []byte, organization, namespace string) (Result, error) {
	var result Result
	if len(raw) == 0 || len(raw) > 1<<20 {
		return result, errors.New("analysis result size must be 1 byte to 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("invalid analysis result: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return result, errors.New("trailing data after analysis result")
	}
	if result.ContractVersion != "1.0.0" || result.ResultType != "anomaly" {
		return result, errors.New("unsupported analysis result contract")
	}
	if result.ResultID == "" || len(result.ResultID) > 256 || result.RuleID == "" || result.RuleVersion == "" || result.RunID == "" || len(result.Document) == 0 {
		return result, errors.New("required analysis result fields are missing")
	}
	if result.OrganizationID != organization || result.Namespace != namespace {
		return result, errors.New("analysis result tenant does not match service identity")
	}
	var document struct {
		Organization struct {
			ID string `json:"id"`
		} `json:"organization"`
	}
	if json.Unmarshal(result.Document, &document) != nil || document.Organization.ID != organization {
		return result, errors.New("analysis document tenant mismatch")
	}
	return result, nil
}
