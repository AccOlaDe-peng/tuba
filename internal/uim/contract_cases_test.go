package uim

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"tuba/product/internal/rawevent"
)

func TestContractValidationCases(t *testing.T) {
	data, err := os.ReadFile("../../contracts/uim/validation-cases.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var suite struct {
		Cases []struct {
			ID     string `json:"id"`
			Vendor struct {
				Name    string `json:"name"`
				Product string `json:"product"`
				Dataset string `json:"dataset"`
			} `json:"vendor"`
			Payload  json.RawMessage `json:"payload"`
			Expected struct {
				Result  string `json:"result"`
				Code    string `json:"code"`
				Route   string `json:"route"`
				Outcome string `json:"outcome"`
			} `json:"expected"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &suite); err != nil {
		t.Fatal(err)
	}
	if len(suite.Cases) == 0 {
		t.Fatal("validation case contract is empty")
	}

	for _, test := range suite.Cases {
		t.Run(test.ID, func(t *testing.T) {
			raw, err := rawevent.New(rawevent.TrustedSource{
				OrganizationID: "tenant_a", Namespace: "tenant_a", SourceInstanceID: "contract-test",
				SourceContextID: "ctx_0123456789abcdef0123456789abcdef", SourceEpoch: "epoch-1",
				VendorName: test.Vendor.Name, VendorProduct: test.Vendor.Product,
				VendorDataset: test.Vendor.Dataset, ReleaseID: "validation-cases-v1",
			}, "validation-case:"+test.ID, test.Payload, time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatal(err)
			}

			actual, err := Normalize(raw)
			if test.Expected.Result == "quarantine" {
				if err == nil {
					t.Fatalf("Normalize unexpectedly succeeded: %+v", actual)
				}
				if test.Expected.Code == "EVENT_UNSUPPORTED" {
					if !errors.Is(err, ErrUnsupported) {
						t.Fatalf("Normalize error=%v, want unsupported mapping", err)
					}
				} else if err.Error() != test.Expected.Code {
					t.Fatalf("Normalize error=%q, want %q", err, test.Expected.Code)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.Expected.Result != "standard_event" {
				t.Fatalf("unknown expected result %q", test.Expected.Result)
			}
			ueba := object(actual["ueba"])
			route := object(ueba["route"])
			event := object(actual["event"])
			if got := stringValue(route["domain"]); got != test.Expected.Route {
				t.Fatalf("route.domain=%q, want %q", got, test.Expected.Route)
			}
			if got := stringValue(event["outcome"]); got != test.Expected.Outcome {
				t.Fatalf("event.outcome=%q, want %q", got, test.Expected.Outcome)
			}
		})
	}
}
