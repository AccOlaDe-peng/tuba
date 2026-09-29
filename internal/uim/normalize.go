// Package uim implements the first controlled DIP + UIM path for Windows Security and Zeek.
package uim

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"tuba/product/internal/rawevent"
)

var ErrUnsupported = errors.New("unsupported source event")

var featureUseByDomain = map[string]string{
	"authentication": "authentication_features",
	"session":        "session_features",
	"iam":            "iam_features",
	"directory":      "directory_features",
	"network":        "network_features",
	"dns":            "dns_features",
	"web":            "web_features",
	"tls":            "tls_features",
}

type Quarantine struct {
	ID             string    `json:"id"`
	OrganizationID string    `json:"organization_id"`
	Namespace      string    `json:"namespace"`
	RawEventID     string    `json:"raw_event_id"`
	ReleaseID      string    `json:"release_id"`
	Stage          string    `json:"stage"`
	Code           string    `json:"code"`
	Reason         string    `json:"reason"`
	OccurredAt     time.Time `json:"occurred_at"`
}

func Normalize(raw rawevent.Envelope) (map[string]any, error) {
	var payload map[string]any
	if err := json.Unmarshal(raw.Payload, &payload); err != nil {
		return nil, fmt.Errorf("DIP_JSON_INVALID: %w", err)
	}
	switch strings.ToLower(raw.Vendor.Name) {
	case "microsoft":
		event, err := normalizeWindows(raw, payload)
		if err != nil {
			return nil, err
		}
		if err := Validate(event); err != nil {
			return nil, err
		}
		return event, nil
	case "zeek":
		payload, err := unwrapZeekBeat(raw, payload)
		if err != nil {
			return nil, err
		}
		event, err := normalizeZeek(raw, payload)
		if err != nil {
			return nil, err
		}
		if err := Validate(event); err != nil {
			return nil, err
		}
		return event, nil
	default:
		return nil, ErrUnsupported
	}
}

func unwrapZeekBeat(raw rawevent.Envelope, payload map[string]any) (map[string]any, error) {
	agent := object(payload["agent"])
	if !strings.EqualFold(stringValue(agent["type"]), "filebeat") {
		return payload, nil
	}
	beatEvent := object(payload["event"])
	if beatEvent == nil || stringValue(beatEvent["dataset"]) != raw.Vendor.Dataset {
		return nil, errors.New("DIP_ZEEK_DATASET_MISMATCH")
	}
	original, originalOK := beatEvent["original"].(string)
	message, messageOK := payload["message"].(string)
	if !originalOK || !messageOK || original == "" || message == "" {
		return nil, errors.New("DIP_ZEEK_ORIGINAL_MISSING")
	}
	if original != message {
		return nil, errors.New("DIP_ZEEK_ORIGINAL_MISMATCH")
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(original), &event); err != nil || event == nil {
		return nil, errors.New("DIP_ZEEK_ORIGINAL_INVALID")
	}
	return event, nil
}

func normalizeWindows(raw rawevent.Envelope, in map[string]any) (map[string]any, error) {
	winlog := object(in["winlog"])
	if winlog == nil {
		winlog = object(in["Winlog"])
	}
	if winlog == nil {
		return nil, errors.New("DIP_WINDOWS_WINLOG_MISSING")
	}
	code := stringValue(winlog["event_id"])
	provider := stringValue(winlog["provider_name"])
	securityProvider := strings.EqualFold(provider, "Microsoft-Windows-Security-Auditing")
	logClearProvider := code == "1102" && strings.EqualFold(provider, "Microsoft-Windows-Eventlog")
	if (!securityProvider && !logClearProvider) || !strings.EqualFold(stringValue(winlog["channel"]), "Security") {
		return nil, ErrUnsupported
	}
	if code == "" {
		return nil, errors.New("DIP_WINDOWS_EVENT_ID_MISSING")
	}
	data := object(winlog["event_data"])
	if data == nil {
		data = map[string]any{}
	}
	eventTime, err := eventTime(in["@timestamp"], winlog["time_created"])
	if err != nil {
		return nil, errors.New("UIM_EVENT_TIME_INVALID")
	}
	computer := stringValue(winlog["computer_name"])
	if computer == "" {
		return nil, errors.New("DIP_WINDOWS_COMPUTER_MISSING")
	}

	domain, category, action, outcome, sub, supported := windowsSemantics(code, data)
	if !supported {
		return nil, ErrUnsupported
	}
	user := map[string]any{}
	group := map[string]any{}
	warnings := []string{}
	targetPrimary := code == "4624" || code == "4625" || code == "4634" || code == "4723" || code == "4724" || code == "4768" || code == "4769" || code == "4771" || code == "4776"
	actorName, actorID := stringValue(data["SubjectUserName"]), stringValue(data["SubjectUserSid"])
	targetName, targetID := stringValue(data["TargetUserName"]), stringValue(data["TargetUserSid"])
	if targetID == "" {
		targetID = stringValue(data["TargetSid"])
	}
	if code == "4648" {
		// Explicit-credential events describe an actor using credentials for a
		// different account. Preserve both roles instead of replacing the actor.
		putNonEmpty(user, "name", actorName)
		putNonEmpty(user, "id", actorID)
		userTarget := map[string]any{}
		putNonEmpty(userTarget, "name", targetName)
		putNonEmpty(userTarget, "id", targetID)
		if len(userTarget) > 0 {
			user["target"] = userTarget
		}
	} else if targetPrimary {
		putNonEmpty(user, "name", targetName)
		putNonEmpty(user, "id", targetID)
	} else {
		putNonEmpty(user, "name", actorName)
		putNonEmpty(user, "id", actorID)
		if domain == "directory" || domain == "iam" {
			if code == "4728" || code == "4729" || code == "4732" || code == "4733" || code == "4756" || code == "4757" {
				group = map[string]any{}
				putNonEmpty(group, "name", stringValue(data["TargetUserName"]))
				putNonEmpty(group, "id", stringValue(data["TargetSid"]))
				userTarget := map[string]any{}
				putNonEmpty(userTarget, "name", stringValue(data["MemberName"]))
				putNonEmpty(userTarget, "id", stringValue(data["MemberSid"]))
				if len(userTarget) > 0 {
					user["target"] = userTarget
				}
			} else {
				userTarget := map[string]any{}
				putNonEmpty(userTarget, "name", targetName)
				putNonEmpty(userTarget, "id", targetID)
				if len(userTarget) > 0 {
					user["target"] = userTarget
				}
			}
		}
	}
	if len(user) == 0 {
		warnings = append(warnings, "missing_user_identity")
	}
	if domain == "directory" && len(user) == 0 {
		return nil, errors.New("UIM_ACTOR_REQUIRED")
	}

	fields := baseFields(raw, eventTime, domain, category, sub)
	fields["event"].(map[string]any)["action"] = action
	fields["event"].(map[string]any)["outcome"] = outcome
	fields["host"] = map[string]any{"name": computer}
	fields["event"].(map[string]any)["code"] = code
	fields["vendor"].(map[string]any)["payload"] = map[string]any{"event_id": code, "computer_name": computer, "record_id": stringValue(winlog["record_id"]), "event_data": data}
	if len(user) > 0 {
		fields["user"] = user
	}
	if len(group) > 0 {
		fields["group"] = group
	}
	if ip := stringValue(data["IpAddress"]); ip != "" && ip != "-" {
		if net.ParseIP(strings.TrimPrefix(strings.ToLower(ip), "::ffff:")) != nil {
			fields["source"] = map[string]any{"ip": strings.TrimPrefix(ip, "::ffff:")}
		} else {
			warnings = append(warnings, "invalid_source_ip")
		}
	}
	setQuality(fields, warnings)
	return fields, nil
}

func windowsSemantics(code string, data map[string]any) (domain, category, action, outcome, semantic string, ok bool) {
	switch code {
	case "4624":
		return "authentication", "authentication", "logon-success", "success", "authentication.logon-success", true
	case "4625":
		return "authentication", "authentication", "logon-failure", "failure", "authentication.logon-failure", true
	case "4672":
		return "authentication", "authentication", "special-privileges-assigned", "success", "authentication.special-privileges-assigned", true
	case "4719":
		return "iam", "iam", "audit-policy-change", "success", "iam.audit-policy-change", true
	case "4634", "4647":
		return "session", "session", "logoff", "success", "session.logoff", true
	case "4648":
		return "authentication", "authentication", "explicit-credentials-use", "success", "authentication.explicit-credentials-use", true
	case "4662":
		return "directory", "iam", "directory-object-access", "success", "directory.object-access", true
	case "4673":
		return "directory", "iam", "privileged-service-call", "success", "directory.privileged-service-call", true
	case "4720", "4722", "4725", "4726":
		actions := map[string]string{"4720": "user-create", "4722": "user-enable", "4725": "user-disable", "4726": "user-delete"}
		return "iam", "iam", actions[code], "success", "iam." + actions[code], true
	case "4723", "4724":
		action := "password-change"
		if code == "4724" {
			action = "password-reset"
		}
		return "iam", "iam", action, "success", "iam." + action, true
	case "4728", "4729", "4732", "4733", "4756", "4757":
		verb := "remove"
		if code == "4728" || code == "4732" || code == "4756" {
			verb = "add"
		}
		return "directory", "iam", "group-member-" + verb, "success", "directory.group-member-" + verb, true
	case "4768", "4769", "4776":
		if stringValue(data["Status"]) == "0x0" || stringValue(data["Status"]) == "0" {
			return "authentication", "authentication", "credential-validation", "success", "authentication.credential-validation", true
		}
		return "authentication", "authentication", "credential-validation", "failure", "authentication.credential-validation", true
	case "4771":
		return "authentication", "authentication", "kerberos-preauth-failure", "failure", "authentication.kerberos-preauth-failure", true
	case "1102":
		return "iam", "iam", "security-log-cleared", "success", "iam.security-log-cleared", true
	default:
		return "", "", "", "", "", false
	}
}

func normalizeZeek(raw rawevent.Envelope, in map[string]any) (map[string]any, error) {
	path := strings.ToLower(strings.TrimPrefix(raw.Vendor.Dataset, "zeek."))
	declaredPath := strings.ToLower(stringValue(in["_path"]))
	if path != "" && declaredPath != "" && declaredPath != path {
		return nil, errors.New("DIP_ZEEK_DATASET_MISMATCH")
	}
	if path == "" {
		path = declaredPath
	}
	if path != "conn" && path != "dns" && path != "http" && path != "ssl" {
		return nil, ErrUnsupported
	}
	stamp, err := zeekTime(in["ts"])
	if err != nil {
		stamp, err = eventTime(in["@timestamp"], nil)
		if err != nil {
			return nil, errors.New("UIM_EVENT_TIME_INVALID")
		}
	}
	identity := object(in["id"])
	origIP, respIP := stringValue(in["id.orig_h"]), stringValue(in["id.resp_h"])
	if identity != nil {
		if origIP == "" {
			origIP = stringValue(identity["orig_h"])
		}
		if respIP == "" {
			respIP = stringValue(identity["resp_h"])
		}
	}
	if origIP == "" {
		origIP = stringValue(in["orig_h"])
	}
	if respIP == "" {
		respIP = stringValue(in["resp_h"])
	}
	if origIP == "" || respIP == "" || net.ParseIP(origIP) == nil || net.ParseIP(respIP) == nil {
		return nil, errors.New("UIM_ZEEK_ENDPOINTS_REQUIRED")
	}
	domain, category, action, semantic := "network", "network", "network-connection", "network.connection"
	fields := baseFields(raw, stamp, domain, category, semantic)
	event := fields["event"].(map[string]any)
	event["action"] = action
	event["outcome"] = "unknown"
	source := map[string]any{"ip": origIP}
	destination := map[string]any{"ip": respIP}
	if port, ok := zeekPort(in["id.orig_p"], identity, "orig_p"); ok {
		source["port"] = port
	}
	if port, ok := zeekPort(in["id.resp_p"], identity, "resp_p"); ok {
		destination["port"] = port
	}
	fields["source"] = source
	fields["destination"] = destination
	network := map[string]any{}
	if proto := stringValue(in["proto"]); proto != "" {
		network["transport"] = strings.ToLower(proto)
	}
	if service := stringValue(in["service"]); service != "" && service != "-" {
		network["protocol"] = strings.ToLower(service)
	}
	fields["network"] = network
	warnings := []string{}
	switch path {
	case "conn":
		if seconds, parseErr := strconv.ParseFloat(stringValue(in["duration"]), 64); parseErr == nil && seconds >= 0 && !math.IsInf(seconds, 0) && !math.IsNaN(seconds) && seconds < float64(math.MaxInt64)/float64(time.Second) {
			event["duration"] = int64(seconds * float64(time.Second))
		}
		if orig, ok := zeekCounter(in["orig_bytes"]); ok {
			if resp, ok := zeekCounter(in["resp_bytes"]); ok && orig <= math.MaxInt64-resp {
				network["bytes"] = orig + resp
			}
		}
		if orig, ok := zeekCounter(in["orig_pkts"]); ok {
			if resp, ok := zeekCounter(in["resp_pkts"]); ok && orig <= math.MaxInt64-resp {
				network["packets"] = orig + resp
			}
		}
	case "dns":
		query := stringValue(in["query"])
		if query == "" {
			return nil, errors.New("UIM_DNS_QUERY_REQUIRED")
		}
		domain, category, action, semantic = "dns", "network", "dns-response", "dns.response"
		dns := map[string]any{"question": map[string]any{"name": query}}
		if responseCode := stringValue(in["rcode_name"]); responseCode != "" && responseCode != "-" {
			dns["response_code"] = responseCode
		}
		fields["dns"] = dns
	case "http":
		host := stringValue(in["host"])
		if host == "" {
			return nil, errors.New("UIM_HTTP_HOST_REQUIRED")
		}
		domain, category, action, semantic = "web", "web", "http-request", "web.request"
		fields["url"] = map[string]any{"domain": host, "path": defaultString(stringValue(in["uri"]), "/"), "scheme": "http"}
		fields["http"] = map[string]any{"request": map[string]any{}}
		if method := stringValue(in["method"]); method != "" {
			fields["http"].(map[string]any)["request"].(map[string]any)["method"] = method
		}
		if status, parseErr := strconv.Atoi(stringValue(in["status_code"])); parseErr == nil {
			fields["http"].(map[string]any)["response"] = map[string]any{"status_code": status}
			if status >= 400 {
				event["outcome"] = "failure"
			} else {
				event["outcome"] = "success"
			}
		}
	case "ssl":
		serverName := stringValue(in["server_name"])
		if serverName == "" {
			return nil, errors.New("UIM_TLS_SERVER_NAME_REQUIRED")
		}
		domain, category, action, semantic = "tls", "network", "tls-session", "tls.session"
		fields["destination"].(map[string]any)["domain"] = serverName
		fields["tls"] = map[string]any{}
		if v := stringValue(in["version"]); v != "" {
			fields["tls"].(map[string]any)["version"] = v
		}
		if v := stringValue(in["cipher"]); v != "" {
			fields["tls"].(map[string]any)["cipher"] = v
		}
	}
	event["dataset"] = domain
	fields["ueba"].(map[string]any)["route"].(map[string]any)["domain"] = domain
	event["category"] = []string{category}
	event["action"] = action
	fields["ueba"].(map[string]any)["event"].(map[string]any)["type"] = semantic
	if _, has := in["uid"]; !has {
		warnings = append(warnings, "missing_source_session_id")
	}
	fields["vendor"].(map[string]any)["payload"] = in
	setQuality(fields, warnings)
	return fields, nil
}

func baseFields(raw rawevent.Envelope, stamp time.Time, domain, category, semantic string) map[string]any {
	eventSeed := "event-v1|" + raw.RawEventID + "|" + semantic
	eventHash := sha256.Sum256([]byte(eventSeed))
	eventID := "evt:" + hex.EncodeToString(eventHash[:])
	fields := map[string]any{
		"@timestamp":   stamp.UTC().Format(time.RFC3339Nano),
		"organization": map[string]any{"id": raw.Organization.ID},
		"event":        map[string]any{"id": eventID, "kind": "event", "category": []string{category}, "dataset": domain},
		"vendor":       map[string]any{"name": raw.Vendor.Name, "product": raw.Vendor.Product, "dataset": raw.Vendor.Dataset},
		"ueba": map[string]any{
			"schema":  map[string]any{"version": "1.0.0"},
			"quality": map[string]any{},
			// The first implementation writes to the sole configured ES generation.
			// Release versions are tracked separately in provenance and must not be
			// mistaken for storage/activation generations.
			"route":      map[string]any{"domain": domain, "generation": "g1"},
			"event":      map[string]any{"type": semantic},
			"provenance": map[string]any{"raw_event_id": raw.RawEventID, "release_id": raw.ReleaseID, "dip_version": "1.0.0", "uim_version": "1.0.0"},
		},
	}
	return fields
}

func setQuality(fields map[string]any, warnings []string) {
	status := "qualified"
	if len(warnings) > 0 {
		status = "partial"
	}
	domain := stringValue(object(fields["event"])["dataset"])
	usableFor := []string{"event_query"}
	if capability, exists := featureUseByDomain[domain]; exists {
		usableFor = append(usableFor, capability)
	}
	fields["ueba"].(map[string]any)["quality"] = map[string]any{"status": status, "reasons": warnings, "usable_for": usableFor}
}

// Validate applies the UIM contract after the vendor-specific DIP has produced a fresh event.
func Validate(fields map[string]any) error {
	event := object(fields["event"])
	organization := object(fields["organization"])
	vendor := object(fields["vendor"])
	ueba := object(fields["ueba"])
	_, stampErr := time.Parse(time.RFC3339Nano, stringValue(fields["@timestamp"]))
	if stampErr != nil || stringValue(organization["id"]) == "" || stringValue(event["id"]) == "" ||
		stringValue(event["kind"]) != "event" || !validCategories(event["category"]) || stringValue(event["dataset"]) == "" ||
		stringValue(vendor["name"]) == "" || stringValue(vendor["product"]) == "" || stringValue(vendor["dataset"]) == "" {
		return errors.New("UIM_COMMON_REQUIRED_FIELD_MISSING")
	}
	route := object(ueba["route"])
	domain := stringValue(route["domain"])
	if !contains([]string{"authentication", "session", "iam", "directory", "network", "dns", "web", "tls"}, domain) {
		return errors.New("UIM_DOMAIN_UNSUPPORTED")
	}
	if stringValue(event["dataset"]) != domain || stringValue(route["generation"]) == "" {
		return errors.New("UIM_ROUTE_MISMATCH")
	}
	if outcome := stringValue(event["outcome"]); outcome != "" && !contains([]string{"success", "failure", "unknown"}, outcome) {
		return errors.New("UIM_EVENT_OUTCOME_INVALID")
	}
	if category := stringValue(object(ueba["schema"])["version"]); category != "1.0.0" {
		return errors.New("UIM_SCHEMA_VERSION_UNSUPPORTED")
	}
	if stringValue(object(ueba["event"])["type"]) == "" {
		return errors.New("UIM_EVENT_TYPE_REQUIRED")
	}
	provenance := object(ueba["provenance"])
	if stringValue(provenance["raw_event_id"]) == "" || stringValue(provenance["release_id"]) == "" {
		return errors.New("UIM_PROVENANCE_REQUIRED")
	}
	if domain == "authentication" || domain == "session" || domain == "iam" || domain == "directory" {
		if stringValue(event["action"]) == "" || stringValue(event["outcome"]) == "" {
			return errors.New("UIM_EVENT_SEMANTICS_REQUIRED")
		}
	}
	if domain == "network" {
		network := object(fields["network"])
		source, destination := object(fields["source"]), object(fields["destination"])
		if stringValue(network["transport"]) == "" || stringValue(source["ip"]) == "" || stringValue(destination["ip"]) == "" {
			return errors.New("UIM_NETWORK_ENDPOINTS_REQUIRED")
		}
	}
	if domain == "dns" {
		question := object(object(fields["dns"])["question"])
		if stringValue(question["name"]) == "" {
			return errors.New("UIM_DNS_QUESTION_REQUIRED")
		}
	}
	if domain == "web" && stringValue(object(fields["url"])["domain"]) == "" {
		return errors.New("UIM_WEB_DOMAIN_REQUIRED")
	}
	if domain == "tls" && stringValue(object(fields["destination"])["domain"]) == "" {
		return errors.New("UIM_TLS_SERVER_NAME_REQUIRED")
	}
	quality := object(ueba["quality"])
	status := stringValue(quality["status"])
	if status != "qualified" && status != "partial" {
		return errors.New("UIM_QUALITY_STATUS_INVALID")
	}
	reasons := stringValues(quality["reasons"])
	usableFor := stringValues(quality["usable_for"])
	if len(usableFor) == 0 || status == "qualified" && len(reasons) != 0 || status == "partial" && len(reasons) == 0 {
		return errors.New("UIM_QUALITY_DETAILS_INVALID")
	}
	return nil
}

func validCategories(value any) bool {
	switch values := value.(type) {
	case []string:
		if len(values) == 0 {
			return false
		}
		for _, item := range values {
			if strings.TrimSpace(item) == "" {
				return false
			}
		}
		return true
	case []any:
		if len(values) == 0 {
			return false
		}
		for _, item := range values {
			if stringValue(item) == "" {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func stringValues(value any) []string {
	var output []string
	switch values := value.(type) {
	case []string:
		for _, item := range values {
			if text := strings.TrimSpace(item); text != "" {
				output = append(output, text)
			}
		}
	case []any:
		for _, item := range values {
			if text := stringValue(item); text != "" {
				output = append(output, text)
			}
		}
	}
	return output
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func eventTime(primary, fallback any) (time.Time, error) {
	for _, value := range []any{primary, fallback} {
		if text := stringValue(value); text != "" {
			parsed, err := time.Parse(time.RFC3339Nano, text)
			if err == nil {
				return parsed.UTC(), nil
			}
		}
	}
	return time.Time{}, errors.New("event timestamp unavailable")
}

func zeekTime(value any) (time.Time, error) {
	seconds, err := strconv.ParseFloat(stringValue(value), 64)
	if err != nil {
		return time.Time{}, err
	}
	whole := int64(seconds)
	nanos := int64((seconds - float64(whole)) * float64(time.Second))
	return time.Unix(whole, nanos).UTC(), nil
}

func object(value any) map[string]any {
	if v, ok := value.(map[string]any); ok {
		return v
	}
	return nil
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		if v == "-" {
			return ""
		}
		return strings.TrimSpace(v)
	case json.Number:
		return v.String()
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func zeekPort(flat any, identity map[string]any, key string) (int, bool) {
	if flat == nil && identity != nil {
		flat = identity[key]
	}
	if flat == nil {
		return 0, false
	}
	port, err := strconv.Atoi(stringValue(flat))
	if err != nil || port < 0 || port > 65535 {
		return 0, false
	}
	return port, true
}

func zeekCounter(value any) (int64, bool) {
	if value == nil {
		return 0, false
	}
	number, err := strconv.ParseFloat(stringValue(value), 64)
	if err != nil || number < 0 || math.IsNaN(number) || math.IsInf(number, 0) || number >= float64(math.MaxInt64) || math.Trunc(number) != number {
		return 0, false
	}
	return int64(number), true
}

func putNonEmpty(target map[string]any, key, value string) {
	if value != "" {
		target[key] = value
	}
}

func defaultString(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
