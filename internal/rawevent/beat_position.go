package rawevent

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// StableBeatPosition returns a Filebeat file-record cursor when the event has
// native filestream coordinates. Kafka delivery coordinates remain separate
// and are used as the fallback for Beats without file metadata.
func StableBeatPosition(payload []byte, deliveryPosition string) string {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var event map[string]any
	if decoder.Decode(&event) != nil {
		return deliveryPosition
	}
	winlog, ok := event["winlog"].(map[string]any)
	if ok {
		computer, computerOK := coordinateString(winlog["computer_name"])
		channel, channelOK := coordinateString(winlog["channel"])
		recordID, recordOK := coordinateUint(winlog["record_id"])
		stamp, stampOK := coordinateString(event["@timestamp"])
		parsedStamp, err := time.Parse(time.RFC3339Nano, stamp)
		if computerOK && channelOK && recordOK && stampOK && err == nil && !strings.ContainsAny(computer+channel, "\r\n") {
			return fmt.Sprintf("winlogbeat-v1:%s:%s:%d:%s", hex.EncodeToString([]byte(strings.ToLower(computer))), hex.EncodeToString([]byte(strings.ToLower(channel))), recordID, parsedStamp.UTC().Format(time.RFC3339Nano))
		}
		return deliveryPosition
	}
	logFields, ok := event["log"].(map[string]any)
	if !ok {
		return deliveryPosition
	}
	fileFields, ok := logFields["file"].(map[string]any)
	if !ok {
		return deliveryPosition
	}
	device, deviceOK := coordinateString(fileFields["device_id"])
	inode, inodeOK := coordinateString(fileFields["inode"])
	offset, offsetOK := coordinateInt(logFields["offset"])
	if !deviceOK || !inodeOK || !offsetOK || offset < 0 || strings.ContainsAny(device+inode, ":\r\n") {
		return deliveryPosition
	}
	// Prefer the file fingerprint when the collector supplies one. Device and
	// inode are reused once a rotated file is deleted, so two unrelated files
	// can present the same cursor and the later record is rejected as a
	// position conflict instead of being indexed. coordinateString already
	// rejects an empty value.
	if fingerprint, ok := coordinateString(fileFields["fingerprint"]); ok &&
		!strings.ContainsAny(fingerprint, ":\r\n") {
		return fmt.Sprintf("filebeat-v2:%s:%d", fingerprint, offset)
	}
	return fmt.Sprintf("filebeat-v1:%s:%s:%d", device, inode, offset)
}

func coordinateUint(value any) (uint64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := strconv.ParseUint(typed.String(), 10, 64)
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseUint(typed, 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func coordinateString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, typed != ""
	case json.Number:
		return typed.String(), typed.String() != ""
	default:
		return "", false
	}
}

func coordinateInt(value any) (int64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Int64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
