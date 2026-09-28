package deadletter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"strconv"
	"time"
)

type Envelope struct {
	ContractVersion string      `json:"contract_version"`
	Source          Source      `json:"source"`
	Failure         Failure     `json:"failure"`
	Payload         interface{} `json:"payload"`
}
type Source struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key,omitempty"`
}
type Failure struct {
	Stage      string    `json:"stage"`
	Code       string    `json:"code"`
	Message    string    `json:"message"`
	Retryable  bool      `json:"retryable"`
	Attempts   int       `json:"attempts"`
	OccurredAt time.Time `json:"occurred_at"`
}

func Message(source kafka.Message, stage, code, message string, retryable bool, attempts int) (kafka.Message, error) {
	var payload interface{}
	if json.Valid(source.Value) {
		payload = json.RawMessage(source.Value)
	} else {
		payload = string(source.Value)
	}
	b, err := json.Marshal(Envelope{"1.0.0", Source{source.Topic, source.Partition, source.Offset, string(source.Key)}, Failure{stage, code, message, retryable, attempts, time.Now().UTC()}, payload})
	return kafka.Message{Key: []byte(source.Topic + ":" + strconv.Itoa(source.Partition) + ":" + strconv.FormatInt(source.Offset, 10)), Value: b, Headers: []kafka.Header{{Key: "content-type", Value: []byte("application/json")}, {Key: "contract-version", Value: []byte("1.0.0")}}}, err
}

// RawReferenceMessage keeps private source payloads out of the DLQ and stores only a reference and digest.
func RawReferenceMessage(source kafka.Message, rawEventID, payloadHash, stage, code, message string) (kafka.Message, error) {
	if rawEventID == "" {
		digest := sha256.Sum256(source.Value)
		payloadHash = hex.EncodeToString(digest[:])
	}
	payload := map[string]string{"raw_event_id": rawEventID, "payload_hash": payloadHash}
	b, err := json.Marshal(Envelope{"1.0.0", Source{source.Topic, source.Partition, source.Offset, string(source.Key)}, Failure{stage, code, message, false, 1, time.Now().UTC()}, payload})
	return kafka.Message{Key: []byte(source.Topic + ":" + strconv.Itoa(source.Partition) + ":" + strconv.FormatInt(source.Offset, 10)), Value: b, Headers: []kafka.Header{{Key: "content-type", Value: []byte("application/json")}, {Key: "contract-version", Value: []byte("1.0.0")}}}, err
}

func ReferenceMessage(source kafka.Message, objectID, payloadHash, stage, code, message string) (kafka.Message, error) {
	if payloadHash == "" {
		digest := sha256.Sum256(source.Value)
		payloadHash = hex.EncodeToString(digest[:])
	}
	payload := map[string]string{"object_id": objectID, "payload_hash": payloadHash}
	b, err := json.Marshal(Envelope{"1.0.0", Source{source.Topic, source.Partition, source.Offset, string(source.Key)}, Failure{stage, code, message, false, 1, time.Now().UTC()}, payload})
	return kafka.Message{Key: []byte(source.Topic + ":" + strconv.Itoa(source.Partition) + ":" + strconv.FormatInt(source.Offset, 10)), Value: b, Headers: []kafka.Header{{Key: "content-type", Value: []byte("application/json")}, {Key: "contract-version", Value: []byte("1.0.0")}}}, err
}
