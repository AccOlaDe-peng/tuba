// One-off ops accelerator: replays tuba.collector.<ns>.analysis.results.v2 from
// the sink consumer group's committed offset to the log end, applying the same
// idempotent PutAnalysisObject semantics as tuba-analysis-sink. Used when the
// sink's single-threaded throughput cannot drain a backlog in reasonable time.
// The real sink later replays the same messages; external-version state writes
// and deterministic history IDs make that replay a no-op.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/analysis"
	"tuba/product/internal/kafkautil"
	"tuba/product/internal/sink"
)

func main() {
	propsPath := env("KAFKA_PROPS", "/etc/tuba/kafka-admin.properties")
	namespace := env("NAMESPACE", "tenant_a")
	organization := env("ORGANIZATION_ID", "d9e836f8-3f52-49dd-9c15-32bad37a5bef")
	topic := env("TOPIC", "tuba.collector."+namespace+".analysis.results.v2")
	esURL := env("ES_URL", "http://127.0.0.1:9200")

	user, pass, brokers := readProps(propsPath)
	kc := kafkautil.Config{Protocol: "SASL_PLAINTEXT", Mechanism: "SCRAM-SHA-512", Username: user, Password: pass}
	dialer, err := kc.Dialer()
	if err != nil {
		log.Fatal(err)
	}
	start := int64(-1)
	if v := os.Getenv("START_OFFSET"); v != "" {
		fmt.Sscan(v, &start)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Dialer: dialer, Brokers: []string{brokers}, Topic: topic, Partition: 0,
		MinBytes: 1, MaxBytes: 10e6, MaxWait: 500 * time.Millisecond,
	})
	defer reader.Close()
	if start >= 0 {
		if err := reader.SetOffset(start); err != nil {
			log.Fatal(err)
		}
	}
	es := sink.New(esURL, "catchup", namespace)
	ctx := context.Background()
	var n, written, stale, skipped, failed int
	deadline := time.Now().Add(45 * time.Minute)
	for {
		if n%2000 == 0 {
			log.Printf("progress messages=%d written=%d stale=%d skipped=%d failed=%d", n, written, stale, skipped, failed)
		}
		if time.Now().After(deadline) {
			log.Printf("deadline reached, stopping")
			break
		}
		mctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := reader.FetchMessage(mctx)
		cancel()
		if err != nil {
			log.Printf("fetch ended at offset %d: %v", reader.Offset(), err)
			break
		}
		n++
		object, err := analysis.ParseObject(msg.Value, organization, namespace)
		if err != nil {
			skipped++
			continue
		}
		wctx, wcancel := context.WithTimeout(ctx, 30*time.Second)
		err = es.PutAnalysisObject(wctx, object)
		wcancel()
		if err == nil {
			written++
			continue
		}
		if sink.IsStaleRevision(err) {
			stale++
			continue
		}
		failed++
		log.Printf("write failed offset=%d type=%s id=%s rev=%d: %v", msg.Offset, object.ObjectType, object.ObjectID, object.Revision, err)
		var permanent sink.PermanentIndexError
		if errors.As(err, &permanent) {
			// The real sink would dead-letter these; for catch-up we only need
			// the backlog applied, so permanent conflicts are non-fatal.
			failed--
			skipped++
			continue
		}
		if failed > 50 {
			log.Fatal("too many failures")
		}
	}
	log.Printf("DONE messages=%d written=%d stale=%d skipped=%d failed=%d", n, written, stale, skipped, failed)
}

func readProps(path string) (user, pass, brokers string) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "bootstrap.servers=") {
			brokers = strings.TrimPrefix(line, "bootstrap.servers=")
		}
		if strings.HasPrefix(line, "sasl.jaas.config=") {
			if i := strings.Index(line, `username="`); i >= 0 {
				rest := line[i+len(`username="`):]
				user = rest[:strings.Index(rest, `"`)]
			}
			if i := strings.Index(line, `password="`); i >= 0 {
				rest := line[i+len(`password="`):]
				pass = rest[:strings.Index(rest, `"`)]
			}
		}
	}
	if user == "" || pass == "" || brokers == "" {
		log.Fatal("could not parse kafka properties")
	}
	return user, pass, brokers
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
