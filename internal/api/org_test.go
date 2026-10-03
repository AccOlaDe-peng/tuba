package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type fakeOrgRow struct {
	id  string
	err error
}

func (r fakeOrgRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*string)) = r.id
	return nil
}

type fakeOrgSource struct {
	id    string
	err   error
	calls int
}

func (s *fakeOrgSource) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	s.calls++
	return fakeOrgRow{id: s.id, err: s.err}
}

func TestOrgResolverCachesPositiveResult(t *testing.T) {
	source := &fakeOrgSource{id: "d9e836f8-3f52-49dd-9c15-32bad37a5bef"}
	resolver := NewOrgResolver(source)
	for i := 0; i < 3; i++ {
		id, err := resolver.Resolve(context.Background(), "tenant_a")
		if err != nil || id != source.id {
			t.Fatalf("id=%s err=%v", id, err)
		}
	}
	if source.calls != 1 {
		t.Fatalf("expected one lookup, got %d", source.calls)
	}
}

func TestOrgResolverCacheExpires(t *testing.T) {
	source := &fakeOrgSource{id: "uuid-1"}
	resolver := NewOrgResolver(source)
	now := time.Now()
	resolver.now = func() time.Time { return now }
	if _, err := resolver.Resolve(context.Background(), "tenant_a"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(10 * time.Minute)
	if _, err := resolver.Resolve(context.Background(), "tenant_a"); err != nil {
		t.Fatal(err)
	}
	if source.calls != 2 {
		t.Fatalf("expected re-lookup after TTL, got %d calls", source.calls)
	}
}

func TestOrgResolverFailuresAreNotCached(t *testing.T) {
	source := &fakeOrgSource{err: errors.New("connection refused")}
	resolver := NewOrgResolver(source)
	if _, err := resolver.Resolve(context.Background(), "tenant_a"); err == nil {
		t.Fatal("expected error")
	}
	source.err = nil
	source.id = "uuid-1"
	id, err := resolver.Resolve(context.Background(), "tenant_a")
	if err != nil || id != "uuid-1" {
		t.Fatalf("id=%s err=%v", id, err)
	}
	if source.calls != 2 {
		t.Fatalf("failures must not be cached, got %d calls", source.calls)
	}
}

func TestOrgResolverUnknownSlugFailsClosed(t *testing.T) {
	source := &fakeOrgSource{err: pgx.ErrNoRows}
	resolver := NewOrgResolver(source)
	if _, err := resolver.Resolve(context.Background(), "ghost"); !errors.Is(err, ErrOrgNotFound) {
		t.Fatalf("err=%v, want ErrOrgNotFound", err)
	}
	if _, err := resolver.Resolve(context.Background(), ""); !errors.Is(err, ErrOrgNotFound) {
		t.Fatalf("empty slug err=%v, want ErrOrgNotFound", err)
	}
}
