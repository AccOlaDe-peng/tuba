package lifecycle

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeHTTPWaitsForActiveRequestBeforeReturning(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	server := &http.Server{Addr: address, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() { serveDone <- ServeHTTP(ctx, server, 2*time.Second) }()

	var probe net.Conn
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		probe, err = net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			_ = probe.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("request did not reach the server: %v", err)
	}
	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := (&http.Client{Timeout: 3 * time.Second}).Get("http://" + address)
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- requestErr
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	cancel()
	select {
	case err := <-serveDone:
		t.Fatalf("ServeHTTP returned before the active handler completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("ServeHTTP returned an error after graceful shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeHTTP did not return after the handler completed")
	}
	if err := <-requestDone; err != nil {
		t.Fatalf("active request did not finish cleanly: %v", err)
	}
}

func TestServeHTTPReturnsListenerFailure(t *testing.T) {
	server := &http.Server{Addr: "127.0.0.1:invalid"}
	err := ServeHTTP(context.Background(), server, time.Second)
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("expected listener error, got %v", err)
	}
}

func TestServeHTTPClosesConnectionsWhenGracefulShutdownTimesOut(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	server := &http.Server{Addr: address, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() { serveDone <- ServeHTTP(ctx, server, 100*time.Millisecond) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, dialErr := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := (&http.Client{Timeout: 2 * time.Second}).Get("http://" + address)
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- requestErr
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	cancel()
	select {
	case serveErr := <-serveDone:
		if !errors.Is(serveErr, context.DeadlineExceeded) {
			t.Fatalf("expected bounded shutdown timeout, got %v", serveErr)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP server did not force close after shutdown timeout")
	}
	close(release)
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("request did not finish after forced connection close")
	}
}
