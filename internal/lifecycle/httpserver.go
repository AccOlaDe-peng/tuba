package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// ServeHTTP runs a server until its parent context is cancelled or the listener
// exits. It waits for graceful shutdown before returning so callers can safely
// release databases, Kafka writers, and other resources used by handlers.
func ServeHTTP(parent context.Context, server *http.Server, timeout time.Duration) error {
	if parent == nil {
		return errors.New("HTTP server parent context is required")
	}
	if server == nil {
		return errors.New("HTTP server is required")
	}
	if timeout <= 0 {
		return errors.New("HTTP shutdown timeout must be positive")
	}

	shutdownContext, cancelShutdown := context.WithCancel(parent)
	defer cancelShutdown()
	shutdownDone := make(chan error, 1)
	go func() {
		<-shutdownContext.Done()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		err := server.Shutdown(ctx)
		if err != nil {
			_ = server.Close()
		}
		shutdownDone <- err
	}()

	serveErr := server.ListenAndServe()
	cancelShutdown()
	shutdownErr := <-shutdownDone
	if shutdownErr != nil {
		return fmt.Errorf("gracefully shut down HTTP server: %w", shutdownErr)
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", serveErr)
	}
	return nil
}
