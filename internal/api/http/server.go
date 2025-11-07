package http

import (
	"context"
	"errors"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/ui"
	"io/fs"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type ClientLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type RateLimiter struct {
	clients map[string]*ClientLimiter
	lock    *sync.Mutex
	limit   int
	burst   int
}

type httpServerInstance struct {
	Addr           string
	Queue          internal.Queue
	QueueInspector internal.QueueInspector
	ApiKey         string
	limiter        RateLimiter
	Logger         *slog.Logger
	uiFS           http.FileSystem
}

func StartServer(
	ctx context.Context, config *internal.Config, queue internal.Queue, logger *slog.Logger) error {
	// Start the HTTP server
	addr := config.HTTP.Port
	if addr == 0 {
		addr = 8080
	}

	rateLimiter := RateLimiter{
		clients: make(map[string]*ClientLimiter),
		lock:    &sync.Mutex{},
		limit:   config.HTTP.Rate.Limit,
		burst:   config.HTTP.Rate.Burst,
	}

	sub, subFSerr := fs.Sub(ui.StaticFiles, "static")
	if subFSerr != nil {
		return fmt.Errorf("failed to create sub filesystem: %w", subFSerr)
	}

	httpServerInstance := &httpServerInstance{
		Addr:           fmt.Sprintf(":%d", addr),
		Queue:          queue,
		QueueInspector: queue.(internal.QueueInspector),
		ApiKey:         config.HTTP.Auth.APIKey,
		limiter:        rateLimiter,
		Logger:         logger,
		uiFS:           http.FS(sub),
	}

	server := &http.Server{
		Addr:    httpServerInstance.Addr,
		Handler: Router(httpServerInstance),
	}

	go func() {
		<-ctx.Done()
		err := server.Shutdown(context.Background())
		if err != nil {
			logger.Error("Error shutting down server", "error", err)
		}
	}()

	go checkExpiredClients(ctx, httpServerInstance.limiter)

	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("error starting server: %w", err)
	}
	return nil
}

func checkExpiredClients(ctx context.Context, limiter RateLimiter) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			limiter.lock.Lock()
			for ip, client := range limiter.clients {
				if time.Since(client.lastSeen) > time.Minute {
					delete(limiter.clients, ip)
				}
			}
			limiter.lock.Unlock()
		case <-ctx.Done():
			return
		}
	}
}
