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

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
		Handler: router(httpServerInstance),
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

func router(httpServerInstance *httpServerInstance) *gin.Engine {
	r := gin.New()
	r.RedirectTrailingSlash = false
	r.RedirectFixedPath = false
	r.Use(gin.Recovery())
	r.Use(LoggerMiddleware())
	r.Use(ErrorHandlingMiddleware())
	r.Use(RequestIDMiddleware())
	r.Use(RateLimitMiddleware(httpServerInstance.limiter))

	page := r.Group("/")
	{
		page.StaticFS("/static", httpServerInstance.uiFS)
		page.GET("/", httpServerInstance.uiHandler)
		page.GET("/queues/:queue_name", httpServerInstance.uiQueueDetailHandler)
	}

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.GET("/health", healthCheckHandler)

	reader_no_queue_name := r.Group("/api/v1")
	{
		reader_no_queue_name.GET("/status/all", httpServerInstance.statusAllHandler)
		reader_no_queue_name.GET("/queues/:queue_name/messages/:message_id", httpServerInstance.detailHandler)
	}

	reader := r.Group("/api/v1")
	reader.Use(queueNameMiddleware())
	{
		reader.GET("/:queue_name/status", httpServerInstance.statusHandler)
		reader.POST("/:queue_name/peek", httpServerInstance.peekHandler)
	}

	writer := r.Group("/api/v1")
	writer.Use(AuthMiddleware(httpServerInstance.ApiKey))
	writer.Use(queueNameMiddleware())
	{
		writer.POST("/:queue_name/create", httpServerInstance.createQueueHandler)
		writer.DELETE("/:queue_name/delete", httpServerInstance.deleteQueueHandler)
		writer.POST("/:queue_name/enqueue", httpServerInstance.enqueueHandler)
		writer.POST("/:queue_name/enqueue/batch", httpServerInstance.enqueueBatchHandler)
		writer.POST("/:queue_name/dequeue", httpServerInstance.dequeueHandler)
		writer.POST("/:queue_name/ack", httpServerInstance.ackHandler)
		writer.POST("/:queue_name/nack", httpServerInstance.nackHandler)
		writer.POST("/:queue_name/renew", httpServerInstance.renewHandler)
	}
	return r
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
