package http

import (
	"context"
	"errors"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/util"
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
	Addr    string
	Queue   internal.Queue
	ApiKey  string
	limiter RateLimiter
}

func StartServer(ctx context.Context, config *internal.Config, queue internal.Queue) error {
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

	httpServerInstance := &httpServerInstance{
		Addr:    fmt.Sprintf(":%d", addr),
		Queue:   queue,
		ApiKey:  config.HTTP.Auth.APIKey,
		limiter: rateLimiter,
	}

	server := &http.Server{
		Addr:    httpServerInstance.Addr,
		Handler: router(httpServerInstance),
	}

	go func() {
		<-ctx.Done()
		err := server.Shutdown(context.Background())
		if err != nil {
			util.Error(fmt.Sprintf("Error shutting down server: %v", err))
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
	r := gin.Default()
	r.Use(LoggerMiddleware())
	r.Use(ErrorHandlingMiddleware())
	r.Use(RequestIDMiddleware())
	r.Use(RateLimitMiddleware(httpServerInstance.limiter, 1))

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	reader := r.Group("/api/v1")
	{
		reader.GET("/health", healthCheckHandler)
		reader.GET("/:queue_name/status", httpServerInstance.statusHandler)
		reader.POST("/:queue_name/peek", httpServerInstance.peekHandler)
	}

	writer := r.Group("/api/v1")
	writer.Use(AuthMiddleware(httpServerInstance.ApiKey))
	{
		writer.POST("/:queue_name/create", httpServerInstance.createQueueHandler)
		writer.DELETE("/:queue_name/delete", httpServerInstance.deleteQueueHandler)
		writer.POST("/:queue_name/enqueue", httpServerInstance.enqueueHandler)
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
