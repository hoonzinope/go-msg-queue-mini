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
	Addr  string
	Queue internal.Queue
	ApiKey string
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
		Addr:  fmt.Sprintf(":%d", addr),
		Queue: queue,
		ApiKey: config.HTTP.Auth.APIKey,
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

	reader := r.Group("/api/v1")
	{
		reader.GET("/health", healthCheckHandler)
		reader.GET("/status", httpServerInstance.statusHandler)
		reader.POST("/peek", httpServerInstance.peekHandler)
	}
	
	writer := r.Group("/api/v1")
	writer.Use(AuthMiddleware(httpServerInstance.ApiKey))
	{
		writer.POST("/enqueue", httpServerInstance.enqueueHandler)
		writer.POST("/dequeue", httpServerInstance.dequeueHandler)
		writer.POST("/ack", httpServerInstance.ackHandler)
		writer.POST("/nack", httpServerInstance.nackHandler)
		writer.POST("/renew", httpServerInstance.renewHandler)
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