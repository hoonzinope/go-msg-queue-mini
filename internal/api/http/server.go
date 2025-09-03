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

type clientLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type httpServerInstance struct {
	Addr  string
	Queue internal.Queue
	ApiKey string
	clients map[string]*clientLimiter
}

var clientMapLock sync.Mutex

func StartServer(ctx context.Context, config *internal.Config, queue internal.Queue) error {
	// Start the HTTP server
	addr := config.HTTP.Port
	if addr == 0 {
		addr = 8080
	}
	httpServerInstance := &httpServerInstance{
		Addr:  fmt.Sprintf(":%d", addr),
		Queue: queue,
		ApiKey: config.HTTP.Auth.APIKey,
		clients: make(map[string]*clientLimiter),
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

	go checkExpiredClients(ctx, httpServerInstance.clients)

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
	r.Use(RateLimitMiddleware(httpServerInstance.clients, 1))

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

func checkExpiredClients(ctx context.Context, clients map[string]*clientLimiter) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			clientMapLock.Lock()
			for ip, limiter := range clients {
				if time.Since(limiter.lastSeen) > time.Second*3 {
					delete(clients, ip)
				}
			}
			clientMapLock.Unlock()
		case <-ctx.Done():
			return
		}
	}
}