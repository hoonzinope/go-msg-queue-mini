package http

import (
	"context"
	"errors"
	"fmt"
	"go-msg-queue-mini/internal"
	"go-msg-queue-mini/util"
	"net/http"

	"github.com/gin-gonic/gin"
)

type httpServerInstance struct {
	Addr  string
	Queue internal.Queue
}

func StartServer(ctx context.Context, config *internal.Config, queue internal.Queue) error {
	// Start the HTTP server
	addr := config.HTTP.Port
	if addr == 0 {
		addr = 8080
	}
	httpServerInstance := &httpServerInstance{
		Addr:  fmt.Sprintf(":%d", addr),
		Queue: queue,
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

	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("error starting server: %w", err)
	}
	return nil
}

func router(httpServerInstance *httpServerInstance) *gin.Engine {
	r := gin.Default()
	r.GET("/api/v1/health", healthCheckHandler)
	r.POST("/api/v1/enqueue", httpServerInstance.enqueueHandler)
	r.POST("/api/v1/dequeue", httpServerInstance.dequeueHandler)
	r.POST("/api/v1/ack", httpServerInstance.ackHandler)
	r.POST("/api/v1/nack", httpServerInstance.nackHandler)
	r.GET("/api/v1/status", httpServerInstance.statusHandler)
	r.POST("/api/v1/peek", httpServerInstance.peekHandler)
	r.POST("/api/v1/renew", httpServerInstance.renewHandler)
	return r
}
