package http

import (
	"go-msg-queue-mini/util"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// logging
func LoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		
		latency := time.Since(start)
		requestID := c.GetHeader("X-Request-ID")
		log.Printf(
			"[Request] Method: %s | Path: %s | Status: %d | Latency: %v | IP: %s | RequestID: %s",
			c.Request.Method,
			c.Request.URL.Path,
			c.Writer.Status(),
			latency,
			c.ClientIP(),
			requestID,
		)
	}
}

// error handling
func ErrorHandlingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		if len(c.Errors) > 0 {
			err := c.Errors.Last().Err
			log.Printf("[Error] %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}else {
			switch c.Writer.Status() {
			case http.StatusNotFound:
				c.JSON(http.StatusNotFound, gin.H{"error": "Resource not found"})
			case http.StatusNoContent:
				c.JSON(http.StatusNoContent, gin.H{"error": "No content"})
			}
		}
	}
}

// request-id
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}
		c.Set("request_id", requestID)
		c.Writer.Header().Set("X-Request-ID", requestID)
		c.Next()
	}
}

// authentication
func AuthMiddleware(apiKey string) gin.HandlerFunc {
	return func(c *gin.Context) {
		header_api_key := c.GetHeader("X-API-Key")
		util.Info("API Key from header: " + header_api_key)
		if header_api_key != apiKey || header_api_key == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			c.Abort()
			return
		}
		c.Next()
	}
}

// rate limit
func RateLimitMiddleware(clients map[string]*clientLimiter, limit int) gin.HandlerFunc {
	return func(c *gin.Context) {
		clientIP := c.ClientIP()
		clientMapLock.Lock()
		client_limiter, exists := clients[clientIP]
		if !exists {
			client_limiter = &clientLimiter{
				limiter: rate.NewLimiter(rate.Limit(limit), limit),
				lastSeen: time.Now(),
			}
			clients[clientIP] = client_limiter
		} else {
			client_limiter.lastSeen = time.Now()
		}
		clientMapLock.Unlock()

		if !client_limiter.limiter.Allow() {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "Too many requests"})
			c.Abort()
			return
		}
		c.Next()
	}
}