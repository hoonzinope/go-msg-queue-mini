package http

import (
	"crypto/subtle"
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
		} else {
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
		if subtle.ConstantTimeCompare([]byte(header_api_key), []byte(apiKey)) != 1 {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			c.Abort()
			return
		}
		c.Next()
	}
}

// rate limit
func RateLimitMiddleware(limiter RateLimiter, limit int) gin.HandlerFunc {
	return func(c *gin.Context) {
		clientIP := c.ClientIP()
		limiter.lock.Lock()
		client_limiter, exists := limiter.clients[clientIP]
		if !exists {
			limit := limiter.limit
			burst := limiter.burst
			client_limiter = &ClientLimiter{
				limiter:  rate.NewLimiter(rate.Limit(limit), burst),
				lastSeen: time.Now(),
			}
			limiter.clients[clientIP] = client_limiter
		} else {
			client_limiter.lastSeen = time.Now()
		}
		limiter.lock.Unlock()

		if !client_limiter.limiter.Allow() {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "Too many requests"})
			c.Abort()
			return
		}
		c.Next()
	}
}

func queueNameMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		queue_name := c.Param("queue_name")
		if queue_name == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Queue name is required"})
			c.Abort()
			return
		}
		c.Set("queue_name", queue_name)
		c.Next()
	}
}
