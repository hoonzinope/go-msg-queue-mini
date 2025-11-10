package http

import (
	"go-msg-queue-mini/internal/api/http/handler"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func Router(HttpServerInstance *httpServerInstance) *gin.Engine {
	r := gin.New()
	r.RedirectTrailingSlash = false
	r.RedirectFixedPath = false
	r.Use(gin.Recovery())
	r.Use(LoggerMiddleware())
	r.Use(ErrorHandlingMiddleware())
	r.Use(RequestIDMiddleware())
	r.Use(RateLimitMiddleware(HttpServerInstance.limiter))

	HttpServerInstance.uiHandler(r)       // UI routes
	HttpServerInstance.queueManipulate(r) // create, delete, health
	HttpServerInstance.produce(r)         // enqueue, enqueue batch
	HttpServerInstance.consume(r)         // dequeue, ack, nack, renew
	HttpServerInstance.inspector(r)       // status, statusAll, peek, detail
	HttpServerInstance.dlqRoutes(r)       // dlq inspection and redrive
	return r
}

func (HttpServerInstance *httpServerInstance) uiHandler(r *gin.Engine) {
	pageHandler := &handler.PageHandler{
		UIFS: HttpServerInstance.uiFS,
	}

	page := r.Group("/")
	{
		page.StaticFS("/static", HttpServerInstance.uiFS)
		page.GET("/", pageHandler.UiHandler)
		page.GET("/queues/:queue_name", pageHandler.UiQueueDetailHandler)
	}
}

func (HttpServerInstance *httpServerInstance) queueManipulate(r *gin.Engine) {
	queueHandler := &handler.QueueHandler{
		Queue:  HttpServerInstance.Queue,
		Logger: HttpServerInstance.Logger,
	}

	r.GET("/health", queueHandler.HealthCheckHandler)
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	queueWriter := r.Group("/api/v1/:queue_name")
	queueWriter.Use(AuthMiddleware(HttpServerInstance.ApiKey))
	queueWriter.Use(queueNameMiddleware())
	{
		queueWriter.POST("/create", queueHandler.CreateQueueHandler)
		queueWriter.DELETE("/delete", queueHandler.DeleteQueueHandler)
	}
}

func (HttpServerInstance *httpServerInstance) produce(r *gin.Engine) {
	producerHandler := &handler.ProducerHandler{
		Queue:  HttpServerInstance.Queue,
		Logger: HttpServerInstance.Logger,
	}
	producer := r.Group("/api/v1/:queue_name")
	producer.Use(AuthMiddleware(HttpServerInstance.ApiKey))
	producer.Use(queueNameMiddleware())
	{
		producer.POST("/enqueue", producerHandler.EnqueueHandler)
		producer.POST("/enqueue/batch", producerHandler.EnqueueBatchHandler)
	}
}

func (HttpServerInstance *httpServerInstance) consume(r *gin.Engine) {
	consumerHandler := &handler.ConsumerHandler{
		Queue:  HttpServerInstance.Queue,
		Logger: HttpServerInstance.Logger,
	}
	consumer := r.Group("/api/v1/:queue_name")
	consumer.Use(AuthMiddleware(HttpServerInstance.ApiKey))
	consumer.Use(queueNameMiddleware())
	{
		consumer.POST("/dequeue", consumerHandler.DequeueHandler)
		consumer.POST("/ack", consumerHandler.AckHandler)
		consumer.POST("/nack", consumerHandler.NackHandler)
		consumer.POST("/renew", consumerHandler.RenewHandler)
	}
}

func (HttpServerInstance *httpServerInstance) dlqRoutes(r *gin.Engine) {
	dlqHandler := &handler.DLQHandler{
		DLQManager:     HttpServerInstance.DLQManager,
		QueueInspector: HttpServerInstance.QueueInspector,
		Logger:         HttpServerInstance.Logger,
	}
	dlq_manager := r.Group("/api/v1/:queue_name")
	dlq_manager.Use(AuthMiddleware(HttpServerInstance.ApiKey))
	dlq_manager.Use(queueNameMiddleware())
	{
		dlq_manager.GET("/dlq", dlqHandler.ListDLQMessagesHandler)
		dlq_manager.GET("/dlq/:message_id", dlqHandler.DetailDLQMessageHandler)
		dlq_manager.POST("/dlq/redrive", dlqHandler.RedriveDLQMessagesHandler)
		dlq_manager.POST("/dlq/delete", dlqHandler.DeleteDLQMessagesHandler)
	}
}

func (HttpServerInstance *httpServerInstance) inspector(r *gin.Engine) {
	inspectHandler := &handler.InspectHandler{
		QueueInspector: HttpServerInstance.QueueInspector,
		Logger:         HttpServerInstance.Logger,
	}

	reader_no_queue_name := r.Group("/api/v1")
	{
		reader_no_queue_name.GET("/status/all", inspectHandler.StatusAllHandler)
		reader_no_queue_name.GET("/queues/:queue_name/messages/:message_id", inspectHandler.DetailHandler)
	}

	reader := r.Group("/api/v1")
	reader.Use(queueNameMiddleware())
	{
		reader.GET("/:queue_name/status", inspectHandler.StatusHandler)
		reader.POST("/:queue_name/peek", inspectHandler.PeekHandler)
	}
}
