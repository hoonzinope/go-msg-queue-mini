package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func (h *httpServerInstance) uiHandler(c *gin.Context) {
	indexpage := "index.html"
	f, err := h.uiFS.Open(indexpage) // h.uiFS = http.FS(sub) with sub = fs.Sub(embedded, "static")
	if err != nil {
		c.String(http.StatusInternalServerError, "internal server error")
		return
	}
	defer f.Close()
	c.Header("Content-Type", "text/html; charset=utf-8")
	http.ServeContent(c.Writer, c.Request, indexpage, time.Time{}, f)
}

func (h *httpServerInstance) uiQueueDetailHandler(c *gin.Context) {
	detailPage := "queueInfo/detail.html"
	f, err := h.uiFS.Open(detailPage) // h.uiFS = http.FS(sub) with sub = fs.Sub(embedded, "static")
	if err != nil {
		c.String(http.StatusInternalServerError, "internal server error")
		return
	}
	defer f.Close()
	c.Header("Content-Type", "text/html; charset=utf-8")
	http.ServeContent(c.Writer, c.Request, detailPage, time.Time{}, f)
}
