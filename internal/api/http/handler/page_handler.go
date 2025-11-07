package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type PageHandler struct {
	UIFS http.FileSystem
}

func (h *PageHandler) UiHandler(c *gin.Context) {
	indexpage := "index.html"
	f, err := h.UIFS.Open(indexpage) // h.UIFS = http.FS(sub) with sub = fs.Sub(embedded, "static")
	if err != nil {
		c.String(http.StatusInternalServerError, "internal server error")
		return
	}
	defer f.Close()
	c.Header("Content-Type", "text/html; charset=utf-8")
	http.ServeContent(c.Writer, c.Request, indexpage, time.Time{}, f)
}

func (h *PageHandler) UiQueueDetailHandler(c *gin.Context) {
	detailPage := "queueInfo/detail.html"
	f, err := h.UIFS.Open(detailPage) // h.UIFS = http.FS(sub) with sub = fs.Sub(embedded, "static")
	if err != nil {
		c.String(http.StatusInternalServerError, "internal server error")
		return
	}
	defer f.Close()
	c.Header("Content-Type", "text/html; charset=utf-8")
	http.ServeContent(c.Writer, c.Request, detailPage, time.Time{}, f)
}
