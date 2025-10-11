package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func (h *httpServerInstance) uiHandler(c *gin.Context) {
	f, err := h.uiFS.Open("index.html") // h.uiFS = http.FS(sub) with sub = fs.Sub(embedded, "static")
	if err != nil {
		c.String(http.StatusInternalServerError, "index.html not found: %v", err)
		return
	}
	defer f.Close()
	c.Header("Content-Type", "text/html; charset=utf-8")
	http.ServeContent(c.Writer, c.Request, "index.html", time.Time{}, f)
}
