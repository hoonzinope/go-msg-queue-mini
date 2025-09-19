package util

import (
	"log/slog"
	"os"
	"time"
)

func InitLogger(loggerType string) *slog.Logger {
	opts := &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				if t, ok := a.Value.Any().(time.Time); ok {
					a.Value = slog.StringValue(t.Format("2006-01-02 15:04:05"))
				}
			}
			return a
		},
	}

	var handler slog.Handler
	if loggerType == "text" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}
	Logger := slog.New(handler)
	return Logger.With(slog.String("app", "go-msg-queue-mini"))
}
