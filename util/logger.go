package util

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

type CustomJsonHandler struct {
	writer *os.File
}

func (h *CustomJsonHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (h *CustomJsonHandler) Handle(ctx context.Context, r slog.Record) error {
	logMap := map[string]interface{}{
		"time":    r.Time.Format("2006-01-02 15:04:05"),
		"level":   r.Level.String(),
		"message": r.Message,
	}
	r.Attrs(func(a slog.Attr) bool {
		logMap[a.Key] = a.Value.Any()
		return true
	})
	jsonBytes, err := json.Marshal(logMap)
	if err != nil {
		return err
	}
	_, err = h.writer.Write(append(jsonBytes, '\n'))
	return err
}

func (h *CustomJsonHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *CustomJsonHandler) WithGroup(name string) slog.Handler {
	return h
}

type CustomTextHandler struct {
	writer *os.File
}

func (h *CustomTextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (h *CustomTextHandler) Handle(ctx context.Context, r slog.Record) error {
	parts := []string{
		"time=" + r.Time.Format("2006-01-02 15:04:05"),
		"level=" + r.Level.String(),
		"msg=\"" + r.Message + "\"",
	}
	r.Attrs(func(a slog.Attr) bool {
		parts = append(parts, fmt.Sprintf("%s=%v", a.Key, a.Value.Any()))
		return true
	})
	line := strings.Join(parts, " ")
	_, err := fmt.Fprintln(h.writer, line)
	return err
}

func (h *CustomTextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *CustomTextHandler) WithGroup(name string) slog.Handler {
	return h
}

func InitLogger(loggerType string) *slog.Logger {
	if loggerType == "text" {
		handler := &CustomTextHandler{writer: os.Stdout}
		Logger := slog.New(handler)
		Logger.With(slog.String("app", "go-msg-queue-mini"))
		return Logger
	}
	// default to json
	handler := &CustomJsonHandler{writer: os.Stdout}
	Logger := slog.New(handler)
	Logger.With(slog.String("app", "go-msg-queue-mini"))
	return Logger
}
