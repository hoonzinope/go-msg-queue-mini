package grpc

import (
	context "context"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	status "google.golang.org/grpc/status"
)

const (
	apiKeyHeader = "x-api-key"
)

// logging
func LoggerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()

	resp, err := handler(ctx, req)

	duration := time.Since(start)
	log.Printf("gRPC call to %s took %v", info.FullMethod, duration)

	return resp, err
}

// error handling
func ErrorInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		log.Printf("gRPC call to %s failed: %v", info.FullMethod, err)
	}
	return resp, err
}

// authentication
func AuthInterceptor(apikey string, protectedMethods map[string]bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !protectedMethods[info.FullMethod] {
			return handler(ctx, req)
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			log.Printf("gRPC call to %s missing metadata", info.FullMethod)
			return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
		}
		key := md.Get(apiKeyHeader)
		if len(key) == 0 || key[0] != apikey {
			log.Printf("gRPC call to %s unauthorized", info.FullMethod)
			return nil, status.Errorf(codes.Unauthenticated, "invalid API key")
		}
		return handler(ctx, req)
	}
}

// 에러 변환 인터셉터 (panic → INTERNAL)
func RecoveryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[PANIC] %v", r)
			err = status.Error(codes.Internal, "internal server error")
		}
	}()
	return handler(ctx, req)
}
