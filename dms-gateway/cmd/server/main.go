package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/sdk/resource"
	"log"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"

	sdklog "go.opentelemetry.io/otel/sdk/log"
)

const storageServiceURL = "http://localhost:8081/internal/process"

// 전역 Tracer 생성 (이름은 프로젝트명)
var tracer = otel.Tracer("dms-gateway")

type Job struct {
	Ctx    context.Context // Trace 정보가 여기 다 들어있음!
	Title  string
	Author string
}

var jobQueue = make(chan Job, 100000)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // 도커 컴포즈 서비스명 쓰려면 "redis:6379"
	})

	go func() {
		// "task.complete" 라는 채널을 구독
		pubsub := rdb.Subscribe(ctx, "task.complete")
		defer pubsub.Close()

		// 채널 획득
		ch := pubsub.Channel()

		fmt.Println("🎧 [Gateway] Redis 구독 시작: task.complete 채널 대기 중...")

		// 메시지가 올 때까지 여기서 대기 (무한 루프)
		for msg := range ch {
			var result struct {
				TraceID string `json:"trace_id"`
				Status  string `json:"status"`
				Message string `json:"message"`
			}
			json.Unmarshal([]byte(msg.Payload), &result)

			// 로그 찍기 (Loki로 전송)
			slog.InfoContext(ctx, "📢 [Redis] 작업 완료 수신!",
				"original_trace_id", result.TraceID,
				"status", result.Status,
				"msg_source", "redis-pubsub",
			)
		}
	}()

	// 1. Trace 초기화
	shutdown := initTracer()
	defer shutdown(context.Background())

	// 2. Log 초기화 (OTLP 연결 설정)
	shutdownLogger := initLogger()
	defer shutdownLogger(ctx)

	// =================================================================
	// [수정된 부분] 로거 설정
	// 기존: os.Stdout (화면 출력) -> Loki로 안 감 ❌
	// 변경: otelslog (OTel 전송) -> Loki로 날아감 ✅
	// =================================================================
	logger := otelslog.NewLogger("dms-gateway-logger")
	slog.SetDefault(logger)

	for i := 0; i < 50; i++ {
		go worker(i)
	}

	r := gin.Default()
	r.Use(otelgin.Middleware("dms-gateway-server"))

	r.POST("/documents", func(c *gin.Context) {
		ctx := c.Request.Context()

		var req struct {
			Title  string `json:"title"`
			Author string `json:"author"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "잘못된 요청입니다."})
			return
		}

		span := trace.SpanFromContext(ctx)
		traceID := span.SpanContext().TraceID().String()
		fmt.Printf("[Gin] 매니저: 요청 접수 (TraceID: %s)\n", traceID)

		// =================================================================
		// [수정된 부분] 로그 찍기
		// Info -> InfoContext 로 변경해야 ctx 안에 있는 TraceID가 자동으로 같이 전송됩니다.
		// =================================================================
		slog.InfoContext(ctx, "요청 접수 완료",
			"doc_title", req.Title,
			"author", req.Author,
			// "trace_id"는 InfoContext가 자동으로 넣어주므로 굳이 수동으로 안 넣어도 됨
		)

		job := Job{
			Ctx:    ctx,
			Title:  req.Title,
			Author: req.Author,
		}

		select {
		case jobQueue <- job:
			c.JSON(http.StatusAccepted, gin.H{
				"status":   "QUEUED",
				"message":  "요청이 접수되었습니다.",
				"trace_id": traceID,
			})
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "서버 혼잡"})
		}
	})

	r.POST("/callbacks/task-complete", func(c *gin.Context) {
		var result struct {
			TraceID string `json:"trace_id"`
			Status  string `json:"status"`
			Message string `json:"message"`
		}
		if err := c.ShouldBindJSON(&result); err != nil {
			return
		}

		// 로그 찍기 (여기서 DB 업데이트나 클라이언트에게 웹소켓 알림을 보냄)
		// context를 새로 만들지 않고, 들어온 요청의 context를 사용해 로그를 남김
		slog.InfoContext(c.Request.Context(), "📨 [Gateway] Worker로부터 완료 보고 수신!",
			"original_trace_id", result.TraceID,
			"status", result.Status,
		)

		c.JSON(200, gin.H{"ack": "ok"})
	})

	fmt.Println("Gin gateway running on :8080")
	r.Run(":8080")
}

func worker(id int) {
	client := &http.Client{Timeout: 10 * time.Second}

	for job := range jobQueue {
		childCtx, span := tracer.Start(job.Ctx, "async_worker_process",
			trace.WithAttributes(attribute.Int("worker_id", id)),
		)

		traceID := span.SpanContext().TraceID().String()
		fmt.Printf("[일꾼 %d] 작업 시작 TraceID: %s\n", id, traceID)

		payload, _ := json.Marshal(map[string]string{
			"doc_title": job.Title,
			"action":    "ARCHIVE_FAST",
		})

		req, _ := http.NewRequest("POST", storageServiceURL, bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")

		otel.GetTextMapPropagator().Inject(childCtx, propagation.HeaderCarrier(req.Header))

		resp, err := client.Do(req)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "HTTP call failed")

			// [추가] 에러 로그도 Loki로 보내기
			slog.ErrorContext(childCtx, "Worker HTTP 요청 실패", "worker_id", id, "error", err)

			span.End()
			continue
		}
		resp.Body.Close()

		fmt.Printf("[일꾼 %d] 처리 완료\n", id)

		// [추가] 완료 로그도 Loki로 보내기
		slog.InfoContext(childCtx, "Worker 처리 완료", "worker_id", id)

		span.End()
	}
}

// ==========================================================
// 아래는 그냥 복사해서 쓰세요 (초기화 보일러플레이트 코드)
// ==========================================================

func initTracer() func(context.Context) error {
	ctx := context.Background()

	// 1. OTLP Exporter 설정 (Docker로 띄운 OTel Collector 주소)
	// insecure 옵션은 로컬 테스트용입니다.
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint("localhost:4317"),
	)
	if err != nil {
		log.Fatalf("OTLP Exporter 생성 실패: %v", err)
	}

	// 2. Resource 설정 (서비스 이름 등)
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("dms-gateway-service"),
		),
	)
	if err != nil {
		log.Fatalf("Resource 생성 실패: %v", err)
	}

	// 3. Tracer Provider 설정
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	// 전역 Provider로 등록
	otel.SetTracerProvider(tp)

	// 4. Propagator 설정 (서버 간 헤더 전파 방식)
	// W3C Trace Context 표준을 사용합니다.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown
}

func initLogger() func(context.Context) error {
	ctx := context.Background()

	// 1. Log Exporter 생성 (Trace랑 똑같이 4317로 쏩니다)
	exporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithInsecure(),
		otlploggrpc.WithEndpoint("localhost:4317"),
	)
	if err != nil {
		log.Fatalf("Log Exporter 에러: %v", err)
	}

	// 2. Resource 정의 (서비스 이름 - 이게 Loki의 service 라벨이 됩니다)
	res, _ := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("dms-gateway"),
		),
	)

	// 3. Logger Provider 생성
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		sdklog.WithResource(res),
	)

	// 4. 전역 설정 등록
	global.SetLoggerProvider(lp)

	return lp.Shutdown
}
