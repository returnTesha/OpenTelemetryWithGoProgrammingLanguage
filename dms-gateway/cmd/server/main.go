package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

const storageServiceURL = "http://localhost:8081/internal/process"

var tracer = otel.Tracer("dms-gateway")

type Job struct {
	Ctx    context.Context
	Title  string
	Author string
}

var jobQueue = make(chan Job, 100000)

func main() {
	ctx := context.Background()

	// ============================================
	// [변경] Redis Subscribe → Kafka Consumer
	// ============================================
	// Kafka Reader (Consumer) 초기화
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"}, // Kafka 브로커 주소 (여러 개 가능)
		Topic:   "task.complete",            // 구독할 Topic
		GroupID: "gateway-service-group",    // Consumer Group ID (중요!)

		// Consumer Group 설명:
		// - 같은 GroupID를 가진 Consumer들은 파티션을 나눠서 처리
		// - 예: 파티션 3개, Consumer 2개 → Consumer A(파티션 0,1), B(파티션 2)
		// - 다른 GroupID는 독립적으로 모든 메시지 받음

		// 읽기 설정
		MinBytes: 1,                      // 최소 1바이트만 있어도 가져옴
		MaxBytes: 10e6,                   // 한 번에 최대 10MB까지 읽기
		MaxWait:  500 * time.Millisecond, // 메시지 없으면 500ms 대기

		// 오프셋 커밋 설정 (중요!)
		// 오프셋 = "어디까지 읽었는지" 기록
		CommitInterval: time.Second, // 1초마다 자동으로 오프셋 커밋

		// 시작 위치 설정
		StartOffset: kafka.LastOffset, // 최신 메시지부터 읽기 (기본값)
		// StartOffset: kafka.FirstOffset,    // 처음부터 읽기 (재처리할 때)

		// 에러 처리
		MaxAttempts: 3, // 읽기 실패 시 3번 재시도
	})
	defer kafkaReader.Close()

	// ============================================
	// [NEW] Kafka Consumer 고루틴
	// 기존: Redis Subscribe goroutine
	// 변경: Kafka ReadMessage loop
	// ============================================
	go func() {
		fmt.Println("🎧 [Gateway] Kafka Consumer 시작: task.complete 구독 중...")

		for {
			// Kafka에서 메시지 읽기 (블로킹 방식)
			// 메시지가 올 때까지 여기서 대기
			msg, err := kafkaReader.ReadMessage(context.Background())

			if err != nil {
				// 읽기 실패 (네트워크 문제, Kafka 다운 등)
				slog.Error("Kafka 읽기 실패", "error", err)
				time.Sleep(time.Second) // 1초 대기 후 재시도
				continue
			}

			// ============================================
			// 메시지 처리 (Redis와 동일한 구조)
			// ============================================
			var result struct {
				TraceID string `json:"trace_id"`
				Status  string `json:"status"`
				Message string `json:"message"`
			}

			// JSON 파싱
			if err := json.Unmarshal(msg.Value, &result); err != nil {
				slog.Error("JSON 파싱 실패",
					"error", err,
					"raw_message", string(msg.Value),
				)
				continue
			}

			// 로그 찍기 (Loki로 전송)
			slog.Info("📥 [Kafka] 작업 완료 수신!",
				"original_trace_id", result.TraceID,
				"status", result.Status,
				"msg_source", "kafka",
				"partition", msg.Partition, // 어느 파티션에서 왔는지
				"offset", msg.Offset, // 오프셋 (순서 번호)
				"key", string(msg.Key), // Key 값
			)

			// ============================================
			// [참고] Kafka는 자동 커밋됨 (CommitInterval 설정에 따라)
			// Redis는 Subscribe만 하면 끝이지만
			// Kafka는 "어디까지 읽었는지" 기록해야 재시작 시 이어서 처리 가능
			// ============================================

			// 여기서 DB 업데이트, 웹소켓 알림 등 비즈니스 로직 처리
			// ...
		}
	}()

	// Trace 초기화
	shutdown := initTracer()
	defer shutdown(context.Background())

	// Log 초기화
	shutdownLogger := initLogger()
	defer shutdownLogger(ctx)

	logger := otelslog.NewLogger("dms-gateway-logger")
	slog.SetDefault(logger)

	// Worker Pool 시작 (변경 없음)
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

		slog.InfoContext(ctx, "요청 접수 완료",
			"doc_title", req.Title,
			"author", req.Author,
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

	// ============================================
	// [참고] HTTP 콜백 엔드포인트는 그대로 유지 가능
	// Kafka + HTTP 콜백 둘 다 사용해도 됨
	// ============================================
	r.POST("/callbacks/task-complete", func(c *gin.Context) {
		var result struct {
			TraceID string `json:"trace_id"`
			Status  string `json:"status"`
			Message string `json:"message"`
		}
		if err := c.ShouldBindJSON(&result); err != nil {
			return
		}

		slog.InfoContext(c.Request.Context(), "📨 [Gateway] Worker로부터 완료 보고 수신!",
			"original_trace_id", result.TraceID,
			"status", result.Status,
		)

		c.JSON(200, gin.H{"ack": "ok"})
	})

	fmt.Println("Gin gateway running on :8080")
	r.Run(":8080")
}

// Worker 함수 (변경 없음)
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
			slog.ErrorContext(childCtx, "Worker HTTP 요청 실패", "worker_id", id, "error", err)
			span.End()
			continue
		}
		resp.Body.Close()

		fmt.Printf("[일꾼 %d] 처리 완료\n", id)
		slog.InfoContext(childCtx, "Worker 처리 완료", "worker_id", id)

		span.End()
	}
}

// Tracer 초기화 (변경 없음)
func initTracer() func(context.Context) error {
	ctx := context.Background()

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint("localhost:4317"),
	)
	if err != nil {
		log.Fatalf("OTLP Exporter 생성 실패: %v", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("dms-gateway-service"),
		),
	)
	if err != nil {
		log.Fatalf("Resource 생성 실패: %v", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown
}

// Logger 초기화 (변경 없음)
func initLogger() func(context.Context) error {
	ctx := context.Background()

	exporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithInsecure(),
		otlploggrpc.WithEndpoint("localhost:4317"),
	)
	if err != nil {
		log.Fatalf("Log Exporter 에러: %v", err)
	}

	res, _ := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("dms-gateway"),
		),
	)

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		sdklog.WithResource(res),
	)

	global.SetLoggerProvider(lp)

	return lp.Shutdown
}
