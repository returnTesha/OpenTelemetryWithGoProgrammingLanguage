package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/segmentio/kafka-go"

	"go.opentelemetry.io/contrib/bridges/otelslog"
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

var tracer = otel.Tracer("dms-storage-worker")

// ============================================
// [NEW] Kafka Writer (Producer) 전역 변수
// Redis Client 대신 Kafka Writer 사용
// ============================================
var kafkaWriter *kafka.Writer

type TaskRequest struct {
	DocTitle string `json:"doc_title"`
	Action   string `json:"action"`
}

func main() {
	ctx := context.Background()

	// ============================================
	// [변경] Redis → Kafka Writer 초기화
	// ============================================
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"), // Kafka 브로커 주소
		Topic:    "task.complete",             // Topic 이름 (Redis의 채널과 동일한 개념)
		Balancer: &kafka.LeastBytes{},         // 파티션 분배 전략: 가장 적게 쓴 파티션으로

		// 성능 튜닝 옵션
		BatchSize:    100,                   // 100개씩 모아서 한 번에 전송 (효율성 UP)
		BatchTimeout: 10 * time.Millisecond, // 100개 안 모여도 10ms 지나면 전송
		Compression:  kafka.Snappy,          // 압축 (네트워크 트래픽 감소)

		// 전달 보장 레벨 설정
		RequiredAcks: kafka.RequireOne, // 1개 브로커만 확인하면 OK (빠름)
		// RequiredAcks: kafka.RequireAll,     // 모든 복제본 확인 (느리지만 안전)

		// 에러 처리
		MaxAttempts: 3, // 전송 실패 시 3번까지 재시도
	}
	defer kafkaWriter.Close() // 프로그램 종료 시 정리

	// Trace 초기화
	shutdownTracer := initTracer()
	defer shutdownTracer(ctx)

	// Log 초기화
	shutdownLogger := initLogger()
	defer shutdownLogger(ctx)

	logger := otelslog.NewLogger("dms-storage-worker-logger")
	slog.SetDefault(logger)

	app := fiber.New(fiber.Config{
		AppName: "DMS Storage Worker",
	})

	// ============================================
	// 미들웨어: Gin이 보낸 TraceID 받기
	// ============================================
	app.Use(func(c *fiber.Ctx) error {
		carrier := &FiberHeaderCarrier{c: c}
		extractedCtx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)

		spanCtx, span := tracer.Start(extractedCtx, c.Path(),
			trace.WithAttributes(
				attribute.String("http.method", c.Method()),
				attribute.String("http.url", c.OriginalURL()),
			),
		)

		c.SetUserContext(spanCtx)
		err := c.Next()

		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "success")
		}
		span.SetAttributes(attribute.Int("http.status_code", c.Response().StatusCode()))
		span.End()

		return err
	})

	app.Post("/internal/process", func(c *fiber.Ctx) error {
		ctx := c.UserContext()
		span := trace.SpanFromContext(ctx)
		traceID := span.SpanContext().TraceID().String()

		req := new(TaskRequest)

		if err := c.BodyParser(req); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "파싱 실패")
			slog.ErrorContext(ctx, "Body Parsing Failed", "error", err)
			return c.Status(400).JSON(fiber.Map{"error": "파싱 실패"})
		}

		slog.InfoContext(ctx, "Fiber 작업 시작",
			"action", req.Action,
			"doc_title", req.DocTitle,
			"manual_trace_id", traceID,
		)

		fmt.Printf("⚡ [Fiber] 작업 처리 중... TraceID: %s\n", traceID)

		// 세부 작업 추적 Span
		_, processSpan := tracer.Start(ctx, "process_document",
			trace.WithAttributes(
				attribute.String("doc_title", req.DocTitle),
				attribute.String("action", req.Action),
			),
		)

		time.Sleep(100 * time.Millisecond) // 작업 시늉
		processSpan.End()

		fmt.Printf("✅ [Fiber] 완료!\n")

		// ============================================
		// [변경] Redis Publish → Kafka 메시지 발행
		// ============================================
		go publishToKafka(ctx, traceID)

		return c.Status(http.StatusOK).JSON(fiber.Map{
			"status": "PROCESSED",
		})
	})

	fmt.Println("⚡ Fiber Storage Worker running on :8081")
	app.Listen(":8081")
}

// ============================================
// [NEW] Kafka로 메시지 발행하는 함수
// 기존: rdb.Publish()
// 변경: kafkaWriter.WriteMessages()
// ============================================
func publishToKafka(ctx context.Context, traceID string) {
	// 메시지 구조체 생성 (Redis와 동일한 양식)
	event := map[string]string{
		"trace_id": traceID,
		"status":   "DONE",
		"message":  "Kafka로 알림 보냄",
	}

	jsonBody, err := json.Marshal(event)
	if err != nil {
		slog.Error("JSON 마샬링 실패", "error", err)
		return
	}

	// Kafka 메시지 생성
	msg := kafka.Message{
		// Key: 같은 Key는 같은 파티션으로 가서 순서 보장됨
		// traceID를 Key로 쓰면 같은 작업은 순서대로 처리됨
		Key: []byte(traceID),

		// Value: 실제 메시지 내용 (JSON)
		Value: jsonBody,

		// Time: 메시지 생성 시간 (Kafka가 자동 기록도 하지만 명시 가능)
		Time: time.Now(),

		// Headers: 추가 메타데이터 (선택사항)
		// 예: OpenTelemetry TraceID를 헤더에도 넣을 수 있음
		Headers: []kafka.Header{
			{Key: "trace-id", Value: []byte(traceID)},
		},
	}

	// Kafka로 메시지 전송
	// WriteMessages는 동기 방식 (전송 완료까지 대기)
	err = kafkaWriter.WriteMessages(ctx, msg)

	if err != nil {
		// 전송 실패 시 (네트워크 문제, Kafka 다운 등)
		slog.ErrorContext(ctx, "Kafka 전송 실패",
			"error", err,
			"trace_id", traceID,
		)
		return
	}

	// 성공 로그
	fmt.Printf("📤 [Kafka] 메시지 발행 완료 (TraceID: %s)\n", traceID)
	slog.InfoContext(ctx, "Kafka 발행 성공",
		"trace_id", traceID,
		"topic", "task.complete",
	)
}

// FiberHeaderCarrier 구조체 (변경 없음)
type FiberHeaderCarrier struct {
	c *fiber.Ctx
}

func (f *FiberHeaderCarrier) Get(key string) string { return f.c.Get(key) }
func (f *FiberHeaderCarrier) Set(key, value string) { f.c.Set(key, value) }
func (f *FiberHeaderCarrier) Keys() []string {
	keys := make([]string, 0)
	f.c.Request().Header.VisitAll(func(key, value []byte) {
		keys = append(keys, string(key))
	})
	return keys
}

// Tracer 초기화 (변경 없음)
func initTracer() func(context.Context) error {
	ctx := context.Background()
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint("localhost:4317"),
	)
	if err != nil {
		log.Fatalf("Trace Init Error: %v", err)
	}

	res, _ := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("dms-storage-worker"),
		),
	)

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
		log.Fatalf("Log Init Error: %v", err)
	}

	res, _ := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("dms-storage-worker"),
		),
	)

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		sdklog.WithResource(res),
	)

	global.SetLoggerProvider(lp)

	return lp.Shutdown
}
