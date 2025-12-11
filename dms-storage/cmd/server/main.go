package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"

	"go.opentelemetry.io/contrib/bridges/otelslog" // ★ 핵심: Log Bridge 추가
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

type TaskRequest struct {
	DocTitle string `json:"doc_title"`
	Action   string `json:"action"`
}

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// 1. Trace 초기화
	shutdownTracer := initTracer()
	defer shutdownTracer(ctx)

	// 2. [NEW] Log 초기화 (로그를 4317로 쏘는 설정)
	shutdownLogger := initLogger()
	defer shutdownLogger(ctx)

	// 3. [NEW] slog를 OTel과 연결
	// 이제부터 찍는 로그는 Loki로 날아갑니다.
	logger := otelslog.NewLogger("dms-storage-worker-logger")
	slog.SetDefault(logger)

	app := fiber.New(fiber.Config{
		AppName: "DMS Storage Worker",
	})

	// ============================================
	// 수동 미들웨어: Gin이 보낸 TraceID 받기
	// ============================================
	app.Use(func(c *fiber.Ctx) error {
		// 1. HTTP 헤더에서 Trace Context 추출
		carrier := &FiberHeaderCarrier{c: c}
		extractedCtx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)

		// 2. 새로운 Span 시작 (Gin의 TraceID를 이어받음)
		spanCtx, span := tracer.Start(extractedCtx, c.Path(),
			trace.WithAttributes(
				attribute.String("http.method", c.Method()),
				attribute.String("http.url", c.OriginalURL()),
			),
		)

		// 3. Context를 Fiber에 저장 (중요: 이 spanCtx에 TraceID가 있음)
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
		// 미들웨어에서 넣어준 Context (Trace info 포함)
		ctx := c.UserContext()

		// TraceID 추출 (디버깅용 - 실제 전송은 Context가 알아서 함)
		span := trace.SpanFromContext(ctx)
		traceID := span.SpanContext().TraceID().String()

		req := new(TaskRequest)

		// [수정] 로그는 파싱 '후'에 찍어야 내용이 보입니다.
		if err := c.BodyParser(req); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "파싱 실패")
			// 에러 로그도 Context와 함께 전송
			slog.ErrorContext(ctx, "Body Parsing Failed", "error", err)
			return c.Status(400).JSON(fiber.Map{"error": "파싱 실패"})
		}

		// [NEW] OTel Log 전송
		// InfoContext를 쓰면 TraceID가 자동으로 붙어서 날아갑니다.
		slog.InfoContext(ctx, "Fiber 작업 시작",
			"action", req.Action,
			"doc_title", req.DocTitle,
			"manual_trace_id", traceID, // 확인용으로 명시적 추가도 가능
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

		go func(traceID string) {
			payload := map[string]string{
				"trace_id": traceID,
				"status":   "DONE",
				"message":  "Redis로 알림 보냄",
			}
			jsonBody, _ := json.Marshal(payload)

			// "task.complete" 채널에 발행(Publish)
			err := rdb.Publish(context.Background(), "task.complete", jsonBody).Err()

			if err != nil {
				slog.Error("Redis 발행 실패", "error", err)
			} else {
				fmt.Printf("📢 [Worker] Redis 이벤트 발행 완료 (TraceID: %s)\n", traceID)
			}
		}(traceID)
		//go func(targetURL, traceID string) {
		//	payload := map[string]string{
		//		"trace_id": traceID,
		//		"status":   "DONE",
		//		"message":  "PDF 아카이빙 성공",
		//	}
		//	jsonBody, _ := json.Marshal(payload)
		//
		//	// Gateway로 POST 요청 발사!
		//	// (실제 운영에선 재시도(Retry) 로직이 필요하지만 지금은 심플하게)
		//	resp, err := http.Post("http://localhost:8080/callbacks/task-complete", "application/json", bytes.NewBuffer(jsonBody))
		//	if err != nil {
		//		fmt.Printf("❌ 콜백 실패: %v\n", err)
		//		return
		//	}
		//	defer resp.Body.Close()
		//	fmt.Printf("📞 [Worker] Gateway로 콜백 완료\n")
		//}("http://localhost:8080/callbacks/task-complete", traceID)

		return c.Status(http.StatusOK).JSON(fiber.Map{
			"status": "PROCESSED",
		})
	})

	fmt.Println("⚡ Fiber Storage Worker running on :8081")
	app.Listen(":8081")
}

// ... FiberHeaderCarrier 구조체 등은 그대로 유지 ...
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

// ============================================
// Trace 초기화
// ============================================
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
			semconv.ServiceNameKey.String("dms-storage-worker"), // 서비스명 확인
		),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp.Shutdown
}

// ============================================
// [NEW] Log 초기화 (Fiber용)
// ============================================
func initLogger() func(context.Context) error {
	ctx := context.Background()

	exporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithInsecure(),
		otlploggrpc.WithEndpoint("localhost:4317"),
	)
	if err != nil {
		log.Fatalf("Log Init Error: %v", err)
	}

	// [수정] 서비스 이름을 Worker로 명확히 지정
	res, _ := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("dms-storage-worker"),
		),
	)

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		sdklog.WithResource(res),
	)

	// 전역 로거 프로바이더 등록
	global.SetLoggerProvider(lp)

	return lp.Shutdown
}
