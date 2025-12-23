package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Loki 전송을 위한 구조체 (Loki API 규격)
type LokiPushRequest struct {
	Streams []LokiStream `json:"streams"`
}

type LokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

type ErrorEvent struct {
	ServiceName string
	TraceID     string
	SpanID      string
	Operation   string
	Message     string
	Details     map[string]string
}

var errorChan = make(chan ErrorEvent, 1000)

// Loki 주소 설정 (Docker로 띄웠을 경우 보통 3100 포트)
const lokiURL = "http://localhost:3100/loki/api/v1/push"

func main() {
	app := fiber.New()

	go errorWorker()

	app.Post("/v1/traces", func(c *fiber.Ctx) error {
		payload := c.Body()
		var req collectortrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(payload, &req); err != nil {
			return c.Status(400).SendString("Invalid OTLP format")
		}

		go func(data collectortrace.ExportTraceServiceRequest) {
			for _, resSpans := range data.ResourceSpans {
				serviceName := "unknown-service"
				for _, attr := range resSpans.Resource.Attributes {
					if attr.Key == "service.name" {
						serviceName = attr.Value.GetStringValue()
						break
					}
				}
				for _, scopeSpans := range resSpans.ScopeSpans {
					for _, span := range scopeSpans.Spans {
						if span.Status != nil && span.Status.Code == v1.Status_STATUS_CODE_ERROR {
							event := ErrorEvent{
								ServiceName: serviceName,
								TraceID:     fmt.Sprintf("%x", span.TraceId),
								SpanID:      fmt.Sprintf("%x", span.SpanId),
								Operation:   span.Name,
								Message:     span.Status.Message,
								Details:     make(map[string]string),
							}
							for _, ev := range span.Events {
								if ev.Name == "exception" {
									for _, attr := range ev.Attributes {
										event.Details[attr.Key] = attr.Value.GetStringValue()
									}
								}
							}
							errorChan <- event
						}
					}
				}
			}
		}(req)

		return c.SendStatus(200)
	})

	log.Fatal(app.Listen(":4318"))
}

func errorWorker() {
	fmt.Println("🚀 Loki 전송 워커 대기 중...")
	for event := range errorChan {
		// 1. Loki에 보낼 로그 내용 구성
		logLine := fmt.Sprintf("[OP:%s] [MSG:%s] [TRACE:%s] \nSTK:%s",
			event.Operation, event.Message, event.TraceID, event.Details["exception.stacktrace"])

		// 2. Loki로 전송
		sendToLoki(event.ServiceName, logLine)

		fmt.Printf("📦 [%s] Loki 전송 완료: %s\n", event.ServiceName, event.TraceID)
	}
}

// Loki API 호출 함수
func sendToLoki(serviceName, logLine string) {
	now := time.Now().UnixNano()

	// Loki API 데이터 구조 형성
	pushReq := LokiPushRequest{
		Streams: []LokiStream{
			{
				Stream: map[string]string{
					"service_name": serviceName, // Grafana에서 필터링할 라벨
					"level":        "error",
					"source":       "go-otlp-server",
				},
				Values: [][]string{
					{fmt.Sprintf("%d", now), logLine},
				},
			},
		},
	}

	jsonData, _ := json.Marshal(pushReq)

	resp, err := http.Post(lokiURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("❌ Loki 전송 실패: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		log.Printf("❌ Loki 응답 에러: %d", resp.StatusCode)
	}
}
