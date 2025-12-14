# Kafka 컨테이너 접속
docker exec -it kafka bash

# Topic 목록 보기
kafka-topics --bootstrap-server localhost:9092 --list

# task.complete Topic 상세 정보
kafka-topics --bootstrap-server localhost:9092 --describe --topic task.complete
```

### 3️⃣ Kafka UI 접속 (선택사항)
```
http://localhost:8089
```

**여기서 확인 가능:**
- Topic 목록
- 메시지 실시간 확인
- Consumer Group 상태
- Partition 분배 상태

---

## 🎯 전체 아키텍처
```
[Docker Compose]
├─ lgtm:3000 (Grafana)
│  └─ :4317 (OTLP Collector)
├─ redis:6379
├─ zookeeper:2181
├─ kafka:9092 (외부), :9093 (내부)
└─ kafka-ui:8089

[호스트]
├─ Gin (:8080)
│  └─ Consumer (kafka:9092)
└─ Fiber (:8081)
└─ Producer (kafka:9092)
