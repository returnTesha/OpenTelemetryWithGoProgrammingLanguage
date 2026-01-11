##[작업 요약: 중앙 집중형 에러 관측 시스템(Observability System) 구축]

**1. Application 단계 (Spring Boot)**

표준화된 데이터 송출: 특정 벤더에 종속되지 않는 OpenTelemetry(OTLP) 표준 프로토콜을 채택했습니다. **SpringBoot3.3 version
지능형 필터링: GlobalExceptionHandler와 Micrometer Tracing을 결합하여, 불필요한 로그는 배제하고 비즈니스 에러 및 예외 상황(Exception) 발생 시에만 즉시 트레이스 데이터를 송출하도록 구현했습니다.

**2. 수집 및 가공 단계 (LCS[Log Collection System] - Go 기반 커스텀 서버)**
고성능 수집기(LCS) 개발: Go 언어의 고루틴(Goroutine)과 채널(Channel)을 활용하여, 다수의 마이크로서비스로부터 들어오는 에러 데이터를 비동기적으로 병목 없이 수집하는 시스템을 구축했습니다.
데이터 가공 및 라우팅: 수신된 바이너리(Protobuf) 데이터를 파싱하여 서비스명, TraceID, 스택트레이스 등을 추출하고, 이를 로그 저장소인 Loki 규격에 맞게 실시간으로 변환 및 전송합니다.

**3. 저장 및 시각화 단계 (Loki & Grafana)**
효율적 로그 저장: 로그 데이터의 특징(시계열)에 최적화된 Grafana Loki를 구축하여 대용량의 에러 로그를 압축 저장하고 빠른 검색 속도를 확보했습니다.
통합 모니터링 대시보드: Grafana를 통해 여러 서비스의 에러 현황을 한눈에 파악하고, 특정 TraceID를 기반으로 에러의 시작부터 끝까지 추적할 수 있는 가시성을 확보했습니다.




<img width="600" height="1300" alt="스크린샷 2026-01-12 08 39 27" src="https://github.com/user-attachments/assets/b8b813f9-edd2-42fc-a74b-f001dc48a0ed" />

##<체크포인트>
**1. Grafana에 안보인다.**
   봐야할 파이프라인이 많다. 바로  WAS로그보는게 나을지도...

**2. 평소 개발할때 기능 on > 현재환경에서 무리없이 되는것 확인**
 > 추후에 on/off기능이 있어야함

**3. 로그 보관기관 확인 : 최대 7일**

**4. 노티는 아직 달지 않음**
