# 1. Docker 컨테이너 확인
docker-compose ps

# 2. Kafka 로그 확인
docker-compose logs kafka | grep "started"

# 3. Topic 확인
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# 4. 메시지 전송 테스트
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic task.complete
# 입력: {"trace_id":"test123","status":"DONE"}

# 5. 메시지 수신 테스트
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic task.complete --from-beginning
