# --- 1단계: 빌드 환경 (Build Stage) ---
# Java 21 JDK 이미지를 사용하여 프로젝트 빌드
FROM eclipse-temurin:21-jdk-jammy AS builder
WORKDIR /app

# Maven 프로젝트 파일을 복사하여 의존성을 캐싱
COPY pom.xml .
COPY ad-common ad-common/
COPY am-kafka-domain am-kafka-domain/
COPY am-kafka-api am-kafka-api/

# 모든 프로젝트를 빌드 (Maven Wrapper 사용)
COPY . .
RUN ./mvnw clean package -DskipTests

# --- 2단계: 실행 환경 (Run Stage) ---
# Java 21 JRE 이미지를 사용하여 가벼운 실행 환경 구성
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# 빌드 단계에서 생성된 JAR 파일을 복사
# ad-dispatcher.jar는 예시이며, 실제 아티팩트명으로 변경해야 합니다.
COPY --from=builder /app/ad-dispatcher/target/ad-dispatcher-1.0.0-SNAPSHOT.jar ad-dispatcher.jar

# 애플리케이션 실행을 위한 포트 노출
EXPOSE 8080

# 애플리케이션 시작 명령어
ENTRYPOINT ["java", "-jar", "ad-dispatcher.jar"]
