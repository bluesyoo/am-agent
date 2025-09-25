FROM eclipse-temurin:21-jdk

ARG APP=am-agent
ARG PROFILE

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Seoul
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Dfile.encoding=UTF-8"

RUN apt-get update && \
    apt-get install -y tzdata

RUN echo 'alias ll="ls -al"' >> ~/.bashrc && \
    echo 'alias debug="tail -fn500 /volume/'${PROFILE}'/logs/'${APP}'.log"' >> ~/.bashrc

WORKDIR /application

COPY target/${APP}.jar app.jar
COPY target/classes/application.yml .
COPY target/classes/log4j2-prod.xml .
COPY target/classes/log4j2-stage.xml .

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -Dspring.config.location=/application/application.yml -jar /application/app.jar"]
