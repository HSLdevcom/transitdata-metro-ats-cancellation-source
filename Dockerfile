FROM eclipse-temurin:11-alpine

RUN apk add --no-cache curl

ADD target/transitdata-metro-ats-cancellation-source-jar-with-dependencies.jar /usr/app/transitdata-metro-ats-cancellation-source.jar
COPY start-application.sh /
RUN chmod +x /start-application.sh

CMD ["/start-application.sh"]
