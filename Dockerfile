FROM gradle:jdk17 as build
# Checks https://github.com/gradle/gradle/issues/18508
ENV GRADLE_OPTS "-Xmx512m --add-opens=java.prefs/java.util.prefs=ALL-UNNAMED"
COPY --chown=gradle:gradle build.gradle.kts /repository/
COPY --chown=gradle:gradle settings.gradle.kts /repository/
COPY --chown=gradle:gradle gradle.properties /repository/
COPY --chown=gradle:gradle src/ /repository/src/
WORKDIR /repository
RUN gradle build installDist --console=plain --no-daemon --no-watch-fs --stacktrace

FROM amazoncorretto:17-alpine
WORKDIR /home/root/
COPY --from=build /repository/build/install/ ./

EXPOSE 50051
ENTRYPOINT ["/home/root/grpc-kotlin-test/bin/grpc-kotlin-test"]
