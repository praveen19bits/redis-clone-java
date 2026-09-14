# --- Build stage ---
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY src ./src
RUN find src -name "*.java" | xargs javac -d out

# --- Runtime stage ---
# Using the full JDK (not JRE) here -- deliberately. jcmd, jstat, and jstack
# are JDK-only diagnostic tools; a JRE-only image can't run them via `docker exec`.
# This makes the image bigger, which would matter for a real production
# deployment, but for a learning/testing container, tool availability wins.
FROM eclipse-temurin:21-jdk
WORKDIR /app
COPY --from=build /app/out ./out

EXPOSE 6380

# exec form: any args passed after the image name in `docker run` are appended here,
# e.g. `docker run redis-clone-java --mode=epoll --port=6380`
# -XX:NativeMemoryTracking=summary unlocks `jcmd 1 VM.native_memory summary`
# (small, acceptable overhead for testing -- normally opt-in only when needed).
ENTRYPOINT ["java", "-XX:NativeMemoryTracking=summary", "-cp", "out", "com.praveen.redisclone.RedisServer"]
