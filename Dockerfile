# ---------- Estágio 1: compilação ----------
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY src/ ./src/
RUN mkdir -p out && javac -d out $(find src -name "*.java")

# ---------- Estágio 2: execução ----------
FROM eclipse-temurin:21-jre
WORKDIR /app

# Usuário sem privilégios
RUN useradd -r -u 1001 app

COPY --from=build /app/out ./classes
USER app

EXPOSE 5000

# Qual classe executar é escolhido em tempo de execução
ENV MAIN_CLASS=ServidorSeguro
ENV JAVA_OPTS="-Xms128m -Xmx256m"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -cp classes $MAIN_CLASS \"$@\"", "--"]