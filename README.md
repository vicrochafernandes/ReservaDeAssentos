# Subir os servidores
docker compose up -d --build servidor-single servidor-naive servidor-seguro

# Comandos disponiveis
- CONSULTAR
- RESERVAR
- ESTATISTICAS

# Rodar a carga contra a versão segura
docker compose run --rm cliente servidor-seguro 5000 100 1000

# Rodar contra a versão com overbooking
docker compose run --rm cliente servidor-naive 5000 100 1000

# Rodar contra a single-thread
docker compose run --rm cliente servidor-single 5000 100 1000

# Ver logs e derrubar tudo
docker compose logs -f servidor-naive
docker compose down

# Teste rápido do servidor usando single thread sem dockerização
```
javac -d out src/ServidorSingle.java
java -cp out ServidorSingle 5000 100
```

em outro terminal

```nc localhost 5000
RESERVAR show1 A10
RESERVAR show1 A10      # -> ERRO ASSENTO_OCUPADO
CONSULTAR show1
ESTATISTICAS
SAIR```

# Explicando o funcionamento do Servidor Seguro

```
Cliente ─► thread da conexão [POOL 1] ─► submit() ─► fila limitada ─► worker [POOL 2] ─► Future ─► resposta
```
*[POOL 1] ```poolConexoes```*
Reaproveita threads ociosas em vez de criar uma Thread por cliente, como no ServidorNaive

*[POOL 2] ```poolWorkers```*
Executa o processamento de cada comando. Limita quantas reservas rodam ao mesmo tempo.