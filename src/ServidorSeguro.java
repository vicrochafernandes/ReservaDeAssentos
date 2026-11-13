import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Servidor de reserva de ingressos MULTITHREAD CORRETO.
 *
 * Duas decisões de projeto:
 *
 * 1) CORREÇÃO: a reserva usa ConcurrentHashMap.putIfAbsent(), que faz
 *    "verificar se está livre + gravar" em UMA operação atômica. Se duas
 *    threads disputam o mesmo assento, exatamente uma recebe null (ganhou)
 *    e a outra recebe o dono atual (perdeu). Não há overbooking.
 *
 * 2) THREAD POOLS (dois níveis):
 *    [POOL 1] poolConexoes: um pool cacheado que atende cada conexão
 *             (lê as linhas do cliente). Reaproveita threads ociosas em vez
 *             de criar e destruir uma Thread por conexão, como o ServidorNaive.
 *    [POOL 2] poolWorkers: um pool de TAMANHO FIXO com fila LIMITADA que
 *             executa o processamento de cada comando. É ele quem limita
 *             quantas reservas rodam ao mesmo tempo e dá BACKPRESSURE: se a
 *             fila enche, o servidor responde ERRO SERVIDOR_OCUPADO em vez
 *             de esgotar memória e CPU.
 *
 * Protocolo (igual às outras versões):
 *   RESERVAR show1 A10 [cliente]  ->  OK | ERRO ASSENTO_OCUPADO | ERRO ASSENTO_INEXISTENTE
 *   CONSULTAR show1               ->  OK <qtd_livres> <assento> <assento> ...
 *   ESTATISTICAS                  ->  OK confirmadas=<n> recusadas=<n> ocupados=<n> overbooking=<n> rejeitadas=<n>
 *   SAIR                          ->  (encerra a conexão)
 *
 * Uso: java ServidorSeguro [porta=5000] [assentos=100] [workers=8] [fila=10000]
 * Variável de ambiente opcional: ATRASO_MS (simula o tempo de processamento
 * de cada reserva; use o mesmo valor nas três versões).
 */
public class ServidorSeguro {

    private static final String SHOW = "show1";
    private static final int TIMEOUT_CLIENTE_MS = 30_000;
    private static final int TIMEOUT_TAREFA_S = 10;

    private final int porta;
    private final long atrasoMs;
    private final Set<String> assentos = new LinkedHashSet<>(); // somente leitura após o construtor
    private final Map<String, String> reservas = new ConcurrentHashMap<>(); // assento -> cliente

    private final AtomicLong confirmadas = new AtomicLong();
    private final AtomicLong recusadas = new AtomicLong();
    private final AtomicLong rejeitadas = new AtomicLong(); // recusadas pelo pool (fila cheia)

    private final ExecutorService poolConexoes; // [POOL 1]
    private final ThreadPoolExecutor poolWorkers; // [POOL 2]
    private volatile ServerSocket server;

    public ServidorSeguro(int porta, int totalAssentos, int workers, int tamanhoFila, long atrasoMs) {
        this.porta = porta;
        this.atrasoMs = atrasoMs;
        for (int i = 1; i <= totalAssentos; i++) {
            assentos.add("A" + i);
        }

        // [POOL 1] Pool cacheado: cria threads sob demanda e as reaproveita.
        this.poolConexoes = Executors.newCachedThreadPool(fabrica("conexao"));

        // [POOL 2] Pool fixo + fila limitada (ArrayBlockingQueue).
        // core = max = workers  -> exatamente N threads processam comandos.
        // Fila cheia            -> AbortPolicy lança RejectedExecutionException.
        this.poolWorkers = new ThreadPoolExecutor(
                workers, workers,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(tamanhoFila),
                fabrica("worker"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public static void main(String[] args) throws IOException {
        int porta = args.length > 0 ? Integer.parseInt(args[0]) : 5000;
        int assentos = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        int workers = args.length > 2 ? Integer.parseInt(args[2]) : 8;
        int fila = args.length > 3 ? Integer.parseInt(args[3]) : 10_000;
        long atraso = Long.parseLong(System.getenv().getOrDefault("ATRASO_MS", "0"));

        new ServidorSeguro(porta, assentos, workers, fila, atraso).iniciar();
    }

    public void iniciar() throws IOException {
        server = new ServerSocket(porta);
        Runtime.getRuntime().addShutdownHook(new Thread(this::encerrar, "shutdown"));

        System.out.printf("[SEGURO] Porta %d | %d assentos | %d workers | fila %d | atraso %d ms%n",
                porta, assentos.size(), poolWorkers.getCorePoolSize(),
                poolWorkers.getQueue().remainingCapacity(), atrasoMs);

        try {
            while (true) {
                Socket cliente = server.accept();
                try {
                    // [POOL 1] em vez de "new Thread(...).start()"
                    poolConexoes.execute(() -> atender(cliente));
                } catch (RejectedExecutionException e) {
                    cliente.close(); // servidor encerrando
                }
            }
        } catch (SocketException e) {
            // server.close() no shutdown hook interrompe o accept(): saída normal
        }
    }

    /** Encerramento limpo: para de aceitar, deixa as tarefas em andamento terminarem. */
    private void encerrar() {
        System.out.println("[SEGURO] Encerrando...");
        try {
            server.close();
        } catch (IOException ignored) {
        }
        poolConexoes.shutdown();
        poolWorkers.shutdown();
        try {
            if (!poolWorkers.awaitTermination(5, TimeUnit.SECONDS)) {
                poolWorkers.shutdownNow();
            }
            if (!poolConexoes.awaitTermination(5, TimeUnit.SECONDS)) {
                poolConexoes.shutdownNow();
            }
        } catch (InterruptedException e) {
            poolWorkers.shutdownNow();
            poolConexoes.shutdownNow();
        }
    }

    // ---------------------------------------------------------------
    // Atendimento de uma conexão (roda em uma thread do [POOL 1])
    // ---------------------------------------------------------------
    private void atender(Socket s) {
        String id = s.getRemoteSocketAddress().toString();
        try (s;
             var in = new BufferedReader(new InputStreamReader(s.getInputStream()));
             var out = new PrintWriter(s.getOutputStream(), true)) {

            s.setSoTimeout(TIMEOUT_CLIENTE_MS);

            String linha;
            while ((linha = in.readLine()) != null) {
                if (linha.trim().equalsIgnoreCase("SAIR")) {
                    break;
                }
                out.println(executarNoPool(linha, id));
            }
        } catch (IOException e) {
            System.err.println("[SEGURO] Cliente " + id + " encerrou com erro: " + e.getMessage());
        }
    }

    /**
     * [POOL 2] Entrega o comando a um worker e espera o resultado (Future).
     * A thread da conexão só lê/escreve no socket; o trabalho de verdade
     * é feito pelos workers, cujo número é limitado.
     */
    private String executarNoPool(String linha, String clienteId) {
        final String cmd = linha;
        try {
            Future<String> futuro = poolWorkers.submit(() -> processar(cmd, clienteId));
            return futuro.get(TIMEOUT_TAREFA_S, TimeUnit.SECONDS);
        } catch (RejectedExecutionException e) {
            rejeitadas.incrementAndGet(); // BACKPRESSURE: fila cheia
            return "ERRO SERVIDOR_OCUPADO";
        } catch (TimeoutException e) {
            return "ERRO TIMEOUT";
        } catch (ExecutionException e) {
            return "ERRO INTERNO";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "ERRO INTERROMPIDO";
        }
    }

    // ---------------------------------------------------------------
    // Lógica dos comandos (roda em uma thread do [POOL 2])
    // ---------------------------------------------------------------
    private String processar(String linha, String clienteId) {
        String[] p = linha.trim().split("\\s+");
        if (p.length == 0 || p[0].isEmpty()) {
            return "ERRO COMANDO_VAZIO";
        }

        switch (p[0].toUpperCase()) {
            case "RESERVAR":
                if (p.length < 3) {
                    return "ERRO USO: RESERVAR <show> <assento> [cliente]";
                }
                return reservar(p[1], p[2], p.length > 3 ? p[3] : clienteId);
            case "CONSULTAR":
                if (p.length < 2) {
                    return "ERRO USO: CONSULTAR <show>";
                }
                return consultar(p[1]);
            case "ESTATISTICAS":
                return estatisticas();
            default:
                return "ERRO COMANDO_INVALIDO";
        }
    }

    /**
     * CORREÇÃO: putIfAbsent é atômico no ConcurrentHashMap.
     *   - retorna null            -> o assento estava livre e agora é deste cliente
     *   - retorna o dono atual    -> alguém já tinha reservado; nada é alterado
     * Não existe janela entre "verificar" e "gravar".
     */
    private String reservar(String show, String assento, String cliente) {
        if (!SHOW.equalsIgnoreCase(show)) {
            return "ERRO SHOW_INEXISTENTE";
        }
        if (!assentos.contains(assento)) {
            return "ERRO ASSENTO_INEXISTENTE";
        }

        simularProcessamento();

        if (reservas.putIfAbsent(assento, cliente) == null) {
            confirmadas.incrementAndGet();
            return "OK";
        }
        recusadas.incrementAndGet();
        return "ERRO ASSENTO_OCUPADO";
    }

    private String consultar(String show) {
        if (!SHOW.equalsIgnoreCase(show)) {
            return "ERRO SHOW_INEXISTENTE";
        }
        StringBuilder livres = new StringBuilder();
        int qtd = 0;
        for (String a : assentos) {
            if (!reservas.containsKey(a)) {
                livres.append(' ').append(a);
                qtd++;
            }
        }
        return "OK " + qtd + livres;
    }

    /** Aqui overbooking = confirmadas - ocupados deve ser SEMPRE 0. */
    private String estatisticas() {
        long conf = confirmadas.get();
        int ocupados = reservas.size();
        return "OK confirmadas=" + conf
                + " recusadas=" + recusadas.get()
                + " ocupados=" + ocupados
                + " overbooking=" + Math.max(0, conf - ocupados)
                + " rejeitadas=" + rejeitadas.get();
    }

    private void simularProcessamento() {
        if (atrasoMs > 0) {
            try {
                Thread.sleep(atrasoMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Dá nomes às threads do pool (conexao-1, worker-3...), útil em logs e jstack. */
    private static ThreadFactory fabrica(String prefixo) {
        AtomicInteger n = new AtomicInteger(1);
        return r -> new Thread(r, prefixo + "-" + n.getAndIncrement());
    }
}