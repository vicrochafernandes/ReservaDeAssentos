import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Servidor de reserva de ingressos MULTITHREAD INGÊNUO (com bug proposital).
 *
 * Cada cliente é atendido por uma thread própria, então vários clientes são
 * atendidos ao mesmo tempo. O problema está em reservar(): a verificação
 * ("o assento está livre?") e a gravação ("reservar") são dois passos
 * separados, sem exclusão mútua. Duas threads podem passar pela verificação
 * juntas e as duas confirmam o mesmo assento: OVERBOOKING.
 *
 * Ponto didático: o mapa é um ConcurrentHashMap, ou seja, a ESTRUTURA é
 * thread-safe, mas a SEQUÊNCIA containsKey + put não é atômica. Usar uma
 * coleção thread-safe não torna correta uma operação composta.
 *
 * Os contadores usam AtomicLong de propósito: eles são só instrumentação.
 * Se fossem long comuns, também perderiam atualizações e atrapalhariam a
 * medição do overbooking.
 *
 * Protocolo (igual ao ServidorSingle):
 *   RESERVAR show1 A10 [cliente]  ->  OK | ERRO ASSENTO_OCUPADO | ERRO ASSENTO_INEXISTENTE
 *   CONSULTAR show1               ->  OK <qtd_livres> <assento> <assento> ...
 *   ESTATISTICAS                  ->  OK confirmadas=<n> recusadas=<n> ocupados=<n> overbooking=<n>
 *   SAIR                          ->  (encerra a conexão)
 *
 * Uso: java ServidorNaive [porta=5000] [assentos=100]
 * Variável de ambiente opcional: ATRASO_MS. Aqui o atraso é colocado ENTRE a
 * verificação e a gravação, o que alarga a janela da condição de corrida e
 * torna o overbooking reproduzível.
 */
public class ServidorNaive {

    private static final String SHOW = "show1";
    private static final int TIMEOUT_CLIENTE_MS = 30_000;

    private final int porta;
    private final long atrasoMs;
    private final Set<String> assentos = new LinkedHashSet<>(); // somente leitura após o construtor
    private final Map<String, String> reservas = new ConcurrentHashMap<>(); // assento -> cliente

    private final AtomicLong confirmadas = new AtomicLong();
    private final AtomicLong recusadas = new AtomicLong();

    public ServidorNaive(int porta, int totalAssentos, long atrasoMs) {
        this.porta = porta;
        this.atrasoMs = atrasoMs;
        for (int i = 1; i <= totalAssentos; i++) {
            assentos.add("A" + i);
        }
    }

    public static void main(String[] args) throws IOException {
        int porta = args.length > 0 ? Integer.parseInt(args[0]) : 5000;
        int assentos = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        long atraso = Long.parseLong(System.getenv().getOrDefault("ATRASO_MS", "0"));

        new ServidorNaive(porta, assentos, atraso).iniciar();
    }

    public void iniciar() throws IOException {
        try (ServerSocket server = new ServerSocket(porta)) {
            System.out.printf("[NAIVE] Porta %d | %d assentos | atraso %d ms (SEM sincronização)%n",
                    porta, assentos.size(), atrasoMs);

            while (true) {
                Socket cliente = server.accept();
                // uma thread por conexão: o accept() volta a aceitar imediatamente
                new Thread(() -> atender(cliente), "cliente-" + cliente.getPort()).start();
            }
        }
    }

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
                out.println(processar(linha, id));
            }
        } catch (IOException e) {
            System.err.println("[NAIVE] Cliente " + id + " encerrou com erro: " + e.getMessage());
        }
    }

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
     * BUG PROPOSITAL: check-then-act sem sincronização.
     *
     *   Thread A: containsKey(A10) -> false
     *   Thread B: containsKey(A10) -> false      (A ainda não gravou)
     *   Thread A: put(A10, clienteA) ; confirma OK
     *   Thread B: put(A10, clienteB) ; confirma OK   <- mesmo assento vendido 2x
     */
    private String reservar(String show, String assento, String cliente) {
        if (!SHOW.equalsIgnoreCase(show)) {
            return "ERRO SHOW_INEXISTENTE";
        }
        if (!assentos.contains(assento)) {
            return "ERRO ASSENTO_INEXISTENTE";
        }

        // 1) CHECK
        if (reservas.containsKey(assento)) {
            recusadas.incrementAndGet();
            return "ERRO ASSENTO_OCUPADO";
        }

        // Janela da condição de corrida: outra thread pode reservar aqui
        simularProcessamento();

        // 2) ACT (a verificação acima pode já estar desatualizada)
        reservas.put(assento, cliente);
        confirmadas.incrementAndGet();
        return "OK";
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

    /**
     * overbooking = reservas confirmadas ao cliente - assentos realmente ocupados.
     * Em um sistema correto, esse valor é sempre 0.
     */
    private String estatisticas() {
        long conf = confirmadas.get();
        int ocupados = reservas.size();
        return "OK confirmadas=" + conf
                + " recusadas=" + recusadas.get()
                + " ocupados=" + ocupados
                + " overbooking=" + Math.max(0, conf - ocupados);
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
}