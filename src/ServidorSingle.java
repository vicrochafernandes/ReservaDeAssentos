import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Servidor de reserva de ingressos SINGLE-THREAD.
 *
 * Um único loop aceita e atende UM cliente por vez: enquanto um cliente
 * está conectado, todos os outros ficam esperando na fila do accept().
 * Como só existe uma thread, não há condição de corrida: o estado pode ser
 * um HashMap comum, sem locks nem estruturas thread-safe.
 *
 * Protocolo (texto, uma linha por mensagem):
 *   RESERVAR show1 A10 [cliente]  ->  OK | ERRO ASSENTO_OCUPADO | ERRO ASSENTO_INEXISTENTE
 *   CONSULTAR show1               ->  OK <qtd_livres> <assento> <assento> ...
 *   ESTATISTICAS                  ->  OK confirmadas=<n> recusadas=<n>
 *   SAIR                          ->  (encerra a conexão)
 *
 * Uso: java ServidorSingle [porta=5000] [assentos=100]
 * Variável de ambiente opcional: ATRASO_MS (simula tempo de processamento
 * de cada reserva, útil para comparar com as versões multithread).
 */
public class ServidorSingle {

    private static final String SHOW = "show1";
    private static final int TIMEOUT_CLIENTE_MS = 30_000;

    private final int porta;
    private final long atrasoMs;
    private final Set<String> assentos = new LinkedHashSet<>();
    private final Map<String, String> reservas = new HashMap<>(); // assento -> cliente

    private long confirmadas = 0;
    private long recusadas = 0;

    public ServidorSingle(int porta, int totalAssentos, long atrasoMs) {
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

        new ServidorSingle(porta, assentos, atraso).iniciar();
    }

    public void iniciar() throws IOException {
        try (ServerSocket server = new ServerSocket(porta)) {
            System.out.printf("[SINGLE] Porta %d | %d assentos | atraso %d ms%n",
                    porta, assentos.size(), atrasoMs);

            while (true) {
                Socket cliente = server.accept();
                atender(cliente); // bloqueia até este cliente terminar
            }
        }
    }

    private void atender(Socket s) {
        String id = s.getRemoteSocketAddress().toString();
        try (s;
             var in = new BufferedReader(new InputStreamReader(s.getInputStream()));
             var out = new PrintWriter(s.getOutputStream(), true)) {

            s.setSoTimeout(TIMEOUT_CLIENTE_MS); // cliente mudo não trava o servidor para sempre

            String linha;
            while ((linha = in.readLine()) != null) {
                if (linha.trim().equalsIgnoreCase("SAIR")) {
                    break;
                }
                out.println(processar(linha, id));
            }
        } catch (IOException e) {
            System.err.println("[SINGLE] Cliente " + id + " encerrou com erro: " + e.getMessage());
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
                return "OK confirmadas=" + confirmadas + " recusadas=" + recusadas;
            default:
                return "ERRO COMANDO_INVALIDO";
        }
    }

    private String reservar(String show, String assento, String cliente) {
        if (!SHOW.equalsIgnoreCase(show)) {
            return "ERRO SHOW_INEXISTENTE";
        }
        if (!assentos.contains(assento)) {
            return "ERRO ASSENTO_INEXISTENTE";
        }

        simularProcessamento();

        // check-then-act: seguro aqui porque só há UMA thread
        if (reservas.containsKey(assento)) {
            recusadas++;
            return "ERRO ASSENTO_OCUPADO";
        }
        reservas.put(assento, cliente);
        confirmadas++;
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