import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
public class ClienteCarga {
    public static void main(String[] args) {
        // Endereço e porta padrão do servidor (ajuste conforme necessário)
        String enderecoServidor = args[0];
        int porta = 5000;
        int numThreads = 100;
        int numRequisicoes = 100;

        System.out.println("Iniciando ClienteCarga para conexão com " + enderecoServidor + ":" + porta + "...");

        Socket socket = null;
        int tentativasMaximas = 15;
        int tentativa = 0;

        // Loop de tentativas para contornar o atraso de inicialização do servidor Java
        while (tentativa < tentativasMaximas) {
            try {
                tentativa++;
                socket = new Socket(enderecoServidor, porta);
                System.out.println("Conexão estabelecida com sucesso na tentativa " + tentativa + "!");
                break;
            } catch (Exception e) {
                System.out.println("Tentativa " + tentativa + "/" + tentativasMaximas + ": Servidor ainda não está pronto. Aguardando...");
                if (tentativa >= tentativasMaximas) {
                    System.err.println("Erro: Não foi possível conectar ao servidor após várias tentativas: " + e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                try {
                    Thread.sleep(1000); // Aguarda 1 segundo antes da próxima tentativa
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // Comunicação após estabelecer a conexão
        try (Socket s = socket;
             PrintWriter out = new PrintWriter(s.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
            
            String mensagemEnvio = String.join(" ", Arrays.copyOfRange(args, 1, args.length));
            out.println(mensagemEnvio);
            System.out.println("Mensagem enviada: " + mensagemEnvio);

            String resposta = in.readLine();
            System.out.println("Resposta recebida do servidor: " + resposta);

        } catch (Exception e) {
            System.err.println("Erro na comunicação com o servidor: " + e.getMessage());
            e.printStackTrace();
        }
    }
}