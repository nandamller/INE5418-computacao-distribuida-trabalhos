import java.io.*;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Biblioteca Cliente em JAVA para o Encurtador de URLs Distribuído.
 * O programador só precisa instanciar esta classe e chamar os métodos.
 */
public class EncurtadorClient {
    private String host = "127.0.0.1";
    private int port = 9000;

    public EncurtadorClient(String configPath) {
        loadConfig(configPath);
    }

    private void loadConfig(String filepath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    if (parts[0].trim().equals("PROXY_HOST")) {
                        this.host = parts[1].trim();
                    } else if (parts[0].trim().equals("PROXY_PORT")) {
                        this.port = Integer.parseInt(parts[1].trim());
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[Aviso] Não foi possível ler propriedades de " + filepath + ". Usando padrao " + host + ":" + port);
        }
    }

    private String sendRequest(String jsonPayload) throws Exception {
        // TCP Sockets API (Berkeley-based)
        try (Socket socket = new Socket(host, port);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))) {
             
            out.write(jsonPayload + "\n");
            out.flush();
            
            return in.readLine();
        }
    }
    
    private String extractJsonValue(String json, String key) {
        String patternStr = "\"" + key + "\"\\s*:\\s*(?:\"([^\"]+)\"|([^,}]+))";
        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(json);
        if (matcher.find()) {
            return matcher.group(1) != null ? matcher.group(1) : matcher.group(2).trim();
        }
        return null;
    }

    /**
     * Encurta uma URL original consultando primeiro o Proxy.
     */
    public String encurta(String urlOriginal) throws Exception {
        // Escapa aspas para JSON
        String payload = String.format("{\"acao\": \"encurta\", \"url\": \"%s\"}", urlOriginal.replace("\"", "\\\""));
        String response = sendRequest(payload);
        
        if (response != null && (response.contains("\"status\":\"ok\"") || response.contains("\"status\": \"ok\""))) {
            return extractJsonValue(response, "codigo");
        }
        throw new Exception("Erro ao encurtar pelo TCP Socket. Resposta: " + response);
    }

    /**
     * Resolve um código curto devolvendo a URL original (Lembrando que bate no Cache-Aside do Proxy).
     */
    public String resolve(String codigoCurto) throws Exception {
        String payload = String.format("{\"acao\": \"resolve\", \"codigo\": \"%s\"}", codigoCurto);
        String response = sendRequest(payload);
        
        if (response != null && (response.contains("\"status\":\"ok\"") || response.contains("\"status\": \"ok\""))) {
            return extractJsonValue(response, "url_original");
        }
        throw new Exception("Erro ao tentar achar a url original (" + codigoCurto + "). Resposta: " + response);
    }

    /**
     * Remove o código da API e invalida o cache remotamente via Socket.
     */
    public boolean removeUrl(String codigoCurto) throws Exception {
        String payload = String.format("{\"acao\": \"remove\", \"codigo\": \"%s\"}", codigoCurto);
        String response = sendRequest(payload);
        
        return response != null && (response.contains("\"status\":\"ok\"") || response.contains("\"status\": \"ok\""));
    }
}
