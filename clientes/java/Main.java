public class Main {
    public static void main(String[] args) {
        System.out.println("=== Testando Cliente JAVA ===");
        
        // Passa o caminho do config.txt (se existir)
        EncurtadorClient cliente = new EncurtadorClient("../config.txt");
        
        try {
            // 1. Encurtando
            String urlTeste = "https://www.youtube.com";
            System.out.println("[*] Encurtando a URL: " + urlTeste);
            String codigo = cliente.encurta(urlTeste);
            System.out.println("[+] Sucesso! Código gerado: " + codigo);
            
            // 2. Resolvendo
            System.out.println("\n[*] Resolvendo código: " + codigo);
            String urlOriginal = cliente.resolve(codigo);
            System.out.println("[+] Sucesso! URL original: " + urlOriginal);
            
            // 3. Removendo
            System.out.println("\n[*] Removendo código: " + codigo);
            boolean removido = cliente.removeUrl(codigo);
            System.out.println("[+] Sucesso! Foi removido? " + removido);
            
            // 4. Tentando resolver novamente (deve falhar)
            System.out.println("\n[*] Resolvendo código deletado: " + codigo);
            try {
                cliente.resolve(codigo);
            } catch (Exception e) {
                System.out.println("[+] Erro perfeitamente esperado ao buscar URL morta: " + e.getMessage());
            }
            
        } catch (Exception e) {
            System.err.println("[-] Ocorreu um erro geral nas chamadas distribuídas: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
