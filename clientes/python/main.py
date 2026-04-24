from encurtador_client import EncurtadorClient

def main():
    print("=== Testando Cliente Python ===")
    cliente = EncurtadorClient()

    try:
        # 1. Encurtando
        url_teste = "https://www.google.com.br"
        print(f"[*] Encurtando a URL: {url_teste}")
        codigo = cliente.encurta(url_teste)
        print(f"[+] Sucesso! Código gerado: {codigo}")
        
        # 2. Resolvendo
        print(f"\n[*] Resolvendo código: {codigo}")
        url_original = cliente.resolve(codigo)
        print(f"[+] Sucesso! URL original: {url_original}")
        
        # 3. Removendo
        print(f"\n[*] Removendo código: {codigo}")
        removido = cliente.remove_url(codigo)
        print(f"[+] Sucesso! Foi removido? {removido}")
        
        # 4. Tentando resolver novamente (deve falhar e cair no exception)
        print(f"\n[*] Resolvendo código deletado: {codigo}")
        try:
            cliente.resolve(codigo)
        except Exception as e:
            print(f"[+] Erro esperado ao buscar URL morta: {e}")
            
    except Exception as erro:
        print(f"[-] Ocorreu um erro nas operações distribuídas: {erro}")

if __name__ == "__main__":
    main()
