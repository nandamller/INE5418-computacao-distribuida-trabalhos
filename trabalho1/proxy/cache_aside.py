from collections import OrderedDict

class LRUCache:
    def __init__(self, capacity: int):
        """
        Inicializa o cache.
        """
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key: str):
        """
        Busca uma URL no cache (Cache Hit).
        Se encontrada, move para o final para marcar como 'recentemente usada'.
        """
        if key not in self.cache:
            return None
        
        # Padrão LRU: move para o final (most recent) ao ser acessada
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key: str, value: str):
        """
        Adiciona ou atualiza uma URL no cache.
        Se exceder a capacidade, remove a entrada menos utilizada (LRU).
        """
        if key in self.cache:
            self.cache.move_to_end(key)
        
        self.cache[key] = value
        
        if len(self.cache) > self.capacity:
            # last=False remove do início (Least Recently Used)
            oldest_key, _ = self.cache.popitem(last=False)
            print(f"[CACHE] Capacidade atingida. Removendo entrada obsoleta: {oldest_key}")

    def invalidate(self, key: str):
        """
        Invalida uma entrada específica quando uma URL é removida no servidor.
        """
        if key in self.cache:
            del self.cache[key]
            print(f"[CACHE] Entrada {key} invalidada com sucesso.")