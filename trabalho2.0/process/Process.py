from abc import ABC, abstractmethod

class BaseProcess(ABC):
    def __init__(self, port: int, host: str ='127.0.0.1'):
        self.host = host
        self.port = port
        # self._processo = None

    @abstractmethod
    def execute(self):
        """Abstract method that must contain the communication logic."""
        pass

    # def iniciar(self):
    #     """Inicia o processo utilizando o multiprocessing."""
    #     self._processo = multiprocessing.Process(target=self.executar)
    #     self._processo.start()

    # def aguardar(self):
    #     """Aguarda a finalização do processo (join)."""
    #     if self._processo:
    #         self._processo.join()