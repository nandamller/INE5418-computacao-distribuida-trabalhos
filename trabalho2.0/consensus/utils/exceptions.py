
class ConsensusError(Exception):
    """Classe base para todas as exceções do protocolo de consenso."""
    pass

class InvalidStatusError(ConsensusError):
    """Gerada quando uma operação é tentada enquanto o nó não está no estado necessário."""
    def __init__(self, current_status, expected_status="NORMAL"):
        self.current_status = current_status
        super().__init__(f"Operation rejected: node status is {current_status}. Expected {expected_status}.")

class ViewMismatchError(ConsensusError):
    """Gerado quando o número da visualização da mensagem não corresponde à visualização atual do nó."""
    def __init__(self, msg_view, node_view):
        self.msg_view = msg_view
        self.node_view = node_view
        super().__init__(f"View mismatch: message view is {msg_view}, but node view is {node_view}.")

class LogInconsistencyError(ConsensusError):
    """Gerado quando há uma lacuna ou incompatibilidade no número de sequência de operação esperado."""
    def __init__(self, expected_op, received_op):
        self.expected_op = expected_op
        self.received_op = received_op
        super().__init__(f"Log inconsistency: expected op_num {expected_op}, but received {received_op}.")