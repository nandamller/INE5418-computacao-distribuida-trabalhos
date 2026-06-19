# exceptions.py

class ConsensusError(Exception):
    """Base class for all consensus protocol exceptions."""
    pass

class InvalidStatusError(ConsensusError):
    """Raised when an operation is attempted while the node is not in the required status."""
    def __init__(self, current_status, expected_status="NORMAL"):
        self.current_status = current_status
        super().__init__(f"Operation rejected: node status is {current_status}. Expected {expected_status}.")

class ViewMismatchError(ConsensusError):
    """Raised when the message view number does not match the node's current view."""
    def __init__(self, msg_view, node_view):
        self.msg_view = msg_view
        self.node_view = node_view
        super().__init__(f"View mismatch: message view is {msg_view}, but node view is {node_view}.")

class LogInconsistencyError(ConsensusError):
    """Raised when there is a gap or mismatch in the expected operation sequence number."""
    def __init__(self, expected_op, received_op):
        self.expected_op = expected_op
        self.received_op = received_op
        super().__init__(f"Log inconsistency: expected op_num {expected_op}, but received {received_op}.")