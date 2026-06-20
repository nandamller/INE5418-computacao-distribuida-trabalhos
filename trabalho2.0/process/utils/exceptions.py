class InvalidStatusError():
    """Raised when an operation is attempted while the node is not in the required status."""
    def __init__(self, current_status, expected_status="NORMAL"):
        self.current_status = current_status
        super().__init__(f"Operation rejected: node status is {current_status}. Expected {expected_status}.")