class InvalidQueryError(Exception):
    """Raised when parsing a query containing an invalid parameter"""

    def __init__(self, param: str, reason: str):
        self.param = param
        self.reason = reason
        super().__init__(f"Invalid query. Error in {param}: {reason}")
