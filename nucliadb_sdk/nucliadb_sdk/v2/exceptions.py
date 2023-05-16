class ClientError(Exception):
    pass


class NotFoundError(ClientError):
    pass


class AuthError(ClientError):
    pass


class RateLimitError(ClientError):
    pass


class ConflictError(ClientError):
    pass


class UnknownError(ClientError):
    pass
