class ReflectorBaseException(Exception):
    pass


class ReflectorClientVersionError(ReflectorBaseException):
    """
    Raised by reflector server if client sends an incompatible or unknown version
    """


class ReflectorRequestError(ReflectorBaseException):
    """
    Raised by reflector server if client sends a message without the required fields
    """


class ReflectorRequestDecodeError(ReflectorBaseException):
    """
    Raised by reflector server if client sends an invalid json request
    """


class IncompleteResponse(ReflectorBaseException):
    """
    Raised by reflector server when client sends a portion of a json request,
    used buffering the incoming request
    """


class InvalidBlobHashError(ReflectorBaseException):
    pass


class AlreadyStarted(ReflectorBaseException):
    pass
