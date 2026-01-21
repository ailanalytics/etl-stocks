# --------------------------------------------------
# Custom Exceptions
# --------------------------------------------------

class ConfigError(Exception):
    pass

class APIError(Exception):
    pass

class ValidationError(Exception):
    pass

class SQLError(Exception):
    pass

class RequestError(Exception):
    pass

class ParsingError(Exception):
    pass