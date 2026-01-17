# -------------------------------------
# Custom Exceptions
# -------------------------------------

class ConfigError(Exception):
    pass

class APIError(Exception):
    pass

class ValidationError(Exception):
    pass

class StagingError(Exception):
    pass

class RequestError(Exception):
    pass

class ParsingError(Exception):
    pass