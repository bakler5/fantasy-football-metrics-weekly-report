class FFMRError(Exception):
    """Base exception for Fantasy Football Metrics Weekly Report."""


class AppConfigError(FFMRError):
    pass


class NetworkError(FFMRError):
    pass


class DataUnavailableError(FFMRError):
    pass


class ExternalServiceError(FFMRError):
    pass


class UpdateError(FFMRError):
    pass

