class AssetPulseError(Exception):
    pass


class IngestionError(AssetPulseError):
    pass


class DataValidationError(AssetPulseError):
    pass


class TransformationError(AssetPulseError):
    pass


class StorageError(AssetPulseError):
    pass


class ConfigurationError(AssetPulseError):
    pass


class PipelineError(AssetPulseError):
    pass
