class MachinaFlowError(Exception):
    pass


class IngestionError(MachinaFlowError):
    pass


class DataValidationError(MachinaFlowError):
    pass


class TransformationError(MachinaFlowError):
    pass


class StorageError(MachinaFlowError):
    pass


class ConfigurationError(MachinaFlowError):
    pass


class PipelineError(MachinaFlowError):
    pass
