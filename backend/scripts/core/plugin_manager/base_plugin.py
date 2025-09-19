from abc import ABC, abstractmethod
from typing import Dict, Any
from ..data_container.container import DataContainer

class BasePlugin(ABC):
    """
    Abstract base class for all ETL plugins.
    Enforces implementation of required plugin methods.
    """

    def __init__(self, params: Dict[str, Any]):
        self.params = params

    def set_params(self, params: Dict[str, Any]):
        self.params = params

    @abstractmethod
    def get_plugin_name(self) -> str:
        pass

    @abstractmethod
    def get_parameters_schema(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def execute(self, input_data: DataContainer) -> DataContainer:
        pass
