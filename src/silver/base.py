
from abc import ABC, abstractmethod


class Model(ABC):
    """
    Abstract base class for dimensional models.
    """
    
    @abstractmethod
    def transform(self):
        pass