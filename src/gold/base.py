
from abc import ABC, abstractmethod


class Model(ABC):
    """
    Abstract base class for dimensional models.
    """

    @abstractmethod
    def transform(self):
        """
        Creates the final golden layer table    
        """
        pass


    def initiate_transform(self):
        """
        Creates the final golden layer table    
        """
        pass