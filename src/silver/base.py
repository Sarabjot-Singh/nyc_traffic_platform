
from abc import ABC, abstractmethod


class Model(ABC):
    """
    Abstract base class for dimensional models.
    """

    @abstractmethod
    def initial_load(self):
        """
        Load the initial data into the dimensional model.

        Args:
            df (pd.DataFrame): Input DataFrame to be transformed.

        Returns:
            pd.DataFrame: Transformed DataFrame representing the dimensional model.
        """
        pass

    @abstractmethod
    def incremental_load(self):
        """
        Load incremental data into the dimensional model.

        Args:
            df (pd.DataFrame): Input DataFrame to be transformed.

        Returns:
            pd.DataFrame: Transformed DataFrame representing the dimensional model.
        """
        pass