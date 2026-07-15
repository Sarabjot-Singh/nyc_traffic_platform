from abc import ABC, abstractmethod

class BaseLoader(ABC):

    @abstractmethod
    def load_dataframe(self):
        pass
    

    @abstractmethod
    def incremental_load(self):
        pass