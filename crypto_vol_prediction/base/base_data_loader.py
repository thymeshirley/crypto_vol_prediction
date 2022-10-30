from torch.utils.data import DataLoader
import pandas as pd

class DataLoaderBase(DataLoader):
    """
    Base class for all data loaders
    """

    def refresh_from_remote(self):
        """
        Refresh local data cache from remote database
        """
        raise NotImplementedError

    def get_dataframe(self) -> pd.DataFrame:
        """
        Return dataframe from raw data
        """
        raise NotImplementedError

    def split_validation(self) -> DataLoader:
        """
        Return a `torch.utils.data.DataLoader` for validation, or None if not available.
        """
        raise NotImplementedError
