import numpy as np
import yaml
import os
import psycopg2
import pandas as pd
from torch.utils.data import DataLoader
from torch.utils.data.sampler import SubsetRandomSampler
from torchvision import datasets

from crypto_vol_prediction.base import DataLoaderBase

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

class UniswapV2SwapTranLoader(DataLoaderBase):
    """
    Loading Uniswap V2 swap transactions
    """
    def __init__(self, pair, data_dir):
        self.pair = pair
        self.data_dir = data_dir
        self.data_cache_name = f"UniswapV2SwapTranLoader_{self.pair}"
        self.data_cache_path = os.path.join(self.data_dir, self.data_cache_name)
    
    def refresh_from_remote(self):
        with open(os.path.join(__location__, "dbconfig.yaml"), "r") as conf_file:
            try:
                db_config = yaml.safe_load(conf_file)
                postgresql_config = db_config['connections']['postgresql']
                hostname = postgresql_config['hostname']
                port = postgresql_config['port']
                dbname = postgresql_config['dbname']
                user = postgresql_config['user']
                password = postgresql_config['password']
                connection_str = f"host={hostname} port={port} dbname={dbname} user={user} password={password} sslmode=disable"
                with psycopg2.connect(connection_str) as conn:
                    cur = conn.cursor()
                    query = """SELECT * FROM public.uniswap_v2_swap_transactions LIMIT 100"""
                    output_query = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
                    with open(self.data_cache_path, 'w') as cache_file:
                        cur.copy_expert(output_query, cache_file)
            except yaml.YAMLError as e:
                print(e)

    def get_dataframe(self):
        return pd.read_csv(self.data_cache_path)

class MnistDataLoader(DataLoaderBase):
    """
    MNIST data loading demo using DataLoaderBase
    """
    def __init__(self, transforms, data_dir, batch_size, shuffle, validation_split, nworkers,
                 train=True):
        self.data_dir = data_dir

        self.train_dataset = datasets.MNIST(
            self.data_dir,
            train=train,
            download=True,
            transform=transforms.build_transforms(train=True)
        )
        self.valid_dataset = datasets.MNIST(
            self.data_dir,
            train=False,
            download=True,
            transform=transforms.build_transforms(train=False)
        ) if train else None

        self.init_kwargs = {
            'batch_size': batch_size,
            'num_workers': nworkers
        }
        super().__init__(self.train_dataset, shuffle=shuffle, **self.init_kwargs)

    def split_validation(self):
        if self.valid_dataset is None:
            return None
        else:
            return DataLoader(self.valid_dataset, **self.init_kwargs)
