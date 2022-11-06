
import os
import sys
os.chdir('/Users/laixu/Documents/Machine Learning CS230/project/crypto_vol_prediction/')
from crypto_vol_prediction.data_loader import UniswapV2SwapTranLoader
import pytest
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

def test_uniswap_dataloader():
    loader = UniswapV2SwapTranLoader("dummy", os.path.join(__location__, "..", "data_cache"))
    loader.refresh_from_remote()
    df = loader.get_dataframe()
    print(df.head())