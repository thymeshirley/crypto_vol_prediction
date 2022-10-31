from crypto_vol_prediction.data_loader import UniswapV2SwapTranLoader
import pytest
import os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

def test_uniswap_dataloader():
    loader = UniswapV2SwapTranLoader("dummy", os.path.join(__location__, "..", "data_cache"))
    loader.refresh_from_remote()
    df = loader.get_dataframe()
    print(df.head())