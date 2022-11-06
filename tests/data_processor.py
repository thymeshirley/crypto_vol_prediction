#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  6 11:01:46 2022

@author: laixu
"""
import os
os.chdir('/Users/laixu/Documents/Machine Learning CS230/project/crypto_vol_prediction/')
import pandas as pd
import numpy as np
from scipy.stats.mstats import winsorize
tmp = pd.read_csv('UniswapV2SwapTranLoader_dummy.csv')

# there are rows that are all zeros. for in/pout amount0 and amount1 filter out those rows
tmp['amount1_max'] = tmp.apply(lambda x : int(max(x['amount1_in'], x['amount1_out'])), axis = 1)
tmp['amount0_max'] =  tmp.apply(lambda x : int(max(x['amount0_in'], x['amount0_out'])), axis = 1)
tmp = tmp[tmp['amount0_max'] != 0]
tmp = tmp[tmp['amount1_max'] != 0]

#amount1_max = tmp.apply(lambda x : int(max(x['amount1_in'], x['amount1_out'])), axis = 1)
#amount0_max = tmp.apply(lambda x : int(max(x['amount0_in'], x['amount0_out'])), axis = 1)
tmp['price'] = tmp.amount1_max/tmp.amount0_max *(10**12)

price_df = pd.DataFrame(index = tmp.tx_timestamp, data = tmp['price'].values,columns =['price'])
price_df.index = pd.to_datetime(price_df.index)

def resample_func_mean(values):
    if np.isnan(values).any():
        return np.nan
    else:
        return np.mean(values)
    
    
def resample_func_vol(values):
    if np.isnan(values).any():
        return np.nan
    else:
        return np.std(values)

def price_grouping(time_interval, price_df):
    first_price = price_df.resample(time_interval).first()
    last_price  = price_df.resample(time_interval).last()
    mean_price  = price_df['price'].astype(int).resample(time_interval).agg(np.mean)
    vol_of_vol_price =  price_df['price'].astype(int).resample(time_interval).agg(np.std)
    price_max   = price_df.resample(time_interval).max()
    price_min   = price_df.resample(time_interval).min()
    grouped_df = pd.concat([first_price, last_price, mean_price, vol_of_vol_price, price_max, price_min], axis =1)
    grouped_df.columns = ['first','last','mean','vol_of_vol', 'max','min']
    return grouped_df

# you can use the grouping function to abitrarily adjust what kind of interval you want
price_df_10min = price_grouping('10min', price_df)
price_df_10min.to_csv('./processed_data/price_df_10mins.csv')

def remove_extreme(log_ret):
    log_ret[log_ret>1] = 0
    log_ret[log_ret<-1] = 0
    return log_ret

def get_log_ret(price_df, field):
    price_df[field] = price_df[field].fillna(method='ffill')
    log_ret = np.log(price_df[field])-np.log(price_df[field].shift(1))
    return log_ret

def get_vol(log_ret, annualization_factor):
    if annualization_factor = 'True':
        
    return log_ret

log_ret_last =  get_log_ret(price_df_10min , 'last')
log_ret_last = remove_extreme(log_ret_last )
