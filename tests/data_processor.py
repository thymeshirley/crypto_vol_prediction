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
from matplotlib import pyplot as plt
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

block_df = pd.DataFrame(index = tmp.tx_timestamp, data = tmp['block_number'].values,columns =['block_number'])
block_df.index = pd.to_datetime(block_df.index)
def exp_avg(price_df, fixed_len):
    moving_avg_price_df = price_df.ewm(fixed_len).mean()
    return moving_avg_price_df


def price_filter(price_df):
    price_df[price_df.price > 10000] = np.nan
    price_df[price_df.price < 50] = np.nan
    price_df = price_df.dropna()
    return price_df

price_df = price_filter(price_df)
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
price_df_10min.to_csv('./processed_data/price_df_10mins_furtherpreprocess.csv')

def remove_extreme(log_ret):
    log_ret[log_ret>1] = 0
    log_ret[log_ret<-1] = 0
    return log_ret

def get_log_ret(price_df, field):
    price_df[field] = price_df[field].fillna(method='ffill')
    log_ret = np.log(price_df[field])-np.log(price_df[field].shift(1))
    return log_ret

def get_vol( grouped_df):
    vol = np.log(grouped_df['max'])- np.log(grouped_df['min'])
    return vol

price_df_vol = price_df

lower_bounds = pd.cut(price_df.index, 
                      bins=expavg_vol.index,
                      right=False, include_lowest=True)



def get_price_vol_mapping(vol_df, price_df):
    
    concated_df 

log_ret_last =  get_log_ret(price_df_10min , 'last')
log_ret_last = remove_extreme(log_ret_last )
log_ret_last.to_csv('./processed_data/log_ret_last_10mins_furtherpreprocess.csv')

vol = get_vol(price_df_10min)
vol = remove_extreme(vol )
vol.to_csv('./processed_data/vol_10mins_furtherpreprocess.csv')
vol.plot()

# data check
data_public = pd.read_csv('/Users/laixu/Documents/Machine Learning CS230/project/crypto_vol_prediction/data_check/ETH-USD.csv', index_col =0)
data_public.index = pd.to_datetime(data_public.index)
# group price by day

price_df_1day = price_grouping('D', price_df)

plt.figure()
plt.plot(data_public.index, data_public['High'], label= 'High price')
plt.plot(price_df_1day.index, price_df_1day['max'], label= 'On chain max price')
plt.legend()


plt.figure()
plt.plot(data_public.index, data_public['Low'], label= 'Low price')
plt.plot(price_df_1day.index, price_df_1day['min'], label= 'On chain min price')
plt.legend()


expavg_price_df = exp_avg(price_df, 100)
expavg_price_df_1day = price_grouping('D', expavg_price_df )

# getting the exponentially moving average price plot vs public data

plt.figure()
plt.plot(data_public.index, data_public['High'], label= 'High price')
plt.plot(expavg_price_df_1day .index, expavg_price_df_1day ['max'], label= 'On chain max price')
plt.legend()


plt.figure()
plt.plot(data_public.index, data_public['Low'], label= 'Low price')
plt.plot(expavg_price_df_1day .index, expavg_price_df_1day ['min'], label= 'On chain min price')
plt.legend()


expavg_price_df_10mins = price_grouping('10min', expavg_price_df)

expavg_log_ret_last =  get_log_ret(expavg_price_df_10mins , 'last')
expavg_log_ret_last = remove_extreme(expavg_log_ret_last )
expavg_log_ret_last.to_csv('./processed_data/log_ret_last_10mins_smoothtransaction.csv')

expavg_vol = get_vol(expavg_price_df_10mins)
expavg_vol = remove_extreme(expavg_vol )
expavg_vol.to_csv('./processed_data/vol_10mins_smoothtransaction.csv')
expavg_vol.plot()
