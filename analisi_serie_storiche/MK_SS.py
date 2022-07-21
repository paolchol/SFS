# -*- coding: utf-8 -*-
"""
Mann-Kendall test and Sen's slope analysis on the head observations

@author: paolo
"""

# %% Setup

import pandas as pd
import numpy as np
import dataanalysis as da
import dataviz as dv
import matplotlib.pyplot as plt

# %% Load data and metadata

head = pd.read_csv('data/head_IT03GWBISSAPTA.csv', index_col = 'DATA')
meta = pd.read_csv('data/metadata_piezometri_ISS.csv')

# %% Select solid time series and fill missing values

cn = da.CheckNA(head)
filtered, removed = cn.filter_col(20, True)
co = da.CheckOutliers(filtered, False)
head_clean = co.remove(skip = ['PO0120750R2020'])
head_fill = head_clean.interpolate('linear', limit = 14)

# %% Perform the Mann-Kendall test and calculate the Sen's slope

confidence = 0.95

db_mk = pd.DataFrame(np.zeros((len(head_fill.columns), 3)), columns = ['z','p','tr'], index = head_fill.columns)
db_slope = pd.DataFrame(np.zeros((len(head_fill.columns), 1)), columns = ['slope'], index = head_fill.columns)

for col in head_fill.columns:
    idx = db_mk.index.isin([col])
    db_mk.loc[idx, 'z'], db_mk.loc[idx, 'p'], db_mk.loc[idx, 'tr'] = da.mann_kendall(head_fill[col].dropna(), confidence)
    db_slope[db_slope.index.isin([col])], _, _, _ = da.sen_slope(head_fill[col].dropna())

db_mk.to_csv('analisi_serie_storiche/res_MK.csv')
db_slope.to_csv('analisi_serie_storiche/res_sslope.csv')

# %% Calculate a 5-year step Sen's slope

#check the length of the series
#if it is longer than 5-year

def step_sslope(df, step, dropna = True):
    """
    Parameters
    ----------
    df : pandas.DataFrame
    step : int
        step in which compute the sen's slope each
        time. needs to be in the unit of the df index
    dropna : boolean
        remove the na from the columns passed to the sen's slope
        function
    """
    ncol = round(len(df.index)/step)
    db_slope = pd.DataFrame(np.zeros((len(df.columns), ncol)),
                            columns = [f'slope{i}' for i in range(1, ncol+1)],
                            index = df.columns)
    for col in df.columns:
        series = df[col].dropna() if dropna else df[col]
        # if len(series) >= 2*step:
        start, end = 0, 0
        for n in range(round(len(series)/step)):
            end = step*(n+1) if step*(n+1) < len(series) else len(series)
            db_slope.loc[db_slope.index.isin([col]), f'slope{n+1}'], _, _, _ = da.sen_slope(series[start:end])
            start = end
    return db_slope

db = step_sslope(head_fill, 5*12)

# %% Visualize the slope overlayed to 


# %% Compare two methods of sen's slope computation

confidence = 0.95

import scipy.stats as st

for col in head_fill.columns:
    _, _, tr_type = da.mann_kendall(head_fill[col].dropna(), confidence)
    slope, _, _ = da.sen_slope(head_fill[col].dropna(), confidence, scipy = False)
    slopesc, _, _, _ = st.mstats.theilslopes(head_fill[col].dropna())
    print(f"{col}")
    print(f"Mann-Kendall: {tr_type}")
    print(f"Sen's slope: {slope}")
    print(f"Sen's slope (scipy): {slopesc}")
