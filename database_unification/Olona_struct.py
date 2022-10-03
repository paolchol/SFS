# -*- coding: utf-8 -*-
"""
Olona database rearrangement

- Load the database
- Clean the database
- Rearrange it to create a meta and a head

@author: paolo
"""

#%% Setup

import datawrangling as dw
import pandas as pd
import numpy as np

# %% Load

original = pd.read_csv('data/Olona/piez_Olona.csv')

# %% Clean

#split COMUNE and DENOMINAZIONE
df = original.copy()
df.rename(columns = {'COMUNE - DENOMINAZIONE': 'COMUNE'}, inplace = True)
df.insert(2, 'DENOMINAZIONE', [' '.join(df['COMUNE'][i].split('-')[1:]) for i in range(len(df['COMUNE']))])
df['COMUNE'] = [df['COMUNE'][i].split('-')[0].upper() for i in range(len(df['COMUNE']))]

#clean data cols which present strings
mask = np.column_stack([df[col].str.contains('n', na = False) for col in df.columns[33:38]])
for i, col in enumerate(df.columns[33:38]):
    df.loc[mask[:, i], col] = np.nan
#substitute commas with dots
df.loc[:, df.columns[33:38]] = np.column_stack([df[col].str.replace(',', '.') for col in df.columns[33:38]])
#set as float
for col in df.columns[33:38]:
    df.loc[:, col] = pd.to_numeric(df.loc[:, col])
#place a 0 upfront the SIF codes
df = dw.join_twocols(df, ['CODICE', 'CODICE ACQUAGEST'], onlyna = False)
df['CODICE'] = [str(code) for code in df['CODICE']]
df['CODICE'] = [f'0{code}' if (len(code)>6) and (code[0] != 'P') else code for code in df['CODICE']]

# %% Split in meta and data databases

cols = df.columns.to_list()
meta = df.iloc[:, 0:13].copy()
head = df.loc[:, [cols[3]] + cols[12:]].copy()

#rearrange head
head = head.set_index('CODICE').transpose()
head.set_index(pd.date_range('2001-05-01', '2003-08-01', freq = 'MS'), inplace = True)

meta.to_csv('data/Olona/meta_Olona.csv')
head.to_csv('data/Olona/head_Olona.csv')
