# -*- coding: utf-8 -*-
"""
Rearrangement of the PTUA2022 dataset in a more easily usable dataset

Creation of a time series files structured as:
    - columns: piezometers and wells, identified by their code
    - rows: date (monthly)

author: paolo
"""

# %% Setup

import numpy as np
import pandas as pd

# %% Obtain a monthly dataset of the total head for each piezometer

#Load the basin's piezometer dataset
fname = 'data/PTUA2022/original/piezometria_ISS.csv'
df = pd.read_csv(fname)
#Set the data format
df['DATA'] = pd.to_datetime(df['DATA'], format = '%Y-%m-%d')
#Define a date/piezometer database
head = df.pivot(index = 'DATA', columns = 'CODICE PUNTO', values = 'PIEZOMETRIA [m s.l.m.]')
#Set the database to monthly
head = head.resample("1MS").mean()

head.to_csv('data/PTUA2022/head_PTUA2022_ISS.csv')

# %% Obtain the piezometer metadata of the GW basin considered 

#Load the full metadata dataset
meta = pd.read_csv('data/PTUA2022/original/CSV-RETE ACQUE SOTTERRANEE_ANAGRAFICA COMPLETA.csv')
meta.dropna(subset = 'PROVINCIA', inplace = True)
meta.drop(columns = ['Area GWB', 'DATA_FINE'], inplace = True)
meta['ORIGINE'] = 'PTUA2022'
#correct "DATA_INIZIO"
meta['DATA_INIZO'] = [head[col].first_valid_index() if col in head.columns else np.nan for col in meta['CODICE']]
meta.rename(columns = {'DATA_INIZO': 'DATA_INIZIO'}, inplace = True)

meta[meta == 'n.d.'] = np.nan
meta[meta == 'da definire'] = np.nan
meta[meta == 'DA DEFINIRE'] = np.nan

#remove duplicated codes, keep first to maintain the ones wiht RETE = 1
meta.drop_duplicates('CODICE', 'first', inplace = True)

meta.to_csv('data/PTUA2022/meta_PTUA2022.csv', index = False)
metaiss = meta.loc[meta['Codice WISE Corpo idrico'].isin(list(dict.fromkeys([x for x in meta['Codice WISE Corpo idrico'] if 'ISS' in str(x)]))), :]
metaiss.to_csv('data/PTUA2022/meta_PTUA2022_ISS.csv', index = False)
