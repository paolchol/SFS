# -*- coding: utf-8 -*-
"""
Created on Fri May 12 18:22:05 2023

@author: paolo
"""

import pandas as pd
import numpy as np
import datawrangling as dw

# %% Controllo sui fogli presenti in LAU REG ticiadd.xls

og1 = pd.read_csv('data/PTUA2003/original/oglminc/LauraReg Oglminc_OglioMincio.csv')
og2 = pd.read_csv('data/PTUA2003/original/oglminc/LauraReg Oglminc_piezometrie.csv')

# %% og1

df1 = og1.copy()
df1['COMUNE - DENOMINAZIONE'] = [x.split('-')[0].rstrip() for x in og1['COMUNE - DENOMINAZIONE']]
df1.insert(2, 'DENOMINAZIONE', [x.split('-')[1] if len(x.split('-')) > 1 else np.nan for x in og1['COMUNE - DENOMINAZIONE']])
df1.rename(columns = {'COMUNE - DENOMINAZIONE': 'COMUNE', "LOCALITA'": 'LOCALITA'}, inplace = True)
df1.insert(0, 'CODICETOOL', [''.join([str(x),str(y)]) for x,y in zip(df1['COMUNE'], df1['CODICE'])])
df1.set_index('CODICE', inplace = True)
df1 = df1[~df1.index.duplicated(keep='first')]

df1.info()

meta1 = df1.iloc[:, 0:11]
ts1 = df1.iloc[:, 12:]
ts1[ts1 < 0] = np.nan

ts1 = ts1.transpose()
ts1.set_index(pd.date_range('2001-01-01', '2003-04-01', freq = 'MS'), inplace = True)

# %% og2

df2 = og2.copy()
df2['COMUNE - DENOMINAZIONE'] = [x.split('-')[0].rstrip() for x in og2['COMUNE - DENOMINAZIONE']]
df2.insert(2, 'DENOMINAZIONE', [x.split('-')[1] if len(x.split('-')) > 1 else np.nan for x in og2['COMUNE - DENOMINAZIONE']])
df2.rename(columns = {'COMUNE - DENOMINAZIONE': 'COMUNE', "LOCALITA'": 'LOCALITA'}, inplace = True)
df2.insert(0, 'CODICETOOL', [''.join([str(x),str(y)]) for x,y in zip(df2['COMUNE'], df2['CODICE'])])
df2.set_index('CODICE', inplace = True)
df2 = df2[~df2.index.duplicated(keep='first')]

meta2 = df2.iloc[:, 0:11]
ts2 = df2.iloc[:, 12:]
ts2[ts2 < 0] = np.nan

ts2 = ts2.transpose()
ts2.set_index(pd.date_range('2001-01-01', '2003-04-01', freq = 'MS'), inplace = True)

# %% merge metadata

meta = dw.joincolumns(meta1.merge(meta2, left_index = True, right_index = True, how = 'outer'))

# aggiornare MILANO comune
# aggiornare mortara comune
# aggiornare codice tool

# %% merge time series

ts = dw.joincolumns(ts1.merge(ts2, left_index = True, right_index = True, how = 'outer'))

# %% save

meta.to_csv('data/PTUA2003/wrangled/addogl/meta_PTUA2003_ADDOGL.csv')
ts.to_csv('data/PTUA2003/wrangled/addogl/head_PTUA2003_ADDOGL.csv')