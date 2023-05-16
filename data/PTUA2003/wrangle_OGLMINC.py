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
#fix comune column
df1['COMUNE - DENOMINAZIONE'] = [x.split('-')[0].rstrip().upper() for x in og1['COMUNE - DENOMINAZIONE']]
df1.insert(2, 'DENOMINAZIONE', [' '.join(x.split('-')[1:]).lstrip() if len(x.split('-')) > 1 else np.nan for x in og1['COMUNE - DENOMINAZIONE']])
df1.rename(columns = {'COMUNE - DENOMINAZIONE': 'COMUNE', "LOCALITA'": 'LOCALITA'}, inplace = True)
#identify rows with numbers in "comune"
ns = [f'{x}' for x in range(10)]
lst = []
for n in ns:
    lst += [df1['COMUNE'].str.contains(n)]
#keep only the municipality name in "comune" and put the number in "denominazione"
for idx in lst:
    df1.loc[idx.values, 'DENOMINAZIONE'] = [f"{y} - {' '.join(x.split()[-1])}" if y==y else ' '.join(x.split()[-1]) for x,y in zip(df1.loc[idx.values, 'COMUNE'], df1['DENOMINAZIONE']) ]
    df1.loc[idx.values, 'COMUNE'] = [' '.join(x.split()[:-1]) for x in df1.loc[idx.values, 'COMUNE']]
#fix SIF code
df1['CODICE'] = [f'0{code}' if (len(str(int(code)))>6) and (code[0] != 'P') else code for code in df1['CODICE']]
#generate a custom unique code for the merge
df1.insert(0, 'CODICETOOL', [''.join([str(x),str(int(y))]) for x,y in zip(df1['COMUNE'], df1['CODICE'])])
df1.loc[df1['CODICE'].isna(), 'CODICETOOL'] = df1.loc[df1['CODICE'].isna(), 'COMUNE']
df1.set_index('CODICETOOL', inplace = True)
df1 = df1[~df1.index.duplicated(keep='first')]

df1.info()

meta1 = df1.iloc[:, 0:11]
ts1 = df1.iloc[:, 12:]
ts1[ts1 < 0] = np.nan

ts1 = ts1.transpose()
ts1.set_index(pd.date_range('2001-01-01', '2003-04-01', freq = 'MS'), inplace = True)

# %% og2

df2 = og2.copy()
df2['COMUNE - DENOMINAZIONE'] = [x.split('-')[0].rstrip().upper() for x in og2['COMUNE - DENOMINAZIONE']]
df2.insert(2, 'DENOMINAZIONE', [' '.join(x.split('-')[1:]).lstrip() if len(x.split('-')) > 1 else np.nan for x in og2['COMUNE - DENOMINAZIONE']])
df2.rename(columns = {'COMUNE - DENOMINAZIONE': 'COMUNE', "LOCALITA'": 'LOCALITA'}, inplace = True)
#identify rows with numbers in "comune"
ns = [f'{x}' for x in range(10)]
lst = []
for n in ns:
    lst += [df2['COMUNE'].str.contains(n)]
#keep only the municipality name in "comune" and put the number in "denominazione"
for idx in lst:
    df2.loc[idx.values, 'DENOMINAZIONE'] = [f"{y} - {' '.join(x.split()[-1])}" if y==y else ' '.join(x.split()[-1]) for x,y in zip(df2.loc[idx.values, 'COMUNE'], df2['DENOMINAZIONE']) ]
    df2.loc[idx.values, 'COMUNE'] = [' '.join(x.split()[:-1]) for x in df2.loc[idx.values, 'COMUNE']]
#fix SIF code
df2['CODICE'] = [f'0{code}' if (len(str(code))>6) and (code[0] != 'P') else code for code in df2['CODICE']]
#generate a custom unique code for the merge
df2.insert(0, 'CODICETOOL', [''.join([str(x),str(y)]) for x,y in zip(df2['COMUNE'], df2['CODICE'])])
df2.loc[df2['CODICE'].isna(), 'CODICETOOL'] = df2.loc[df2['CODICE'].isna(), 'COMUNE']
df2.set_index('CODICETOOL', inplace = True)
df2 = df2[~df2.index.duplicated(keep='first')]

meta2 = df2.iloc[:, 0:11]
ts2 = df2.iloc[:, 12:]
ts2[ts2 < 0] = np.nan

ts2 = ts2.transpose()
ts2.set_index(pd.date_range('2001-01-01', '2003-04-01', freq = 'MS'), inplace = True)

# %% merge metadata

meta1.reset_index().set_index('CODICE', inplace = True)
meta2.reset_index().set_index('CODICE', inplace = True)
meta = dw.joincolumns(meta1.merge(meta2, left_index = True, right_index = True, how = 'outer'))
# meta['CODICETOOL'] = [''.join([str(x),str(y)]) for x,y in zip(meta['COMUNE'], meta['CODICE'])]

# %% merge time series

ts = dw.joincolumns(ts1.merge(ts2, left_index = True, right_index = True, how = 'outer'))
ts.index.names = ['DATA']

# %% save

meta.to_csv('data/PTUA2003/wrangled/oglminc/meta_PTUA2003_OGLMINC.csv')
ts.to_csv('data/PTUA2003/wrangled/oglminc/head_PTUA2003_OGLMINC.csv')