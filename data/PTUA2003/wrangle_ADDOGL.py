# -*- coding: utf-8 -*-
"""
Created on Fri May 12 18:22:05 2023

@author: paolo
"""

import pandas as pd
import numpy as np
import datawrangling as dw

# %% Load fogli LauraReg addogl

og1 = pd.read_csv('data/PTUA2003/original/addogl/LauraReg addogl_Arpa 2001 2002.csv')
og2 = pd.read_csv('data/PTUA2003/original/addogl/LauraReg addogl_Adda Oglio.csv')
og3 = pd.read_csv('data/PTUA2003/original/addogl/LauraReg addogl_Adda Oglio vecchi.csv')

# %% og1

df1 = og1.copy()
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
df1['CODICE'] = [f'0{code}' if (len(str(code))>6) and (code[0] != 'P') else code for code in df1['CODICE']]
#generate a custom unique code for the merge
df1.insert(0, 'CODICETOOL', [''.join([str(x),str(y)]) for x,y in zip(df1['COMUNE'], df1['CODICE'])])
df1.loc[df1['CODICETOOL'] == '', 'CODICETOOL'] = df1.loc[df1['CODICETOOL'] == '', 'COMUNE']
df1.set_index('CODICETOOL', inplace = True)
df1 = df1[~df1.index.duplicated(keep='first')]

df1.info()

meta1 = df1.iloc[:, 0:11]
ts1 = df1.iloc[:, 12:]
ts1[ts1 < 0] = np.nan

ts1 = ts1.transpose()
ts1.set_index(pd.date_range('2001-01-01', '2002-12-01', freq = 'MS'), inplace = True)

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
df2.loc[df2['CODICETOOL'] == '', 'CODICETOOL'] = df2.loc[df2['CODICETOOL'] == '', 'COMUNE']
df2.set_index('CODICETOOL', inplace = True)
df2 = df2[~df2.index.duplicated(keep='first')]

cols = df2.columns.to_list()
meta2 = df2[cols[0:11] + cols[40:]]
ts2 = df2[cols[12:39]]

ts2 = ts2.transpose()
ts2.set_index(pd.date_range('2001-01-01', '2003-03-01', freq = 'MS'), inplace = True)

# %% og3

df3 = og3.copy()
df3.set_index('Nome', inplace = True)
ts3 = df3.transpose()

dic = {'gen': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'mag': '05', 'giu': '06', 'lug': '07', 'ago': '08', 'set': '09', 'ott': '10', 'nov': '11', 'dic': '12'}
nw = []
for x in ts3.index:
    if float(x.split('-')[1]) < 10:
        y = '20'
    else:
        y = '19'
    nw += [f"01/{dic[x.split('-')[0]]}/{y}{x.split('-')[1]}"]
nw = pd.to_datetime(nw, format = '%d/%m/%Y')
ts3.index = nw

# %% merge metadata

# meta1.reset_index().set_index('CODICE', inplace = True)
# meta2.reset_index().set_index('CODICE', inplace = True)
meta = dw.joincolumns(meta1.merge(meta2, left_index = True, right_index = True, how = 'outer'))

# #identify rows with numbers in "comune"
# ns = [f'{x}' for x in range(10)]
# lst = []
# for n in ns:
#     lst += [meta['COMUNE'].str.contains(n)]
# #keep only the municipality name in "comune" and put the number in "denominazione"
# for idx in lst:
#     meta.loc[idx.values, 'DENOMINAZIONE'] = [f"{y} - {' '.join(x.split()[-1])}" if y==y else ' '.join(x.split()[-1]) for x,y in zip(meta.loc[idx.values, 'COMUNE'], meta['DENOMINAZIONE']) ]
#     meta.loc[idx.values, 'COMUNE'] = [' '.join(x.split()[:-1]) for x in meta.loc[idx.values, 'COMUNE']]

# meta['CODICETOOL'] = [''.join([str(x),str(y)]) for x,y in zip(meta['COMUNE'], meta['CODICE'])]
 
# %% merge time series

ts3.columns = [x.upper() for x in ts3.columns]
cols = {
 'POZZO 543 DI SUISIO ': 'SUISIO0162091633',
 'POZZO DI BREMBATE 403': 'BREMBATE0160370011',
 'POZZO DI PNTE SAN PIETRO 514': 'PONTE S. PIETRO0161700005',
 'POZZO DI DALMINE 155': 'DALMINE0160910003',
 'POZZO DI TREVIGLIO 322': 'TREVIGLIO0162190302',
 'POZZO DI URGNANO 339': 'URGNANO0162220004',
 'POZZO DI CARAVAGGIO 80': 'CARAVAGGIO0160530002',
 'BOLGARE 397': 'BOLGARE0160281385',
 'PUMENENGO 268': 'PUMENENGO0161770001',
 'MARTINENGO 487': 'MARTINENGO0161330158'
 }
ts3.rename(columns = cols, inplace = True)
ts3 = ts3.loc[:,ts3.columns.isin(meta.index)]
ts3.dropna(inplace = True)

ts = dw.joincolumns(ts1.merge(ts2, left_index = True, right_index = True, how = 'outer'))
ts = dw.joincolumns(ts.merge(ts3, left_index = True, right_index = True, how = 'outer'))
ts.index.names = ['DATA']

# %% save

meta.to_csv('data/PTUA2003/wrangled/addogl/meta_PTUA2003_ADDOGL.csv')
ts.to_csv('data/PTUA2003/wrangled/addogl/head_PTUA2003_ADDOGL.csv')