# -*- coding: utf-8 -*-
"""
Created on Fri May 12 18:22:05 2023

@author: paolo
"""

import pandas as pd
import numpy as np
import datawrangling as dw

# %% Controllo sui fogli presenti in LAU REG ticiadd.xls

og1 = pd.read_csv('data/PTUA2003/original/lomell/LauraReg lomell_Arpa 2001 2002.csv')
og2 = pd.read_csv('data/PTUA2003/original/lomell/LauraReg lomell_Lomellina.csv')
og3 = pd.read_csv('data/PTUA2003/original/lomell/LauraReg lomell_Lomellina vecchi.csv')

# %% og1

df1 = og1.copy()
df1['COMUNE - DENOMINAZIONE'] = [' '.join(x.split()[:-1]) if len(x.split()) > 1 else x for x in og1['COMUNE - DENOMINAZIONE']]
df1.insert(2, 'DENOMINAZIONE', [str(x).split()[-1] if len(str(x).split()) > 1 else np.nan for x in og1['COMUNE - DENOMINAZIONE']])
df1.rename(columns = {'COMUNE - DENOMINAZIONE': 'COMUNE', "LOCALITA'": 'LOCALITA'}, inplace = True)
df1.insert(0, 'CODICETOOL', [''.join([str(x),str(y)]) for x,y in zip(df1['COMUNE'], df1['CODICE'])])
df1.set_index('CODICETOOL', inplace = True)
df1 = df1[~df1.index.duplicated(keep='first')]

meta1 = df1.iloc[:, 0:11]
ts1 = df1.iloc[:, 12:]
ts1[ts1 < 0] = np.nan

ts1 = ts1.transpose()
ts1.set_index(pd.date_range('2001-01-01', '2002-09-01', freq = 'MS'), inplace = True)

# %% og2

df2 = og2.copy()
df2['COMUNE - DENOMINAZIONE'] = [' '.join(x.split()[:-1]) if len(x.split()) > 1 else x for x in og2['COMUNE - DENOMINAZIONE']]
df2.insert(2, 'DENOMINAZIONE', [str(x).split()[-1] if len(str(x).split()) > 1 else np.nan for x in og2['COMUNE - DENOMINAZIONE']])
df2.rename(columns = {'COMUNE - DENOMINAZIONE': 'COMUNE', "LOCALITA'": 'LOCALITA'}, inplace = True)
df2.insert(0, 'CODICETOOL', [''.join(''.join([str(x),str(y)]).split('.')[:-1]) for x,y in zip(df2['COMUNE'], df2['CODICE'])])
df2.loc[df2['CODICETOOL'] == '', 'CODICETOOL'] = og2.loc[df2['CODICETOOL'] == '', 'COMUNE - DENOMINAZIONE']
df2.set_index('CODICETOOL', inplace = True)
df2 = df2[~df2.index.duplicated(keep='first')]

cols = df2.columns.to_list()
meta2 = df2[cols[0:11] + cols[40:]]
ts2 = df2[cols[12:39]]

ts2 = ts2.transpose()
ts2.set_index(pd.date_range('2001-01-01', '2003-03-01', freq = 'MS'), inplace = True)

# %% og3

df3 = og3.copy()
df3.set_index('COMUNE', inplace = True)
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

# %% merge

meta = dw.joincolumns(meta1.merge(meta2, left_index = True, right_index = True, how = 'outer'))
ts = dw.joincolumns(ts1.merge(ts2, left_index = True, right_index = True, how = 'outer'))

# %% save

meta.to_csv('data/PTUA2003/meta_PTUA2003_LOMELL.csv')
meta.to_csv('data/PTUA2003/head_PTUA2003_LOMELL.csv')