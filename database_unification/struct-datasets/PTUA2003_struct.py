# -*- coding: utf-8 -*-
"""
dbu: DataBase Unification
Unificazione del dataset utilizzato per il PTUA 2003 con il dataset
reso disponibile da ARPA per il PTUA 2022
pt1: Parte 1
Sistemazione dei database dei piezometri nel bacino Adda-Ticino utilizzati per
il PTUA 2003

@author: paolo
"""

import pandas as pd
import numpy as np
import datawrangling as dw
import os

#%% Load the datasets

folder = 'data/PTUA2003/wrangled'
folder = os.path.abspath(folder)
sub = ['addogl', 'lomell', 'oglminc', 'ticinoadda']

meta1 = pd.read_csv(os.path.join(folder, sub[0], f'meta_PTUA2003_{sub[0].upper()}.csv'), index_col = 'CODICETOOL')
meta2 = pd.read_csv(os.path.join(folder, sub[1], f'meta_PTUA2003_{sub[1].upper()}.csv'), index_col = 'CODICETOOL')
meta3 = pd.read_csv(os.path.join(folder, sub[2], f'meta_PTUA2003_{sub[2].upper()}.csv'), index_col = 'CODICETOOL')
meta4 = pd.read_csv(os.path.join(folder, sub[3], f'meta_PTUA2003_{sub[3].upper()}_meta1.csv'), index_col = 'CODICETOOL')

head1 = pd.read_csv(os.path.join(folder, sub[0], f'head_PTUA2003_{sub[0].upper()}.csv'), index_col = 'DATA')
head2 = pd.read_csv(os.path.join(folder, sub[1], f'head_PTUA2003_{sub[1].upper()}.csv'), index_col = 'DATA')
head3 = pd.read_csv(os.path.join(folder, sub[2], f'head_PTUA2003_{sub[2].upper()}.csv'), index_col = 'DATA')
head4 = pd.read_csv(os.path.join(folder, sub[3], f'head_PTUA2003_{sub[3].upper()}_ts1.csv'), index_col = 'DATA')

# %% Merge

mrg = dw.joincolumns((meta1.merge(meta2, left_index = True, right_index = True, how = 'outer')))
mrg = dw.joincolumns((mrg.merge(meta3, left_index = True, right_index = True, how = 'outer')))
mrg = dw.joincolumns((mrg.merge(meta4, left_index = True, right_index = True, how = 'outer'))) #codicetool
mrg.reset_index(drop = False, inplace = True)
mrg = dw.joincolumns((mrg.merge(meta4, left_on = 'CODICE', right_on = 'CODICE', how = 'outer'))) #codice

mrg = dw.join_twocols(mrg, cols = ['Q', ' Q (m s.l.m.)'])
mrg = dw.join_twocols(mrg, cols = ['Q', 'z'])
mrg = dw.join_twocols(mrg, cols = ['X', 'x'])
mrg = dw.join_twocols(mrg, cols = ['Y', 'y'])
mrg = dw.join_twocols(mrg, cols = ['PROV', 'PROVINCIA'])
mrg = dw.join_twocols(mrg, cols = ['NOTE', 'INFO'])

#%% Clean

#complete origine column
mrg['ORIGINE'] = 'PTUA2003'

#check duplicates
# test = mrg.loc[mrg['CODICE'].duplicated(False), :]
seldrop = [
    'CORREZZANA0150920003',
    'RONCO BRIANTINO0151870001',
    'SAN DONATO0151920005',
    'S.GIULIANO0151950015',
    'TREZZANO S/N0152210001',
    "CAVENAGO D'ADDA0980170003",
    "CAVENAGO D'ADDA0980170064",
    "CERVIGNANO D'ADDA0980180001",
    "SAN ROCCO AL PORTO0980490001",
    "SANT'ANGELO LODIGIANO0980500005",
    'VIGAVANO47'
    ]
#drop duplicates
mrg.drop(mrg.loc[mrg['CODICETOOL'].isin(seldrop), :].index, inplace = True)

replace = [
 ['COREZZANA0150920003', 'CORREZZANA', 'CORREZZANA0150920003'],
 ['RONCO BR.0151870001', 'RONCO BRIANTINO', 'RONCO BRIANTINO0151870001'],
 ['SAN  DONATO0151920005', 'SAN DONATO', 'SAN DONATO0151920005'],
 ['S.GIULIANO0151950015', 'SAN GIULIANO', 'SAN GIULIANO0151950015'],
 ['CAVENAGO ADDA0980170064', "CAVENAGO D'ADDA", "CAVENAGO D'ADDA0980170064"],
 ['CAVENAGO ADDA0980170003', "CAVENAGO D'ADDA", "CAVENAGO D'ADDA0980170003"],
 ['CERVIGNANO A.0980180001', "CERVIGNANO D'ADDA", "CERVIGNANO D'ADDA0980180001"],
 ['S.ROCCO AL PORTO0980490001', 'SAN ROCCO AL PORTO', 'SAN ROCCO AL PORTO0980490001'],
 ['S.ANGELO LODIGIANO0980500005', "SANT'ANGELO LODIGIANO", "SANT'ANGELO LODIGIANO0980500005"],
 ['MASATE0codice_mancante_1', 'MASATE', 'MASATE']
 ]
for r in replace:
    mrg.loc[mrg['CODICETOOL'] == r[0], 'COMUNE'] = r[1]
    mrg.loc[mrg['CODICETOOL'] == r[0], 'CODICETOOL'] = r[2]

mrg.loc[mrg['CODICETOOL'] == 'MASATE', 'CODICE'] = np.nan

#set codice column as string
mrg['CODICE'] = [str(int(x)) if x ==x and isinstance(x, float) else x for x in mrg['CODICE']]

# %% Select

#select points with FALDA = 1 or "SUPERFICIALE"
mrg['FALDA'].unique()
sel = ['1', 'SUPERF. (acquifero locale)', 'SUPERF.', 'MISTA', 'MISTA 1/2', 1.0]
mrgsel = mrg.loc[mrg['FALDA'].isin(sel), :]

# %% Clean head4

head4.columns = meta4.reset_index().set_index('CODICE').loc[head4.columns, 'CODICETOOL']

# %% Merge ts

hmrg = dw.joincolumns((head1.merge(head2, left_index = True, right_index = True, how = 'outer')))
hmrg = dw.joincolumns((hmrg.merge(head3, left_index = True, right_index = True, how = 'outer')))
hmrg = dw.joincolumns((hmrg.merge(head4, left_index = True, right_index = True, how = 'outer')))

# %% Clean

mergecols = [
 ['COREZZANA0150920003', 'CORREZZANA', 'CORREZZANA0150920003'],
 ['RONCO BR.0151870001', 'RONCO BRIANTINO', 'RONCO BRIANTINO0151870001'],
 ['SAN  DONATO0151920005', 'SAN DONATO', 'SAN DONATO0151920005'],
 ['S.GIULIANO0151950015', 'SAN GIULIANO', 'SAN GIULIANO0151950015'],
 ['CAVENAGO ADDA0980170064', "CAVENAGO D'ADDA", "CAVENAGO D'ADDA0980170064"],
 ['CAVENAGO ADDA0980170003', "CAVENAGO D'ADDA", "CAVENAGO D'ADDA0980170003"],
 ['CERVIGNANO A.0980180001', "CERVIGNANO D'ADDA", "CERVIGNANO D'ADDA0980180001"],
 ['S.ROCCO AL PORTO0980490001', 'SAN ROCCO AL PORTO', 'SAN ROCCO AL PORTO0980490001'],
 ['S.ANGELO LODIGIANO0980500005', "SANT'ANGELO LODIGIANO", "SANT'ANGELO LODIGIANO0980500005"],
 ['VIGAVANO47', '', 'VIGEVANO47'],
 ['TREZZANO S/N0152210001', '', 'TREZZANO SUL NAVIGLIO0152210001',]
 ]

hmrg.rename(columns = {'MASATE0codice_mancante_1': 'MASATE'}, inplace = True)

for r in mergecols:
    hmrg = dw.join_twocols(hmrg, cols = [r[2], r[0]])

hmrgsel = hmrg[mrgsel['CODICETOOL']]    

# %% Save

mrg.to_csv(os.path.join(folder, 'meta_PTUA2003.csv'))
hmrg.to_csv(os.path.join(folder, 'head_PTUA2003.csv'))

mrgsel.to_csv(os.path.join(folder, 'meta_PTUA2003_FALDA1.csv'))
hmrgsel.to_csv(os.path.join(folder, 'head_PTUA2003_FALDA1.csv'))


