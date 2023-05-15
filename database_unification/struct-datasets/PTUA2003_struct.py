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

mrg['ORIGINE'] = 'PTUA2003'

test = mrg.loc[mrg['CODICE'].duplicated(False), :]
seldrop = [] #fare elenco manuale di CODICETOOL da rimuovere
test.drop(seldrop)

# %% Select

#select points with FALDA = 1 or "SUPERFICIALE"
mrg['FALDA'].unique()
sel = ['1', 'SUPERF. (acquifero locale)', 'SUPERF.', 'MISTA', 'MISTA 1/2', 1.0]
mrgsel = mrg.loc[mrg['FALDA'].isin(sel), :]

# %% Merge ts

hmrg = dw.joincolumns((head1.merge(head2, left_index = True, right_index = True, how = 'outer')))
hmrg = dw.joincolumns((hmrg.merge(head3, left_index = True, right_index = True, how = 'outer')))
hmrg = dw.joincolumns((hmrg.merge(head4, left_index = True, right_index = True, how = 'outer'))) #codicetool

