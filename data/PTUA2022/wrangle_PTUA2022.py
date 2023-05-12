# -*- coding: utf-8 -*-
"""
Ottieni un unico dataset dai dati del PTUA2022 suddivisi per bacino

"""

# %% Setup

import glob
import pandas as pd
import numpy as np

# %% Load excel and save only the needed sheet as csv

lst = glob.glob('data/PTUA2022/original/PTUA2022_single_acquifer/*.xlsx')
lst = [path.replace('\\', '/') for path in lst]

for path in lst:
    try:
        sheet = pd.read_excel(path, 'PIEZOMETRIE')
        acquifer = path.split('/')[-1].split('.')[0].split('_')[0]
    except:
        try:
            sheet = pd.read_excel(path, 'Piezometrie')
            acquifer = 'IT03AL'
        except:
            sheet = pd.read_excel(path, 'PIEZOMETRIA')
            acquifer = path.split('/')[-1].split('.')[0].split('_')[0]
    sheet.to_csv(f"data/PTUA2022/original/PTUA2022_single_acquifer_csv/{acquifer}.csv")

# %% Unify all the head data

lst = glob.glob('data/PTUA2022/original/PTUA2022_single_acquifer_csv/*.csv')
lst = [path.replace('\\', '/') for path in lst]
lst = [x for x in lst if 'ISS' in (x)]

cols = ['CODICE PUNTO', 'DATA','PIEZOMETRIA [m s.l.m.]']

#aggiungi soggiacenza

df = pd.DataFrame()

for path in lst:
    tool = pd.read_csv(path)
    tool = tool[cols]
    df = pd.concat([df, tool])

df.dropna().to_csv('data/PTUA2022/original/piezometria_ISS.csv', index = False)

