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


# pulire i csv: togliere le colonne in più
#metterli nel formato per ts ma mantenere anche i metadati
# così è possibile confrontare con metadai_ISS