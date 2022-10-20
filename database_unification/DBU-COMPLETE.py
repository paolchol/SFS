# -*- coding: utf-8 -*-
"""
All the DBU operations in a single script

No visualizations
No comments
Only saved at the end as DBU-COMPLETE.csv

@author: paolo
"""

# %% Setup

import dataanalysis as da
import datawrangling as dw
import geodata as gd
import numpy as np
import pandas as pd

# %% Load

code_db = pd.read_csv('data/general/codes_SIF_PP.csv')

# %% Tool functions

def identify_couples_codes(codes_SIF_PP, metaDBU, meta, xcol, ycol):
    #Search by code
    sifpp = codes_SIF_PP.loc[codes_SIF_PP['CODICE_SIF'].isin(meta.index), ['CODICE_SIF', 'CODICE_PP']]
    sifpp.set_index('CODICE_SIF', inplace = True)

    #Search by position
    #Transform meta's coordinates: from Monte Mario to WGS84
    meta['lat'], meta['lon'] = gd.transf_CRS(meta.loc[:, xcol], meta.loc[:, ycol], 'EPSG:3003', 'EPSG:4326', series = True)
    db_nrst = gd.find_nearestpoint(meta, metaDBU,
                         id1 = 'CODICE', coord1 = ['lon', 'lat'],
                         id2 = 'CODICE', coord2 = ['lon', 'lat'],
                         reset_index = True)
    #Select the points with distance less than 100 meters
    db_nrst = db_nrst[db_nrst['dist'] < 100]

    #Merge the two code lists
    codelst = pd.merge(sifpp, db_nrst.loc[:, ['CODICE', 'CODICE_nrst']], how = 'outer', left_index = True, right_on = 'CODICE')
    codelst.reset_index(inplace = True, drop = True)
    codelst.loc[np.invert(codelst['CODICE_nrst'].isna()), 'CODICE_PP'] = codelst.loc[np.invert(codelst['CODICE_nrst'].isna()), 'CODICE_nrst']
    codelst.drop(columns = 'CODICE_nrst', inplace = True)
    codelst.rename(columns = {'CODICE': 'CODICE_SIF', 'CODICE_PP': 'CODICE_link'}, inplace = True)
    codelst.set_index('CODICE_SIF', inplace = True)
    return codelst

# %% DBU-1
#PTUA2003

metaDBU = pd.read_csv('data/PTUA2022/meta_PTUA2022.csv', index_col = 'CODICE')
meta = pd.read_csv('data/PTUA2003/meta_PTUA2003_TICINOADDA_filtered.csv', index_col = 'CODICE')
idx = meta.index.isin(code_db['CODICE_SIF'])
sifpp = code_db.loc[code_db['CODICE_SIF'].isin(meta.index), ['CODICE_SIF', 'CODICE_PP']]
sifpp.set_index('CODICE_SIF', inplace = True)

idx = meta.loc[:, ['x', 'y']].isna().apply(all, 1)
tool2003 = meta.drop(meta.index[idx]).copy()
tool2003 = tool2003.loc[tool2003['FALDA'].isin(['1', np.nan, 'SUPERF.', 'SUPERF. (acquifero locale)']), :]
out = gd.transf_CRS(tool2003.loc[:, 'x'], tool2003.loc[:, 'y'], 'EPSG:3003', 'EPSG:4326', series = True)
tool2003['lat'], tool2003['lon'] = out[0], out[1]
out = gd.transf_CRS(metaDBU.loc[:, 'X_WGS84'], metaDBU.loc[:, 'Y_WGS84'], 'EPSG:32632', 'EPSG:4326', series = True)
metaDBU['lat'], metaDBU['lon'] = out[0], out[1]
db_nrst = gd.find_nearestpoint(tool2003, metaDBU,
                     id1 = 'CODICE', coord1 = ['lon', 'lat'],
                     id2 = 'CODICE', coord2 = ['lon', 'lat'],
                     reset_index = True)
db_nrst = db_nrst[db_nrst['dist'] < 100]
codelst = pd.merge(sifpp, db_nrst.loc[:, ['CODICE', 'CODICE_nrst']], how = 'outer', left_index = True, right_on = 'CODICE')
codelst.reset_index(inplace = True, drop = True)
codelst.loc[np.invert(codelst['CODICE_nrst'].isna()), 'CODICE_PP'] = codelst.loc[np.invert(codelst['CODICE_nrst'].isna()), 'CODICE_nrst']
codelst.drop(columns = 'CODICE_nrst', inplace = True)
codelst.rename(columns = {'CODICE': 'CODICE_PTUA2003', 'CODICE_PP': 'CODICE_link'}, inplace = True)
codelst.set_index('CODICE_PTUA2003', inplace = True)

sum(codelst.isin(metaDBU.index).values)
metamerge = dw.mergemeta(metaDBU, meta, link = codelst,
                    firstmerge = dict(left_index = True, right_index = True),
                    secondmerge = dict(left_index = True, right_on = 'CODICE_link',
                                       suffixes = [None, "_PTUA2003"]))
metamerge.rename(columns = {'CODICE_link': 'CODICE', 'index': 'CODICE_PTUA2003'}, inplace = True)
metamerge.set_index('CODICE', inplace = True)
metamerge.drop(columns = ['SETTORE', 'STRAT.', 'PROVINCIA_PTUA2003', 'x', 'y', 'COMUNE_PTUA2003'], inplace = True)
metamerge.rename(columns = {'z': 'z_PTUA2003', 'INFO': 'INFO_PTUA2003'}, inplace = True)

#Split SIF code from the other PTUA2003 codes
codePTUA = []
codeSIF = []
for code in metamerge['CODICE_PTUA2003']:
    if isinstance(code, str):
        if (code[0:3] == '015') | (code[0:3] == '098'):
            codeSIF += [code]
            codePTUA += [np.nan]
        else:
            codeSIF += [np.nan]
            codePTUA += [code]
    else:
        codePTUA += [np.nan]
        codeSIF += [np.nan]
metamerge['CODICE_PTUA2003'] = codePTUA
metamerge.insert(0, column = 'CODICE_SIF', value = codeSIF)
metamerge = dw.join_twocols(metamerge, cols = ['ORIGINE', 'ORIGINE_PTUA2003'], onlyna = False, add = True)

headDBU = pd.read_csv('data/PTUA2022/head_IT03GWBISSAPTA.csv', index_col='DATA')
head = pd.read_csv('data/PTUA2003/head_PTUA2003_TICINOADDA_unfiltered.csv', index_col='DATA')
headDBU.index = pd.DatetimeIndex(headDBU.index)
head.index = pd.DatetimeIndex(head.index)

codes = metamerge.loc[metamerge['BACINO_WISE'] == 'IT03GWBISSAPTA', 'CODICE_PTUA2003'].dropna()
codes = pd.concat([codes, metamerge.loc[metamerge['BACINO_WISE'] == 'IT03GWBISSAPTA', 'CODICE_SIF'].dropna()])
headmerge = dw.mergets(headDBU, head, codes)

# %% DBU-2
#CAP

meta = pd.read_csv('data/CAP/meta_CAP.csv', index_col = 'CODICE')
meta.index = [f"0{int(idx)}" if not np.isnan(idx) else np.nan for idx in meta.index]
meta.index.names = ['CODICE']
head = pd.read_csv('data/CAP/head_CAP.csv', index_col = 'DATA')
head.index = pd.DatetimeIndex(head.index)

codelst = identify_couples_codes(code_db, metamerge, meta, 'X', 'Y')

metamerge = dw.mergemeta(metamerge, meta, link = codelst,
                    firstmerge = dict(left_index = True, right_index = True),
                    secondmerge = dict(left_index = True, right_on = 'CODICE_link',
                                       suffixes = [None, "_CAP"]))
metamerge.rename(columns = {'CODICE_link': 'CODICE'}, inplace = True)
metamerge.set_index('CODICE', inplace = True)
metamerge.drop(columns = ['COMUNE_CAP', 'X', 'Y', 'lat_CAP', 'lon_CAP'], inplace = True)
metamerge.insert(24, 'z_CAP', metamerge['Z'])
metamerge.drop(columns = 'Z', inplace = True)

metamerge.rename(columns = {'CODICE_SIF': 'CODICE_SIF_keep', 'index': 'CODICE_SIF_remove'}, inplace = True)
metamerge = dw.joincolumns(metamerge, '_keep', '_remove')
metamerge = dw.join_twocols(metamerge, ['ORIGINE', 'ORIGINE_CAP'], add = True, onlyna = False)

to_insert = pd.read_csv('data/CAP/DBU-2_joinQGIS_PROV.csv')
to_insert['CODICE'] = [f"0{int(idx)}" if not np.isnan(idx) else np.nan for idx in to_insert['CODICE']]
to_insert.drop(columns = ['NOME_CI', 'SHAPE_AREA'], inplace = True)
to_insert['X'], to_insert['Y'] = gd.transf_CRS(to_insert.loc[:, 'X'], to_insert.loc[:, 'Y'], 'EPSG:3003', 'EPSG:32632', series = True)
to_insert.rename(columns = {'COD_PTUA16': 'BACINO_WISE',
                            'CODICE': 'CODICE_SIF',
                            'X': 'X_WGS84',
                            'Y': 'Y_WGS84',
                            'Z': 'QUOTA_MISU',
                            'SIGLA': 'PROVINCIA'}, inplace = True)
to_insert['CODICE'] = [f"CAP-{idx}" for idx in to_insert['CODICE_SIF']]
to_insert['z_CAP'] = to_insert['QUOTA_MISU']
metamerge = pd.merge(metamerge, to_insert, how = 'outer', left_index = True, right_on = 'CODICE')
metamerge = dw.joincolumns(metamerge)
metamerge.set_index('CODICE', inplace = True)

# %% DBU-3
#Idroscalo2005

meta = pd.read_csv('data/Idroscalo2005/meta_Idroscalo2005.csv', index_col = 'CODICE')
head = pd.read_csv('data/Idroscalo2005/head_Idroscalo2005.csv', index_col = 'DATA')

meta.index = [f"0{idx}" for idx in meta.index]
meta.index.names = ['CODICE']
head.index = pd.DatetimeIndex(head.index)

codelst = identify_couples_codes(code_db, metamerge, meta, 'x', 'y')

metamerge = dw.mergemeta(metamerge, meta, link = codelst,
                    firstmerge = dict(left_index = True, right_index = True),
                    secondmerge = dict(left_index = True, right_on = 'CODICE_link',
                                       suffixes = [None, "_I2005"]))
metamerge.rename(columns = {'CODICE_link': 'CODICE', 'index': 'CODICE_SIF_I2005'}, inplace = True)
metamerge.set_index('CODICE', inplace = True)

metamerge.insert(25, 'z_I2005', metamerge['z'])
metamerge.drop(columns = ['z', 'x', 'y', 'lat_I2005', 'lon_I2005'], inplace = True)
metamerge = dw.join_twocols(metamerge, ['ORIGINE', 'ORIGINE_I2005'], add = True, onlyna = False)
metamerge = dw.join_twocols(metamerge, ['CODICE_SIF', 'CODICE_SIF_I2005'])
metamerge = dw.joincolumns(metamerge, '_dbu', '_I2005')

idx = (metamerge['CODICE_SIF'].isin(meta.index)) & (metamerge['BACINO_WISE'] == 'IT03GWBISSAPTA')
codes = metamerge.loc[idx, 'CODICE_SIF']
headmerge = dw.mergets(headmerge, head, codes)

#add lake
lake_mt = pd.read_csv('data/Idroscalo2005/meta_lake.csv')
lake_ts = pd.read_csv('data/Idroscalo2005/lake_levels_monthly.csv', index_col = 'DATA')
lake_ts.index =  pd.DatetimeIndex(lake_ts.index)

lake_mt['X_WGS84'], lake_mt['Y_WGS84'] = gd.transf_CRS(lake_mt['lat'], lake_mt['lon'], 'EPSG:4326', 'EPSG:32632', series = True)

metamerge = pd.merge(metamerge, lake_mt, how = 'outer', left_index = True, right_on = 'CODICE')
metamerge = dw.joincolumns(metamerge)
metamerge.set_index('CODICE', inplace = True)

# %% DBU-4
#Olona

meta = pd.read_csv('data/Olona/meta_Olona_filtered.csv', index_col = 'CODICE')
head = pd.read_csv('data/Olona/head_Olona.csv', index_col = 'DATA')
head.index = pd.DatetimeIndex(head.index)

codelst = identify_couples_codes(code_db, metamerge, meta, 'X', 'Y')

metamerge = dw.mergemeta(metamerge, meta, link = codelst,
                    firstmerge = dict(left_index = True, right_index = True),
                    secondmerge = dict(left_index = True, right_on = 'CODICE_link',
                                       suffixes = ['_DBU', "_Olona"]))
metamerge.rename(columns = {'CODICE_link': 'CODICE', 'index': 'CODICE_Olona'}, inplace = True)
metamerge.set_index('CODICE', inplace = True)

metamerge.insert(26, 'z_Olona', metamerge['Q'])
#CODICE_Olona doesn't provide additional information, thus it's dropped
todrop = ['Q', 'DENOMINAZIONE', 'PUBBLICO', 'STRAT.', 'X', 'Y', 'PROV', 'CODICE_Olona']
metamerge.drop(columns = todrop, inplace = True)

metamerge = dw.join_twocols(metamerge, ["INDIRIZZO", "LOCALITA'"], rename = "INFO", add = True, onlyna = False)
metamerge = dw.join_twocols(metamerge, ["INFO_PTUA2003", "INFO"], rename = "INFO", add = True, onlyna = False)
metamerge = dw.join_twocols(metamerge, ["ORIGINE_DBU", "ORIGINE_Olona"], rename = "ORIGINE", add = True, onlyna = False)
metamerge = dw.joincolumns(metamerge, '_DBU', '_Olona')

codelst = codelst.loc[codelst.isin(metamerge.index).values, :]
idx = metamerge['BACINO_WISE'] == 'IT03GWBISSAPTA'
codelst = codelst.loc[codelst['CODICE_link'].isin(metamerge.loc[idx, :].index), :]

codelst.reset_index(drop = False, inplace = True)
codelst.set_index('CODICE_link', inplace = True)
codelst = codelst.squeeze()
headmerge = dw.mergets(headmerge, head, codelst)

# %% DBU-5

meta = pd.read_csv('data/Milano1950/meta_Milano1950.csv', index_col = 'CODICE')
head = pd.read_csv('data/Milano1950/head_Milano1950.csv', index_col = 'DATA')
head.index = pd.DatetimeIndex(head.index)

codelst = identify_couples_codes(code_db, metamerge, meta, 'Coord_X', 'Coord_Y')

metamerge = dw.mergemeta(metamerge, meta, link = codelst,
                    firstmerge = dict(left_index = True, right_index = True),
                    secondmerge = dict(left_index = True, right_on = 'CODICE_link',
                                       suffixes = ['_DBU', '_Milano1950']))
metamerge.rename(columns = {'CODICE_link': 'CODICE', 'index': 'CODICE_Milano1950'}, inplace = True)
metamerge.set_index('CODICE', inplace = True)
metamerge.insert(27, 'z_Milano1950', metamerge['RIFERIMENTO'])
metamerge.insert(3, 'CODICE_FOG', metamerge['PIEZOMETRO'])
todrop = ['RIFERIMENTO', 'Coord_X', 'Coord_Y', 'PIEZOMETRO']
metamerge.drop(columns = todrop, inplace = True)
dw.join_twocols(metamerge, ['INFO', 'UBICAZIONE'], rename = "INFO", onlyna = False, add = True, inplace = True)
dw.join_twocols(metamerge, ['CODICE_SIF', 'CODICE_Milano1950'], inplace = True)
dw.join_twocols(metamerge, ['ORIGINE_DBU', 'ORIGINE_Milano1950'], rename = 'ORIGINE', onlyna = False, add = True, inplace = True)
metamerge = dw.joincolumns(metamerge, '_DBU', '_Milano1950')

codelst = codelst.loc[codelst.isin(metamerge.index).values, :]
idx = metamerge['BACINO_WISE'] == 'IT03GWBISSAPTA'
codelst = codelst.loc[codelst['CODICE_link'].isin(metamerge.loc[idx, :].index), :]

codelst.reset_index(drop = False, inplace = True)
codelst.set_index('CODICE_link', inplace = True)
codelst = codelst.squeeze()
headmerge = dw.mergets(headmerge, head, codelst)

notmrg = head.loc[:, np.invert(head.columns.isin(codelst))].copy()
remove = ['SIF_NA']
notmrg.drop(columns = remove, inplace = True)

points = pd.read_csv('data/Milano1950/DBU-5_leftout_joinQGIS.csv')
points.drop(columns = points.columns[np.invert(points.columns.isin(['CODICE', 'COD_PTUA16']))], inplace = True)
points['CODICE'] = [f"0{int(idx)}" if not np.isnan(idx) else np.nan for idx in points['CODICE']]

#create a new df with the informations of points left out, then join with the basin code found
to_insert = meta.loc[meta.index.isin(notmrg.columns), : ].copy()
to_insert = to_insert.loc[np.invert(to_insert.index.isin(metamerge['CODICE_SIF'])), :]
to_insert = pd.merge(to_insert, points, how = 'inner', left_index = True, right_on = 'CODICE')

#merge to_insert with metamerge
to_insert.reset_index(drop = True, inplace = True)
to_insert['Coord_X'], to_insert['Coord_Y'] = gd.transf_CRS(to_insert.loc[:, 'Coord_X'], to_insert.loc[:, 'Coord_Y'], 'EPSG:3003', 'EPSG:32632', series = True)
to_insert.rename(columns = {'COD_PTUA16': 'BACINO_WISE',
                            'CODICE': 'CODICE_SIF',
                            'PIEZOMETRO': 'CODICE',
                            'Coord_X': 'X_WGS84',
                            'Coord_Y': 'Y_WGS84',
                            'UBICAZIONE': 'INFO',
                            'RIFERIMENTO': 'QUOTA_MISU'}, inplace = True)
to_insert['PROVINCIA'] = 'MI'
to_insert['CODICE_FOG'] = to_insert['CODICE']

metamerge = pd.merge(metamerge, to_insert, how = 'outer', left_index = True, right_on = 'CODICE')
metamerge = dw.joincolumns(metamerge)
metamerge.set_index('CODICE', inplace = True)

idx = metamerge.loc[to_insert['CODICE'], 'BACINO_WISE'] == 'IT03GWBISSAPTA'
codes = to_insert.loc[idx.values, 'CODICE_SIF']
codes.index = to_insert.loc[idx.values, 'CODICE']

headmerge = dw.mergets(headmerge, head, codes)

#%% DBU-6

meta = pd.read_csv('data/SSGiovanni/meta_SSGiovanni.csv', index_col = 'CODICE')
head = pd.read_csv('data/SSGiovanni/head_SSGiovanni.csv', index_col = 'DATA')
meta.index = [f"0{int(idx)}" if not np.isnan(idx) else np.nan for idx in meta.index]
meta.index.names = ['CODICE']
head.index = pd.DatetimeIndex(head.index)

codelst = identify_couples_codes(code_db, metamerge, meta, 'X_GB', 'Y_GB')

to_insert = pd.read_csv('data/SSGiovanni/DBU-6_joinQGIS.csv', index_col = 'CODICE')
to_insert.reset_index(inplace = True)
to_insert['CODICE'] = [f"0{int(idx)}" if not np.isnan(idx) else np.nan for idx in to_insert['CODICE']]
to_insert.drop(columns = ['NOME_CI', 'SHAPE_AREA', 'TIPOLOGIA'], inplace = True)
to_insert['X_GB'], to_insert['Y_GB'] = gd.transf_CRS(to_insert.loc[:, 'X_GB'], to_insert.loc[:, 'Y_GB'], 'EPSG:3003', 'EPSG:32632', series = True)
to_insert.rename(columns = {'COD_PTUA16': 'BACINO_WISE',
                            'CODICE': 'CODICE_SIF',
                            'COD_LOCALE': 'CODICE',
                            'X_GB': 'X_WGS84',
                            'Y_GB': 'Y_WGS84',
                            'QUOTA': 'QUOTA_MISU',
                            'FILTRO_DA': 'FILTRI_TOP',
                            'FILTRO_A': 'FILTRI_BOT',
                            'PROF': 'PROFONDITA'}, inplace = True)
to_insert = dw.join_twocols(to_insert, ['TIPO', 'COMPARTO'], rename = "INFO", add = True, onlyna = False)
to_insert['PROVINCIA'] = 'MI'
to_insert['COMUNE'] = 'SESTO SAN GIOVANNI'
to_insert['CODICE_SSG'] = to_insert['CODICE']

metamerge = pd.merge(metamerge, to_insert, how = 'outer', left_index = True, right_on = 'CODICE')
metamerge = dw.joincolumns(metamerge)
metamerge.set_index('CODICE', inplace = True)

tool = metamerge['CODICE_SSG'].copy()
metamerge.drop(columns = 'CODICE_SSG', inplace = True)
metamerge.insert(3, 'CODICE_SSG', tool)

idx = metamerge.loc[to_insert['CODICE'], 'BACINO_WISE'] == 'IT03GWBISSAPTA'
codes = to_insert.loc[idx.values, 'CODICE_SIF']
codes.index = to_insert.loc[idx.values, 'CODICE']

headmerge = dw.mergets(headmerge, head, codes)

# %% Final operations



# %% Export

metamerge.to_csv('data/results/db-unification/meta_DBU-COMPLETE.csv')
headmerge.to_csv('data/results/db-unification/head_DBU-COMPLETE.csv')


# test = pd.read_csv('data/results/db-unification/head_DBU-6.csv', index_col = 'DATA')
# test['CODICE_SIF'] = [f"0{int(idx)}" if not np.isnan(idx) else np.nan for idx in test['CODICE_SIF']]
