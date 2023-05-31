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

def identify_couples_codes(codes_SIF_PP, metaDBU, meta, xcol, ycol,
                           id1 = 'CODICE', retdist = False):
    #Search by code
    sifpp = codes_SIF_PP.loc[codes_SIF_PP['CODICE_SIF'].isin(meta.index), ['CODICE_SIF', 'CODICE_PP']]
    sifpp.set_index('CODICE_SIF', inplace = True)

    #Search by position
    #Transform meta's coordinates: from Monte Mario to WGS84
    meta['lat'], meta['lon'] = gd.transf_CRS(meta.loc[:, xcol], meta.loc[:, ycol], 'EPSG:3003', 'EPSG:4326', series = True)
    #Find the nearest point
    db_nrst = gd.find_nearestpoint(meta, metaDBU,
                         id1 = id1, coord1 = ['lon', 'lat'],
                         id2 = 'CODICE', coord2 = ['lon', 'lat'],
                         reset_index = True)
    #Select the points with distance less than 100 meters
    db_nrst = db_nrst[db_nrst['dist'] < 100]

    #Merge the two code lists
    codelst = pd.merge(sifpp, db_nrst.loc[:, [id1, 'CODICE_nrst']], how = 'outer', left_index = True, right_on = id1)
    codelst.reset_index(inplace = True, drop = True)
    codelst.loc[np.invert(codelst['CODICE_nrst'].isna()), 'CODICE_PP'] = codelst.loc[np.invert(codelst['CODICE_nrst'].isna()), 'CODICE_nrst']
    codelst.drop(columns = 'CODICE_nrst', inplace = True)
    codelst.rename(columns = {id1: 'CODICE_SIF', 'CODICE_PP': 'CODICE_link'}, inplace = True)
    codelst.set_index('CODICE_SIF', inplace = True)
    if retdist: 
        return codelst, db_nrst
    else:
        return codelst

# %% Load and clean the original dataset (PTUA2022)

metaDBU = pd.read_csv('data/PTUA2022/meta_PTUA2022_ISS.csv', index_col = 'CODICE')
# Rename QUOTA_MISU
metaDBU.rename(columns = {'QUOTA_MISURA_SLM (Qr)': 'QUOTA_MISU'}, inplace = True)
# - Set QUOTA_MISU equal to QUOTA_PC_S if missing
idx = (metaDBU['QUOTA_MISU'] == 0) | metaDBU['QUOTA_MISU'].isna()
metaDBU.loc[idx, 'QUOTA_MISU'] = metaDBU.loc[idx, 'QUOTA_PC_SLM']
#drop remaining points with no z field associated
idx = (metaDBU['QUOTA_MISU'] == 0) | metaDBU['QUOTA_MISU'].isna()
metaDBU.drop(index = metaDBU.loc[idx, :].index, inplace = True)
# obtain latitude and longitude
metaDBU['lat'], metaDBU['lon'] = gd.transf_CRS(metaDBU.loc[:, 'X_WGS84'], metaDBU.loc[:, 'Y_WGS84'], 'EPSG:32632', 'EPSG:4326', series = True)
# load time series
headDBU = pd.read_csv('data/PTUA2022/head_PTUA2022_ISS.csv', index_col='DATA', parse_dates = True)

metaDBU = metaDBU.loc[metaDBU.index.isin(headDBU.columns), :]
headDBU = headDBU.loc[:, headDBU.columns.isin(metaDBU.index)]

# %% DBU-1
#PTUA2003

#Carica metadati PTUA2003
meta = pd.read_csv('data/PTUA2003/wrangled/meta_PTUA2003_FALDA1_QGIS.csv', index_col = 'CODICE')

#Associazione dei codici di metaDBU e meta: codici comuni, posizione
meta.index = [f'0{code}' if (len(str(code))>6) and (code[0] != 'P') else code for code in meta.index]
meta.index.names = ['CODICE']
codelst = identify_couples_codes(code_db, metaDBU, meta, 'X', 'Y', id1 = 'CODICETOOL')
meta.reset_index(drop = False, inplace = True)
meta.set_index('CODICETOOL', inplace = True)

#Merge dei due database
metamerge = dw.mergemeta(metaDBU, meta, link = codelst,
                         firstmerge = dict(left_index = True, right_index = True),
                         secondmerge = dict(left_index = True, right_on = 'CODICE_link',
                                            suffixes = [None, "_PTUA2003"]))
metamerge.rename(columns = {'index': 'CODICETOOL'}, inplace = True)
metamerge.rename(columns = {'CODICE': 'CODICE_PTUA2003'}, inplace = True) #CODICE arriva da PTUA2003 ora
metamerge.rename(columns = {'CODICE_link': 'CODICE'}, inplace = True) #CODICE_link è il CODICE corretto
metamerge.set_index('CODICE', inplace = True)
#Rimozione colonne non necessarie
metamerge.drop(columns = metamerge.columns[51:71], inplace = True)
metamerge.drop(columns = ['PROV', 'COMUNE_PTUA2003', 'X', 'Y', 'SETTORE',
                                        'L.S.', 'L.D.', 'Q.1', 'ANNO', 'COD_PTUA16',
                                        'lat_PTUA2003', 'lon_PTUA2003'], inplace = True)
#Rinominazione colonne
metamerge.rename(columns = {'PROFONDITà': 'PROFONDITA', 'Q': 'z_PTUA2003',
                            "PROFONDITA' COLONNA": "PROFONDITA_PTUA2003", 'STRAT.': 'STRAT'}, inplace = True)
#Merge colonne duplicate
metamerge = dw.join_twocols(metamerge, cols = ['ORIGINE', 'ORIGINE_PTUA2003'], onlyna = False, add = True)
metamerge = dw.join_twocols(metamerge, cols = ['PROFONDITA', 'PROFONDITA_PTUA2003'])
metamerge = dw.join_twocols(metamerge, cols = ['FILTRI_TOP', 'INZ_FLT'])
metamerge = dw.join_twocols(metamerge, cols = ['FILTRI_BOTTOM', 'FIN_FLT'])
metamerge = dw.join_twocols(metamerge, cols = ['NOTE', 'NOTE_PTUA2003'], onlyna = False, add = True)
metamerge = dw.join_twocols(metamerge, cols = ['NOTE', 'DENOMINAZIONE'], onlyna = False, add = True)
metamerge = dw.join_twocols(metamerge, cols = ['NOTE', 'LOCALITA'], onlyna = False, add = True)
#Print colonne rimaste
# dw.print_colN(metamerge)
#Rimozione di eventuali codici duplicati
metamerge = metamerge[~metamerge.index.duplicated(keep = 'first')] #Torrazza era duplicato

#Carica serie storiche PTUA2003
head = pd.read_csv('data/PTUA2003/wrangled/head_PTUA2003_FALDA1.csv', index_col='DATA', parse_dates = True)
#Ottieni codici per far comunicare i due database
codes = metamerge['CODICETOOL'].dropna()

#Ottieni database con quota corretta
headcorr = da.correct_quota(meta, head, metamerge, codes, quotacols = ['Q', 'QUOTA_MISU'])
#Merge dei database (senza correzione, con correzione)
headmerge, rprtmerge = dw.mergets(headDBU, head, codes, report = True, tag = 'PTUA2003')
hmrgcorr = dw.mergets(headDBU, headcorr, codes)

#Rimozione colonna CODICETOOL
metamerge.drop(columns = 'CODICETOOL', inplace = True)

#Salvataggio intermedio
# metamerge.to_csv('data/results/db-unification/DBU-steps/meta_DBU-1.csv')
# hmrgcorr.to_csv('data/results/db-unification/DBU-steps/head_DBU-1.csv')

# %% DBU-2
#CAP

meta = pd.read_csv('data/CAP/meta_CAP_QGIS.csv', index_col = 'CODICE')
meta.index = [f"0{int(idx)}" if not np.isnan(idx) else np.nan for idx in meta.index]
meta.index.names = ['CODICE']
head = pd.read_csv('data/CAP/head_CAP.csv', index_col = 'DATA')
head.index = pd.DatetimeIndex(head.index)

codelst, db_nrst = identify_couples_codes(code_db, metamerge, meta, 'X', 'Y', retdist = True)
# u, c = np.unique(codelst.values, return_counts=True)
# dup = u[c > 1]
# check = min(db_nrst.loc[db_nrst['CODICE_nrst'] == dup[0], 'dist'])
# idx = db_nrst.loc[(db_nrst['CODICE_nrst'] == dup[0]) & (db_nrst['dist'] == check), 'CODICE'].values[0]
# tool = codelst[(codelst.values == dup[0])]
# tool = tool[tool.index != idx].index
# codelst.drop(tool, inplace = True)

metamerge = dw.mergemeta(metamerge, meta, link = codelst,
                    firstmerge = dict(left_index = True, right_index = True),
                    secondmerge = dict(left_index = True, right_on = 'CODICE_link',
                                       suffixes = [None, "_CAP"]))
metamerge.rename(columns = {'CODICE_link': 'CODICE'}, inplace = True)
metamerge.set_index('CODICE', inplace = True)
metamerge.drop(columns = ['COMUNE_CAP', 'X', 'Y', 'lat_CAP', 'lon_CAP'], inplace = True)
metamerge.insert(36, 'z_CAP', metamerge['Z'])
metamerge.drop(columns = 'Z', inplace = True)

metamerge.rename(columns = {'index': 'CODICE_SIF'}, inplace = True)
# metamerge = dw.joincolumns(metamerge, '_keep', '_remove')
metamerge = dw.join_twocols(metamerge, ['ORIGINE', 'ORIGINE_CAP'], add = True, onlyna = False)

#Inserisci le serie che non sono state aggiunte come serie ex-novo
to_insert = meta.loc[np.invert(meta.index.isin(metamerge['CODICE_SIF'])), :].copy()
to_insert.reset_index(drop = False, inplace = True)
# to_insert = pd.read_csv('data/CAP/DBU-2_joinQGIS_PROV.csv')
# to_insert['CODICE'] = [f"0{int(idx)}" if not np.isnan(idx) else np.nan for idx in to_insert['CODICE']]
# to_insert.drop(columns = ['NOME_CI', 'SHAPE_AREA'], inplace = True)
to_insert['X'], to_insert['Y'] = gd.transf_CRS(to_insert.loc[:, 'X'], to_insert.loc[:, 'Y'], 'EPSG:3003', 'EPSG:32632', series = True)
to_insert.rename(columns = {'COD_PTUA16': 'BACINO_WISE',
                            'CODICE': 'CODICE_SIF',
                            'X': 'X_WGS84',
                            'Y': 'Y_WGS84',
                            'Z': 'QUOTA_MISU',
                            'SIGLA': 'PROVINCIA'}, inplace = True)
to_insert['CODICE'] = [f"CAP-{idx}" for idx in to_insert['CODICE_SIF']]
to_insert['z_CAP'] = to_insert['QUOTA_MISU']
#Merge dei metadati
metamerge = dw.joincolumns(pd.merge(metamerge, to_insert, how = 'outer', left_index = True, right_on = 'CODICE'))
metamerge.set_index('CODICE', inplace = True)
#Pulizia del dataset unificato
metamerge.loc[metamerge['INDIRIZZO'].notna(), 'INDIRIZZO'] = [' '.join(x.split()) for x in metamerge.loc[metamerge['INDIRIZZO'].notna(), 'INDIRIZZO'] if x == x]
metamerge = dw.join_twocols(metamerge, ['NOTE', 'INDIRIZZO'], onlyna = False, add=True)
metamerge = dw.join_twocols(metamerge, ['Codice WISE Corpo idrico', 'BACINO_WISE'])
metamerge = dw.join_twocols(metamerge, ['PROVINCIA', 'SIGLA'])
metamerge.drop(columns = 'COD_PTUA16', inplace = True)
metamerge['NOTE'] = [x if x != '' else np.nan for x in metamerge['NOTE']]

#Merge di tutte le serie storiche unite
head = pd.read_csv('data/CAP/head_CAP.csv', index_col='DATA', parse_dates = True)
#Ottieni codici per far comunicare i due database
codes = metamerge['CODICE_SIF'].dropna()

#Ottieni database con quota corretta
headcorr = da.correct_quota(meta, head, metamerge, codes, quotacols = ['Z', 'QUOTA_MISU'])
#Merge dei database (senza correzione, con correzione)
headmerge, rprt = dw.mergets(headDBU, head, codes, report = True, tag = 'CAP')
rprtmerge = dw.merge_rprt(rprtmerge.set_index('CODICE'), rprt.set_index('CODICE'))
hmrgcorr = dw.mergets(headDBU, headcorr, codes)

#Rimozione eventuali codici duplicati
metamerge = metamerge[~metamerge.index.duplicated(keep = 'first')]

#Salvataggio intermedio
# metamerge.to_csv('data/results/db-unification/DBU-steps/meta_DBU-2.csv')
# hmrgcorr.to_csv('data/results/db-unification/DBU-steps/head_DBU-2.csv')

# %% DBU-3
#Idroscalo2005

meta = pd.read_csv('data/Idroscalo2005/meta_Idroscalo2005.csv', index_col = 'CODICE')
head = pd.read_csv('data/Idroscalo2005/head_Idroscalo2005.csv', index_col = 'DATA')

meta.index = [f"0{idx}" for idx in meta.index]
meta.index.names = ['CODICE']
head.index = pd.DatetimeIndex(head.index)

codelst = identify_couples_codes(code_db, metamerge, meta, 'x', 'y')

#Merge dei metadati
metamerge = dw.mergemeta(metamerge, meta, link = codelst,
                    firstmerge = dict(left_index = True, right_index = True),
                    secondmerge = dict(left_index = True, right_on = 'CODICE_link',
                                       suffixes = [None, "_I2005"]))
metamerge.rename(columns = {'CODICE_link': 'CODICE', 'index': 'CODICE_SIF_I2005'}, inplace = True)
metamerge.set_index('CODICE', inplace = True)
#Pulisci il risultato
metamerge.insert(38, 'z_Idroscalo2005', metamerge['z'])
metamerge.drop(columns = ['z', 'x', 'y', 'lat_I2005', 'lon_I2005'], inplace = True)
metamerge = dw.join_twocols(metamerge, ['ORIGINE', 'ORIGINE_I2005'], add = True, onlyna = False)
metamerge = dw.join_twocols(metamerge, ['CODICE_SIF', 'CODICE_SIF_I2005'])
metamerge = dw.joincolumns(metamerge, '_dbu', '_I2005')

#Merge delle serie storiche
idx = metamerge['CODICE_SIF'].isin(meta.index)
codes = metamerge.loc[idx, 'CODICE_SIF']
headmerge, rprt = dw.mergets(headmerge, head, codes, report = True, tag = 'Idroscalo2005')
rprtmerge = dw.merge_rprt(rprtmerge, rprt.set_index('CODICE'))
headcorr = da.correct_quota(meta, head, metamerge, codes, quotacols = ['z', 'QUOTA_MISU'])
hmrgcorr = dw.mergets(hmrgcorr, headcorr, codes)

#Aggiunta serie storica lago Idroscalo
lake_mt = pd.read_csv('data/Idroscalo2005/meta_lake.csv')
lake_ts = pd.read_csv('data/Idroscalo2005/lake_levels_monthly.csv', index_col = 'DATA')
lake_ts.index =  pd.DatetimeIndex(lake_ts.index)
lake_ts.columns = ['idroscalo']
lake_mt['X_WGS84'], lake_mt['Y_WGS84'] = gd.transf_CRS(lake_mt['lat'], lake_mt['lon'], 'EPSG:4326', 'EPSG:32632', series = True)
metamerge = pd.merge(metamerge, lake_mt, how = 'outer', left_index = True, right_on = 'CODICE')
metamerge = dw.join_twocols(metamerge, ['Codice WISE Corpo idrico', 'BACINO_WISE'])
metamerge = dw.joincolumns(metamerge)
metamerge.set_index('CODICE', inplace = True)
headmerge = headmerge.merge(lake_ts, how = 'outer', left_index = True, right_index= True)
hmrgcorr = hmrgcorr.merge(lake_ts, how = 'outer', left_index = True, right_index= True)

#Salvataggio intermedio
# metamerge.to_csv('data/results/db-unification/DBU-steps/meta_DBU-3.csv')
# hmrgcorr.to_csv('data/results/db-unification/DBU-steps/head_DBU-3.csv')

# %% DBU-4
#Olona

meta = pd.read_csv('data/Olona/meta_Olona_filtered.csv', index_col = 'CODICE')
head = pd.read_csv('data/Olona/head_Olona.csv', index_col = 'DATA')
head.index = pd.DatetimeIndex(head.index)

codelst = identify_couples_codes(code_db, metamerge, meta, 'X', 'Y')

#Merge dei metadati
metamerge = dw.mergemeta(metamerge, meta, link = codelst,
                    firstmerge = dict(left_index = True, right_index = True),
                    secondmerge = dict(left_index = True, right_on = 'CODICE_link',
                                       suffixes = [None, "_Olona"]))
metamerge.rename(columns = {'CODICE_link': 'CODICE', 'index': 'CODICE_Olona'}, inplace = True)
metamerge.set_index('CODICE', inplace = True)
#Pulizia del risultato
metamerge.insert(39, 'z_OLONA', metamerge['Q'])
todrop = ['Q', 'DENOMINAZIONE', 'PUBBLICO', 'X', 'Y', 'PROV', 'COMUNE_Olona', 'lat_Olona', 'lon_Olona']
metamerge.drop(columns = todrop, inplace = True)
metamerge = dw.join_twocols(metamerge, ["NOTE", "LOCALITA'"], add = True, onlyna = False)
metamerge = dw.join_twocols(metamerge, ["NOTE", "NOTE_Olona"], add = True, onlyna = False)
metamerge = dw.join_twocols(metamerge, ["ORIGINE", "ORIGINE_Olona"], rename = "ORIGINE", add = True, onlyna = False)
metamerge = dw.join_twocols(metamerge, ['CODICE_PTUA2003', 'CODICE_Olona'], onlyna = False, add = True)
metamerge = dw.join_twocols(metamerge, ['STRAT', 'STRAT.'])
metamerge = dw.join_twocols(metamerge, ['FALDA', 'FALDA_Olona'], rename = 'FALDA')
metamerge.rename(columns = {'CODICE_PTUA2003': 'ALTRI CODICI'}, inplace = True)

#Merge delle serie storiche
# sum(metamerge['ORIGINE'].str.contains('OLONA'))
codelst = codelst.loc[codelst.isin(metamerge.index).values, :]
codelst.reset_index(drop = False, inplace = True)
codelst.set_index('CODICE_link', inplace = True)
codelst = codelst.squeeze()
#Merge
headmerge, rprt = dw.mergets(headmerge, head, codelst, report = True, tag = 'OLONA')
rprtmerge = dw.merge_rprt(rprtmerge, rprt.set_index('CODICE_link'))
headcorr = da.correct_quota(meta, head, metamerge, codelst, quotacols = ['Q', 'QUOTA_MISU'])
hmrgcorr = dw.mergets(hmrgcorr, headcorr, codelst)

#Salvataggio intermedio
# metamerge.to_csv('data/results/db-unification/DBU-steps/meta_DBU-4.csv')
# hmrgcorr.to_csv('data/results/db-unification/DBU-steps/head_DBU-4.csv')

# %% DBU-5
#Milano1950

meta = pd.read_csv('data/Milano1950/meta_Milano1950_QGIS.csv', index_col = 'CODICE')
head = pd.read_csv('data/Milano1950/head_Milano1950.csv', index_col = 'DATA')
head.index = pd.DatetimeIndex(head.index)

codelst = identify_couples_codes(code_db, metamerge, meta, 'Coord_X', 'Coord_Y')

#Merge dei metadati
metamerge = dw.mergemeta(metamerge, meta, link = codelst,
                    firstmerge = dict(left_index = True, right_index = True),
                    secondmerge = dict(left_index = True, right_on = 'CODICE_link',
                                       suffixes = [None, '_Milano1950']))
metamerge.rename(columns = {'CODICE_link': 'CODICE', 'index': 'CODICE_Milano1950'}, inplace = True)
metamerge.set_index('CODICE', inplace = True)
#Pulizia del risultato
metamerge.insert(40, 'z_Milano1950', metamerge['RIFERIMENTO'])
metamerge.insert(2, 'CODICE_FOG', metamerge['PIEZOMETRO'])
todrop = ['RIFERIMENTO', 'Coord_X', 'Coord_Y', 'PIEZOMETRO', 'COMUNE_Milano1950', 'lat_Milano1950', 'lon_Milano1950']
metamerge.drop(columns = todrop, inplace = True)
dw.join_twocols(metamerge, ['NOTE', 'UBICAZIONE'], rename = "NOTE", onlyna = False, add = True, inplace = True)
dw.join_twocols(metamerge, ['CODICE_SIF', 'CODICE_Milano1950'], inplace = True)
dw.join_twocols(metamerge, ['ORIGINE', 'ORIGINE_Milano1950'], rename = 'ORIGINE', onlyna = False, add = True, inplace = True)

#Inserisci le serie che non sono state aggiunte come serie ex-novo
to_insert = meta.loc[np.invert(meta.index.isin(metamerge['CODICE_SIF'])), :].copy()
to_insert.reset_index(drop = False, inplace = True)
to_insert['X'], to_insert['Y'] = gd.transf_CRS(to_insert.loc[:, 'Coord_X'], to_insert.loc[:, 'Coord_Y'], 'EPSG:3003', 'EPSG:32632', series = True)
to_insert.rename(columns = {'COD_PTUA16': 'Codice WISE Corpo idrico',
                            'CODICE': 'CODICE_SIF',
                            'PIEZOMETRO': 'CODICE_FOG',
                            'Coord_X': 'X_GB',
                            'Coord_Y': 'Y_GB',
                            'X': 'X_WGS84',
                            'Y': 'Y_WGS84',
                            'RIFERIMENTO': 'QUOTA_MISU',
                            }, inplace = True)
to_insert['z_Milano1950'] = to_insert['QUOTA_MISU']
to_insert['PROVINCIA'] = 'MI'
to_insert['CODICE'] = to_insert['CODICE_FOG']
#Merge dei metadati
metamerge = dw.joincolumns(pd.merge(metamerge, to_insert, how = 'outer', left_index = True, right_on = 'CODICE'))
metamerge.set_index('CODICE', inplace = True)
#Pulizia del dataset unificato
dw.join_twocols(metamerge, ['NOTE', 'UBICAZIONE'],onlyna = False, add = True, inplace = True)
metamerge.drop(columns = ['COD_PTUA16'], inplace = True)
metamerge[metamerge == 'SIF_NA'] = np.nan

#Merge delle serie storiche
#Recupero i codici delle serie che sono state unite
codelst = metamerge.loc[metamerge['ORIGINE'].str.contains('FOG'), 'CODICE_SIF']
codelst = codelst[codelst.isin(meta.index)]
#Opero il merge
headmerge, rprt = dw.mergets(headmerge, head, codelst, report = True, tag = 'Milano1950')
rprtmerge = dw.merge_rprt(rprtmerge, rprt.set_index('CODICE'))

headcorr = da.correct_quota(meta, head, metamerge, codelst, quotacols = ['RIFERIMENTO', 'QUOTA_MISU'])
hmrgcorr = dw.mergets(hmrgcorr, headcorr, codelst)

#Salvataggio intermedio
# metamerge.to_csv('data/results/db-unification/DBU-steps/meta_DBU-5.csv')
# hmrgcorr.to_csv('data/results/db-unification/DBU-steps/head_DBU-5.csv')

#%% DBU-6
#SSGiovanni

head = pd.read_csv('data/SSGiovanni/head_SSGiovanni.csv', index_col = 'DATA')
head.index = pd.DatetimeIndex(head.index)

to_insert = pd.read_csv('data/SSGiovanni/meta_SSGiovanni_QGIS.csv', index_col = 'CODICE')
to_insert.reset_index(inplace = True)

codelst = identify_couples_codes(code_db, metamerge, to_insert, 'X_GB', 'Y_GB')
#Nessun piezometro presente già in metamerge

#Aggiungi nuovi piezometri in metamerge
to_insert['CODICE'] = [f"0{int(idx)}" if not np.isnan(idx) else np.nan for idx in to_insert['CODICE']]
to_insert['X_GB'], to_insert['Y_GB'] = gd.transf_CRS(to_insert.loc[:, 'X_GB'], to_insert.loc[:, 'Y_GB'], 'EPSG:3003', 'EPSG:32632', series = True)
to_insert.rename(columns = {'COD_PTUA16': 'Codice WISE Corpo idrico',
                            'CODICE': 'CODICE_SIF',
                            'COD_LOCALE': 'CODICE',
                            'X_GB': 'X_WGS84',
                            'Y_GB': 'Y_WGS84',
                            'QUOTA': 'QUOTA_MISU',
                            'FILTRO_DA': 'FILTRI_TOP',
                            'FILTRO_A': 'FILTRI_BOTTOM',
                            'PROF': 'PROFONDITA',
                            'TIPOLOGIA': 'FALDA',
                            'INFO': 'NOTE'}, inplace = True)
to_insert = dw.join_twocols(to_insert, ['TIPO', 'COMPARTO'], rename = "INFO", add = True, onlyna = False)
to_insert['PROVINCIA'] = 'MI'
to_insert['COMUNE'] = 'SESTO SAN GIOVANNI'
to_insert['CODICE_SSG'] = to_insert['CODICE']
#Merge dei metadati
metamerge = pd.merge(metamerge, to_insert, how = 'outer', left_index = True, right_on = 'CODICE')
metamerge = dw.joincolumns(metamerge)
metamerge.set_index('CODICE', inplace = True)
#Pulizia del risultato
tool = metamerge['CODICE_SSG'].copy()
metamerge.drop(columns = 'CODICE_SSG', inplace = True)
metamerge.insert(3, 'CODICE_SSG', tool)
metamerge.insert(42, 'z_SSG', metamerge.loc[metamerge['CODICE_SSG'].notna(), 'QUOTA_MISU'])

#Merge delle serie storiche
#Costruzione serie con codici
codes = to_insert.loc[:, 'CODICE_SIF']
codes.index = to_insert.loc[:, 'CODICE']
#Merge
headmerge, rprt = dw.mergets(headmerge, head, codes, report = True, tag = 'SSGiovanni')
rprtmerge = dw.merge_rprt(rprtmerge, rprt.set_index('CODICE'))
headcorr = da.correct_quota(to_insert.set_index('CODICE_SIF'), head, metamerge, codes, quotacols = ['QUOTA_MISU', 'QUOTA_MISU'])
hmrgcorr = dw.mergets(hmrgcorr, headcorr, codes)

#Salvataggio intermedio
metamerge.to_csv('data/results/db-unification/DBU-steps/meta_DBU-6.csv')
hmrgcorr.to_csv('data/results/db-unification/DBU-steps/head_DBU-6.csv')

# %% Final operations

# - Set CODICE as the index in report
rprtmerge.index.names = ['CODICE']

# - Update 'DATA_INIZIO' (starting date) column in metamerge
metamerge['DATA_INIZIO'] = dw.datecol_arrange(metamerge['DATA_INIZIO'])
df = pd.DataFrame(headmerge.columns, columns = ['CODICE'])
df['DATA_INIZIO'] = [headmerge[col].first_valid_index() for col in headmerge.columns]
df['DATA_FINE'] = [headmerge[col].last_valid_index() for col in headmerge.columns]
df.set_index('CODICE', inplace = True)
metamerge = pd.merge(metamerge, df, how = 'left', left_index = True, right_index = True)
# -- Check with old dataset
old = pd.to_datetime(metamerge['DATA_INIZIO_x'], format = '%Y-%m-%d')
new = pd.to_datetime(metamerge['DATA_INIZIO_y'], format = '%Y-%m-%d')
delta = new - old
delta = delta[pd.notnull(delta)]
delta = [dt.days for dt in delta]
delta = [dt for dt in delta if dt != 0]
# -- Fill new ones and replace old ones
metamerge = dw.join_twocols(metamerge, ['DATA_INIZIO_x', 'DATA_INIZIO_y'], rename = 'DATA_INIZIO', onlyna = False)

# - Re-order the columns in a more intuitive way
# cols = metamerge.columns.to_list()
# nc = cols[0:6] + [cols[30]] + cols[6:8] + cols[21:23] + cols[8:21] + [cols[24]] + [cols[23]] + cols[25:30]
# metamerge = metamerge[nc]
tool = metamerge['DATA_FINE'].copy()
metamerge.drop(columns = 'DATA_FINE', inplace = True)
metamerge.insert(11, 'DATA_FINE', tool)

# - Rename the coordinate columns and merge the info columns
# metamerge.rename(columns = {'X_WGS84': 'X', 'Y_WGS84': 'Y'}, inplace = True)
dw.join_twocols(metamerge, ['NOTE', 'INFO'], onlyna = False, add = True, inplace = True)

# - Clean headmerge from series not present in metamerge
test = np.invert(headmerge.columns.isin(metamerge.index))
headmerge.drop(columns = headmerge.columns[test], inplace = True)
hmrgcorr.drop(columns = hmrgcorr.columns[test], inplace = True)

# - Search and replace added codes in metamerge with existing ones in codes_db
# -- Add eventual SIF codes not present based on CODICE and CODICE_PP
code_db.set_index('CODICE_PP', inplace = True)
idx = metamerge.index[metamerge.index.isin(code_db.index)]
codes = code_db.loc[idx, 'CODICE_SIF']
metamerge = pd.merge(metamerge, codes, how = 'left', left_index = True, right_index = True)
metamerge = dw.joincolumns(metamerge)
# -- From codes_db, select only codes starting with P
idx = [code for code in metamerge.index if code[0] != 'P']
codes = code_db.loc[code_db.loc[:, 'CODICE_SIF'].isin(metamerge.loc[idx, 'CODICE_SIF']), 'CODICE_SIF']
idx = [pp for pp in codes.index if pp[0] == 'P']
codes = pd.DataFrame(codes[idx])
codes.reset_index(drop = False, inplace = True)
# -- Add the new codes as a separate column
metamerge.reset_index(drop = False, inplace = True)
metamerge = pd.merge(metamerge, pd.DataFrame(codes), how = 'left', left_on = 'CODICE_SIF', right_on = 'CODICE_SIF')
# -- Replace the codes in headmerge
metamerge.set_index('CODICE', inplace = True)
newcols = []
for col in headmerge.columns:
    if metamerge.loc[col, 'CODICE_PP'] == metamerge.loc[col, 'CODICE_PP']:
        newcols += [metamerge.loc[col, 'CODICE_PP']]
    else:
        newcols += [col]
headmerge.columns = newcols
hmrgcorr.columns = newcols
# -- Replace the codes in rprtmerge
newseries = len([code for code in rprtmerge.index if code[0] != 'P'])
idx = metamerge.loc[rprtmerge.index, 'CODICE_PP'].notna()
rprtmerge.reset_index(drop = False, inplace = True)
rprtmerge.loc[idx.values, 'CODICE'] = metamerge.loc[rprtmerge['CODICE'], 'CODICE_PP'][idx].values
rprtmerge.set_index('CODICE', inplace = True)
# -- Replace the codes in metamerge
metamerge.reset_index(drop = False, inplace = True)
metamerge = dw.join_twocols(metamerge, ['CODICE', 'CODICE_PP'], onlyna = False)
metamerge.set_index('CODICE', inplace = True)
# - Clean 'CODICE_SIF' column
for x in metamerge['CODICE_SIF']:
    if x == x:
        if str(x)[0] == 'P':
            metamerge.loc[metamerge['CODICE_SIF'] == x, 'CODICE_SIF'] = np.nan

# - Clean 'RESCALDINA' time series
idx = metamerge.loc[metamerge['COMUNE'] == 'RESCALDINA', :].index
headmerge.loc[pd.to_datetime(['06-01-2002', '08-01-2002']), idx] = np.nan
hmrgcorr.loc[pd.to_datetime(['06-01-2002', '08-01-2002']), idx] = np.nan

# - Clean 'FOG4' time series (headcorr only)
ogmeta = pd.read_csv('data/Milano1950/meta_Milano1950.csv', index_col = 'CODICE')
oghead = pd.read_csv('data/Milano1950/head_Milano1950.csv', index_col = 'DATA')
oghead.index = pd.DatetimeIndex(oghead.index)
tool = oghead.loc[pd.date_range('2002-01-01', '2007-03-01', freq = 'MS'), ogmeta.loc[ogmeta['PIEZOMETRO'] == 'FOG4'].index]
hmrgcorr = pd.merge(hmrgcorr, tool, how = 'outer', left_index = True, right_index = True)
hmrgcorr = dw.join_twocols(hmrgcorr, [metamerge.loc[metamerge['CODICE_FOG'] == 'FOG4', :].index.values[0], tool.columns.values[0]], onlyna = False)
hmrgcorr.loc[pd.to_datetime('2006-01-01'), metamerge.loc[metamerge['CODICE_FOG'] == 'FOG4', :].index.values[0]] = np.nan
hmrgcorr.index.names = ['DATA']

# - Print information on the completed merge
print(f"Numero totale di serie aggregate o aggiunte al database: {rprtmerge.shape[0]}")
print(f"Numero di serie che hanno avuto un allungamento della serie storica: {len(delta)}")
print(f"Numero di serie aggiunte ex-novo: {newseries}")
print(f"Incremento medio di dati: {round(abs(sum(delta)/len(delta))/365, 2)} anni")

# %% Export

metamerge.to_csv('data/results/db-unification/meta_DBU-COMPLETE-sel.csv')
headmerge.to_csv('data/results/db-unification/head_DBU-COMPLETE-sel.csv')
rprtmerge.to_csv('data/results/db-unification/report_merge_DBU-COMPLETE-sel.csv')
hmrgcorr.to_csv(('data/results/db-unification/headcorr_DBU-COMPLETE-sel.csv'))
