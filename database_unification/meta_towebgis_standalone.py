# -*- coding: utf-8 -*-
"""
Created on Fri May 19 12:04:06 2023

@author: paolo
"""

# import datawrangling as dw
# import geodata as gd
import numpy as np
import pandas as pd

# %% Classe e funzione

class arrange_metats():
    """
    Joins two dataframes ('meta' and 'ts') in a single dataframe
    
    Parameters for __init__
    -----------------------
    meta : pandas.DataFrame
        A DataFrame with rows containing the metadata of the time series 
        contained in 'ts'. 'meta' and 'ts' are linked through 'ts' column labels,
        stored in column 'idcol' in 'meta'.
    ts : pandas.DataFrame
        A DataFrame with columns containing the time series which metadata are
        contained in 'meta'.
    idcol : str
        The label of the column in meta containing the IDs used for ts columns
        labels.    
    """
    def __init__(self, meta, ts, idcol):
        self.meta = meta.copy()
        self.ts = ts.copy()
        self.id = idcol
    
    #Methods
    #-------
    
    def to_webgis(self, anfields, ancouples, pzfields = None, pzcouples = None, idcol = None,
                  ids = None, stacklab = None, onlymeta = False):
        """
        Returns the database in a format which can be uploaded in a WebGIS

        Parameters
        ----------
        anfields : TYPE
            DESCRIPTION.
        ancouples : TYPE
            DESCRIPTION.
        pzfields : TYPE
            DESCRIPTION.
        pzcouples : TYPE
            DESCRIPTION.
        idcol : TYPE
            DESCRIPTION.
        ids : TYPE, optional
            DESCRIPTION. The default is None.
        stacklab : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        TYPE
            DESCRIPTION.
        TYPE
            DESCRIPTION.

        """
        # an - Anagrafica (metadata)
        an = pd.DataFrame(np.zeros((self.meta.shape[0],len(anfields))), columns = anfields)
        an[:] = np.nan
        for tag in ancouples:
            an[tag] = self.meta[ancouples[tag]]
        if ids is not None:
            an[ids[0]] = [x for x in range(1, an.shape[0]+1)]
        # dpz - Piezometric data
        if not onlymeta:
            dpz = self.ts.stack().reset_index(drop = False)
            if stacklab is not None:
                dpz.columns = stacklab
            dump = self.meta.loc[self.meta[self.id].isin(self.ts.columns), :].copy()
            dump.reset_index(drop = True, inplace = True)
            tool = pd.DataFrame(np.zeros((dump.shape[0],len(pzfields))), columns = pzfields)
            tool[:] = np.nan
            for tag in pzcouples:
                tool[tag] = dump[pzcouples[tag]]
            if ids is not None:
                tool[ids[1]] = an.loc[an[idcol].isin(tool[idcol]), ids[0]].values        
            dpz = joincolumns(pd.merge(dpz, tool, how = 'right', left_on = idcol, right_on = idcol))
            if ids is not None:
                dpz[ids[0]] = [x for x in range(1, dpz.shape[0]+1)]
            dpz = dpz[pzfields]
            return an, dpz
        return an
    
    def to_stackeDF(self):
        #transform to a format passable to the class stackeDF
        pass

def transf_CRS(x, y, frm, to, series = False, **kwargs):
    """
    Transforms coordinates from one CRS to another

    Parameters
    ----------
    x : scalar or array
        x coordinate (latitude).
    y : scalar or array
        y coordinate (longitude).
    frm : Any
        The CRS of the coordinates provided.
        It can be in any format to indicate the CRS used in pyproj.Trasformer.from_crs.
        Example: 'EPSG:4326'
    to : Any
        The CRS in which to transform the coordinates provided.
        It can be in any format to indicate the CRS used in pyproj.Trasformer.from_crs.
        Example: 'EPSG:4326'.
    series : bool, optional
        If True, x and y are treated as pandas.Series and transformed into arrays.
        The default is False.
    **kwargs : kwargs, optional
        Additional arguments forwarded to pyproj.Transformer.transform().
    
    Returns
    -------
    out : tuple
        When transforming to WGS84:
         - out[0] contains the LATITUDE or X
         - out[1] contains the LONGITUDE or Y
    """
    if series:
        x = x.reset_index(drop = True).to_numpy()
        y = y.reset_index(drop = True).to_numpy()
    trs = pyproj.Transformer.from_crs(frm, to)
    out = trs.transform(x, y, **kwargs)
    return out

# %% Operazione

estr = pd.read_csv("C:/Users/paolo/OneDrive - Politecnico di Milano/hydrogeo-modelling/Progetti/MAURICE/Poli-riservato/rete-piezometrica-MAURICE/db-da-unificare/Da unificare/estrazione-area-MAURICE.csv")
dbPP = pd.read_csv("C:/Users/paolo/OneDrive - Politecnico di Milano/hydrogeo-modelling/Progetti/MAURICE/Poli-riservato/rete-piezometrica-MAURICE/db-da-unificare/Da unificare/db_Piez_Pozz.csv")

fields = ['gid','id_punto','id_originale','id_ente',
            'tipo_punto','id_acquifero','id_hydros','presenza_strat',
            'stratigrafia_pdf','istat_comune','istat_prov',
            'x_sr1','y_sr1','x_sr2','y_sr2','quota_testa_tubo',
            'nota_coordinate','quota_piano_campagna',
            'fonte_quota_piano_campagna','indirizzo_punto',
            'ente_gestore','proprieta'	,'indirizzo_proprieta']

db_Piez_Pozz = {
    'id_punto': 'cod_punto',
    'tipo_punto': 'tipo_punto',
    'istat_comune': 'istat_comune',
    'x_sr1': 'long',
    'y_sr1': 'lat',
    'quota_piano_campagna': 'quota_pc',
    'indirizzo_punto': 'Indirizzo',
    }

estrazione = {
    'id_punto': 'id',
    'tipo_punto': 'type',
    'x_sr1': 'x_loc_epsg',
    'y_sr1': 'y_loc_epsg',
    'quota_piano_campagna': 'z_dtm2015', #fa riferimento al dtm e non misure prese con GPS
    'fonte': 'ref_auth',
    'indirizzo_punto': 'address',       
    }

j = arrange_metats(estr, estr, 'id')
an = j.to_webgis(fields, estrazione, ['gid', 'fkid'], onlymeta = True)
an['x_sr2'], an['y_sr2'] = transf_CRS(an['x_sr1'], an['y_sr1'], 'EPSG:3003', 'EPSG:32632', series = True)

an.to_csv("C:/Users/paolo/OneDrive - Politecnico di Milano/hydrogeo-modelling/Progetti/MAURICE/Poli-riservato/rete-piezometrica-MAURICE/db-da-unificare/Da unificare/estrazione-formato-webgis.csv", index = False)

j = arrange_metats(dbPP, dbPP, 'cod_punto')
an = j.to_webgis(fields, db_Piez_Pozz, ['gid', 'fkid'], onlymeta = True)
an['x_sr2'], an['y_sr2'] = transf_CRS(an['x_sr1'], an['y_sr1'], 'EPSG:3003', 'EPSG:32632', series = True)

an.to_csv("C:/Users/paolo/OneDrive - Politecnico di Milano/hydrogeo-modelling/Progetti/MAURICE/Poli-riservato/rete-piezometrica-MAURICE/db-da-unificare/Da unificare/db_Piez_Pozz-formato-webgis.csv", index = False)
