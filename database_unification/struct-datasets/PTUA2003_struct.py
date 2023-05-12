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



# rimuovere punti con falda =/= 1 o mancante
#mantenere solo punti con falda = 1