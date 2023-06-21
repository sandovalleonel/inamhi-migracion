from scripts import constants
from scripts import utils
from datetime import datetime
import pandas as pd
import numpy as np
import os

"""
ingresar datos en la tabla termperatura de las 7,13,19 (h)
[(1,"a",3),
(1,"b",3),
(1,"c",3)]
"""


def ingresar_tabla_temperatura(df_datos):

    sql = f"INSERT INTO convencionales2._293161h " \
          f"(id_estacion, id_usuario, fecha_toma, fecha_ingreso,term_seco,term_hmd) " \
          f"values (%s,%s,%s,%s,%s,%s)"

    # obtener los dataframes de las tres horas
    df_07 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "codigo", "anio", "mes", "dia", "ts07", "th07"]]
    df_07 = df_07.rename(columns={"ts07": "ts", "th07": "th"})
    df_07["hora"] = "07"
    df_13 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "codigo", "anio", "mes", "dia", "ts13", "th13"]]
    df_13 = df_13.rename(columns={"ts13": "ts", "th13": "th"})
    df_13["hora"] = "13"
    df_19 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "codigo", "anio", "mes", "dia", "ts19", "th19"]]
    df_19 = df_19.rename(columns={"ts19": "ts", "th19": "th"})
    df_19["hora"] = "19"
    dataframes = [df_07, df_13, df_19]
    df_total = pd.concat(dataframes)

    ##Temepratura 07
    columnas_salida = ["id_estacion", "id_usuario", "fecha_toma", "fecha_ingreso", "ts", "th"]

    df_total = df_total.drop(df_total[df_total['dia'] == 32].index)
    df_total.reset_index(drop=True, inplace=True)  # reiniciar index
    df_total["fecha_toma"] = df_total["anio"].astype(str) \
                             + "-" + df_total["mes"].astype(str).str.zfill(2) \
                             + "-" + df_total["dia"].astype(str).str.zfill(2) \
                             + " " + df_total["hora"].astype(str).str.zfill(2) + ":00:00"
    df_total['fecha_toma'] = pd.to_datetime(df_total['fecha_toma'])
    df_total = df_total.sort_values('fecha_toma')
    df_total.reset_index(drop=True, inplace=True)

    df_total['ts'], df_total['th'] = df_total['ts'].astype(float), df_total['th'].astype(float) #oas to float
    df_total.loc[df_total['ts'].isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'ts'] = None  # null valores 99.9, 99...
    df_total.loc[df_total['th'].isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'th'] = None  # null valores 99.9, 99...
    df_total.loc[df_total['ts'].isin(constants.VALUE_TO_FLAG), 'ts'] = constants.NEW_VALUE_TO_FLAG  # 888.88 a -888.88
    df_total.loc[df_total['th'].isin(constants.VALUE_TO_FLAG), 'th'] = constants.NEW_VALUE_TO_FLAG  # 888.88 a -888.88

    tupla = [tuple(row) for row in df_total[columnas_salida].values]

    #reoprtes
    #total por variables
    totales = []
    total_ts = f"{df_datos['codigo'].loc[1]},ts,{df_total['ts'].count()}"
    total_th = f"{df_datos['codigo'].loc[1]},th,{df_total['th'].count()}"
    totales.append(total_ts)
    totales.append(total_th)
    utils.csv_write_array_string(totales,constants.NOMBRE_CARPETA+"/total/tota.csv")
    #temperatura masyor a 40
    tem_mas_40_ts = df_total.loc[(df_total['ts'] > constants.TEMP_MAXIMA)]
    tem_mas_40_ts = tem_mas_40_ts[['codigo','fecha_toma','ts']]
    tem_mas_40_ts = tem_mas_40_ts.rename(columns={"ts": "valor"})
    tem_mas_40_ts["variable"] = "ts"
    tem_mas_40_th = df_total.loc[(df_total['th'] > constants.TEMP_MAXIMA)]
    tem_mas_40_th = tem_mas_40_th[['codigo', 'fecha_toma', 'th']]
    tem_mas_40_th = tem_mas_40_th.rename(columns={"th": "valor"})
    tem_mas_40_th["variable"] = "th"

    temp_mas40 = pd.concat([tem_mas_40_th,tem_mas_40_ts])
    array_tuplas = np.array(temp_mas40.to_records(index=False).tolist())
    utils.write_tuplas_csv(array_tuplas,"resultados/total/temp_mas40.csv")

    #temperatura menor a 5
    tem_min_5_ts = df_total.loc[(df_total['ts'] < constants.TEMP_MINIMO) & (df_total['ts'] != constants.NEW_VALUE_TO_FLAG)]
    tem_min_5_ts = tem_min_5_ts[['codigo','fecha_toma','ts']]
    tem_min_5_ts = tem_min_5_ts.rename(columns={"ts": "valor"})
    tem_min_5_ts["variable"] = "ts"
    tem_min_5_th = df_total.loc[(df_total['th'] < constants.TEMP_MINIMO) & (df_total['th'] != constants.NEW_VALUE_TO_FLAG)]
    tem_min_5_th = tem_min_5_th[['codigo', 'fecha_toma', 'th']]
    tem_min_5_th = tem_min_5_th.rename(columns={"th": "valor"})
    tem_min_5_th["variable"] = "th"

    temp_min5 = pd.concat([tem_min_5_th,tem_min_5_ts])
    array_tuplas = np.array(temp_min5.to_records(index=False).tolist())
    utils.write_tuplas_csv(array_tuplas,"resultados/total/temp_min5.csv")

    """
    """

    return sql, tupla
