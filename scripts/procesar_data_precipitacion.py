from scripts import constants
from scripts import utils
from datetime import datetime
import pandas as pd
import numpy as np


def ingresar_tabla_precipitacion(df_datos, df_precipitacion, is_empty_data_precipitacion):
    sql = f"INSERT INTO convencionales2._171481h " \
          f"(id_estacion, id_usuario, fecha_toma, fecha_ingreso,valor,num_lecturas) " \
          f"values (%s,%s,%s,%s,%s,0)"

    # precipitacion 07
    df = df_datos[
        ["id_estacion", "id_usuario", "fecha_ingreso", "codigo", "anio", "mes", "dia", "rr07", "rr13", "rr19"]]
    if not is_empty_data_precipitacion:
        df_precipitacion = df_precipitacion[
            ["id_estacion", "id_usuario", "fecha_ingreso", "codigo", "anio", "mes", "dia", "rr07", "rr13", "rr19"]]

        df_result = pd.merge(df, df_precipitacion,
                             on=["id_estacion", "id_usuario", "fecha_ingreso", "codigo", "anio", "mes", "dia"],
                             how='outer')
    else:
        df_result = pd.merge(df, df,
                             on=["id_estacion", "id_usuario", "fecha_ingreso", "codigo", "anio", "mes", "dia"],
                             how='outer')
        # df_result.tail(1000).to_csv("resultados/resul.csv", index=False)

    ##craer columna
    #df_result["rr07_t"] = df_result.apply(lambda row: row['rr07_x'] if row['rr07_x'] == row['rr07_y'] else None, axis=1)
    df_result['rr07_t'] = df_result.apply(lambda row: row['rr07_x'] if pd.isna(row['rr07_y']) else (
        row['rr07_y'] if pd.isna(row['rr07_x']) else (row['rr07_x'] if row['rr07_x'] == row['rr07_y'] else None)),
                            axis=1)
    df_result['rr07_eq'] = df_result.apply(lambda row: True if row['rr07_x'] == row['rr07_y'] else False, axis=1)

    #df_result["rr13_t"] = df_result.apply(lambda row: row['rr13_x'] if row['rr13_x'] == row['rr13_y'] else None, axis=1)
    df_result['rr13_t'] = df_result.apply(lambda row: row['rr13_x'] if pd.isna(row['rr13_y']) else (
        row['rr13_y'] if pd.isna(row['rr13_x']) else (row['rr13_x'] if row['rr13_x'] == row['rr13_y'] else None)),
                                          axis=1)
    df_result['rr13_eq'] = df_result.apply(lambda row: True if row['rr13_x'] == row['rr13_y'] else False, axis=1)

    #df_result["rr19_t"] = df_result.apply(lambda row: row['rr19_x'] if row['rr19_x'] == row['rr19_y'] else None, axis=1)
    df_result['rr19_t'] = df_result.apply(lambda row: row['rr19_x'] if pd.isna(row['rr19_y']) else (
        row['rr19_y'] if pd.isna(row['rr19_x']) else (row['rr19_x'] if row['rr19_x'] == row['rr19_y'] else None)),
                                          axis=1)
    df_result['rr19_eq'] = df_result.apply(lambda row: True if row['rr19_x'] == row['rr19_y'] else False, axis=1)

    # obtener los dataframes de las tres horas
    df_07 = df_result[["id_estacion", "id_usuario", "fecha_ingreso", "codigo", "anio", "mes", "dia", "rr07_t"]]
    df_07 = df_07.rename(columns={"rr07_t": "rr"})
    df_07["hora"] = "07"

    df_13 = df_result[["id_estacion", "id_usuario", "fecha_ingreso", "codigo", "anio", "mes", "dia", "rr13_t"]]
    df_13 = df_13.rename(columns={"rr13_t": "rr"})
    df_13["hora"] = "13"

    df_19 = df_result[["id_estacion", "id_usuario", "fecha_ingreso", "codigo", "anio", "mes", "dia", "rr19_t"]]
    df_19 = df_19.rename(columns={"rr19_t": "rr"})
    df_19["hora"] = "19"

    df_total = pd.concat([df_07, df_13, df_19])
    df_total = df_total.drop(df_total[df_total['dia'] == 32].index)
    df_total.reset_index(drop=True, inplace=True)  # reiniciar index
    df_total["fecha_toma"] = df_total["anio"].astype(str) \
                             + "-" + df_total["mes"].astype(str).str.zfill(2) \
                             + "-" + df_total["dia"].astype(str).str.zfill(2) \
                             + " " + df_total["hora"].astype(str).str.zfill(2) + ":00:00"
    df_total['fecha_toma'] = pd.to_datetime(df_total['fecha_toma'])
    df_total = df_total.sort_values('fecha_toma')
    df_total.reset_index(drop=True, inplace=True)
    df_total['rr'] = df_total['rr'].astype(float)  # pas to float
    df_total.loc[df_total['rr'].isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'rr'] = None  # null valores 99.9, 99...
    df_total.loc[df_total['rr'].isin(constants.VALUE_TO_FLAG), 'rr'] = constants.NEW_VALUE_TO_FLAG


    columnas_salida = ['id_estacion', 'id_usuario', 'fecha_toma', 'fecha_ingreso', 'rr']
    tupla = [tuple(row) for row in df_total[columnas_salida].values]

    df_total.tail(1000).to_csv("resultados/resul.csv", index=False)
    return sql, tupla
