import pandas as pd
from scripts import constants, utils


def ingresar_tabla_viento(df_datos, dic_id_viento):
    sql = f"INSERT INTO convencionales2._3711161h" \
          f"(id_estacion, id_usuario, fecha_toma, fecha_ingreso, id_dir_viento, velocidad, recorrido)" \
          f"VALUES(%s,%s,%s,%s,%s,%s,%s)"

    # obtener los dataframes de las tres horas
    df_07 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso","fecha_toma", "dv07", "vv07", "an07"]]
    df_13 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso","fecha_toma", "dv13", "vv13", "an13"]]
    df_19 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso","fecha_toma", "dv19", "vv19", "an19"]]

    ##viento
    tupla_total = []

    codigo_estacion = df_datos['codigo'].iloc[0]
    tupla_07 = _limpiar_diccionario_viento(df_07, "07", codigo_estacion, dic_id_viento)
    tupla_13 = _limpiar_diccionario_viento(df_13, "13", codigo_estacion, dic_id_viento)
    tupla_19 = _limpiar_diccionario_viento(df_19, "19", codigo_estacion, dic_id_viento)

    """

    df_total['id_dir_viento'] = df_total['dv'].map(dic_id_viento)
    # datos_filtrados = df_total[df_total['dv'] == '99']
    # datos_filtrados.to_csv("datos_viento_a_guardar.csv", index=False)


    df_total['vv'], df_total['an'] = df_total['vv'].astype(float), df_total['an'].astype(float)  # oas to float
    df_total.loc[df_total['vv'].isin(constants.OLD_VALUES_VELOCIDAD_VIENTO), 'vv'] = constants.VALUE_NULL  # null valores 99.9, 99...
    df_total.loc[df_total['an'].isin(constants.OLD_VALUES_ANEMOMETRO), 'an'] = constants.VALUE_NULL

    """
    return sql, tupla_total


def _limpiar_diccionario_viento(df_viento, hora, codigo, dic_id_viento):
    columna_dv = 'dv' + hora
    columna_vv = 'vv' + hora
    columna_an = 'an' + hora
    df_viento = df_viento.rename(columns={columna_dv: 'dv', columna_vv: 'vv', columna_an: 'an'})

    hora = int(hora)
    df_viento['fecha_toma'] = pd.to_datetime(df_viento['fecha_toma'], format='%Y-%m-%d')
    df_viento['fecha_toma'] = df_viento['fecha_toma'] + pd.Timedelta(hours=hora)
    df_viento = df_viento.sort_values('fecha_toma')

    df_viento['vv'] = df_viento['vv'].astype(float)
    df_viento['an'] = df_viento['an'].astype(float)
    # limpiar valores
    df_viento.loc[df_viento['vv'].isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'vv'] = constants.VALUE_NULL
    df_viento.loc[df_viento['an'].isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'an'] = constants.VALUE_NULL
    # remplazar valores negativos bandera
    df_viento.loc[df_viento['vv'].isin(constants.VALUE_TO_FLAG), 'vv'] = constants.NEW_VALUE_TO_FLAG
    df_viento.loc[df_viento['an'].isin(constants.VALUE_TO_FLAG), 'an'] = constants.NEW_VALUE_TO_FLAG

    # idpara relacion direccion viento
    df_viento['id_dir_viento'] = df_viento['dv'].map(dic_id_viento)
    df_viento['id_dir_viento'] = df_viento['id_dir_viento'].fillna(constants.REFERENCE_ID_DIR_VIENTO)

   #generar reportes
    _reportes_viento(df_viento, codigo, hora)

    # exportar datos a arrays para guardaer en la  base
    return df_viento.to_records(index=False).tolist()

def _reportes_viento(df_total, codigo, hora):
    # reportes
    # total por variables
    totales = []
    total_vv = (codigo, f"vv{hora}", df_total['vv'].count(), df_total.shape[0])
    total_an = (codigo, f"an{hora}", df_total['an'].count(), df_total.shape[0])
    totales.extend([total_vv, total_an])
    utils.write_tuplas_csv(totales, constants.NOMBRE_CARPETA + "/total_por_variable.csv")

    # totales que direccion de viento no titene id
    tupla_dv_sin_id = []
    contador = df_total['id_dir_viento'].value_counts()[constants.REFERENCE_ID_DIR_VIENTO]
    tupla_dv_sin_id.append((codigo,f"dv{hora}",contador))
    utils.write_tuplas_csv(tupla_dv_sin_id, constants.NOMBRE_CARPETA + "/dir_viento_sin_id.csv")

    # vellocida del viento negativos por estacion
    tupla_negativo = []

    df_negativo = df_total[['fecha_toma', 'vv']]
    df_negativo = df_negativo.loc[(df_negativo['vv'] < constants.VALUE_CERO) & (df_negativo['vv'] != constants.NEW_VALUE_TO_FLAG)]
    df_negativo["variable"] = "vv"+str(hora)
    df_negativo["codigo"] = codigo
    df_negativo['fecha_toma'] = pd.to_datetime(df_negativo['fecha_toma']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tupla_negativo.extend(df_negativo.to_records(index=False).tolist())

    utils.write_tuplas_csv(tupla_negativo, constants.NOMBRE_CARPETA + "/viento_velocidad_negativos.csv")

# anamometros negativos
