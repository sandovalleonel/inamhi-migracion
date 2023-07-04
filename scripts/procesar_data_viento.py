import pandas as pd
from scripts import constants, utils
from scripts import reporte_comun
"""
versión: 1.00, fecha : 20/06/2023
Class Viento limpiar datos y generar reportes
Copyright. INAMHI <www.inamhi.gob.ec>. Todos los derechos reservados.
"""
def ingresar_tabla_viento(df_datos, dic_id_viento):
    sql = f"INSERT INTO convencionales2._3711161h" \
          f"(id_estacion, id_usuario, fecha_ingreso,fecha_toma,  velocidad, recorrido,id_dir_viento)" \
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

    tupla_total.extend(tupla_07)
    tupla_total.extend(tupla_13)
    tupla_total.extend(tupla_19)


    return (sql, tupla_total) if constants.SAVE_DATA else (sql, [])


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
    df_viento.loc[df_viento['an'].isin(constants.OLD_VALUES_ANEMOMETRO), 'an'] = constants.VALUE_NULL
    # remplazar valores negativos bandera
    df_viento.loc[df_viento['vv'].isin(constants.VALUE_TO_FLAG), 'vv'] = constants.NEW_VALUE_TO_FLAG
    df_viento.loc[df_viento['an'].isin(constants.VALUE_TO_FLAG), 'an'] = constants.NEW_VALUE_TO_FLAG

    # idpara relacion direccion viento
    df_dv_nulos = df_viento[df_viento['dv'].isnull()]

    df_viento = df_viento.dropna(subset=['dv'])
    df_viento['id_dir_viento'] = df_viento['dv'].map(dic_id_viento)
    df_viento['id_dir_viento'] = df_viento['id_dir_viento'].fillna(constants.REFERENCE_ID_DIR_VIENTO)

   #generar reportes
    if constants.GENERAR_REPORTES:
        _reportes_viento(df_viento, codigo, hora,df_dv_nulos)

    # exportar datos a arrays para guardaer en la  base
    df_viento["fecha_toma"] = df_viento["fecha_toma"].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_viento = df_viento.drop('dv', axis=1)

    return df_viento.to_records(index=False).tolist()

def _reportes_viento(df_total, codigo, hora, datos_no_guardados):
    # reportes
    ################## total datos guardados por variable ##################
    reporte_comun.total_por_variable(codigo, hora, "vv", df_total)
    reporte_comun.total_por_variable(codigo, hora, "an", df_total)


    #reporte datos no guardados
    tupla_no_guardados = []
    if datos_no_guardados.shape[0] > 0:
        tupla_no_guardados.append((codigo,'dv'+str(hora), datos_no_guardados.shape[0]))
        utils.write_tuplas_csv(tupla_no_guardados, constants.NOMBRE_CARPETA + "/viento_dv_no_guardados.csv")

    # anamometros negativos y viento negativo
    viento_min_reporte = 0
    nombre_archivo = f'(total)viento_menor_a_{viento_min_reporte}'
    reporte_comun.total_registros_menores_por_variable(codigo,hora,'vv',viento_min_reporte,df_total,nombre_archivo)
    reporte_comun.total_registros_menores_por_variable(codigo,hora,'an',viento_min_reporte,df_total,nombre_archivo)

    ################## fecha sin registrar datos ##################
    nombre_archivo = 'dato_no_tegistrado_viento(fechas)'
    reporte_comun.fechas_no_registrada_por_variable(codigo, hora, 'vv', df_total, nombre_archivo)
    reporte_comun.fechas_no_registrada_por_variable(codigo, hora, 'an', df_total, nombre_archivo)
