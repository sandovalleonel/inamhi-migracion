from scripts import constants
from scripts import utils
import pandas as pd
from scripts import reporte_comun

def ingresar_tabla_nubosidad(df_datos):
    sql = f""

    # obtener los dataframes de las tres horas
    df_07 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "nu07"]]
    df_13 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "nu13"]]
    df_19 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "nu19"]]

    ##nubosidad
    tupla_total = []

    codigo_estacion = df_datos['codigo'].iloc[0]
    tupla_07 = _limpiar_diccionario_nubosidad(df_07, "07", codigo_estacion)
    tupla_13 = _limpiar_diccionario_nubosidad(df_13, "13", codigo_estacion)
    tupla_19 = _limpiar_diccionario_nubosidad(df_19, "19", codigo_estacion)

    return (sql, tupla_total) if constants.SAVE_DATA else (sql, [])


def _limpiar_diccionario_nubosidad(df_nubosidad, hora, codigo):
    columna = 'nu' + hora
    df_nubosidad = df_nubosidad.rename(columns={columna: 'nu'})

    hora = int(hora)
    df_nubosidad['fecha_toma'] = pd.to_datetime(df_nubosidad['fecha_toma'], format='%Y-%m-%d')
    df_nubosidad['fecha_toma'] = df_nubosidad['fecha_toma'] + pd.Timedelta(hours=hora)
    df_nubosidad = df_nubosidad.sort_values('fecha_toma')

    df_nubosidad['nu'] = df_nubosidad['nu'].astype(float)
    # limpiar valores
    df_nubosidad.loc[df_nubosidad['nu'].isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'nu'] = constants.VALUE_NULL
    # remplazar valores negativos bandera
    df_nubosidad.loc[df_nubosidad['nu'].isin(constants.VALUE_TO_FLAG), 'nu'] = constants.NEW_VALUE_TO_FLAG

    # generar reportes
    if constants.GENERAR_REPORTES:
        _reportes_nubosidad(df_nubosidad, codigo, hora)

    # exportar datos a arrays para guardaer en la  base
    return df_nubosidad.to_records(index=False).tolist()


def _reportes_nubosidad(df_total, codigo, hora):
    # reportes
    ################## total datos guardados por variable ##################
    reporte_comun.total_por_variable(codigo, hora, "nu", df_total)


    ################## fecha sin registrar datos ##################
    nombre_archivo = 'dato_no_tegistrado_nubosidad(fechas)'
    reporte_comun.fechas_no_registrada_por_variable(codigo, hora, 'nu', df_total, nombre_archivo)

