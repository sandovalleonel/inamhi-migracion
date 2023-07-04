from scripts import constants
from scripts import utils
import pandas as pd
from scripts import reporte_comun

"""
versión: 1.00, fecha : 20/06/2023
Class Temperatura limpiar datos y generar reportes
Copyright. INAMHI <www.inamhi.gob.ec>. Todos los derechos reservados.
"""
def ingresar_tabla_tmaxtmin(df_datos):
    sql = f"INSERT INTO convencionales2._293161d " \
          f"(id_estacion, id_usuario, fecha_ingreso, fecha_toma,temp_max,temp_min) " \
          f"values (%s,%s,%s,%s,%s,%s)"

    # obtener los dataframes de las tres horas
    df_00 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "tmax", "tmin"]]

    ##Temepratura
    tupla_total = []

    codigo_estacion = df_datos['codigo'].iloc[0]
    tupla_00 = limpiar_diccionario_temperatura_maxmin(df_00, codigo_estacion)
    tupla_total.extend(tupla_00)

    return (sql, tupla_total) if constants.SAVE_DATA else (sql, [])


def limpiar_diccionario_temperatura_maxmin(df_temperatura, codigo):
    df_temperatura = df_temperatura.rename(columns={'id_usuario':'id_usuario'})
    df_temperatura['fecha_toma'] = pd.to_datetime(df_temperatura['fecha_toma'], format='%Y-%m-%d')
    df_temperatura = df_temperatura.sort_values('fecha_toma')

    df_temperatura['tmax'] = df_temperatura['tmax'].astype(float)
    df_temperatura['tmin'] = df_temperatura['tmin'].astype(float)
    # limpiar valores
    df_temperatura.loc[df_temperatura['tmax']
    .isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'tmax'] = constants.VALUE_NULL  # null valores 99.9, 99...
    df_temperatura.loc[df_temperatura['tmin']
    .isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'tmin'] = constants.VALUE_NULL  # null valores 99.9, 99...
    # remplazar valores negativos bandera
    df_temperatura.loc[df_temperatura['tmax']
    .isin(constants.VALUE_TO_FLAG), 'tmax'] = constants.NEW_VALUE_TO_FLAG  # 888.88 a -888.88
    df_temperatura.loc[df_temperatura['tmin']
    .isin(constants.VALUE_TO_FLAG), 'tmin'] = constants.NEW_VALUE_TO_FLAG  # 888.88 a -888.88

    # generar reportes
    if constants.GENERAR_REPORTES:
        _reportes_temperatura_maxmin(df_temperatura, codigo)
    df_temperatura["fecha_toma"] = df_temperatura["fecha_toma"].dt.strftime('%Y-%m-%d %H:%M:%S')
    # exportar datos a arrays para guardaer en la  base

    return df_temperatura.to_records(index=False).tolist()


def _reportes_temperatura_maxmin(df_total, codigo):
    hora = 0
    # reoprtes
    ################## total datos guardados por variable ##################
    reporte_comun.total_por_variable(codigo, hora, "tmax", df_total)
    reporte_comun.total_por_variable(codigo, hora, "tmin", df_total)

    # temperatura masyor a 40
    temperatu_maxima_reporte = 40
    nombre_archivo = f'(total)temperatura_mayor_a_{temperatu_maxima_reporte}maxmin'
    reporte_comun.total_registros_mayores_por_variable(codigo, hora, 'tmax', temperatu_maxima_reporte, df_total,
                                                       nombre_archivo)
    reporte_comun.total_registros_mayores_por_variable(codigo, hora, 'tmin', temperatu_maxima_reporte, df_total,
                                                       nombre_archivo)

    # temperatura menor a -5
    temperatura_minima_reporte = -1
    nombre_archivo = f'(total)temperatura_menor_a_{temperatura_minima_reporte}maxmin'
    reporte_comun.total_registros_menores_por_variable(codigo, hora, 'tmax', temperatura_minima_reporte, df_total,
                                                       nombre_archivo)
    reporte_comun.total_registros_menores_por_variable(codigo, hora, 'tmin', temperatura_minima_reporte, df_total,
                                                       nombre_archivo)

    nombre_archivo = f'(detalle)temperatura_menor_a_{temperatura_minima_reporte}maxmin'
    reporte_comun.registros_menores_por_variable(codigo, hora, 'tmax', temperatura_minima_reporte, df_total,
                                                       nombre_archivo)
    reporte_comun.registros_menores_por_variable(codigo, hora, 'tmin', temperatura_minima_reporte, df_total,
                                                       nombre_archivo)

    ################## fecha sin registrar datos ##################
    nombre_archivo = 'dato_no_tegistrado_temeperatura_maxmin(fechas)'
    reporte_comun.fechas_no_registrada_por_variable(codigo, hora, 'tmax', df_total, nombre_archivo)
    reporte_comun.fechas_no_registrada_por_variable(codigo, hora, 'tmin', df_total, nombre_archivo)
