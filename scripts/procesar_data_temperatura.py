from scripts import constants
from scripts import utils
import pandas as pd
from scripts import reporte_comun

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
"""
versión: 1.00, fecha : 20/06/2023
Class Temperatura limpiar datos y generar reportes
Copyright. INAMHI <www.inamhi.gob.ec>. Todos los derechos reservados.
"""

"""
recibe por estacion se separa las variables de tempertarua para cada hora registrada
y  se llama a un metodo para realizar la limepza por variable
Args:
    df_datos: dataframe
"""


def ingresar_tabla_temperatura(df_datos):
    sql = f"INSERT INTO convencionales2._293161h " \
          f"(id_estacion, id_usuario, fecha_ingreso, fecha_toma,term_seco,term_hmd) " \
          f"values (%s,%s,%s,%s,%s,%s)"

    # obtener los dataframes de las tres horas
    df_07 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "ts07", "th07"]]
    df_13 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "ts13", "th13"]]
    df_19 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "ts19", "th19"]]

    ##Temepratura
    tupla_total = []

    codigo_estacion = df_datos['codigo'].iloc[0]
    tupla_07 = limpiar_diccionario_temperatura(df_07, "07", codigo_estacion)
    tupla_13 = limpiar_diccionario_temperatura(df_13, "13", codigo_estacion)
    tupla_19 = limpiar_diccionario_temperatura(df_19, "19", codigo_estacion)

    tupla_total.extend(tupla_07)
    tupla_total.extend(tupla_13)
    tupla_total.extend(tupla_19)

    return (sql, tupla_total) if constants.SAVE_DATA else (sql, [])


def limpiar_diccionario_temperatura(df_temperatura, hora, codigo):
    columa_ts = 'ts' + hora
    columna_th = 'th' + hora
    df_temperatura = df_temperatura.rename(columns={columna_th: 'th', columa_ts: 'ts'})

    hora = int(hora)
    df_temperatura['fecha_toma'] = pd.to_datetime(df_temperatura['fecha_toma'], format='%Y-%m-%d')
    df_temperatura['fecha_toma'] = df_temperatura['fecha_toma'] + pd.Timedelta(hours=hora)
    df_temperatura = df_temperatura.sort_values('fecha_toma')

    df_temperatura['ts'] = df_temperatura['ts'].astype(float)
    df_temperatura['th'] = df_temperatura['th'].astype(float)
    # limpiar valores
    df_temperatura.loc[df_temperatura['ts']
    .isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'ts'] = constants.VALUE_NULL  # null valores 99.9, 99...
    df_temperatura.loc[df_temperatura['th']
    .isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'th'] = constants.VALUE_NULL  # null valores 99.9, 99...
    # remplazar valores negativos bandera
    df_temperatura.loc[df_temperatura['ts']
    .isin(constants.VALUE_TO_FLAG), 'ts'] = constants.NEW_VALUE_TO_FLAG  # 888.88 a -888.88
    df_temperatura.loc[df_temperatura['th']
    .isin(constants.VALUE_TO_FLAG), 'th'] = constants.NEW_VALUE_TO_FLAG  # 888.88 a -888.88

    # generar reportes
    if constants.GENERAR_REPORTES:
        _reportes_temperatura(df_temperatura, codigo, hora)
    df_temperatura["fecha_toma"] = df_temperatura["fecha_toma"].dt.strftime('%Y-%m-%d %H:%M:%S')
    # exportar datos a arrays para guardaer en la  base
    return df_temperatura.to_records(index=False).tolist()


def _reportes_temperatura(df_total, codigo, hora):
    # reoprtes
    ################## total datos guardados por variable ##################
    reporte_comun.total_por_variable(codigo, hora, "ts", df_total)
    reporte_comun.total_por_variable(codigo, hora, "th", df_total)

    # temperatura masyor a 40
    temperatu_maxima_reporte = 40
    nombre_archivo = f'(total)temperatura_mayor_a_{temperatu_maxima_reporte}'
    reporte_comun.total_registros_mayores_por_variable(codigo,hora,'ts',temperatu_maxima_reporte,df_total,nombre_archivo)
    reporte_comun.total_registros_mayores_por_variable(codigo,hora,'th',temperatu_maxima_reporte,df_total,nombre_archivo)

    # temperatura menor a 5
    temperatura_minima_reporte = -5
    nombre_archivo = f'(total)temperatura_menor_a_{temperatura_minima_reporte}'
    reporte_comun.total_registros_menores_por_variable(codigo,hora,'ts',temperatura_minima_reporte,df_total,nombre_archivo)
    reporte_comun.total_registros_menores_por_variable(codigo,hora,'th',temperatura_minima_reporte,df_total,nombre_archivo)


    ################## fecha sin registrar datos ##################
    nombre_archivo = 'dato_no_tegistrado_temeperatura(fechas)'
    reporte_comun.fechas_no_registrada_por_variable(codigo, hora, 'ts', df_total, nombre_archivo)
    reporte_comun.fechas_no_registrada_por_variable(codigo, hora, 'th', df_total, nombre_archivo)
