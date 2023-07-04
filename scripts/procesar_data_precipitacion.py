from scripts import constants
from scripts import utils
import pandas as pd
from scripts import reporte_comun

"""
versión: 1.00, fecha : 20/06/2023
Class Precipitacion limpiar datos y generar reportes
Copyright. INAMHI <www.inamhi.gob.ec>. Todos los derechos reservados.
"""
def ingresar_tabla_precipitacion(df_datos, df_precipitacion, is_empty_data_precipitacion,df_dias32):
    sql = f"INSERT INTO convencionales2._171481h " \
          f"(id_estacion, id_usuario, fecha_toma, fecha_ingreso,valor,num_lecturas) " \
          f"values (%s,%s,%s,%s,%s,0)"

    # precipitacion merger entre tablas
    df = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "rr07", "rr13", "rr19"]]
    if not is_empty_data_precipitacion:
        df_precipitacion = df_precipitacion[
            ["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "rr07", "rr13", "rr19"]]
    else:
        df_precipitacion = df
    df_result = pd.merge(df, df_precipitacion,
                         on=["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma"],
                         how='outer')
    # df_result.tail(1000).to_csv("resultados/resul.csv", index=False)

    ##precion 07
    df_result['rr07_t'] = df_result['rr07_x'].fillna(df_result['rr07_y'])
    df_result['rr07_eq'] = df_result.apply(lambda row: False if pd.notnull(row['rr07_x']) and pd.notnull(row['rr07_y']) and row['rr07_x'] != row['rr07_y'] else True, axis=1)

    df_result['rr13_t'] = df_result['rr13_x'].fillna(df_result['rr13_y'])
    df_result['rr13_eq'] = df_result.apply(lambda row: False if pd.notnull(row['rr13_x']) and pd.notnull(row['rr13_y']) and row['rr13_x'] != row['rr13_y'] else True, axis=1)

    df_result['rr19_t'] = df_result['rr19_x'].fillna(df_result['rr19_y'])
    df_result['rr19_eq'] = df_result.apply(lambda row: False if pd.notnull(row['rr19_x']) and pd.notnull(row['rr19_y']) and row['rr19_x'] != row['rr19_y'] else True, axis=1)

    # obtener los dataframes de las tres horas
    df_07 = df_result[["id_estacion", "id_usuario", "fecha_toma", "fecha_ingreso", "rr07_t"]]
    df_13 = df_result[["id_estacion", "id_usuario", "fecha_toma", "fecha_ingreso", "rr13_t"]]
    df_19 = df_result[["id_estacion", "id_usuario", "fecha_toma", "fecha_ingreso", "rr19_t"]]

    ##reporte datos difernetes entre tablas
    codigo_estacion = df_datos['codigo'].iloc[0]
    if constants.GENERAR_REPORTES:
        _reporte_diferentes_entre_tablas(codigo_estacion,df_result)
        _reporte_fecha_dias32(codigo_estacion,df_result,df_dias32)
    ##precipitacion
    tupla_total = []


    tupla_07 = _limpiar_diccionario_precipitacion(df_07, "07", codigo_estacion)
    tupla_13 = _limpiar_diccionario_precipitacion(df_13, "13", codigo_estacion)
    tupla_19 = _limpiar_diccionario_precipitacion(df_19, "19", codigo_estacion)
    tupla_total.extend(tupla_07)
    tupla_total.extend(tupla_13)
    tupla_total.extend(tupla_19)
    return (sql, tupla_total) if constants.SAVE_DATA else (sql, [])


def _limpiar_diccionario_precipitacion(df_precipitacion, hora, codigo):
    columna_rr = f'rr{hora}_t'

    df_precipitacion = df_precipitacion.rename(columns={columna_rr: 'rr'})

    hora = int(hora)
    df_precipitacion['fecha_toma'] = pd.to_datetime(df_precipitacion['fecha_toma'], format='%Y-%m-%d')
    df_precipitacion['fecha_toma'] = df_precipitacion['fecha_toma'] + pd.Timedelta(hours=hora)
    df_precipitacion = df_precipitacion.sort_values('fecha_toma')

    df_precipitacion['rr'] = df_precipitacion['rr'].astype(float)
    # limpiar valores
    df_precipitacion.loc[df_precipitacion['rr']
    .isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'rr'] = constants.VALUE_NULL  # null valores 99.9, 99...

    # remplazar valores negativos bandera
    df_precipitacion.loc[df_precipitacion['rr']
    .isin(constants.VALUE_TO_FLAG), 'rr'] = constants.NEW_VALUE_TO_FLAG  # 888.88 a -888.88

    # generar reportes
    if constants.GENERAR_REPORTES:
        _reportes_precipitacoin(df_precipitacion, codigo, hora)

    df_precipitacion["fecha_toma"] = df_precipitacion["fecha_toma"].dt.strftime('%Y-%m-%d %H:%M:%S')
    # exportar datos a arrays para guardaer en la  base


    return df_precipitacion.to_records(index=False).tolist()


def _reportes_precipitacoin(df_total, codigo, hora):

    # reportes
    ################## total datos guardados por variable ##################
    reporte_comun.total_por_variable(codigo, hora, "rr", df_total)

    ################## precipitacion mayor a 200 ##################
    precip_maxima_reporte = 40
    nombre_archivo = f'(total)precipitacion_mayor_a_{precip_maxima_reporte}'
    reporte_comun.total_registros_mayores_por_variable(codigo,hora,'rr',precip_maxima_reporte,df_total,nombre_archivo)
    ################## precipitacion menor a cero ##################
    precip_min_reporte = 0
    nombre_archivo = f'(total)precipitacion_menor_a_{precip_min_reporte}'
    reporte_comun.total_registros_menores_por_variable(codigo,hora,'rr',precip_min_reporte,df_total,nombre_archivo)


    ################## fecha sin registrar datos ##################
    nombre_archivo = 'dato_no_tegistrado_precipitacion(fechas)'
    reporte_comun.fechas_no_registrada_por_variable(codigo, hora, 'rr', df_total, nombre_archivo)


def _reporte_diferentes_entre_tablas(codigo,df_general):
    nuevo_df = df_general.loc[(df_general['rr07_eq'] == False) | (df_general['rr13_eq'] == False) | (df_general['rr19_eq'] == False)]
    nuevo_df = nuevo_df.drop(['id_estacion','id_usuario','fecha_ingreso','rr07_eq','rr13_eq','rr19_eq'], axis=1)
    nuevo_df['codigo'] = codigo

    nuevo_df['fecha_toma'] = pd.to_datetime(nuevo_df['fecha_toma']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tupla_list = nuevo_df.to_records(index=False).tolist()
    utils.write_tuplas_csv(tupla_list, constants.NOMBRE_CARPETA + f"/datos_diferentes_precipitacion.csv")

def _reporte_fecha_dias32(codigo,df_general,df_dias32):

    df_general['fecha_toma'] = pd.to_datetime(df_general['fecha_toma'])
    df_general['day'] = df_general['fecha_toma'].dt.day
    nuevo_df = df_general.loc[(df_general['day'] == 1)]
    nuevo_df = nuevo_df.drop(['id_estacion', 'id_usuario', 'fecha_ingreso', 'rr07_eq', 'rr13_eq', 'rr19_eq','day','rr07_x', 'rr13_x', 'rr19_x', 'rr07_y', 'rr13_y', 'rr19_y'], axis=1)
    nuevo_df = nuevo_df.sort_values('fecha_toma')

    df_dias32['dia'] = 1
    df_dias32["fecha_toma"] = df_dias32["anio"].astype(str) \
                                     + "-" + df_dias32["mes"].astype(str).str.zfill(2) \
                                     + "-" + df_dias32["dia"].astype(str).str.zfill(2)

    df_dias32['fecha_toma'] = pd.to_datetime(df_dias32['fecha_toma'], format='%Y-%m-%d')
    df_dias32['fecha_toma'] = pd.to_datetime(df_dias32['fecha_toma']) + pd.DateOffset(months=1)
    df_dias32 = df_dias32[['fecha_toma','rr07','rr13','rr19']]
    df_dias32 = df_dias32.sort_values('fecha_toma')

    df_merge = pd.merge(nuevo_df, df_dias32, on='fecha_toma', how='inner')
    df_merge['diferencia'] = 0

    df_merge = df_merge.apply(sum_if_different, args=('rr07_t', 'rr07', 'diferencia'), axis=1)
    df_merge = df_merge.apply(sum_if_different, args=('rr13_t', 'rr13', 'diferencia'), axis=1)
    df_merge = df_merge.apply(sum_if_different, args=('rr19_t', 'rr19', 'diferencia'), axis=1)

    df_merge = df_merge.loc[(df_merge['diferencia'] > 0)]
    df_merge["codigo"] = codigo
    df_merge['fecha_toma'] = pd.to_datetime(df_merge['fecha_toma']).dt.strftime('%Y-%m-%d %H:%M:%S')

    tupla_list = df_merge.to_records(index=False).tolist()
    utils.write_tuplas_csv(tupla_list, constants.NOMBRE_CARPETA + f"/datos_diferentes_precipitacion_dia32.csv")



def sum_if_different(row, column1, column2, total_column):
    if row[column1] != row[column2]:
        row[total_column] += 1
    return row