from scripts import constants
from scripts import utils
import pandas as pd
from scripts import reporte_comun

def ingresar_tabla_precipitacion(df_datos, df_precipitacion, is_empty_data_precipitacion):
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
    df_07 = df_result[["id_estacion", "id_usuario", "fecha_toma", "fecha_ingreso", "rr07_t", "rr07_eq"]]
    df_13 = df_result[["id_estacion", "id_usuario", "fecha_toma", "fecha_ingreso", "rr13_t", "rr13_eq"]]
    df_19 = df_result[["id_estacion", "id_usuario", "fecha_toma", "fecha_ingreso", "rr19_t", "rr19_eq"]]

    ##reporte datos difernetes entre tablas
    codigo_estacion = df_datos['codigo'].iloc[0]
    if constants.GENERAR_REPORTES:
        _reporte_diferentes_entre_tablas(codigo_estacion,df_result)
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

    ################## fecha sin registrar datos ##################
    nombre_archivo = 'dato_no_tegistrado_precipitacion(fechas)'
    reporte_comun.fechas_no_registrada_por_variable(codigo, hora, 'rr', df_total, nombre_archivo)


def _reporte_diferentes_entre_tablas(codigo,df_general):
    nuevo_df = df_general.loc[(df_general['rr07_eq'] == False) | (df_general['rr13_eq'] == False) | (df_general['rr19_eq'] == False)]
    nuevo_df = nuevo_df.drop(['id_estacion','id_usuario','fecha_ingreso'], axis=1)
    nuevo_df['codigo'] = codigo
    nuevo_df['fecha_toma'] = pd.to_datetime(nuevo_df['fecha_toma']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tupla_list = nuevo_df.to_records(index=False).tolist()
    utils.write_tuplas_csv(tupla_list, constants.NOMBRE_CARPETA + f"/datos_diferentes_precipitacion.csv")
