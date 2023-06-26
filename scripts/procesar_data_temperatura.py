from scripts import constants
from scripts import utils
import pandas as pd


def ingresar_tabla_temperatura(df_datos):
    sql = f"INSERT INTO convencionales2._293161h " \
          f"(id_estacion, id_usuario, fecha_toma, fecha_ingreso,term_seco,term_hmd) " \
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

    return sql, tupla_total


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
    reportes_temperatura(df_temperatura, codigo, hora)

    # exportar datos a arrays para guardaer en la  base
    return df_temperatura.to_records(index=False).tolist()


def reportes_temperatura(df_total, codigo, hora):
    # reoprtes
    # total por variables
    totales = []
    total_ts = (codigo, f"ts{hora}", df_total['ts'].count(), df_total.shape[0])
    total_th = (codigo, f"th{hora}", df_total['th'].count(), df_total.shape[0])
    totales.extend([total_ts, total_th])
    utils.write_tuplas_csv(totales, constants.NOMBRE_CARPETA + "/total_por_variable.csv")

    # temperatura masyor a 40
    tupla40 = []

    df40_ts = df_total[['fecha_toma', 'ts']]
    df40_ts = df40_ts.loc[(df40_ts['ts'] > constants.TEMP_MAXIMA)]
    df40_ts["variable"] = "ts" + str(hora)
    df40_ts["codigo"] = codigo
    df40_ts['fecha_toma'] = pd.to_datetime(df40_ts['fecha_toma']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tupla40.extend(df40_ts.to_records(index=False).tolist())

    df40_th = df_total[['fecha_toma', 'th']]
    df40_th = df40_th.loc[(df40_th['th'] > constants.TEMP_MAXIMA)]
    df40_th["variable"] = "th" + str(hora)
    df40_th["codigo"] = codigo
    df40_th['fecha_toma'] = pd.to_datetime(df40_th['fecha_toma']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tupla40.extend(df40_th.to_records(index=False).tolist())

    utils.write_tuplas_csv(tupla40, constants.NOMBRE_CARPETA + "/temperatura_mayor40.csv")

    # temperatura menor a 5
    tupla5 = []

    df5_ts = df_total[['fecha_toma', 'ts']]
    df5_ts = df5_ts.loc[(df5_ts['ts'] < constants.TEMP_MINIMO) & (df5_ts['ts'] != constants.NEW_VALUE_TO_FLAG)]
    df5_ts["variable"] = "ts" + str(hora)
    df5_ts["codigo"] = codigo
    df5_ts['fecha_toma'] = pd.to_datetime(df5_ts['fecha_toma']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tupla5.extend(df5_ts.to_records(index=False).tolist())

    df5_th = df_total[['fecha_toma', 'th']]
    df5_th = df5_th.loc[(df5_th['th'] < constants.TEMP_MINIMO) & (df5_th['th'] != constants.NEW_VALUE_TO_FLAG)]
    df5_th["variable"] = "th" + str(hora)
    df5_th["codigo"] = codigo
    df5_th['fecha_toma'] = pd.to_datetime(df5_th['fecha_toma']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tupla5.extend(df5_th.to_records(index=False).tolist())

    utils.write_tuplas_csv(tupla5, constants.NOMBRE_CARPETA + "/temperatura_menor-5.csv")

    # fechas no ingresadas
    # mas de dos nulos seguidos
    # Recorrer el DataFrame utilizando iterrows()
    # variable ts
    tupla_no_guardados = []

    cod_subgrupo_ts = cod_subgrupo_th = 0
    tupla_temp_ts = tupla_temp_th = []
    for index, row in df_total.iterrows():
        fecha = row['fecha_toma']
        valor_ts, valor_th = row['ts'], row['th']

        # reporte ts
        if pd.isna(valor_ts):
            tupla_temp_ts.append((fecha, cod_subgrupo_ts))
        else:
            if len(tupla_temp_ts) >= constants.NUMERO_DIAS_NO_REGISTRADOS:
                tupla_no_guardados.append((tupla_temp_ts[0][0], codigo, 'ts' + str(hora), len(tupla_temp_ts)))
            cod_subgrupo_ts = cod_subgrupo_ts + 1
            tupla_temp_ts = []

        #reporte th
        if pd.isna(valor_th):
            tupla_temp_th.append((fecha,cod_subgrupo_th))
        else:
            if len(tupla_temp_th) >= constants.NUMERO_DIAS_NO_REGISTRADOS:
                tupla_no_guardados.append((tupla_temp_th[0][0], codigo, 'th' + str(hora), len(tupla_temp_th)))
            cod_subgrupo_th = cod_subgrupo_th + 1
            tupla_temp_th = []

    # reporte ts
    if len(tupla_temp_ts) >= constants.NUMERO_DIAS_NO_REGISTRADOS:
        tupla_no_guardados.append((tupla_temp_ts[0][0], codigo, 'ts' + str(hora), len(tupla_temp_ts)))

    # reporte th
    if len(tupla_temp_th) >= constants.NUMERO_DIAS_NO_REGISTRADOS:
        tupla_no_guardados.append((tupla_temp_th[0][0], codigo, 'th' + str(hora), len(tupla_temp_th)))

    utils.write_tuplas_csv(tupla_no_guardados, constants.NOMBRE_CARPETA + "/temperatura_fecha_no_registradas.csv")
