from scripts import constants
from scripts import utils
import pandas as pd


def ingresar_tabla_evaporacion(df_datos):
    sql = f""

    # obtener los dataframes de las tres horas
    df_07 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "ev07"]]
    df_13 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "ev13"]]
    df_19 = df_datos[["id_estacion", "id_usuario", "fecha_ingreso", "fecha_toma", "ev19"]]

    ##evaporacion
    tupla_total = []

    codigo_estacion = df_datos['codigo'].iloc[0]
    tupla_07 = _limpiar_diccionario_evaporacion(df_07, "07", codigo_estacion)
    tupla_13 = _limpiar_diccionario_evaporacion(df_13, "13", codigo_estacion)
    tupla_19 = _limpiar_diccionario_evaporacion(df_19, "19", codigo_estacion)

    return sql, tupla_total


def _limpiar_diccionario_evaporacion(df_evaporacion, hora, codigo):
    columna = 'ev' + hora
    df_evaporacion = df_evaporacion.rename(columns={columna: 'ev'})

    hora = int(hora)
    df_evaporacion['fecha_toma'] = pd.to_datetime(df_evaporacion['fecha_toma'], format='%Y-%m-%d')
    df_evaporacion['fecha_toma'] = df_evaporacion['fecha_toma'] + pd.Timedelta(hours=hora)
    df_evaporacion = df_evaporacion.sort_values('fecha_toma')

    df_evaporacion['ev'] = df_evaporacion['ev'].astype(float)
    # limpiar valores
    df_evaporacion.loc[df_evaporacion['ev'].isin(constants.VALES_OUT_RANGE_TEMPERATURE), 'ev'] = constants.VALUE_NULL
    # remplazar valores negativos bandera
    df_evaporacion.loc[df_evaporacion['ev'].isin(constants.VALUE_TO_FLAG), 'ev'] = constants.NEW_VALUE_TO_FLAG

    # generar reportes
    _reportes_evaporacion(df_evaporacion, codigo, hora)

    # exportar datos a arrays para guardaer en la  base
    return df_evaporacion.to_records(index=False).tolist()


def _reportes_evaporacion(df_total, codigo, hora):
    # reportes
    # total por variables
    totales = []
    total_ts = (codigo, f"ev{hora}", df_total['ev'].count(), df_total.shape[0])
    totales.append(total_ts)
    utils.write_tuplas_csv(totales, constants.NOMBRE_CARPETA + "/total_por_variable.csv")

    ##valores negativos
    tupla_negativos = []

    df_negativos = df_total[['fecha_toma', 'ev']]
    df_negativos = df_negativos.loc[(df_negativos['ev'] < constants.VALUE_CERO) & (df_negativos['ev'] != constants.NEW_VALUE_TO_FLAG)]
    df_negativos["variable"] = "ev" + str(hora)
    df_negativos["codigo"] = codigo
    df_negativos['fecha_toma'] = pd.to_datetime(df_negativos['fecha_toma']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tupla_negativos.extend(df_negativos.to_records(index=False).tolist())
    utils.write_tuplas_csv(tupla_negativos, constants.NOMBRE_CARPETA + "/evaporacion_negativos.csv")

    ##valores mayoresa a 30
    tupla_mayor30 = []
    df_mayor30 = df_total[['ev']]
    df_mayor30 = df_mayor30.loc[(df_mayor30['ev'] > constants.VaLUE_MAX_EVAPORACION)]
    variable = "ev" + str(hora)
    total_mayor30 = df_mayor30.shape[0]
    if total_mayor30 > 0:
        tupla_mayor30.append((codigo,variable,total_mayor30))
        utils.write_tuplas_csv(tupla_mayor30, constants.NOMBRE_CARPETA + "/evaporacion_mayor30.csv")




