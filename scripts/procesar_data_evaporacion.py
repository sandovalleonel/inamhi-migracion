from scripts import constants
from scripts import utils
import pandas as pd
from scripts import reporte_comun


def ingresar_tabla_evaporacion(df_datos):
    sql = f"INSERT INTO convencionales2._614161h" \
          f"(id_estacion, id_usuario, fecha_ingreso, fecha_toma,  evaporacion_media , id_tipo_calculo)" \
          f"VALUES(%s, %s, %s, %s, %s, %s)"

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

    tupla_total.extend(tupla_07)
    tupla_total.extend(tupla_13)
    tupla_total.extend(tupla_19)

    return (sql, tupla_total) if constants.SAVE_DATA else (sql, [])


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
    if constants.GENERAR_REPORTES:
        _reportes_evaporacion(df_evaporacion, codigo, hora)

    df_evaporacion['id_tipo_calculo'] = constants.ID_TIPO_CALCULO
    df_evaporacion["fecha_toma"] = df_evaporacion["fecha_toma"].dt.strftime('%Y-%m-%d %H:%M:%S')

    # exportar datos a arrays para guardaer en la  base
    return df_evaporacion.to_records(index=False).tolist()


def _reportes_evaporacion(df_total, codigo, hora):
    # reportes
    ################## total datos guardados por variable ##################
    reporte_comun.total_por_variable(codigo, hora, "ev", df_total)

    ##valores negativos
    reporte_comun.registros_menores_por_variable(codigo, hora, 'ev', -100, df_total, 'negativos_test')

    ##valores mayoresa a 30

    ################## fecha sin registrar datos ##################
    nombre_archivo = 'dato_no_tegistrado_evaporacion(fechas)'
    reporte_comun.fechas_no_registrada_por_variable(codigo, hora, 'ev', df_total, nombre_archivo)
