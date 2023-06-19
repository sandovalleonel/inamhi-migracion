from scripts import constants
from scripts import utils
from datetime import datetime
import pandas as pd

"""
ingresar datos en la tabla termperatura de las 7,13,19 (h)
[(1,"a",3),
(1,"b",3),
(1,"c",3)]
"""


def ingresar_tabla_temperatura(df_datos, id_estacion):
    id_usuario = constants.ID_ADMIN
    fecha_Actual = str(datetime.now())
    sql = f"INSERT INTO convencionales2._293161h " \
          f"(id_estacion, id_usuario, fecha_toma, fecha_ingreso,term_seco,term_hmd) " \
          f"values (%s,%s,%s,%s,%s,%s)"

    temp_df_07 = df_datos[["codigo", "anio", "mes", "dia", "ts07", "th07"]]
    temp_df_13 = df_datos[["codigo", "anio", "mes", "dia", "ts13", "th13"]]
    temp_df_19 = df_datos[["codigo", "anio", "mes", "dia", "ts19", "th19"]]

    nuevo_df = temp_df_07[temp_df_07['dia'] != 32].copy()
    nuevo_df.reset_index(drop=True, inplace=True)
    print(temp_df_07.head(34))
    print(nuevo_df.head(34))

    """
    tupla_07 = []
    tupla_13 = []
    tupla_19 = []

    filas_dia_32 = []
    for item in tupla_datos:

        if item[3] > 31:
            ##validar si el dia 32 de precipitacion es
            filas_dia_32.append(item)
            continue
        fecha_registro_07 = utils.fecha_Registro(item[1], item[2], item[3], "07")
        fecha_registro_13 = utils.fecha_Registro(item[1], item[2], item[3], "13")
        fecha_registro_19 = utils.fecha_Registro(item[1], item[2], item[3], "19")

        tupla_07.append((id_estacion, id_usuario, fecha_registro_07, fecha_Actual, item[6], item[9]))
        tupla_13.append((id_estacion, id_usuario, fecha_registro_13, fecha_Actual, item[7], item[10]))
        tupla_19.append((id_estacion, id_usuario, fecha_registro_19, fecha_Actual, item[8], item[11]))
    utils.csv_write_estcion_dia_32(filas_dia_32,f"{constants.NOMBRE_CARPETA}/dia32/{id_estacion}_dia_32.csv")

    #print('\n'.join(map(str, tupla_total[:10])))
    tupla_final = []
    limpiar_registros_a_guardar(tupla_07)
    #tupla_final.extend(limpiar_registros_a_guardar(tupla_13))
    #tupla_final.extend(limpiar_registros_a_guardar(tupla_19))
    return sql,tupla_final
    """


"""
limpiar datos temperatura
"""


def limpiar_registros_a_guardar(data):
    df = pd.DataFrame(data)
    df.columns = ["id_estacion", "id_ususario", "fecha_registro", "fecha_actual", "ts", "th"]

    print(df.head(10))

    total_datos_ts = 0
    total_datos_th = 0
    anio = 0

    nueva_lista = []
    string_reporte = []
    index = 0
    for fila in data:
        nuevo_valor_ts = float(fila[4])
        nuevo_valor_th = float(fila[5])

        # fuera de rango
        if nuevo_valor_ts in constants.VALES_OUT_RANGE_TEMPERATURE:
            nuevo_valor_ts = constants.NEW_VALUE_OUT_RANGE_TEMPERATURE

        if nuevo_valor_th in constants.VALES_OUT_RANGE_TEMPERATURE:
            nuevo_valor_th = constants.NEW_VALUE_OUT_RANGE_TEMPERATURE

        # banderas
        if nuevo_valor_ts in constants.VALUE_TO_FLAG:
            nuevo_valor_ts = constants.NEW_VALUE_TO_FLAG

        if nuevo_valor_th in constants.VALUE_TO_FLAG:
            nuevo_valor_th = constants.NEW_VALUE_TO_FLAG

        # valores no nulso a guardar
        if nuevo_valor_ts is not None:
            total_datos_ts = total_datos_ts + 1

        if nuevo_valor_th is not None:
            total_datos_th = total_datos_th + 1

        tupla_actualizada = (fila[0], fila[1], fila[2], fila[3], nuevo_valor_ts, nuevo_valor_th)
        nueva_lista.append(tupla_actualizada)

        if anio != fila[2].year and anio != 0:
            # campo,id_estacion,año,datos,
            string_reporte.append(f"TS, {fila[0]},{anio},{data[index - 1][2]} , {total_datos_ts}")
            string_reporte.append(f"TH, {fila[0]},{anio},{data[index - 1][2]} , {total_datos_th}")
            total_datos_ts = 0
            total_datos_th = 0

        anio = fila[2].year
        index = index + 1

    string_reporte.append("*" * 23)
    utils.csv_write_array_string(string_reporte, f"{constants.NOMBRE_CARPETA}/reporte.csv")
    return nueva_lista
