from scripts import constants
from scripts import utils
from datetime import datetime

"""
ingresar datos en la tabla termperatura de las 7,13,19 (h)
[(1,"a",3),
(1,"b",3),
(1,"c",3)]
"""


def ingresar_tabla_temperatura(tupla_datos, id_estacion):
    id_usuario = constants.ID_ADMIN
    fecha_Actual = str(datetime.now())
    sql = f"INSERT INTO convencionales2._293161h " \
          f"(id_estacion, id_usuario, fecha_toma, fecha_ingreso,term_seco,term_hmd) " \
          f"values (%s,%s,%s,%s,%s,%s)"

    tupla_total = []

    filas_dia_32 = []
    for item in tupla_datos:

        if item[3] > 31:
            filas_dia_32.append(item)
            continue
        fecha_registro_07 = utils.fecha_Registro(item[1], item[2], item[3], "07")
        fecha_registro_13 = utils.fecha_Registro(item[1], item[2], item[3], "13")
        fecha_registro_19 = utils.fecha_Registro(item[1], item[2], item[3], "19")

        tupla_total.append((id_estacion, id_usuario, fecha_registro_07, fecha_Actual, item[6], item[9]))
        tupla_total.append((id_estacion, id_usuario, fecha_registro_13, fecha_Actual, item[7], item[10]))
        tupla_total.append((id_estacion, id_usuario, fecha_registro_19, fecha_Actual, item[8], item[11]))
    utils.csv_write_estcion_dia_32(filas_dia_32,f"{constants.NOMBRE_CARPETA}/dia32/{id_estacion}_dia_32.csv")

    #print('\n'.join(map(str, tupla_total[:10])))
    return sql,limpiar_registros_a_guardar(tupla_total)



"""
limpiar datos temperatura
"""


def limpiar_registros_a_guardar(data):
    total_datos_ts = 0
    total_datos_th = 0

    anio = 0

    nueva_lista = []
    string_reporte = []
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

        #valores no nulso a guardar
        if nuevo_valor_ts is not None:
            total_datos_ts = total_datos_ts + 1


        if nuevo_valor_th is not None:
            total_datos_th = total_datos_th + 1

        # fila vacia evitar guardar
        if (nuevo_valor_ts is None) and (nuevo_valor_th is None):
            #print("no tenemos valores a guardar")
            anio = fila[2].year
            continue

        tupla_actualizada = (fila[0], fila[1], fila[2], fila[3], nuevo_valor_ts, nuevo_valor_th)
        nueva_lista.append(tupla_actualizada)

        if anio != fila[2].year and anio != 0:
            #campo,id_estacion,año,datos,
            string_reporte.append(f"TS, {fila[0]},{anio} , {total_datos_ts}")
            string_reporte.append(f"TH, {fila[0]},{anio} , {total_datos_th}")
            total_datos_ts = 0
            total_datos_th = 0

        anio = fila[2].year

    utils.csv_write_array_string(string_reporte,f"{constants.NOMBRE_CARPETA}/reporte.csv")
    return nueva_lista
