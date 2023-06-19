from scripts import constants
from scripts import utils
from datetime import datetime

def ingresar_tabla_precipitacion(tupla_datos, id_estacion,dic_precipitacion):

    id_usuario = constants.ID_ADMIN
    fecha_Actual = str(datetime.now())
    sql = f"INSERT INTO convencionales2._171481h " \
          f"(id_estacion, id_usuario, fecha_toma, fecha_ingreso,valor) " \
          f"values (%s,%s,%s,%s,%s)"

    tupla_total = []
    for item in tupla_datos:
        if item[3] > 31:
            continue
        fecha_registro_07 = utils.fecha_Registro(item[1], item[2], item[3], "07")
        fecha_registro_13 = utils.fecha_Registro(item[1], item[2], item[3], "13")
        fecha_registro_19 = utils.fecha_Registro(item[1], item[2], item[3], "19")

        tupla_total.append((id_estacion, id_usuario, fecha_registro_07, fecha_Actual, item[12]))
        tupla_total.append((id_estacion, id_usuario, fecha_registro_13, fecha_Actual, item[13]))
        tupla_total.append((id_estacion, id_usuario, fecha_registro_19, fecha_Actual, item[14]))

    #print('\n'.join(map(str, tupla_total[:10])))
    #return sql,limpiar_registros_a_guardar(tupla_total)