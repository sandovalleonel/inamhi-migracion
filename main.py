import constants
import csv_util
from mysql_conecction import MysqlConsultas
from postgre_connection import PostgresConsultas
from csv_util import csv_write_estacion_sin_id
import constants
import multiprocessing

def obtener_estaciones_ids():
    obj_mysql = MysqlConsultas()
    obj_postgres = PostgresConsultas()
    listado_codigos_estacion = obj_mysql.obtener_lista_estaciones()
    dicc_estaciones = obj_postgres.buscar_id_estacion(listado_codigos_estacion)
    csv_write_estacion_sin_id(dicc_estaciones[1], constants.NOMBRE_CARPETA+'/'+constants.NOMBRE_ARCHIVO_ESTACION_SI_ID)
    return dicc_estaciones[0]

"""
def obtener_datos_estaciones(dic_estaciones):
    obj_mysql = MysqlConsultas()
    obj_postgres = PostgresConsultas()

    for clave, valor in dic_estaciones.items():
        print(clave, valor)
        data = obj_mysql.obtener_datos_por_estacion(clave)
        print("total datos obtenidos ",len(data))
        #data_preciptacion = obj_mysql.obtener_datos_presipitacion_por_estacion(clave)
        #obj_postgres.construir_data(data,valor)
"""
def procesar_elemento(elemento):
    clave,valor = elemento
    obj_mysql = MysqlConsultas()
    obj_postgres = PostgresConsultas()
    data = obj_mysql.obtener_datos_por_estacion(clave)

    print(f"total datos obtenidos de estacion: {clave} id: {valor} tot:", len(data))
    data_preciptacion = obj_mysql.obtener_datos_presipitacion_por_estacion(clave)

    obj_postgres.construir_data(data,data_preciptacion,valor)

def lanzar_paralelismo():
    dic_estaciones_con_id = obtener_estaciones_ids()
    #print(dic_estaciones_con_id)
    ##########TEMP
    primer_registro = next(iter(dic_estaciones_con_id.items()))
    dic_estaciones_con_id = dict([primer_registro])
    #print(dic_estaciones_con_id)
    ######
    num_procesos = 1#multiprocessing.cpu_count() #4
    pool = multiprocessing.Pool(processes=num_procesos)
    pool.map(procesar_elemento, dic_estaciones_con_id.items())
    pool.close()
    pool.join()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    csv_util.crear_carpeta(constants.NOMBRE_CARPETA)
    csv_util.eliminar_archivo(constants.NOMBRE_CARPETA+'/'+constants.NOMBRE_ARCHIVO_DIA_32)
    lanzar_paralelismo()
