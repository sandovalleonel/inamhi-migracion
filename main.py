from scripts import  utils
from scripts import constants
from scripts.mysql_conecction import MysqlConsultas
from scripts.postgre_connection import PostgresConsultas
from scripts.utils import csv_write_array_string
import multiprocessing

def obtener_estaciones_ids():
    obj_mysql = MysqlConsultas()
    obj_postgres = PostgresConsultas()
    listado_codigos_estacion = obj_mysql.obtener_lista_estaciones()
    dicc_estaciones = obj_postgres.buscar_id_estacion(listado_codigos_estacion)
    csv_write_array_string(dicc_estaciones[1], f"{constants.NOMBRE_CARPETA}/'estaciones_si_id.csv")
    csv_write_array_string(dicc_estaciones[0], f"{constants.NOMBRE_CARPETA}/estaciones_cod_id.csv")
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
    dic_data_preciptacion = obj_mysql.obtener_datos_presipitacion_por_estacion(clave)

    obj_postgres.construir_data(data,dic_data_preciptacion,valor)

"""
funcion para procesar las estaciones en procesos paralelos
"""
def lanzar_paralelismo():
    dic_estaciones_con_id = obtener_estaciones_ids()
    ####################TEMP
    primer_registro = next(iter(dic_estaciones_con_id.items()))
    dic_estaciones_con_id = dict([primer_registro])
    #print(dic_estaciones_con_id)
    ########################
    num_procesos = 1#multiprocessing.cpu_count() #4
    pool = multiprocessing.Pool(processes=num_procesos)
    pool.map(procesar_elemento, dic_estaciones_con_id.items())
    pool.close()
    pool.join()


"""
Main
"""
if __name__ == '__main__':
    utils.borrar_carpeta(constants.NOMBRE_CARPETA)
    utils.crear_carpeta(constants.NOMBRE_CARPETA)
    utils.crear_carpeta(f"{constants.NOMBRE_CARPETA}/dia32")
    lanzar_paralelismo()
