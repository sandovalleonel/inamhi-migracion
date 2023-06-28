import multiprocessing
from scripts import  utils
from scripts import constants
from scripts.mysql_conecction import MysqlConsultas
from scripts.postgre_connection import PostgresConsultas
"""
versión: 1.00, fecha : 20/06/2023
Class Main
Copyright. INAMHI <www.inamhi.gob.ec>. Todos los derechos reservados.
"""



"""
buscar todas las estaciones que ya estan registrado en en postgres
genera reporte de las estaciones que ya estan registradas en postgres 
y estaciones que no estan registradas en postgres

Return:
    dicc_estaciones:diccionario con las estaciones registrados en postgres
"""
def obtener_estaciones_ids():
    obj_mysql = MysqlConsultas()
    obj_postgres = PostgresConsultas()
    listado_codigos_estacion = obj_mysql.obtener_lista_estaciones()
    dicc_estaciones = obj_postgres.buscar_id_estacion(listado_codigos_estacion)
    if constants.GENERAR_REPORTES:
        utils.csv_write_array_string(dicc_estaciones[1], f"{constants.NOMBRE_CARPETA}/estaciones_sin_id.csv")
        utils.csv_write_array_string(dicc_estaciones[0], f"{constants.NOMBRE_CARPETA}/estaciones_con_id.csv")
    return dicc_estaciones[0]

"""
trae datos de mysql por estacion y mandar a procesar por cada variable
Args:
    dic_estaciones: diccionario con las estaciones

"""
def obtener_datos_estaciones(dic_estaciones):
    obj_mysql = MysqlConsultas()
    obj_postgres = PostgresConsultas()
    dic_id_viento = obj_postgres.buscar_direccion_viento()

    for clave, valor in dic_estaciones.items():
        data = obj_mysql.obtener_datos_por_estacion(clave)
        print(f"total datos obtenidos de estacion: {clave} id: {valor} tot:", len(data))
        data_preciptacion = obj_mysql.obtener_datos_presipitacion_por_estacion(clave)
        obj_postgres.construir_data(data,data_preciptacion,valor,dic_id_viento)

"""
def procesar_elemento(elemento):
    clave,valor = elemento
    obj_mysql = MysqlConsultas()
    obj_postgres = PostgresConsultas()
    data = obj_mysql.obtener_datos_por_estacion(clave)

    print(f"total datos obtenidos de estacion: {clave} id: {valor} tot:", len(data))
    dic_data_preciptacion = obj_mysql.obtener_datos_presipitacion_por_estacion(clave)

    dic_id_viento = obj_postgres.buscar_direccion_viento()
    obj_postgres.construir_data(data,dic_data_preciptacion,valor,dic_id_viento)
"""
"""
funcion para procesar las estaciones en procesos paralelos
"""
"""
def lanzar_paralelismo():
    dic_estaciones_con_id = obtener_estaciones_ids()
    ####################TEMP
    #primer_registro = next(iter(dic_estaciones_con_id.items()))
    #dic_estaciones_con_id = dict([primer_registro])
    #print(dic_estaciones_con_id)
    ########################
    num_procesos = 1#multiprocessing.cpu_count() #4
    pool = multiprocessing.Pool(processes=num_procesos)
    pool.map(procesar_elemento, dic_estaciones_con_id.items())
    pool.close()
    pool.join()
"""

"""
Main funcion principal
"""
if __name__ == '__main__':
    utils.borrar_carpeta(constants.NOMBRE_CARPETA)
    utils.crear_carpeta(constants.NOMBRE_CARPETA)
    #lanzar_paralelismo()
    dic_estaciones_con_id = obtener_estaciones_ids()
    obtener_datos_estaciones(dic_estaciones_con_id)
