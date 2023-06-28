import mysql.connector

"""
versión: 1.00, fecha : 20/06/2023
Class Mysql crear una coneccion obtener ddatos de la base
Copyright. INAMHI <www.inamhi.gob.ec>. Todos los derechos reservados.
"""


class MysqlConsultas:
    def __init__(self):
        self.mysql_conn = None
        self.mysql_cursor = None

    """
    abrir la coneccion a mysql
    """

    def abrir_conexion(self):
        self.mysql_conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="mch"
        )
        self.mysql_cursor = self.mysql_conn.cursor()

    """
    Cerrar coneccion de mysql
    """

    def cerrar_conexion(self):
        if self.mysql_cursor:
            self.mysql_cursor.close()
        if self.mysql_conn:
            self.mysql_conn.close()

    """
    obtner todos los codigo de estaciones a migrar
    Return:
        data: array de tuplas de la consulta
    """

    def obtener_lista_estaciones(self):
        self.abrir_conexion()
        sql = "select  e.codigo from mch.clm0002 e group by  e.codigo; "
        self.mysql_cursor.execute(sql)
        data = self.mysql_cursor.fetchall()
        self.cerrar_conexion()
        return data

    """ 
    obtener todos los datos a mygrar de una estacion especifica
    Args:
        estacion:String con el codigo de estacion
    Return:
        data: array de tuplas de la consulta
    """

    def obtener_datos_por_estacion(self, estacion):
        self.abrir_conexion()
        sql = f"SELECT * FROM clm0002  where codigo = '{estacion}'  ORDER BY anio ASC,mes ASC,dia ASC"
        self.mysql_cursor.execute(sql)
        data = self.mysql_cursor.fetchall()
        self.cerrar_conexion()
        return data

    """ 
    obtner datos de precicpitacion de la tabla secundaria
    Args:
        estacion:String con el codigo de estacion
    Return:
        data: array de tuplas de la consulta
    """

    def obtener_datos_presipitacion_por_estacion(self, estacion):

        self.abrir_conexion()
        sql = f"SELECT * FROM plv0002  where codigo = '{estacion}'  ORDER BY anio ASC,mes ASC,dia ASC"
        self.mysql_cursor.execute(sql)
        data = self.mysql_cursor.fetchall()
        self.cerrar_conexion()

        return data
