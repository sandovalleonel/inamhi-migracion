import mysql.connector


class MysqlConsultas:
    def __init__(self):
        self.mysql_conn = None
        self.mysql_cursor = None

    def abrir_conexion(self):
        self.mysql_conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="mch"
        )
        self.mysql_cursor = self.mysql_conn.cursor()

    def cerrar_conexion(self):
        if self.mysql_cursor:
            self.mysql_cursor.close()
        if self.mysql_conn:
            self.mysql_conn.close()



    """
    funcion obtener el listado de todas la estaciones registradas en clm0002
    """

    def obtener_lista_estaciones(self):
        self.abrir_conexion()
        sql = "select  e.codigo from mch.clm0002 e group by  e.codigo; "
        self.mysql_cursor.execute(sql)
        data = self.mysql_cursor.fetchall()
        self.cerrar_conexion()
        return data


    """ 
    obtener una lista de registros por estacion
    """
    def obtener_datos_por_estacion(self, estacion):
        self.abrir_conexion()
        sql = f"SELECT * FROM clm0002  where codigo = '{estacion}'  ORDER BY anio ASC,mes ASC,dia ASC"
        self.mysql_cursor.execute(sql)
        data = self.mysql_cursor.fetchall()
        self.cerrar_conexion()
        return data

    """ 
    obtener una lista de registros precipitacion por estacion en tabla 2
    """
    def obtener_datos_presipitacion_por_estacion(self, estacion):

        self.abrir_conexion()
        sql = f"SELECT * FROM plv0002  where codigo = '{estacion}'  ORDER BY anio ASC,mes ASC,dia ASC"
        self.mysql_cursor.execute(sql)
        data = self.mysql_cursor.fetchall()
        self.cerrar_conexion()
        dic_data = {}

        for item in data:
            condPk = item[0]+'-'+str(item[1])+'-'+str(item[2])+'-'+str(item[3])
            dic_data[condPk] = item
        return dic_data


