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
    obtener una lista de registros paginado
    """

    def obtener_datos_lotes_por_columna(self, inicia_en, cantida_lote, codigos_elements):
        self.abrir_conexion()
        offset = max(0, inicia_en)  # Asegurarse de que el offset no sea negativo
        limit = max(0, cantida_lote)  # Asegurarse de que el límite no sea negativo
        sql = f"SELECT * FROM clm0002  where codigo IN ({codigos_elements})  ORDER BY codigo,anio,mes,dia ASC  LIMIT %s OFFSET %s"
        self.mysql_cursor.execute(sql, (limit, offset))
        data = self.mysql_cursor.fetchall()
        self.cerrar_conexion()
        return data


    """ 
    obtener una lista de registros por estacion
    """

    def obtener_datos_por_estacion(self, estacion):
        self.abrir_conexion()
        sql = f"SELECT * FROM clm0002  where codigo = '{estacion}'  ORDER BY codigo,anio,mes,dia ASC"
        self.mysql_cursor.execute(sql)
        data = self.mysql_cursor.fetchall()
        self.cerrar_conexion()
        return data

    """ 
    obtener una lista de registros precipitacion por estacion en tabla 2
    """

    def obtener_datos_presipitacion_por_estacion(self, estacion):
        self.abrir_conexion()
        sql = f"SELECT * FROM plv0002  where codigo = '{estacion}'  ORDER BY codigo,anio,mes,dia ASC"
        self.mysql_cursor.execute(sql)
        data = self.mysql_cursor.fetchall()
        self.cerrar_conexion()
        return data


