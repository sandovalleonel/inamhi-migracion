import psycopg2

from scripts.procesar_data_temperatura import ingresar_tabla_temperatura
from scripts.procesar_data_precipitacion import ingresar_tabla_precipitacion
class PostgresConsultas:
    def __init__(self):
        self.pg_conn = None
        self.pg_cursor = None

    def _abrir_conexion(self):
        self.pg_conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="@dmin98",
            database="bandahm"
        )
        self.pg_cursor = self.pg_conn.cursor()

    def _cerrar_conexion(self):
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.pg_conn:
            self.pg_conn.close()

    def obtener_estaciones(self):
        self._abrir_conexion()
        schema = "administrativo"
        tabla = "vta_estaciones"
        sql = f"SELECT count(*) FROM {schema}.{tabla}"
        self.pg_cursor.execute(sql)
        data = self.pg_cursor.fetchall()
        self._cerrar_conexion()
        return data

    def _find_id_estacion(self):
        self._abrir_conexion()
        schema = "administrativo"
        tabla = "vta_estaciones"

        sql = f"SELECT codigo, id_estacion FROM {schema}.{tabla} where captor = 'MANUAL'"

        self.pg_cursor.execute(sql)
        data = self.pg_cursor.fetchall()
        self._cerrar_conexion()
        result_dict = {}
        for row in data:
            codigo = row[0]
            id_estacion = row[1]
            result_dict[codigo] = id_estacion

        return result_dict

    """
    obtener id_estacion del codigo_estacion
    """
    def buscar_id_estacion(self, lista_codigo_estaciones):
        diccionario_total_estacion_manual = self._find_id_estacion()
        data = {}
        estacion_sin_id = []
        for item in lista_codigo_estaciones:
            cod_estacion = item[0]
            if cod_estacion in diccionario_total_estacion_manual:
                data[cod_estacion] = diccionario_total_estacion_manual[cod_estacion]
            else:
                estacion_sin_id.append(cod_estacion)
                continue
        return data, estacion_sin_id
    def buscar_direccion_viento(self):
        self._abrir_conexion()
        schema = "administrativo"
        tabla = "direcciones_viento"

        sql = f"SELECT abreviacion,id_dir_viento  FROM {schema}.{tabla}"

        self.pg_cursor.execute(sql)
        data = self.pg_cursor.fetchall()
        self._cerrar_conexion()
        result_dict = {}
        for row in data:
            codigo = row[0]
            id_dir_Viento = row[1]
            result_dict[codigo] = id_dir_Viento

        return result_dict

    """
    preparar datos para guardar en postgres por estacion
    """

    def construir_data(self, data_list, dic_data_precip, id_estacion):
        self._abrir_conexion()

        ##liempieza y guardar lote temperatura
        ##guardar temperatura
        sql_temperatura,data_temperatura = ingresar_tabla_temperatura(data_list,id_estacion)
        self.pg_cursor.executemany(sql_temperatura,data_temperatura)
        self.pg_conn.commit()

        ##limpieza y guardar lote precipitacion
        #guardar precipitacion
        ingresar_tabla_precipitacion(data_list,id_estacion,dic_data_precip)


        self._cerrar_conexion()

