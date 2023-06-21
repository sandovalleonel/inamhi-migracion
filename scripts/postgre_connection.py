import psycopg2
import pandas as pd
from scripts.procesar_data_temperatura import ingresar_tabla_temperatura
from scripts.procesar_data_precipitacion import ingresar_tabla_precipitacion
from scripts import constants


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

    def construir_data(self, data_list, data_precipitacion, id_estacion):
        id_usuario = constants.ID_ADMIN
        fecha_actual = pd.Timestamp.now()
        is_data_precipitacion_empty = False

        df = pd.DataFrame(data_list)
        df.columns = constants.COLUMNS_CLM0002
        df["id_estacion"] = id_estacion
        df["id_usuario"] = id_usuario
        df["fecha_ingreso"] = fecha_actual

        if data_precipitacion:  # Verifica si `data_precipitacion` tiene elementos
            df_precipitacion = pd.DataFrame(data_precipitacion)
            df_precipitacion.columns = constants.COLUMNS_PVL0002
            df_precipitacion["id_estacion"] = id_estacion
            df_precipitacion["id_usuario"] = id_usuario
            df_precipitacion["fecha_ingreso"] = fecha_actual
        else:
            df_precipitacion = pd.DataFrame()
            is_data_precipitacion_empty = True

        self._abrir_conexion()

        ##liempieza y guardar lote temperatura
        ##guardar temperatura
        #sql_temperatura,data_temperatura = ingresar_tabla_temperatura(df)
        #self.pg_cursor.executemany(sql_temperatura,data_temperatura)
        #self.pg_conn.commit()

        ##limpieza y guardar lote precipitacion
        # guardar precipitacion
        sql_precipitacion,data_precipitacion = ingresar_tabla_precipitacion(df,  df_precipitacion, is_data_precipitacion_empty)
        self.pg_cursor.executemany(sql_precipitacion, data_precipitacion)
        self.pg_conn.commit()
        self._cerrar_conexion()
