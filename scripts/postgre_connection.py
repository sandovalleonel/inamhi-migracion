import psycopg2
import pandas as pd
from scripts.procesar_data_temperatura import ingresar_tabla_temperatura
from scripts.procesar_data_precipitacion import ingresar_tabla_precipitacion
from scripts.procesar_data_evaporacion import ingresar_tabla_evaporacion
from scripts.procesar_data_viento import ingresar_tabla_viento
from scripts.procesar_data_nubosidad import ingresar_tabla_nubosidad
from scripts.procesar_data_tmaxtmin import ingresar_tabla_tmaxtmin
from scripts import constants
from datetime import datetime
"""
versión: 1.00, fecha : 20/06/2023
Class Postgres crear una coneccion , obtner y guardar datos en la base
Copyright. INAMHI <www.inamhi.gob.ec>. Todos los derechos reservados.

"""


class PostgresConsultas:
    def __init__(self):
        self.pg_conn = None
        self.pg_cursor = None

    """
    abrir la coneccion de postgres
    """

    def _abrir_conexion(self):
        self.pg_conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="@dmin98",
            database="bandahm"
        )
        self.pg_cursor = self.pg_conn.cursor()

    """
    cerrar la coneccion de postgres
    """

    def _cerrar_conexion(self):
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.pg_conn:
            self.pg_conn.close()

    """
    obtener todas las estaciones ya registradas en postgres
    Return:
        data: array de tuplas con las estaciones ya registradas
    """

    def obtener_estaciones(self):
        self._abrir_conexion()
        schema = "administrativo"
        tabla = "vta_estaciones"
        sql = f"SELECT count(*) FROM {schema}.{tabla}"
        self.pg_cursor.execute(sql)
        data = self.pg_cursor.fetchall()
        self._cerrar_conexion()
        return data

    """
    buscar todas las estaciones ya registradas en postgres
    Return:
        result_dict: diccionario con el codigo y id de la estacion
    """

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
    obtener las estaciones con id registrados en postgres
    Args:
        lista_codigo_estaciones:array con las estaciones registradas en mysql clm0002
    Return:
        data:diccionario con las estaciones y ids de las que se pueden guardar
        estacion_sin_id: array con los codigos de estaciones que no se pueden guardar
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

    """
    buscar las claves refencias para direccion del viento
    Return:
        result_dict: diccionario con la abreviacion y el id_direccion_viento
    """

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
    Limpar datos por variable y guardar en las respectivas tablas
    Args:
        data_list:array de tuplas con los datos de mysql por estacion
        data_precipitacion:array de tuplas con los datos de precipitacion de tabla secundaria
        id_estacion:String con el id para guardar en postgres
        dic_id_viento:diccionario con los id para relacionar direccion viento
    """

    def construir_data(self, data_list, data_precipitacion, id_estacion, dic_id_viento):
        id_usuario = constants.ID_ADMIN
        fecha_actual = datetime.now()
        is_data_precipitacion_empty = False

        df = pd.DataFrame(data_list)
        df.columns = constants.COLUMNS_CLM0002
        df["id_estacion"] = id_estacion
        df["id_usuario"] = id_usuario
        df["fecha_ingreso"] = fecha_actual
        df["fecha_ingreso"] = df["fecha_ingreso"].dt.strftime('%Y-%m-%d %H:%M:%S')

        df_dias32 = df.loc[(df['dia'] == 32)]

        df = df.drop(df[df['dia'] == 32].index)

        # crear columna fecha toma con con 00h la hora se setea en cada metodo
        df["fecha_toma"] = df["anio"].astype(str) \
                           + "-" + df["mes"].astype(str).str.zfill(2) \
                           + "-" + df["dia"].astype(str).str.zfill(2)
        df.reset_index(drop=True, inplace=True)  # reiniciar index

        if data_precipitacion:  # Verifica si `data_precipitacion` tiene elementos
            df_precipitacion = pd.DataFrame(data_precipitacion)
            df_precipitacion.columns = constants.COLUMNS_PVL0002
            df_precipitacion["id_estacion"] = id_estacion
            df_precipitacion["id_usuario"] = id_usuario
            df_precipitacion["fecha_ingreso"] = fecha_actual
            df_precipitacion["fecha_ingreso"] = df_precipitacion["fecha_ingreso"].dt.strftime('%Y-%m-%d %H:%M:%S')
            df_precipitacion = df_precipitacion.drop(df_precipitacion[df_precipitacion['dia'] == 32].index)
            df_precipitacion["fecha_toma"] = df_precipitacion["anio"].astype(str) \
                                             + "-" + df_precipitacion["mes"].astype(str).str.zfill(2) \
                                             + "-" + df_precipitacion["dia"].astype(str).str.zfill(2)


            df_precipitacion.reset_index(drop=True, inplace=True)  # reiniciar index
        else:
            df_precipitacion = pd.DataFrame()
            is_data_precipitacion_empty = True

        self._abrir_conexion()

        ##liempieza y guardar lote temperatura
        ##guardar temperatura
        sql_temperatura, data_temperatura = ingresar_tabla_temperatura(df)
        self.pg_cursor.executemany(sql_temperatura, data_temperatura)
        self.pg_conn.commit()

        ##limpieza y guardar lote precipitacion
        # guardar precipitacion
        sql_precipitacion, data_precipitacion = ingresar_tabla_precipitacion(df, df_precipitacion,is_data_precipitacion_empty,df_dias32)
        self.pg_cursor.executemany(sql_precipitacion, data_precipitacion)
        self.pg_conn.commit()


        # limpieza y guardar lote evaporacion
        # guardar evaporacion
        sql_evaporacion, data_evaporacion = ingresar_tabla_evaporacion(df)
        self.pg_cursor.executemany(sql_evaporacion, data_evaporacion)
        self.pg_conn.commit()


        # limpieza y guardar lote viento
        # guardar viento
        sql_viento, data_viento = ingresar_tabla_viento(df, dic_id_viento)
        self.pg_cursor.executemany(sql_viento, data_viento)
        self.pg_conn.commit()


        # limpieza y guardar lote nubosidad
        # guardar nubosidad
        sql_nubosidad, data_nubosidad = ingresar_tabla_nubosidad(df)
        self.pg_cursor.executemany(sql_nubosidad, data_nubosidad)
        self.pg_conn.commit()

        # limpieza y guardar lote tmaxtmin
        # guardar tmaxtmin
        sql_tmaxtmin, data_tmaxtmin = ingresar_tabla_tmaxtmin(df)
        self.pg_cursor.executemany(sql_tmaxtmin, data_tmaxtmin)
        self.pg_conn.commit()

        self._cerrar_conexion()
