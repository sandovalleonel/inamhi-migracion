import psycopg2
import datetime
import csv_util
import constants

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
        return data,estacion_sin_id

    """
    preparar datos para gardar en postgres por estacion
    """

    def construir_data(self, data_list, id_estacion):
        self._abrir_conexion()
        schema = "convencionales2"
        id_admin = 0
        lista_horas = ['07', '13', '19']
        registros_dia_32 = []

        for fila in data_list:
            if fila[3] > 31:
                registros_dia_32.append(fila)
                continue
            try:
                a=1
                #self.ingresar_tabla_temperatura(schema, id_admin, id_estacion, lista_horas, fila)
                ##ERR la columna «num_lecturas» no puede ser nula..
                #self.ingresar_tabla_precipitacion(schema, id_admin, id_estacion, lista_horas, fila)
            except Exception as e:
                print("error fila ",fila)

        if len(registros_dia_32)>0:
            csv_util.csv_write_estcion_dia_32(registros_dia_32,constants.NOMBRE_CARPETA+'/'+constants.NOMBRE_ARCHIVO_DIA_32)
        self._cerrar_conexion()

    """
    construir fecha de registro
    """
    def fecha_Registro(self,fila,hora):
        return "{}-{}-{} {}:00:00".format(str(fila[1]),
                                          str(fila[2]).zfill(2),
                                          str(fila[3]).zfill(2),
                                          str(hora).zfill(2))
    """
    ingresar datos en la tabla termperatura de las 7,13,19 (h)
    """
    def ingresar_tabla_temperatura(self, schema, id_admin, id_estacion, horas, fila):

        for index, item_hora in horas:
            index = int(index)
            fecha_historico = self.fecha_Registro(fila,item_hora)
            fecha_actual = str(datetime.datetime.now())
            sql_termometro = f"INSERT INTO {schema}._293161h " \
                             f"(id_estacion, id_usuario, fecha_toma, fecha_ingreso,term_seco,term_hmd) " \
                             f"values ({id_estacion},{id_admin},'{fecha_historico}','{fecha_actual}',{fila[6 + index]},{fila[9 + index]})"
            self.pg_cursor.execute(sql_termometro)
            self.pg_conn.commit()


    """
    ingresar datos tabla precipitacion 171481h
    """
    def ingresar_tabla_precipitacion(self, schema, id_admin, id_estacion, horas, fila):

        for index, item_hora in horas:
            index = int(index)
            fecha_historico = self.fecha_Registro(fila,item_hora)
            fecha_actual = str(datetime.datetime.now())
            sql_termometro = f"INSERT INTO {schema}._171481h " \
                             f"(id_estacion, id_usuario, fecha_toma, fecha_ingreso,valor,num_lecturas) " \
                             f"values ({id_estacion},{id_admin},'{fecha_historico}','{fecha_actual}',{fila[12 + index]},1)"
            self.pg_cursor.execute(sql_termometro)
            self.pg_conn.commit()

