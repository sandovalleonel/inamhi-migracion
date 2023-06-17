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
        return data, estacion_sin_id

    """
    construir fecha de registro
    """

    def fecha_Registro(self, fila, hora):
        return "{}-{}-{} {}:00:00".format(str(fila[1]),
                                          str(fila[2]).zfill(2),
                                          str(fila[3]).zfill(2),
                                          str(hora).zfill(2))

    """
    preparar datos para guardar en postgres por estacion
    """

    def construir_data(self, data_list, data_precip, id_estacion):
        self._abrir_conexion()
        id_admin = 0
        lista_horas = ['07', '13', '19']
        registros_dia_32 = []

        data_tabla_temperatura = []
        sql_tabla_temperatura = None

        for fila in data_list:
            if fila[3] > 31:
                registros_dia_32.append(fila)
                continue

            # formar matriz datos temperatura
            datos_temp_return = self.ingresar_tabla_temperatura(id_admin, id_estacion, lista_horas, fila)
            data_tabla_temperatura.extend(datos_temp_return[0])
            sql_tabla_temperatura = datos_temp_return[1]

            # self.ingresar_tabla_precipitacion(schema, id_admin, id_estacion, lista_horas, fila,data_precip)

        # escribir registros con dias 32
        if len(registros_dia_32) > 0:
            csv_util.csv_write_estcion_dia_32(registros_dia_32,
                                              constants.NOMBRE_CARPETA + '/' + constants.NOMBRE_ARCHIVO_DIA_32)

        ##liempieza y guardar lote temperatura
        data_tabla_temperatura = self.limpiar_registros_a_guardar(data_tabla_temperatura)
        self.pg_cursor.executemany(sql_tabla_temperatura, data_tabla_temperatura)
        self.pg_conn.commit()

        self._cerrar_conexion()

    """
    ingresar datos en la tabla termperatura de las 7,13,19 (h)
    """

    def ingresar_tabla_temperatura(self, id_admin, id_estacion, horas, fila):

        sql = f"INSERT INTO convencionales2._293161h " \
              f"(id_estacion, id_usuario, fecha_toma, fecha_ingreso,term_seco,term_hmd) " \
              f"values (%s,%s,%s,%s,%s,%s)"

        datos_tre_horas = []
        cont = 0
        for item_hora in horas:
            term_seco = fila[6 + cont]
            term_hmd = fila[9 + cont]
            fecha_historico = self.fecha_Registro(fila, item_hora)
            fecha_actual = str(datetime.datetime.now())
            datos_tre_horas.append((id_estacion, id_admin, fecha_historico, fecha_actual, term_seco, term_hmd))
            cont += cont
        return datos_tre_horas, sql

    """
    limpiar datos temperatura
    """

    def limpiar_registros_a_guardar(self, data):
        total_datos_ts = len(data)
        total_datos_th = len(data)
        total_out_range_ts = 0
        total_out_range_th = 0
        total_flag_ts = 0
        total_flag_th = 0

        nueva_lista = []
        for fila in data:
            nuevo_valor_ts = float(fila[4])
            nuevo_valor_th = float(fila[5])

            if nuevo_valor_ts in constants.VALUE_TO_FLAG:
                total_flag_ts = total_flag_ts + 1
                nuevo_valor_ts = constants.NEW_VALUE_TO_FLAG

            if nuevo_valor_ts in constants.VALES_OUT_RANGE_TEMPERATURE:
                nuevo_valor_ts = constants.NEW_VALUE_OUT_RANGE_TEMPERATURE
                total_out_range_ts = total_out_range_ts + 1
                # print("valor fuera de rango ",fila)

            if nuevo_valor_th in constants.VALUE_TO_FLAG:
                total_flag_th = total_flag_th + 1
                nuevo_valor_th = constants.NEW_VALUE_TO_FLAG

            if nuevo_valor_th in constants.VALES_OUT_RANGE_TEMPERATURE:
                nuevo_valor_th = constants.NEW_VALUE_OUT_RANGE_TEMPERATURE
                total_out_range_th = total_out_range_th + 1
                # print("valor fuera de rango ",fila)

            tupla_actualizada = (fila[0], fila[1], fila[2], fila[3], nuevo_valor_ts, nuevo_valor_th)
            nueva_lista.append(tupla_actualizada)

        print(f"total datos ts {total_datos_ts} out_range {total_out_range_ts} flags {total_flag_ts}")
        print(f"total datos ts {total_datos_th} out_range {total_out_range_th} flags {total_flag_th}")

        return nueva_lista
