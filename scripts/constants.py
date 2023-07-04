# Comun
SAVE_DATA = True
GENERAR_REPORTES = True
VALUE_NULL = None
VALUE_CERO = 0
NUMERO_DIAS_NO_REGISTRADOS = 3

NOMBRE_ARCHIVO_DIA_32 = 'archivo_con_dia_32.csv'
NOMBRE_ARCHIVO_ESTACION_SI_ID = 'estaciones_sin_id.csv'
NOMBRE_CARPETA = 'resultados'
VALES_OUT_RANGE_TEMPERATURE = [99.9, 999.9, 9999.99, 9999999999]  # solo para temperatura#si tiene 99.8 puede ser real
NEW_VALUE_OUT_RANGE_TEMPERATURE = None
VALUE_TO_FLAG = [888.8]
NEW_VALUE_TO_FLAG = -888.8
ID_ADMIN = 999
COLUMNS_CLM0002 = ["codigo", "anio", "mes", "dia", "tmax", "tmin", "ts07", "ts13", "ts19", "th07", "th13", "th19",
                   "rr07", "rr13", "rr19", "ev07", "ev071", "ev13", "ev131", "ev19", "ev191", "nu07", "nu13", "nu19",
                   "dv07", "vv07", "dv13", "vv13", "dv19", "vv19", "an07", "an13", "an19", "tipev", "tipcal", "fuente",
                   "tipane"]

COLUMNS_PVL0002 = ["codigo", "anio", "mes", "dia", "tipolec", "rr07", "rr13", "rr19", "fuente"]

##temperatura
TEMP_MAXIMA = 33
TEMP_MINIMO = -1

# viento
OLD_VALUES_ANEMOMETRO = [9999999999, 999999999, 99.9, 999.9, 9999.99]
OLD_VALUES_VELOCIDAD_VIENTO = [99.9]
REFERENCE_ID_DIR_VIENTO = 18  # en la tabla relacion es el valor cunao no viene direccion viento

# evaporacion
VALUE_MAX_EVAPORACION = 30  # reporte sacar total mayores a 30
ID_TIPO_CALCULO = 1

# nubosidad
OLD_VALUES_NUBOSIDAD = [99, 99.9, 999.9, 9999.99, 9999999999]
