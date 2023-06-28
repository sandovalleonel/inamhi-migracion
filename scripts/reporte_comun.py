from scripts import utils
from scripts import constants
import pandas as pd


def total_por_variable(codigo, hora, variable, df_reporte):
    total_ts = (codigo, f"{variable}{hora}", df_reporte[variable].count(), df_reporte.shape[0])
    utils.write_tuplas_csv([total_ts], constants.NOMBRE_CARPETA + "/total_por_variable.csv")


def total_registros_mayores_por_variable(codigo, hora, variable, valor, df_reporte, nombre_archivo):
    df_mayor = df_reporte[[variable]]
    df_mayor = df_mayor.loc[(df_mayor[variable] > valor)]
    total_mayor = df_mayor.shape[0]
    if total_mayor > 0:
        variable = variable + str(hora)
        tupla = (codigo, variable, total_mayor)
        utils.write_tuplas_csv([tupla], constants.NOMBRE_CARPETA + f"/{nombre_archivo}")


def total_registros_menores_por_variable(codigo, hora, variable, valor, df_reporte, nombre_archivo):
    df_menor = df_reporte[[variable]]
    df_menor = df_menor.loc[(df_menor[variable] < valor) & (df_menor[variable] != constants.NEW_VALUE_TO_FLAG)]
    total_menor = df_menor.shape[0]
    if total_menor > 0:
        variable = variable + str(hora)
        tupla = (codigo, variable, total_menor)
        utils.write_tuplas_csv([tupla], constants.NOMBRE_CARPETA + f"/{nombre_archivo}")


def registros_mayores_por_variable(codigo, hora, variable, valor, df_reporte, nombre_archivo):
    df_minimo = df_reporte[['fecha_toma', variable]]
    df_minimo = df_minimo.loc[(df_minimo[variable] > valor)]
    df_minimo["variable"] = variable + str(hora)
    df_minimo["codigo"] = codigo
    df_minimo['fecha_toma'] = pd.to_datetime(df_minimo['fecha_toma']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tupla_list = df_minimo.to_records(index=False).tolist()
    utils.write_tuplas_csv(tupla_list, constants.NOMBRE_CARPETA + f"/{nombre_archivo}.csv")
def registros_menores_por_variable(codigo, hora, variable, valor, df_reporte, nombre_archivo):
    df_minimo = df_reporte[['fecha_toma', variable]]
    df_minimo = df_minimo.loc[(df_minimo[variable] < valor) & (df_minimo[variable] != constants.NEW_VALUE_TO_FLAG)]
    df_minimo["variable"] = variable + str(hora)
    df_minimo["codigo"] = codigo
    df_minimo['fecha_toma'] = pd.to_datetime(df_minimo['fecha_toma']).dt.strftime('%Y-%m-%d %H:%M:%S')
    tupla_list = df_minimo.to_records(index=False).tolist()
    utils.write_tuplas_csv(tupla_list, constants.NOMBRE_CARPETA + f"/{nombre_archivo}.csv")

def fechas_no_registrada_por_variable(codigo, hora, variable,df_reporte,nombre_archivo):
    tupla_no_guardados = []
    cod_subgrupo = 0
    tupla_temp = []
    for index, row in df_reporte.iterrows():
        fecha = row['fecha_toma']
        valor = row[variable]

        # reporte
        if pd.isna(valor):
            tupla_temp.append((fecha, cod_subgrupo))
        else:
            if len(tupla_temp) >= constants.NUMERO_DIAS_NO_REGISTRADOS:
                tupla_no_guardados.append((tupla_temp[0][0], codigo, variable + str(hora), len(tupla_temp)))
            cod_subgrupo = cod_subgrupo + 1
            tupla_temp = []

    # reporte
    if len(tupla_temp) >= constants.NUMERO_DIAS_NO_REGISTRADOS:
        tupla_no_guardados.append((tupla_temp[0][0], codigo, variable + str(hora), len(tupla_temp)))

    utils.write_tuplas_csv(tupla_no_guardados, constants.NOMBRE_CARPETA + f"/{nombre_archivo}.csv")
