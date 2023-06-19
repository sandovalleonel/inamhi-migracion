import csv
from decimal import Decimal
import os
from datetime import datetime
import shutil



def cargar_archivo_csv(nombre_archivo):
    matriz_datos = []
    with open(nombre_archivo, 'r') as archivo_csv:
        lector_csv = csv.reader(archivo_csv)
        for fila in lector_csv:
            matriz_datos.append(fila)
    return matriz_datos


def escribir_csv(datos, nombre_archivo):
    with open(nombre_archivo, 'a', newline='') as archivo_csv:
        escritor = csv.writer(archivo_csv)
        for fila in datos:
            fila_actualizada = []
            for elemento in fila:
                if isinstance(elemento, Decimal):
                    fila_actualizada.append(str(elemento))
                else:
                    fila_actualizada.append(elemento)
            escritor.writerow(fila_actualizada)


def csv_write_estcion_dia_32(datos, nombre_archivo):
    with open(nombre_archivo, 'a', newline='') as archivo_csv:
        escritor = csv.writer(archivo_csv)
        for elemento in datos:
            escritor.writerow([elemento])


def csv_write_array_string(array_strings, nombre_archivo):
    with open(nombre_archivo, 'a', newline='') as archivo_csv:
        writer = csv.writer(archivo_csv)
        for string in array_strings:
            writer.writerow([string])


def crear_carpeta(nombre_carpeta, ruta_carpeta=None):
    if ruta_carpeta:
        ruta_completa = os.path.join(ruta_carpeta, nombre_carpeta)
    else:
        ruta_completa = nombre_carpeta
    try:
        os.mkdir(ruta_completa)
        print("Carpeta creada exitosamente.")
    except FileExistsError:
        print(f"La carpeta ya existe. {nombre_carpeta}")
    except Exception as e:
        print("Error al crear la carpeta:", str(e))

"""
elimnar carpeta con archivos
"""
def borrar_carpeta(ruta_carpeta):
    if os.path.exists(ruta_carpeta):
        shutil.rmtree(ruta_carpeta)

"""
construir fecha de registro
"""
def fecha_Registro(anio, mes, dia, hora):
    string_data = "{}-{}-{} {}:00:00".format(str(anio),
                                             str(mes).zfill(2),
                                             str(dia).zfill(2),
                                             str(hora).zfill(2))

    return datetime.strptime(string_data, "%Y-%m-%d %H:%M:%S")