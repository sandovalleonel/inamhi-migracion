import os
import csv
import shutil

"""
versión: 1.00, fecha : 20/06/2023
Class Utils funciones utilitarias
Copyright. INAMHI <www.inamhi.gob.ec>. Todos los derechos reservados.
"""
"""
escribir un archivo csv apartir de un array de tuplas
"""
def write_tuplas_csv(tuplas, nombre_archivo):
    with open(nombre_archivo, 'a', newline='') as archivo_csv:
        escritor_csv = csv.writer(archivo_csv)
        escritor_csv.writerows(tuplas)

"""
leer archivo csv
Args:
    nombre_archivo: string nombre de archivo
Return:
    matriz_datos: array de tuplas con datos del csv
"""
def cargar_archivo_csv(nombre_archivo):
    matriz_datos = []
    with open(nombre_archivo, 'r') as archivo_csv:
        lector_csv = csv.reader(archivo_csv)
        for fila in lector_csv:
            matriz_datos.append(fila)
    return matriz_datos


"""
escribir un csv apartir de un array de strings
Args:
    array_strings:array de string con datos
    nombre_archivo:string con nombre del archivo
"""
def csv_write_array_string(array_strings, nombre_archivo):
    with open(nombre_archivo, 'a', newline='') as archivo_csv:
        writer = csv.writer(archivo_csv)
        for string in array_strings:
            writer.writerow([string])


"""
crear carpeta
Args:
    nombre_carpeta:string con nombre carpeta
    ruta_carpeta:string ruta de carpeta
"""
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
Args:
    ruta_carpeta:string con ruta de carpeta
"""
def borrar_carpeta(ruta_carpeta):
    if os.path.exists(ruta_carpeta):
        shutil.rmtree(ruta_carpeta)


