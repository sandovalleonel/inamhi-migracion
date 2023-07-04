import pandas as pd

df = pd.DataFrame({'Columna': [1, 20, 3, 4, 5, 6, 7, 8, 9, 10]})

media = df['Columna'].mean()
desviacion_estandar = df['Columna'].std()

limite_inferior = media - 2 * desviacion_estandar
limite_superior = media + 2 * desviacion_estandar


datos_dentro_del_rango = df[(df['Columna'] >= limite_inferior) & (df['Columna'] <= limite_superior)]

print(datos_dentro_del_rango)