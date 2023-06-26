"""import pandas as pd

# Crear DataFrame de ejemplo
data = {'a': [1, 2, 3, 4, 5, 6, 7, 8, 9],
        'b': [None, None, 3, None, 5, None, None, None, 9]
        }
df = pd.DataFrame(data)

null_count = df['b'].isnull().astype(int)
consecutive_nulls = (null_count.groupby((null_count != null_count.shift()).cumsum()).transform('sum') >= 2)
result = df[consecutive_nulls]

print(result)
"""
import pandas as pd

data = {
    'codigo': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'],
    'valor1': [None, None, 2, 1, None, 1, None, 1, None]
}

df = pd.DataFrame(data)

# Recorrer el DataFrame utilizando iterrows()
cod_subgrupo = 0
tupla_temp = []
for index, row in df.iterrows():
    codigo = row['codigo']
    valor1 = row['valor1']

    if pd.isna(valor1):
        tupla_temp.append((codigo, cod_subgrupo))
    else:
        if len(tupla_temp) > 1:
            print(tupla_temp[0][0], len(tupla_temp))
        cod_subgrupo = cod_subgrupo + 1
        tupla_temp = []

if len(tupla_temp) > 1:
    print(tupla_temp[0][0], len(tupla_temp))
