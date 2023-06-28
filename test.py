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
import numpy as np

# Crear un DataFrame de ejemplo
data = {'a': [1, np.nan, 3, 4],
        'b': [np.nan, 2, 2, 4]}
df_123 = pd.DataFrame(data)

# Aplicar la lógica y crear la columna 'c'
df_123['c'] = df_123.apply(lambda row: True if pd.notnull(row['a']) and pd.notnull(row['b']) and row['a'] != row['b'] else False, axis=1)

# Imprimir el DataFrame resultante
print(df_123)