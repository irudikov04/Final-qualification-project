import json
import pandas as pd

with open('Heroes.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

df = pd.DataFrame.from_dict(data, orient='index')

df_filtered = df[['id', 'localized_name']]
print(df_filtered.head())
print(df.columns.tolist())

df_filtered.to_csv('heroes.csv', index=False, encoding='utf-8')
print("CSV файл 'heroes.csv' успешно создан!")