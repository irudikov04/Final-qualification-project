import json
import pandas as pd

# Загрузка JSON файла
with open('Heroes.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Преобразование словаря в DataFrame
# Обратите внимание: данные хранятся как словарь, где ключ - id героя
df = pd.DataFrame.from_dict(data, orient='index')

# Просмотр основных сведений о датасете
df_filtered = df[['id', 'localized_name']]
print(df_filtered.head())
print(df.columns.tolist())