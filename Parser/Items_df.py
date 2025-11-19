import pandas as pd
import json

# Чтение и обработка JSON
with open('Items_id.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Создание DataFrame с нужными названиями столбцов
df = pd.DataFrame(list(data.items()), columns=['id', 'item_name'])

# Сохранение в CSV
df.to_csv('items.csv', index=False)
print(df.head())