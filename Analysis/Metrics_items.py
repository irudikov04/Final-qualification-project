import pandas as pd
import numpy as np
import json

# Загрузка данных
players_df = pd.read_csv('dota_players_20251118_2251.csv')
heroes_df = pd.read_csv('heroes.csv')

# Загрузка данных о предметах
with open('items_id.json', 'r') as f:
    items_id = json.load(f)

with open('items.json', 'r') as f:
    items_data = json.load(f)

# Создаем словари для быстрого доступа
hero_names = dict(zip(heroes_df['id'], heroes_df['localized_name']))

# Создаем обратный словарь для предметов (id -> название)
item_id_to_name = {int(k): v for k, v in items_id.items()}

# Создаем словарь с информацией о предметах
item_info = {}
for item_name, data in items_data.items():
    item_info[item_name] = {
        'display_name': data.get('dname', item_name),
        'cost': data.get('cost', 0),
        'qual': data.get('qual', ''),
        'lore': data.get('lore', '')
    }

# Добавляем информацию о победителе в матче
match_results = players_df.groupby('match_id').first()[['team']].reset_index()
match_results['radiant_win'] = match_results['team'] == 'radiant'
players_with_results = players_df.merge(match_results[['match_id', 'radiant_win']], on='match_id')
players_with_results['win'] = (players_with_results['team'] == 'radiant') == players_with_results['radiant_win']

# АНАЛИЗ ПРЕДМЕТОВ

# Собираем все слоты предметов
item_slots = ['item_0', 'item_1', 'item_2', 'item_3', 'item_4', 'item_5', 
              'backpack_0', 'backpack_1', 'backpack_2']

# Создаем длинный формат данных для предметов
items_long = []
for slot in item_slots:
    temp_df = players_with_results[['match_id', 'player_slot', 'hero_id', 'win', slot]].copy()
    temp_df = temp_df.rename(columns={slot: 'item_id'})
    temp_df['slot'] = slot
    items_long.append(temp_df)

items_df = pd.concat(items_long, ignore_index=True)

# Фильтруем пустые предметы (item_id = 0)
items_df = items_df[items_df['item_id'] != 0]

# Добавляем названия предметов
items_df['item_name'] = items_df['item_id'].map(item_id_to_name)
items_df['item_display_name'] = items_df['item_name'].map(lambda x: item_info.get(x, {}).get('display_name', 'Unknown'))
items_df['item_cost'] = items_df['item_name'].map(lambda x: item_info.get(x, {}).get('cost', 0))

# Расчет метрик для предметов
print("РАСЧЕТ МЕТРИК ДЛЯ ПРЕДМЕТОВ")
print("="*80)

# Базовые метрики предметов
item_stats = items_df.groupby('item_name').agg({
    'match_id': 'count',
    'win': 'sum',
    'item_cost': 'first'
}).rename(columns={'match_id': 'total_purchases', 'win': 'wins'})

item_stats['winrate'] = (item_stats['wins'] / item_stats['total_purchases'] * 100).round(2)
item_stats['winrate_pct'] = item_stats['winrate'].astype(str) + '%'

# Добавляем отображаемые названия
item_stats['display_name'] = item_stats.index.map(lambda x: item_info.get(x, {}).get('display_name', x))

# Сортируем по популярности
item_stats = item_stats.sort_values('total_purchases', ascending=False)

# Выводим топ-30 самых популярных предметов
print("\nТОП-30 самых популярных предметов:")
print("="*100)
print(f"{'Предмет':<25} {'Покупки':<8} {'Победы':<8} {'Винрейт':<10} {'Стоимость':<10} {'Качество':<15}")
print("-"*100)

for idx, row in item_stats.head(30).iterrows():
    qual = item_info.get(idx, {}).get('qual', '')
    print(f"{row['display_name']:<25} {row['total_purchases']:<8} {row['wins']:<8} {row['winrate_pct']:<10} {row['item_cost']:<10} {qual:<15}")

# Анализ предметов по стоимости
print(f"\nАНАЛИЗ ПРЕДМЕТОВ ПО СТОИМОСТИ:")
print("="*60)

# Группируем предметы по ценовым категориям
def get_cost_category(cost):
    if cost == 0:
        return 'Бесплатные'
    elif cost <= 500:
        return 'Дешевые (≤500)'
    elif cost <= 1500:
        return 'Средние (501-1500)'
    elif cost <= 3000:
        return 'Дорогие (1501-3000)'
    else:
        return 'Очень дорогие (>3000)'

item_stats['cost_category'] = item_stats['item_cost'].apply(get_cost_category)

cost_analysis = item_stats.groupby('cost_category').agg({
    'total_purchases': 'sum',
    'wins': 'sum',
    'item_cost': 'mean'
}).round(2)

cost_analysis['winrate'] = (cost_analysis['wins'] / cost_analysis['total_purchases'] * 100).round(2)
cost_analysis['avg_cost'] = cost_analysis['item_cost']

print(f"{'Категория':<20} {'Покупки':<10} {'Винрейт':<10} {'Ср. стоимость':<15}")
print("-"*60)
for idx, row in cost_analysis.iterrows():
    print(f"{idx:<20} {row['total_purchases']:<10} {row['winrate']:<10}% {row['avg_cost']:<15}")

# Анализ стартовых предметов (первые 10 минут)
print(f"\nАНАЛИЗ СТАРТОВЫХ ПРЕДМЕТОВ (дешевые предметы):")
print("="*70)

cheap_items = item_stats[item_stats['item_cost'] <= 500].sort_values('total_purchases', ascending=False)

print(f"{'Предмет':<25} {'Покупки':<8} {'Винрейт':<10} {'Стоимость':<10}")
print("-"*70)
for idx, row in cheap_items.head(15).iterrows():
    print(f"{row['display_name']:<25} {row['total_purchases']:<8} {row['winrate_pct']:<10} {row['item_cost']:<10}")

# Анализ ключевых предметов (дорогие)
print(f"\nКЛЮЧЕВЫЕ ПРЕДМЕТЫ (стоимость >2000):")
print("="*80)

expensive_items = item_stats[item_stats['item_cost'] > 2000].sort_values('total_purchases', ascending=False)

print(f"{'Предмет':<25} {'Покупки':<8} {'Винрейт':<10} {'Стоимость':<10}")
print("-"*80)
for idx, row in expensive_items.head(20).iterrows():
    print(f"{row['display_name']:<25} {row['total_purchases']:<8} {row['winrate_pct']:<10} {row['item_cost']:<10}")

# Анализ винрейта для популярных предметов (минимум 100 покупок)
print(f"\nТОП-15 ПРЕДМЕТОВ ПО ВИНРЕЙТУ (минимум 100 покупок):")
print("="*80)

min_purchases = 100
high_winrate_items = item_stats[item_stats['total_purchases'] >= min_purchases].sort_values('winrate', ascending=False)

print(f"{'Предмет':<25} {'Покупки':<8} {'Винрейт':<10} {'Стоимость':<10}")
print("-"*80)
for idx, row in high_winrate_items.head(15).iterrows():
    print(f"{row['display_name']:<25} {row['total_purchases']:<8} {row['winrate_pct']:<10} {row['item_cost']:<10}")

# Анализ комбинаций предметов и героев
print(f"\nАНАЛИЗ КОМБИНАЦИЙ ПРЕДМЕТОВ И ГЕРОЕВ:")
print("="*80)

# Берем топ-10 самых популярных героев
top_heroes = players_with_results['hero_id'].value_counts().head(10).index

for hero_id in top_heroes:
    hero_name = hero_names.get(hero_id, f'Hero {hero_id}')
    hero_items = items_df[items_df['hero_id'] == hero_id]
    
    if len(hero_items) > 0:
        hero_item_stats = hero_items.groupby('item_name').agg({
            'match_id': 'count',
            'win': 'sum'
        }).rename(columns={'match_id': 'purchases', 'win': 'wins'})
        
        hero_item_stats['winrate'] = (hero_item_stats['wins'] / hero_item_stats['purchases'] * 100).round(2)
        hero_item_stats = hero_item_stats.sort_values('purchases', ascending=False)
        
        print(f"\nТоп-5 предметов для {hero_name}:")
        for idx, row in hero_item_stats.head(5).iterrows():
            display_name = item_info.get(idx, {}).get('display_name', idx)
            print(f"  {display_name:<25} {row['purchases']:<4} покупок, винрейт: {row['winrate']}%")

print(f"\nАНАЛИЗ ПО СЛОТАМ ПРЕДМЕТОВ:")
print("="*50)

slot_analysis = items_df.groupby('slot').agg({
    'match_id': 'count',
    'win': 'sum'
}).rename(columns={'match_id': 'total_items', 'win': 'wins'})

slot_analysis['winrate'] = (slot_analysis['wins'] / slot_analysis['total_items'] * 100).round(2)

print(f"{'Слот':<12} {'Предметы':<10} {'Винрейт':<10}")
print("-"*50)
for idx, row in slot_analysis.iterrows():
    print(f"{idx:<12} {row['total_items']:<10} {row['winrate']:<10}%")

# Сохранение результатов
item_stats_with_names = item_stats.reset_index()
item_stats_with_names['display_name'] = item_stats_with_names['item_name'].map(
    lambda x: item_info.get(x, {}).get('display_name', x)
)
item_stats_with_names['qual'] = item_stats_with_names['item_name'].map(
    lambda x: item_info.get(x, {}).get('qual', '')
)

# Сохраняем полную статистику по предметам
item_stats_with_names.to_csv('item_winrate_analysis.csv', index=False)
print(f"\nПолная статистика по предметам сохранена в: item_winrate_analysis.csv")

# Дополнительная общая статистика
print(f"\nОБЩАЯ СТАТИСТИКА ПО ПРЕДМЕТАМ:")
print(f"Всего уникальных предметов: {len(item_stats)}")
print(f"Всего покупок предметов: {items_df['match_id'].count()}")
print(f"Средний винрейт предметов: {item_stats['winrate'].mean():.2f}%")
print(f"Самый популярный предмет: {item_stats.iloc[0]['display_name']} ({item_stats.iloc[0]['total_purchases']} покупок)")
print(f"Предмет с наивысшим винрейтом: {high_winrate_items.iloc[0]['display_name']} ({high_winrate_items.iloc[0]['winrate']}%)")