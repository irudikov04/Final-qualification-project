import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Загружаем данные
data = open('dota_players_20251118_2251.csv')


df = pd.read_csv(data)
df_filtered = df[['match_id', ' hero_id', ' kills', ' deaths', ' assists', ' gold_per_min', ' hero_damage', ' tower_damage', ' item_0', ' item_1', ' item_2', ' item_3', ' item_4', ' item_5', ' team']]
print(df_filtered.head())

print("=== ОСНОВНАЯ ИНФОРМАЦИЯ О ДАННЫХ ===")
print(f"Размер датасета: {df.shape}")
print("\nПервые 5 строк:")
print(df.head())

print("\n=== ИНФОРМАЦИЯ О СТОЛБЦАХ ===")
print(df.info())

print("\n=== ОСНОВНЫЕ СТАТИСТИКИ ===")
print(df.describe())

# Анализ по командам
print("\n=== СРАВНЕНИЕ КОМАНД ===")
team_stats = df.groupby('team').agg({
    'kills': 'sum',
    'deaths': 'sum',
    'assists': 'sum',
    'gold_per_min': 'mean',
    'xp_per_min': 'mean',
    'net_worth': 'sum',
    'hero_damage': 'sum',
    'tower_damage': 'sum',
    'level': 'mean'
}).round(2)

print(team_stats)

# Визуализация
plt.style.use('default')
fig, axes = plt.subplots(2, 3, figsize=(18, 12))

# 1. Распределение показателей по командам
metrics_to_plot = ['kills', 'gold_per_min', 'net_worth', 'hero_damage', 'level', 'last_hits']
for i, metric in enumerate(metrics_to_plot):
    row, col = i // 3, i % 3
    df.groupby('team')[metric].mean().plot(kind='bar', ax=axes[row, col], color=['green', 'red'])
    axes[row, col].set_title(f'Средний {metric} по командам')
    axes[row, col].set_ylabel(metric)

plt.tight_layout()
plt.show()

# Анализ игроков
print("\n=== ТОП ИГРОКИ ПО КЛЮЧЕВЫМ ПОКАЗАТЕЛЯМ ===")

# Самый высокий KDA
df['KDA'] = (df['kills'] + df['assists']) / np.where(df['deaths'] == 0, 1, df['deaths'])
print("Топ-3 игрока по KDA:")
print(df.nlargest(3, 'KDA')[['player_slot', 'team', 'kills', 'deaths', 'assists', 'KDA']])

# Лучший фарм
print("\nТоп-3 игрока по GPM:")
print(df.nlargest(3, 'gold_per_min')[['player_slot', 'team', 'gold_per_min', 'last_hits', 'net_worth']])

# Самый высокий урон по героям
print("\nТоп-3 игрока по урону по героям:")
print(df.nlargest(3, 'hero_damage')[['player_slot', 'team', 'hero_damage', 'kills']])

# Анализ предметов
print("\n=== АНАЛИЗ ПРЕДМЕТОВ ===")
# Собираем все предметы в один список
all_items = []
for i in range(6):
    all_items.extend(df[f'item_{i}'].values)
    
item_counts = pd.Series(all_items).value_counts()
print("Самые популярные предметы:")
print(item_counts.head(10))

# Анализ героев
print("\n=== ИНФОРМАЦИЯ О ГЕРОЯХ ===")
print("Распределение героев по командам:")
hero_counts = df.groupby(['team', 'hero_id']).size().unstack(fill_value=0)
print(hero_counts)

# Детальный анализ отдельных игроков
print("\n=== ДЕТАЛЬНЫЙ АНАЛИЗ ИГРОКОВ ===")

# Игрок с максимальным net worth
richest_player = df.loc[df['net_worth'].idxmax()]
print(f"Самый богатый игрок: слот {richest_player['player_slot']}, команда {richest_player['team']}")
print(f"Net worth: {richest_player['net_worth']}, GPM: {richest_player['gold_per_min']}")

# MVP анализа (комбинированная метрика)
df['performance_score'] = (
    df['kills'] * 2 + df['assists'] * 1.5 - df['deaths'] * 1 +
    df['gold_per_min'] / 100 + df['hero_damage'] / 1000 +
    df['tower_damage'] / 500 + df['level'] * 2
)

print("\nТоп-3 игрока по производительности:")
top_performers = df.nlargest(3, 'performance_score')[['player_slot', 'team', 'performance_score', 'kills', 'deaths', 'assists', 'hero_damage']]
print(top_performers)

# Визуализация распределения показателей
fig, axes = plt.subplots(2, 2, figsize=(15, 10))

# Распределение KDA
axes[0, 0].hist(df['KDA'], bins=10, alpha=0.7, color='skyblue')
axes[0, 0].set_title('Распределение KDA')
axes[0, 0].set_xlabel('KDA')
axes[0, 0].set_ylabel('Количество игроков')

# Соотношение убийств/смертей
axes[0, 1].scatter(df['kills'], df['deaths'], c=df['team'].map({'radiant': 'green', 'dire': 'red'}), alpha=0.6)
axes[0, 1].set_title('Соотношение убийств и смертей')
axes[0, 1].set_xlabel('Kills')
axes[0, 1].set_ylabel('Deaths')

# Сравнение GPM и XPM
axes[1, 0].scatter(df['gold_per_min'], df['xp_per_min'], c=df['team'].map({'radiant': 'green', 'dire': 'red'}), alpha=0.6)
axes[1, 0].set_title('Соотношение GPM и XPM')
axes[1, 0].set_xlabel('Gold Per Minute')
axes[1, 1].set_ylabel('XP Per Minute')

# Net worth по командам
team_networth = df.groupby('team')['net_worth'].sum()
axes[1, 1].bar(team_networth.index, team_networth.values, color=['green', 'red'])
axes[1, 1].set_title('Общий Net Worth по командам')
axes[1, 1].set_ylabel('Net Worth')

plt.tight_layout()
plt.show()

# Финальный анализ результата матча
print("\n=== РЕЗУЛЬТАТ МАТЧА ===")
radiant_kills = df[df['team'] == 'radiant']['kills'].sum()
dire_kills = df[df['team'] == 'dire']['kills'].sum()

print(f"Radiant kills: {radiant_kills}")
print(f"Dire kills: {dire_kills}")

if radiant_kills > dire_kills:
    print("ПОБЕДА RADIANT!")
else:
    print("ПОБЕДА DIRE!")

# Анализ эффективности команд
print("\n=== ЭФФЕКТИВНОСТЬ КОМАНД ===")
team_efficiency = df.groupby('team').agg({
    'gold_per_min': 'mean',
    'xp_per_min': 'mean', 
    'last_hits': 'mean',
    'hero_damage': 'sum',
    'tower_damage': 'sum'
}).round(2)

print(team_efficiency)