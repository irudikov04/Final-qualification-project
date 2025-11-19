import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Загрузка данных
df = pd.read_csv('dota_players_20251118_2251.csv')

columns_to_analyze = ['match_id', ' hero_id', ' kills', ' deaths', ' assists', ' gold_per_min', ' hero_damage', ' tower_damage', ' item_0',' item_1', ' item_2', ' item_3', ' item_4', ' item_5', ' team']



df_analysis = df[columns_to_analyze]

# Базовая информация о данных
print("=== БАЗОВАЯ ИНФОРМАЦИЯ О ДАТАСЕТЕ ===")
print(f"Размер датасета: {df_analysis.shape}")
print(f"Количество матчей: {df_analysis['match_id'].nunique()}")
print("\n")

print("=== ИНФОРМАЦИЯ О ТИПАХ ДАННЫХ ===")
print(df_analysis.info())
print("\n")

print("=== СТАТИСТИКА ПО ЧИСЛОВЫМ КОЛОНКАМ ===")
print(df_analysis.describe())
print("\n")

# Проверка пропущенных значений
print("=== ПРОПУЩЕННЫЕ ЗНАЧЕНИЯ ===")
missing_values = df_analysis.isnull().sum()
print(missing_values[missing_values > 0])
print("\n")

# 1. Анализ основных статистик по игрокам
print("=== АНАЛИЗ ОСНОВНЫХ СТАТИСТИК ===")

# KDA Ratio (Kills + Assists) / Deaths (избегаем деления на 0)
df_analysis['kda'] = np.where(df_analysis['deaths'] > 0, 
                             (df_analysis['kills'] + df_analysis['assists']) / df_analysis['deaths'],
                             df_analysis['kills'] + df_analysis['assists'])

print(f"Средний KDA: {df_analysis['kda'].mean():.2f}")
print(f"Средние убийства: {df_analysis['kills'].mean():.2f}")
print(f"Средние смерти: {df_analysis['deaths'].mean():.2f}")
print(f"Средние помощи: {df_analysis['assists'].mean():.2f}")
print(f"Средний GPM: {df_analysis['gold_per_min'].mean():.2f}")
print(f"Средний урон по героям: {df_analysis['hero_damage'].mean():.2f}")
print(f"Средний урон по башням: {df_analysis['tower_damage'].mean():.2f}")
print("\n")




# 3. Топ героев по различным метрикам
print("=== ТОП ГЕРОЕВ ПО РАЗЛИЧНЫМ МЕТРИКАМ ===")

# Создаем функцию для анализа топ героев
def analyze_top_heroes(metric, top_n=10):
    hero_stats = df_analysis.groupby('hero_id').agg({
        metric: 'mean',
        'match_id': 'count'  # количество игр
    }).rename(columns={'match_id': 'games_played'})
    
    # Фильтруем героев с достаточным количеством игр
    hero_stats = hero_stats[hero_stats['games_played'] >= 5]  # минимум 5 игр
    
    return hero_stats.nlargest(top_n, metric)

print("Топ героев по KDA:")
print(analyze_top_heroes('kda'))
print("\n")

print("Топ героев по GPM:")
print(analyze_top_heroes('gold_per_min'))
print("\n")

print("Топ героев по урону по героям:")
print(analyze_top_heroes('hero_damage'))
print("\n")

print("Топ героев по урону по башням:")
print(analyze_top_heroes('tower_damage'))
print("\n")