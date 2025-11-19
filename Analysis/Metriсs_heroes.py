import pandas as pd
import numpy as np

# Загрузка данных
players_df = pd.read_csv('dota_players_20251118_2251.csv')
heroes_df = pd.read_csv('heroes.csv')

# Предварительный просмотр данных
print("Данные игроков:")
print(players_df.head())
print("\nДанные героев:")
print(heroes_df.head())

# Базовые проверки данных
print(f"\nКоличество записей в датасете игроков: {len(players_df)}")
print(f"Количество уникальных матчей: {players_df['match_id'].nunique()}")
print(f"Количество уникальных героев: {players_df['hero_id'].nunique()}")

# Создаем словарь для быстрого доступа к именам героев
hero_names = dict(zip(heroes_df['id'], heroes_df['localized_name']))

# Функция для определения победившей команды
def get_winner(row):
    return 'radiant' if row['team'] == 'radiant' and row['radiant_win'] else 'dire'

# Добавляем информацию о победителе в матче
# Сначала создаем DataFrame с информацией о победителях матчей
match_results = players_df.groupby('match_id').first()[['team']].reset_index()
match_results['radiant_win'] = match_results['team'] == 'radiant'

# Объединяем с основным датасетом
players_with_results = players_df.merge(match_results[['match_id', 'radiant_win']], on='match_id')

# Добавляем колонку с результатом для каждого игрока
players_with_results['win'] = (players_with_results['team'] == 'radiant') == players_with_results['radiant_win']

# Расчет винрейта для каждого героя
winrate_data = players_with_results.groupby('hero_id').agg({
    'win': ['count', 'sum']
}).round(2)

winrate_data.columns = ['total_games', 'wins']
winrate_data['winrate'] = (winrate_data['wins'] / winrate_data['total_games'] * 100).round(2)
winrate_data['winrate_pct'] = winrate_data['winrate'].astype(str) + '%'

# Расчет пикрейта для каждого героя (сколько раз герой был выбран)
total_matches = players_with_results['match_id'].nunique()
pickrate_data = players_with_results.groupby('hero_id').agg({
    'match_id': 'count'
}).rename(columns={'match_id': 'picks'})

pickrate_data['pickrate'] = (pickrate_data['picks'] / total_matches * 100).round(2)
pickrate_data['pickrate_pct'] = pickrate_data['pickrate'].astype(str) + '%'

# Объединяем все метрики
hero_stats = winrate_data.merge(pickrate_data, left_index=True, right_index=True)

# Добавляем названия героев
hero_stats['hero_name'] = hero_stats.index.map(lambda x: hero_names.get(x, f'Unknown Hero {x}'))

# Переупорядочиваем колонки для лучшей читаемости
hero_stats = hero_stats[['hero_name', 'total_games', 'wins', 'winrate', 'winrate_pct', 'picks', 'pickrate', 'pickrate_pct']]

# Сортируем по количеству игр (убыванию)
hero_stats = hero_stats.sort_values('total_games', ascending=False)

# Выводим топ-20 героев по популярности
print("\nТОП-20 героев по популярности (винрейт и пикрейт):")
print("="*80)
print(f"{'Герой':<20} {'Игры':<6} {'Победы':<6} {'Винрейт':<8} {'Пикры':<6} {'Пикрейт':<8}")
print("-"*80)

for idx, row in hero_stats.head(20).iterrows():
    print(f"{row['hero_name']:<20} {row['total_games']:<6} {row['wins']:<6} {row['winrate_pct']:<8} {row['picks']:<6} {row['pickrate_pct']:<8}")

# Дополнительная статистика
print(f"\nОбщая статистика:")
print(f"Всего матчей: {total_matches}")
print(f"Всего героев в датасете: {len(hero_stats)}")
print(f"Средний винрейт: {hero_stats['winrate'].mean():.2f}%")
print(f"Средний пикрейт: {hero_stats['pickrate'].mean():.2f}%")

# Топ-10 героев по винрейту (минимум 10 игр)
min_games = 10
high_winrate_heroes = hero_stats[hero_stats['total_games'] >= min_games].sort_values('winrate', ascending=False)
print(f"\nТОП-10 героев по винрейту (минимум {min_games} игр):")
print("="*60)
print(f"{'Герой':<20} {'Игры':<6} {'Винрейт':<8}")
print("-"*60)
for idx, row in high_winrate_heroes.head(10).iterrows():
    print(f"{row['hero_name']:<20} {row['total_games']:<6} {row['winrate_pct']:<8}")

# Топ-10 самых популярных героев
print(f"\nТОП-10 самых популярных героев:")
print("="*50)
print(f"{'Герой':<20} {'Пикрейт':<8} {'Винрейт':<8}")
print("-"*50)
for idx, row in hero_stats.head(10).iterrows():
    print(f"{row['hero_name']:<20} {row['pickrate_pct']:<8} {row['winrate_pct']:<8}")

# Сохранение результатов в CSV (опционально)
hero_stats.to_csv('hero_winrate_pickrate_analysis.csv', index=True)
print(f"\nРезультаты сохранены в файл: hero_winrate_pickrate_analysis.csv")