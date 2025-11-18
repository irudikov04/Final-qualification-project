import requests
import pandas as pd
import csv
import os
from datetime import datetime

class DotaStatsParser:
    def __init__(self):
        self.base_url = "https://api.opendota.com/api"
    
    def get_match_stats(self, match_id):
        """Получение детальной статистики матча"""
        url = f"{self.base_url}/matches/{match_id}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при получении данных матча {match_id}: {e}")
            return None
    
    def parse_player_stats(self, match_data):
        """Парсинг статистики игроков из данных матча"""
        players_data = []
        
        for player in match_data.get('players', []):
            player_stats = {
                'match_id': match_data['match_id'],
                'player_slot': player['player_slot'],
                'hero_id': player['hero_id'],
                'kills': player['kills'],
                'deaths': player['deaths'],
                'assists': player['assists'],
                'gold_per_min': player['gold_per_min'],
                'xp_per_min': player['xp_per_min'],
                'last_hits': player['last_hits'],
                'denies': player['denies'],
                'hero_damage': player['hero_damage'],
                'tower_damage': player['tower_damage'],
                'hero_healing': player['hero_healing'],
                'level': player['level'],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            players_data.append(player_stats)
        
        return players_data

def append_to_csv(data, filename):
    """Добавление данных в CSV файл"""
    if not data:
        print("Нет данных для сохранения")
        return
    
    file_exists = os.path.isfile(filename)
    
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())

        if not file_exists:
            writer.writeheader()
        
        writer.writerows(data)
    
    print(f"Данные добавлены в файл {filename}")

def append_multiple_matches(match_ids, filename='Matches_data.csv'):
    """Добавление статистики нескольких матчей"""
    parser = DotaStatsParser()
    
    for match_id in match_ids:
        print(f"Обрабатывается матч {match_id}...")
        match_data = parser.get_match_stats(match_id)
        
        if match_data:
            player_stats = parser.parse_player_stats(match_data)
            append_to_csv(player_stats, filename)
            print(f"Матч {match_id} успешно добавлен")
        else:
            print(f"Не удалось получить данные матча {match_id}")



# Использование
match_ids = [8565426161, 8565363948, 8565362901]  # Замените на реальные ID
append_multiple_matches(match_ids, 'Matches_data.csv')