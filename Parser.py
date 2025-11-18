import requests
import pandas as pd
import time
import json
import csv
import os
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dota_parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class MassDotaParser:
    def __init__(self, rate_limit_delay=0.001):
        self.base_url = "https://api.opendota.com/api"
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Dota2-Stats-Parser/1.0',
            'Accept': 'application/json'
        })
        
    def get_public_matches_batch(self, limit: int = 100, less_than_match_id: Optional[int] = None) -> List[Dict]:
        """Получение батча публичных матчей"""
        params = {'limit': limit}
        if less_than_match_id:
            params['less_than_match_id'] = less_than_match_id
            
        try:
            response = self.session.get(f"{self.base_url}/publicMatches", params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при получении публичных матчей: {e}")
            return []
    
    def get_match_details(self, match_id: int) -> Optional[Dict]:
        """Получение детальной информации о матче"""
        try:
            response = self.session.get(f"{self.base_url}/matches/{match_id}", timeout=30)
            
            if response.status_code == 429:  # Rate limit
                logging.warning("Достигнут лимит запросов, ждем 60 секунд...")
                time.sleep(60)
                return self.get_match_details(match_id)
                
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при получении матча {match_id}: {e}")
            return None
    
    def parse_match_data(self, match_data: Dict) -> Dict:
        """Парсинг основных данных матча"""
        if not match_data:
            return {}
            
        try:
            # Основная информация о матче
            parsed = {
                'match_id': match_data.get('match_id'),
                'start_time': match_data.get('start_time'),
                'duration': match_data.get('duration'),
                'game_mode': match_data.get('game_mode'),
                'lobby_type': match_data.get('lobby_type'),
                'radiant_win': match_data.get('radiant_win'),
                'leagueid': match_data.get('leagueid'),
                'league_name': match_data.get('league_name', ''),
                'series_id': match_data.get('series_id'),
                'series_type': match_data.get('series_type'),
                'skill': match_data.get('skill'),
                'patch': match_data.get('patch'),
                'region': match_data.get('region'),
                'radiant_score': match_data.get('radiant_score', 0),
                'dire_score': match_data.get('dire_score', 0),
                'players_count': len(match_data.get('players', [])),
                'parsed_timestamp': datetime.now().isoformat()
            }
            
            # Агрегированная статистика по командам
            players = match_data.get('players', [])
            if players:
                radiant_players = [p for p in players if p.get('player_slot', 128) < 128]
                dire_players = [p for p in players if p.get('player_slot', 128) >= 128]
                
                # Статистика Radiant
                if radiant_players:
                    parsed.update({
                        'radiant_total_kills': sum(p.get('kills', 0) for p in radiant_players),
                        'radiant_total_deaths': sum(p.get('deaths', 0) for p in radiant_players),
                        'radiant_total_assists': sum(p.get('assists', 0) for p in radiant_players),
                        'radiant_avg_gpm': sum(p.get('gold_per_min', 0) for p in radiant_players) / len(radiant_players),
                        'radiant_avg_xpm': sum(p.get('xp_per_min', 0) for p in radiant_players) / len(radiant_players),
                        'radiant_total_lh': sum(p.get('last_hits', 0) for p in radiant_players),
                        'radiant_total_dn': sum(p.get('denies', 0) for p in radiant_players),
                    })
                
                # Статистика Dire
                if dire_players:
                    parsed.update({
                        'dire_total_kills': sum(p.get('kills', 0) for p in dire_players),
                        'dire_total_deaths': sum(p.get('deaths', 0) for p in dire_players),
                        'dire_total_assists': sum(p.get('assists', 0) for p in dire_players),
                        'dire_avg_gpm': sum(p.get('gold_per_min', 0) for p in dire_players) / len(dire_players),
                        'dire_avg_xpm': sum(p.get('xp_per_min', 0) for p in dire_players) / len(dire_players),
                        'dire_total_lh': sum(p.get('last_hits', 0) for p in dire_players),
                        'dire_total_dn': sum(p.get('denies', 0) for p in dire_players),
                    })
            
            return parsed
            
        except Exception as e:
            logging.error(f"Ошибка при парсинге матча {match_data.get('match_id')}: {e}")
            return {}
    
    def parse_player_data(self, match_data: Dict) -> List[Dict]:
        """Парсинг данных игроков"""
        players_data = []
        
        for player in match_data.get('players', []):
            try:
                player_data = {
                    'match_id': match_data.get('match_id'),
                    'player_slot': player.get('player_slot'),
                    'account_id': player.get('account_id'),
                    'hero_id': player.get('hero_id'),
                    'kills': player.get('kills', 0),
                    'deaths': player.get('deaths', 0),
                    'assists': player.get('assists', 0),
                    'gold_per_min': player.get('gold_per_min', 0),
                    'xp_per_min': player.get('xp_per_min', 0),
                    'last_hits': player.get('last_hits', 0),
                    'denies': player.get('denies', 0),
                    'hero_damage': player.get('hero_damage', 0),
                    'tower_damage': player.get('tower_damage', 0),
                    'hero_healing': player.get('hero_healing', 0),
                    'level': player.get('level', 1),
                    'net_worth': player.get('total_gold', 0),
                    'item_0': player.get('item_0', 0),
                    'item_1': player.get('item_1', 0),
                    'item_2': player.get('item_2', 0),
                    'item_3': player.get('item_3', 0),
                    'item_4': player.get('item_4', 0),
                    'item_5': player.get('item_5', 0),
                    'backpack_0': player.get('backpack_0', 0),
                    'backpack_1': player.get('backpack_1', 0),
                    'backpack_2': player.get('backpack_2', 0),
                    'team': 'radiant' if player.get('player_slot', 128) < 128 else 'dire'
                }
                players_data.append(player_data)
            except Exception as e:
                logging.error(f"Ошибка при парсинге игрока в матче {match_data.get('match_id')}: {e}")
                continue
                
        return players_data
    
    def collect_matches(self, total_matches: int = 50000, batch_size: int = 100, resume_from: Optional[int] = None):
        """Основной метод сбора матчей"""
        logging.info(f"Начинаем сбор {total_matches} матчей...")
        
        # Файлы для сохранения
        matches_file = f"dota_matches_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        players_file = f"dota_players_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        progress_file = "progress.json"
        
        # Загружаем прогресс если есть
        if resume_from and os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
            last_match_id = progress.get('last_match_id')
            collected_count = progress.get('collected_count', 0)
            logging.info(f"Продолжаем с матча {last_match_id}, собрано {collected_count} матчей")
        else:
            last_match_id = None
            collected_count = 0
        
        # Инициализация CSV файлов
        matches_header_written = not os.path.exists(matches_file)
        players_header_written = not os.path.exists(players_file)
        
        with open(matches_file, 'a', newline='', encoding='utf-8') as matches_csv, \
             open(players_file, 'a', newline='', encoding='utf-8') as players_csv:
            
            matches_writer = None
            players_writer = None
            
            batch_count = 0
            
            while collected_count < total_matches:
                batch_count += 1
                logging.info(f"Батч {batch_count}, собрано {collected_count}/{total_matches} матчей")
                
                # Получаем батч ID матчей
                public_matches = self.get_public_matches_batch(batch_size, last_match_id)
                
                if not public_matches:
                    logging.warning("Не удалось получить матчи, ждем 10 секунд...")
                    time.sleep(10)
                    continue
                
                # Обрабатываем каждый матч в батче
                for i, match_info in enumerate(public_matches):
                    if collected_count >= total_matches:
                        break
                    
                    match_id = match_info['match_id']
                    logging.info(f"Обрабатываем матч {match_id} ({collected_count + 1}/{total_matches})")
                    
                    # Получаем детальную информацию о матче
                    match_data = self.get_match_details(match_id)
                    time.sleep(self.rate_limit_delay)  # Задержка между запросами
                    
                    if not match_data:
                        continue
                    
                    # Парсим данные матча
                    match_parsed = self.parse_match_data(match_data)
                    players_parsed = self.parse_player_data(match_data)
                    
                    if match_parsed and players_parsed:
                        # Инициализируем writers при первом использовании
                        if matches_writer is None:
                            matches_writer = csv.DictWriter(matches_csv, fieldnames=match_parsed.keys())
                            if not matches_header_written:
                                matches_writer.writeheader()
                                matches_header_written = True
                        
                        if players_writer is None:
                            players_writer = csv.DictWriter(players_csv, fieldnames=players_parsed[0].keys())
                            if not players_header_written:
                                players_writer.writeheader()
                                players_header_written = True
                        
                        # Записываем данные
                        matches_writer.writerow(match_parsed)
                        for player_data in players_parsed:
                            players_writer.writerow(player_data)
                        
                        collected_count += 1
                        
                        # Сохраняем прогресс каждые 100 матчей
                        if collected_count % 100 == 0:
                            self.save_progress(progress_file, match_id, collected_count)
                            logging.info(f"Прогресс сохранен: {collected_count} матчей")
                    
                    # Обновляем last_match_id для пагинации
                    last_match_id = match_id
                
                logging.info(f"Батч {batch_count} завершен. Собрано: {collected_count}/{total_matches}")
                
                # Сохраняем прогресс после каждого батча
                self.save_progress(progress_file, last_match_id, collected_count)
        
        logging.info(f"Сбор завершен! Всего собрано {collected_count} матчей")
        logging.info(f"Файлы сохранены: {matches_file}, {players_file}")
        
        return matches_file, players_file
    
    def save_progress(self, progress_file: str, last_match_id: int, collected_count: int):
        """Сохранение прогресса"""
        progress = {
            'last_match_id': last_match_id,
            'collected_count': collected_count,
            'last_update': datetime.now().isoformat()
        }
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
    
    def generate_report(self, matches_file: str, players_file: str):
        """Генерация отчета по собранным данным"""
        try:
            matches_df = pd.read_csv(matches_file)
            players_df = pd.read_csv(players_file)
            
            report = {
                'total_matches': len(matches_df),
                'total_players': len(players_df),
                'collection_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
                'date_range': {
                    'earliest': datetime.fromtimestamp(matches_df['start_time'].min()).strftime('%Y-%m-%d'),
                    'latest': datetime.fromtimestamp(matches_df['start_time'].max()).strftime('%Y-%m-%d')
                },
                'win_rate': {
                    'radiant_win_rate': matches_df['radiant_win'].mean(),
                    'dire_win_rate': 1 - matches_df['radiant_win'].mean()
                },
                'average_duration': matches_df['duration'].mean(),
                'skill_distribution': matches_df['skill'].value_counts().to_dict(),
                'regions': matches_df['region'].value_counts().to_dict()
            }
            
            report_file = f"report_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Отчет сохранен в {report_file}")
            return report
            
        except Exception as e:
            logging.error(f"Ошибка при генерации отчета: {e}")
            return {}

def main():
    """Основная функция запуска парсера"""
    parser = MassDotaParser(rate_limit_delay=1.0)  # 1 секунда между запросами
    
    try:
        # Запускаем сбор 50,000 матчей
        matches_file, players_file = parser.collect_matches(
            total_matches=50000,
            batch_size=100,
            resume_from=None  # Установите ID матча чтобы продолжить с места остановки
        )
        
        # Генерируем отчет
        report = parser.generate_report(matches_file, players_file)
        
        print("\n" + "="*50)
        print("СБОР ДАННЫХ ЗАВЕРШЕН!")
        print("="*50)
        print(f"Собрано матчей: {report.get('total_matches', 0)}")
        print(f"Собрано записей игроков: {report.get('total_players', 0)}")
        print(f"Винрейт Radiant: {report.get('win_rate', {}).get('radiant_win_rate', 0):.2%}")
        print(f"Средняя продолжительность: {report.get('average_duration', 0)/60:.1f} минут")
        
    except KeyboardInterrupt:
        logging.info("Парсер остановлен пользователем")
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    main()