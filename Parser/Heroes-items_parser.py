import requests
import csv
import time
import random

def make_request_with_retry(url, max_retries=5, base_delay=1):
    """Делает запрос с повторными попытками и экспоненциальной задержкой"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            
            if response.status_code == 429:
                # Получаем время ожидания из заголовка или используем экспоненциальную задержку
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    wait_time = int(retry_after)
                else:
                    wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                
                print(f"Получена ошибка 429. Ждем {wait_time:.2f} секунд...")
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            print(f"Попытка {attempt + 1} не удалась: {e}")
            if attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait_time)
            else:
                raise e
    
    return None

def get_heroes_data_safe():
    """Безопасное получение данных о героях"""
    url = "https://api.opendota.com/api/heroes"
    print("Получаем данные о героях...")
    time.sleep(1)  # Базовая задержка между запросами
    response = make_request_with_retry(url)
    return response.json() if response else None

def get_items_data_safe():
    """Безопасное получение данных о предметах"""
    url = "https://api.opendota.com/api/constants/items"
    print("Получаем данные о предметах...")
    time.sleep(1)  # Базовая задержка между запросами
    response = make_request_with_retry(url)
    return response.json() if response else None

def parse_data_safe():
    """Безопасный парсинг с задержками"""
    heroes_data = get_heroes_data_safe()
    if not heroes_data:
        print("Не удалось получить данные о героях")
        return
    
    items_data = get_items_data_safe()
    if not items_data:
        print("Не удалось получить данные о предметах")
        return
    
    # Создаем CSV файл
    with open('dota_data_safe.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Hero ID', 'Hero Name', 'Item ID', 'Item Name'])
        
        for hero in heroes_data:
            for item_key, item in items_data.items():
                if isinstance(item, dict) and 'id' in item and 'dname' in item:
                    writer.writerow([
                        hero['id'],
                        hero['localized_name'],
                        item['id'],
                        item['dname']
                    ])
    
    print("Данные успешно сохранены!")

if __name__ == "__main__":
    parse_data_safe()