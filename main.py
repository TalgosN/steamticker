import os
import sqlite3
import requests
import pygsheets
from dotenv import load_dotenv
import time
import pandas as pd
import schedule

load_dotenv()
API_KEY = os.getenv('STEAM_API_KEY')
TABLE_NAME = os.getenv('TABLE_NAME')

gc = pygsheets.authorize(service_file='service_account.json')
sh = gc.open(TABLE_NAME)
wks_config = sh.worksheet_by_title('Config')

def get_accounts_from_sheets(sh):
    wks_config = sh.worksheet_by_title('Config')
    return wks_config.get_all_records()

def init_and_sync_db(sheet_data):
    with sqlite3.connect('steam_stats.db') as conn:
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS accounts (
                vanity_url TEXT PRIMARY KEY,
                steam_id TEXT,
                club_name TEXT,
                status TEXT DEFAULT 'ACTIVE'
            );
            CREATE TABLE IF NOT EXISTS snapshots (
                steam_id TEXT,
                app_id INTEGER,
                playtime_minutes INTEGER,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS games (
                app_id INTEGER PRIMARY KEY,
                name TEXT
            );
        ''')
        cursor = conn.cursor()
        
        for row in sheet_data:
            club = row.get('Club')
            url = row.get('Nickname')
            
            if not url:
                continue
            
            cursor.execute('SELECT steam_id FROM accounts WHERE vanity_url = ?', (url,))
            
            if cursor.fetchone() is None:
                res = requests.get(
                    'http://api.steampowered.com/ISteamUser/ResolveVanityURL/v0001/',
                    params={'key': API_KEY, 'vanityurl': url}
                )
                
                if res.status_code == 200:
                    data = res.json().get('response', {})
                    if data.get('success') == 1:
                        steam_id = data['steamid']
                        status = 'ACTIVE'
                    else:
                        steam_id = None
                        status = 'INVALID_URL'
                        
                    cursor.execute(
                        'INSERT INTO accounts (vanity_url, steam_id, club_name, status) VALUES (?, ?, ?, ?)',
                        (url, steam_id, club, status)
                    )

# Запуск первого этапа
accounts_data = get_accounts_from_sheets(sh)
init_and_sync_db(accounts_data)


def fetch_games_and_snapshot(api_key):
    with sqlite3.connect('steam_stats.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT steam_id FROM accounts WHERE status = 'ACTIVE' AND steam_id IS NOT NULL")
        active_accounts = cursor.fetchall()

        for (steam_id,) in active_accounts:
            params = {
                'key': api_key,
                'steamid': steam_id,
                'format': 'json',
                'include_appinfo': 1,
                'include_played_free_games': 1
            }
            res = requests.get('http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/', params=params)

            if res.status_code == 200:
                data = res.json().get('response', {})
                games = data.get('games', [])

                for game in games:
                    cursor.execute(
                        'INSERT INTO snapshots (steam_id, app_id, playtime_minutes) VALUES (?, ?, ?)',
                        (steam_id, game.get('appid'), game.get('playtime_forever', 0))
                    )
                    cursor.execute(
                        'INSERT OR IGNORE INTO games (app_id, name) VALUES (?, ?)',
                        (game.get('appid'), game.get('name'))
                    )
            
            # Архитектурная необходимость: пауза 1.5 сек между аккаунтами для обхода rate limit
            time.sleep(1.5)

def export_clubs_to_sheets(sh):
    wks_games = sh.worksheet_by_title('Игры')
    # Берем первый столбец, пропуская заголовок
    target_games = wks_games.get_col(1, include_tailing_empty=False)[1:]
    with sqlite3.connect('steam_stats.db') as conn:
        # Динамически генерируем знаки вопроса для безопасной подстановки в SQL
        placeholders = ','.join('?' * len(target_games))
        
        query = f'''
            WITH LatestSnapshots AS (
                SELECT steam_id, app_id, playtime_minutes,
                       ROW_NUMBER() OVER(PARTITION BY steam_id, app_id ORDER BY recorded_at DESC) as rn
                FROM snapshots
            )
            SELECT a.club_name, a.vanity_url, g.name AS game_name, s.playtime_minutes
            FROM LatestSnapshots s
            JOIN accounts a ON s.steam_id = a.steam_id
            JOIN games g ON s.app_id = g.app_id
            WHERE s.rn = 1 AND g.name IN ({placeholders})
        '''
        # Передаем target_games как параметры для правильной работы SQL
        df = pd.read_sql_query(query, conn, params=target_games)

    for club_name, club_df in df.groupby('club_name'):
        try:
            wks = sh.worksheet_by_title(club_name)
        except pygsheets.WorksheetNotFound:
            wks = sh.add_worksheet(club_name)
        
        wks.clear()
        wks.set_dataframe(club_df, start='A1', copy_head=True)


def main_pipeline():
    print("Старт цикла обновления...")
    accounts_data = get_accounts_from_sheets(sh)
    init_and_sync_db(accounts_data)
    fetch_games_and_snapshot(API_KEY)
    export_clubs_to_sheets(sh)
    print("Синхронизация завершена. Ожидание.")

# Запускаем один раз сразу при включении
main_pipeline()

# Настраиваем фоновое расписание (например, каждые 4 часа)
schedule.every(4).hours.do(main_pipeline)

# Бесконечный цикл, который просто проверяет, не настало ли время
while True:
    schedule.run_pending()
    time.sleep(60)