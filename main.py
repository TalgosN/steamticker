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
    try:
        wks_games = sh.worksheet_by_title('Игры')
        target_games = wks_games.get_col(1, include_tailing_empty=False)[1:]
    except pygsheets.WorksheetNotFound:
        print("Лист 'Игры' не найден, выгрузка отменена.")
        return

    if not target_games:
        print("Список игр пуст, выгружать нечего.")
        return

    with sqlite3.connect('steam_stats.db') as conn:
        placeholders = ','.join('?' * len(target_games))
        
        # Запрос 1: Только актуальное состояние (последний замер по каждой игре)
        query_current = f'''
            WITH LatestSnapshots AS (
                SELECT steam_id, app_id, playtime_minutes, recorded_at,
                       ROW_NUMBER() OVER(PARTITION BY steam_id, app_id ORDER BY recorded_at DESC) as rn
                FROM snapshots
            )
            SELECT a.club_name, a.vanity_url AS nickname, g.name AS game_name, s.playtime_minutes, s.recorded_at
            FROM LatestSnapshots s
            JOIN accounts a ON s.steam_id = a.steam_id
            JOIN games g ON s.app_id = g.app_id
            WHERE s.rn = 1 AND g.name IN ({placeholders})
        '''
        df_current = pd.read_sql_query(query_current, conn, params=target_games)

        # Запрос 2: Вся история отсечек для таймлайнов
        query_history = f'''
            SELECT s.recorded_at, a.club_name, a.vanity_url AS nickname, g.name AS game_name, s.playtime_minutes
            FROM snapshots s
            JOIN accounts a ON s.steam_id = a.steam_id
            JOIN games g ON s.app_id = g.app_id
            WHERE g.name IN ({placeholders})
            ORDER BY s.recorded_at DESC
        '''
        # Передаем target_games еще раз для второго запроса
        df_history = pd.read_sql_query(query_history, conn, params=target_games)

    # Заливаем актуальный срез
    try:
        wks_current = sh.worksheet_by_title('Current_State')
    except pygsheets.WorksheetNotFound:
        wks_current = sh.add_worksheet('Current_State')
    
    wks_current.clear()
    wks_current.set_dataframe(df_current, start='A1', copy_head=True, fit=True)

    # Заливаем исторический лог
    try:
        wks_history = sh.worksheet_by_title('Historical_Log')
    except pygsheets.WorksheetNotFound:
        wks_history = sh.add_worksheet('Historical_Log')
    
    wks_history.clear()
    wks_history.set_dataframe(df_history, start='A1', copy_head=True, fit=True)


def main_pipeline():
    print("Старт цикла обновления...")
    accounts_data = get_accounts_from_sheets(sh)
    init_and_sync_db(accounts_data)
    fetch_games_and_snapshot(API_KEY)
    export_clubs_to_sheets(sh)
    print("Синхронизация завершена. Ожидание.")

if __name__ == '__main__':
    main_pipeline()
    schedule.every(24).hours.do(main_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)