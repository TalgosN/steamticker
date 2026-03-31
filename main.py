import os
import sqlite3
import requests
import pygsheets
from dotenv import load_dotenv
import time
import pandas as pd
import schedule
import telebot
import threading
import ai_generator



load_dotenv()
API_KEY = os.getenv('STEAM_API_KEY')
TABLE_NAME = os.getenv('TABLE_NAME')
TG_BOT_TOKEN = os.getenv('TG_BOT_TOKEN')
ADMIN_CHAT_ID = os.getenv('ADMIN_CHAT_ID')
TG_CHAT_ID = os.getenv('ADMIN_CHAT_ID')

bot = telebot.TeleBot(TG_BOT_TOKEN)

gc = pygsheets.authorize(service_file='service_account.json')
sh = gc.open(TABLE_NAME)

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
                name TEXT,
                description TEXT,
                player_count INTEGER
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
            
            time.sleep(1.5)

def get_game_description(app_id):
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}&l=english"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            data = res.json()
            if data and str(app_id) in data and data[str(app_id)].get('success'):
                desc = data[str(app_id)]['data'].get('short_description', '')
                clean_desc = desc.replace('&quot;', '"').replace('<br>', '\n')
                return clean_desc
    except Exception as e:
        print(f"Ошибка получения описания для {app_id}: {e}")
    return None

def enrich_games_with_descriptions(sh):
    try:
        wks_games = sh.worksheet_by_title('Игры')
        # Читаем Название(A), Кол-во(B) и Описание(C)
        raw_data = wks_games.get_all_values(include_tailing_empty=False)[1:]
        # Убираем пустые строки
        raw_data = [row for row in raw_data if row and str(row[0]).strip()]
    except pygsheets.WorksheetNotFound:
        return

    if not raw_data:
        return

    with sqlite3.connect('steam_stats.db') as conn:
        cursor = conn.cursor()
        
        # 1. Синхронизируем ручные правки из таблицы в базу
        for row in raw_data:
            name = row[0]
            count = row[1] if len(row) > 1 else 0
            # Забираем описание из колонки C, если оно там есть
            manual_desc = row[2] if len(row) > 2 else ""
            
            cursor.execute("UPDATE games SET player_count = ? WHERE name = ?", (count, name))
            
            # Если в ячейке что-то написано, обновляем базу этим текстом
            if manual_desc.strip():
                cursor.execute("UPDATE games SET description = ? WHERE name = ?", (manual_desc, name))
        conn.commit()

        # 2. Докачиваем из Steam только то, чего нет ни в базе, ни в таблице
        target_names = [r[0] for r in raw_data]
        placeholders = ','.join('?' * len(target_names))
        
        cursor.execute(f"SELECT app_id, name FROM games WHERE name IN ({placeholders}) AND (description IS NULL OR description = '')", target_names)
        to_fetch = cursor.fetchall()
        
        if to_fetch:
            print(f"Загрузка английских описаний для {len(to_fetch)} игр...")
            for app_id, name in to_fetch:
                desc = get_game_description(app_id)
                if desc:
                    cursor.execute("UPDATE games SET description = ? WHERE app_id = ?", (desc, app_id))
                    conn.commit()
                time.sleep(1.5)

        # 3. Выгружаем актуальный микс (ручное + авто) обратно в таблицу
        query_df = f"SELECT name, player_count, description FROM games WHERE name IN ({placeholders})"
        df_info = pd.read_sql_query(query_df, conn, params=target_names)
        
        df_info['name'] = pd.Categorical(df_info['name'], categories=target_names, ordered=True)
        df_info = df_info.sort_values('name')

    wks_games.set_dataframe(df_info, start='A1', copy_head=True, fit=True)

def export_clubs_to_sheets(sh):
    try:
        wks_games = sh.worksheet_by_title('Игры')
        target_games = wks_games.get_col(1, include_tailing_empty=False)[1:]
    except pygsheets.WorksheetNotFound:
        return

    if not target_games:
        return

    with sqlite3.connect('steam_stats.db') as conn:
        placeholders = ','.join('?' * len(target_games))
        
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

        query_history = f'''
            SELECT s.recorded_at, a.club_name, a.vanity_url AS nickname, g.name AS game_name, s.playtime_minutes
            FROM snapshots s
            JOIN accounts a ON s.steam_id = a.steam_id
            JOIN games g ON s.app_id = g.app_id
            WHERE g.name IN ({placeholders})
            ORDER BY s.recorded_at DESC
        '''
        df_history = pd.read_sql_query(query_history, conn, params=target_games)

    try:
        wks_current = sh.worksheet_by_title('Current_State')
    except pygsheets.WorksheetNotFound:
        wks_current = sh.add_worksheet('Current_State')
    wks_current.clear()
    wks_current.set_dataframe(df_current, start='A1', copy_head=True, fit=True)

    try:
        wks_history = sh.worksheet_by_title('Historical_Log')
    except pygsheets.WorksheetNotFound:
        wks_history = sh.add_worksheet('Historical_Log')
    wks_history.clear()
    wks_history.set_dataframe(df_history, start='A1', copy_head=True, fit=True)

def process_promo_post(sh):
    try:
        wks = sh.worksheet_by_title('Промо-план')
        records = wks.get_all_records()
    except pygsheets.WorksheetNotFound:
        return

    for i, row in enumerate(records):
        if row.get('Статус') == 'Ожидает':
            game_name = row.get('Игра')
            row_idx = i + 2
            
            description = "Описание отсутствует."
            players_text = "Неизвестно"
            
            with sqlite3.connect('steam_stats.db') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT player_count, description FROM games WHERE name = ?", (game_name,))
                res = cursor.fetchone()
                if res:
                    p_count, desc = res
                    if desc: description = desc
                    try:
                        players_text = f"1 - {int(p_count)}" if int(p_count) > 1 else str(p_count)
                    except:
                        players_text = str(p_count) if p_count else "1"

            # Генерируем текст через наш новый файл
            draft_text = ai_generator.generate_promo(game_name, players_text, description)
            
            # Собираем инлайн-кнопки
            markup = telebot.types.InlineKeyboardMarkup()
            markup.add(
                telebot.types.InlineKeyboardButton("✅ Опубликовать", callback_data=f"promo_pub_{row_idx}"),
                telebot.types.InlineKeyboardButton("🔄 Перегенерировать", callback_data=f"promo_regen_{row_idx}")
            )
            
            # Отправляем черновик админу
            bot.send_message(ADMIN_CHAT_ID, draft_text, reply_markup=markup)
            break

@bot.callback_query_handler(func=lambda call: call.data.startswith('promo_'))
def handle_promo_buttons(call):
    action, row_str = call.data.split('_')[1:]
    row_idx = int(row_str)
    
    try:
        wks = sh.worksheet_by_title('Промо-план')
        game_name = wks.get_value(f'A{row_idx}')
    except Exception:
        bot.answer_callback_query(call.id, "Ошибка доступа к таблице")
        return

    if action == 'regen':
        bot.edit_message_text("Генерирую новый вариант...", call.message.chat.id, call.message.message_id)
        
        # Снова лезем в базу за описанием, чтобы не хранить его в памяти бота
        description = ""
        players_text = "1"
        with sqlite3.connect('steam_stats.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT player_count, description FROM games WHERE name = ?", (game_name,))
            res = cursor.fetchone()
            if res:
                p_count, desc = res
                if desc: description = desc
                try:
                    players_text = f"1 - {int(p_count)}" if int(p_count) > 1 else str(p_count)
                except:
                    players_text = str(p_count) if p_count else "1"
                    
        new_text = ai_generator.generate_promo(game_name, players_text, description)
        
        markup = telebot.types.InlineKeyboardMarkup()
        markup.add(
            telebot.types.InlineKeyboardButton("✅ Опубликовать", callback_data=f"promo_pub_{row_idx}"),
            telebot.types.InlineKeyboardButton("🔄 Перегенерировать", callback_data=f"promo_regen_{row_idx}")
        )
        bot.edit_message_text(new_text, call.message.chat.id, call.message.message_id, reply_markup=markup)
        
    elif action == 'pub':
        # Убираем кнопки из сообщения админа
        bot.edit_message_text(f"{call.message.text}\n\n✅ Опубликовано.", call.message.chat.id, call.message.message_id)
        # Отправляем в общий чат сотрудников
        bot.send_message(TG_CHAT_ID, call.message.text)
        # Обновляем таблицу
        wks.update_value(f'B{row_idx}', 'Опубликовано')
        wks.update_value(f'C{row_idx}', call.message.text)

def main_pipeline():
    print("Старт цикла обновления...")
    accounts_data = get_accounts_from_sheets(sh)
    init_and_sync_db(accounts_data)
    fetch_games_and_snapshot(API_KEY)
    
    enrich_games_with_descriptions(sh)
    export_clubs_to_sheets(sh)
    
    print("Синхронизация завершена. Ожидание.")

if __name__ == '__main__':
    # Сначала поднимаем бота, чтобы он сразу был готов слушать кнопки
    threading.Thread(target=bot.infinity_polling, daemon=True).start()

    print("Первичный сбор данных...")
    main_pipeline()

    schedule.every(4).hours.do(main_pipeline)
    schedule.every(5).minutes.do(process_promo_post, sh)
    
    print("Расписание запущено. Ждем 5 минут до проверки промо-плана...")

    while True:
        schedule.run_pending()
        time.sleep(1) # Ставим 1 секунду вместо 60 для точности расписания