import requests
import json
import os

def generate_promo(game_name, players_text, description):
    api_key = os.getenv("OPENROUTER_API_KEY")
    
    # Инструкция для максимально естественного текста без лишнего мусора
    prompt = (
        f"Напиши анонс для игры. Название: {game_name}, Игроки: {players_text}, Описание: {description}.\n\n"
        f"Правила стиля:\n"
        f"1. Пиши как живой человек, дружелюбно и просто. Никакой агрессивной рекламы.\n"
        f"2. КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО использовать списки, буллеты, 'ёлочки' или маркеры.\n"
        f"3. Не используй длинные тире (—), заменяй их на дефисы (-) или запятые.\n"
        f"4. Описание должно быть связным текстом, а не набором фактов.\n\n"
        f"Формат ответа СТРОГО такой:\n"
        f"🎮 Игра недели: {game_name}!\n"
        f"👥 Количество игроков: {players_text}\n\n"
        f"📖 Описание:\n[Твой живой текст без списков]\n\n"
        f"Всем обязательно поиграть и рекомендовать клиентам. На этой неделе на нее действует скидка 100 рублей."
    )
    
    try:
        res = requests.post(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            data=json.dumps({
                "model": "qwen/qwen3.6-plus-preview:free",
                "messages": [
                    {"role": "system", "content": "Ты опытный геймер и администратор клуба. Пишешь кратко, по делу и без типографского пафоса."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.7
            })
        )
        return res.json()['choices'][0]['message']['content']
    except Exception as e:
        print(f"Ошибка генерации текста: {e}")
        return "Ошибка API. Нажми 'Перегенерировать'."